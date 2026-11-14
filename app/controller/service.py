"""
IaCService — single shared service object for the IaC Orchestrator plugin.

Composes IaCConfig, JobDatabase, and DeploymentEngine into one place so that
entrypoint.py and the UI/API layers only ever deal with this object.
"""
import asyncio
import logging
import time
import uuid
from core.api import db_instance

from ..model.models import Base
from ..model.database import JobDatabase
from .config import IaCConfig
from .engine import DeploymentEngine
from .queue import JobQueue, JobWorker, DuplicateJob
from .socket_client import SocketBusClient
from .gitlab_webhooks import sync_group_webhooks_from_ctx, WebhookConfigError
from .job_payload import ALL_ENTITLEMENTS, InvalidJobPayload, strip_unentitled, validate_structure
from .orphan_sweep import LEASE_NAME as SWEEP_LEASE, OrphanSweeper

log = logging.getLogger("IaC:Service")


class IaCService:
    """Facade that composes all controller objects and owns the shared plugin state.

    Cluster (plugin ≥ 1.4): every instance is control plane — it enqueues jobs,
    serves the API/UI from the shared database. Only instances with the
    ``runner`` role start the :class:`JobWorker` that executes claimed jobs.
    """

    def __init__(self, ctx):
        self.ctx = ctx
        self.state: dict = {
            "auto_apply_enabled": False,
            "last_deployment": "Never",
            "latest_logs": [],
            "is_running": False,
            "active_tasks": {},
        }

        self.config = IaCConfig(ctx)
        self.socket_client = SocketBusClient(ctx)
        self.db = JobDatabase()
        self.queue = JobQueue()
        # One claim registry for both stores: the runner-side writes of a claimed
        # job are conditional on its claim token (fencing).
        self.queue.claims = self.db.claims
        # Identity of THIS plugin load: "<core boot nonce>:<nonce>". Claims are
        # stamped with it, so a restarted runner (same instance id) can tell the
        # jobs it claimed itself from what its predecessor left behind.
        core_nonce = getattr(ctx, "incarnation", "")
        self.incarnation = f"{core_nonce if isinstance(core_nonce, str) else ''}:{uuid.uuid4().hex[:12]}"
        self.engine = DeploymentEngine(ctx, self.state, self.db, self.config, self.socket_client)
        self.engine.queue = self.queue
        # The engine's own hand-offs (host rollout after a provision) enter the
        # queue through the same validated door as every other trigger.
        self.engine.submit_job = self.submit_job
        # Maintenance windows (1.6): a job the queue closes itself (orphan sweep →
        # mark_failed) still ends its maintenance override exactly once.
        self.queue.on_terminal = self.engine.end_override_for
        self.worker: JobWorker | None = None
        # Orphan sweep (rD §3): started/stopped by the lease, see review_sweep_lease.
        self.sweeper = OrphanSweeper(ctx, self.queue, self.db, self.config, notify=self._notify_orphaned)
        self._sweep_lease_wanted = False
        # Runner presence: when a runner was last seen and when this service started.
        self._runner_seen_at: float | None = None
        self._started_at = time.monotonic()

        # Restore the Auto-Apply setting from Vault/Config on boot
        self.state["auto_apply_enabled"] = self.config.auto_apply

    # ------------------------------------------------------------------
    # Cluster roles
    # ------------------------------------------------------------------

    @property
    def is_runner(self) -> bool:
        """Does this instance execute jobs? Single mode has every role; older cores
        (no ``ctx.roles``) are treated as runners (today's behaviour)."""
        roles = getattr(self.ctx, "roles", None)
        if roles is None or isinstance(roles, type(self.ctx)):
            return True
        try:
            return "runner" in list(roles)
        except Exception:
            return True

    #: A runner that vanished is "restarting" for this long before it counts as absent
    #: (core < 1.14 has no restarting state: a graceful stop deletes the row at once).
    RUNNER_RESTART_GRACE_SECONDS = 120.0
    #: After start-up the cluster view may be empty/fabricated for a while.
    RUNNER_BOOT_GRACE_SECONDS = 60.0

    def runner_state(self) -> str:
        """``online`` | ``restarting`` | ``absent`` | ``unknown`` — is there a runner
        that executes jobs? Uses the core's role state when it has one (API ≥ 1.14),
        else ``has_live_role`` with a grace period, so one missed heartbeat or a
        runner restart does not flip every instance to "no runner"."""
        role_state = getattr(self.ctx, "role_state", None)
        if callable(role_state):
            try:
                info = role_state("runner")
                state = info.get("state") if isinstance(info, dict) else None
            except Exception:
                state = None
            if state in ("alive", "draining"):
                return "online"
            if state == "restarting":
                return "restarting"
            if state == "dead":
                return "absent"
            if state == "unknown":
                return "unknown"
        probe = getattr(self.ctx, "has_live_role", None)
        now = time.monotonic()
        seen: bool | None = None
        if callable(probe):
            try:
                seen = bool(probe("runner"))
            except Exception:
                seen = None
        if seen is None:
            return "online" if self.is_runner else "unknown"
        if seen:
            self._runner_seen_at = now
            return "online"
        if self._runner_seen_at is not None:
            return "restarting" if now - self._runner_seen_at < self.RUNNER_RESTART_GRACE_SECONDS else "absent"
        return "unknown" if now - self._started_at < self.RUNNER_BOOT_GRACE_SECONDS else "absent"

    def runner_online(self) -> bool:
        """Is a runner online right now? (``runner_state`` has the nuance.)"""
        return self.runner_state() == "online"

    # -- orphan sweep placement (rD §3.1) ------------------------------------------
    def _other_control_plane_alive(self) -> bool:
        """Is a live instance WITHOUT the runner role around (besides this one)?"""
        instances = getattr(self.ctx, "cluster_instances", None)
        if not isinstance(instances, (list, tuple)):
            return False
        me = getattr(self.ctx, "instance_id", None)
        try:
            return any(i.get("alive") and i.get("instance_id") != me and "runner" not in (i.get("roles") or [])
                       for i in instances)
        except Exception:
            return False

    def review_sweep_lease(self) -> None:
        """Contend for the sweep lease on control-plane instances; a runner only
        while no other instance is alive (single mode: always). Called at setup
        and by the presence watcher, so placement follows the cluster."""
        want = (not self.is_runner) or not self._other_control_plane_alive()
        on_lease = getattr(self.ctx, "on_lease", None)
        if not callable(on_lease):
            # Core without leases = one instance: sweep here.
            if want and not self.sweeper.running:
                self.sweeper.start()
            return
        if want and not self._sweep_lease_wanted:
            on_lease(SWEEP_LEASE, acquired=self.sweeper.start, lost=self.sweeper.stop)
            self._sweep_lease_wanted = True
        elif not want and self._sweep_lease_wanted:
            release = getattr(self.ctx, "release_lease", None)
            if callable(release):
                release(SWEEP_LEASE)        # fires ``lost`` → the sweeper stops
                self._sweep_lease_wanted = False

    def _notify_orphaned(self, job_id: int, reason: str, row: dict) -> None:
        self.ctx.notify(
            "job_orphaned",
            payload={"job_id": job_id, "reason": reason, "owner_instance": row.get("owner_instance"),
                     "message": f"Pipeline #{job_id} was closed as FAILED: {reason}."},
            notification_id=f"job_{job_id}",
            title=f"❌ Pipeline #{job_id} — runner lost",
            body=f"{reason}. Nothing is re-run automatically — re-trigger the pipeline.",
            severity="error",
        )

    def enqueue_pipeline(self, payload: dict, *, requested_by: str | None = None) -> int:
        """Queue a pipeline for a runner. Raises :class:`DuplicateJob` when the same
        target is already queued or running. Wakes the runners via the cluster bus.

        Maintenance windows (1.6): a SCHEDULED job for the same target absorbs a
        newer webhook trigger (coalesced — nothing to wake) or is promoted to QUEUED
        when the trigger must run now (UI deploy / ``force``)."""
        result = self.queue.enqueue_ex(payload, requested_by=requested_by)
        job_id = result.job_id
        if result.coalesced:
            log.info(f"ENQUEUE: trigger for '{payload.get('service_name') or payload.get('pipeline_type')}' "
                     f"coalesced into scheduled job #{job_id}.")
            return job_id
        if result.promoted:
            log.info(f"ENQUEUE: scheduled job #{job_id} promoted to run now ({requested_by}).")
        if job_id > 0:
            try:
                self.ctx.emit("iac:job_queued", {"job_id": job_id}, cluster=True)
            except TypeError:  # older core without the cluster flag
                self.ctx.emit("iac:job_queued", {"job_id": job_id})
            except Exception as exc:
                log.debug(f"iac:job_queued not emitted: {exc}")
            if self.worker is not None:
                self.worker.wake()
        return job_id

    def start_runner(self, max_parallel: int | None = None) -> None:
        """Start the job worker (runner instances only)."""
        if self.worker is not None or not self.is_runner:
            return
        if max_parallel is None:
            try:
                max_parallel = int(str(self.ctx.get_secret("iac_max_parallel_jobs") or "3").strip() or "3")
            except (ValueError, Exception):
                max_parallel = 3
        self.worker = JobWorker(self.ctx, self.queue, self.run_claimed, max_parallel=max_parallel,
                                incarnation=self.incarnation,
                                on_fenced=self._on_fenced, extra_attended=self.engine.attended_job_ids)
        self.worker.start()
        log.info(f"IaC Orchestrator: job worker started on '{getattr(self.ctx, 'instance_id', 'local')}' "
                 f"(max {max_parallel} parallel).")

    async def run_runner_boot(self) -> None:
        """Runner start-up in a fixed order (audit OC-04): first settle what a
        previous incarnation left behind, then start claiming. The worker used to
        start first — and the reconciliation then FAILED the jobs it had just
        claimed. The worker starts in any case, also when reconciling raised."""
        try:
            await self.run_startup_reconciliation()
        except Exception as exc:
            log.error(f"IaC Orchestrator: startup reconciliation failed: {exc}")
        try:
            raw = await asyncio.to_thread(self.ctx.get_secret, "iac_max_parallel_jobs")
            max_parallel = int(str(raw or "3").strip() or "3")
        except Exception:
            max_parallel = 3
        self.start_runner(max_parallel)

    async def _on_fenced(self, job_id: int) -> None:
        """A job this runner executes was closed or taken over elsewhere: stop its
        containers here. No status write — the row is not ours any more."""
        await self.engine.stop_job_containers(job_id)

    async def stop_runner(self) -> None:
        if self.worker is not None:
            await self.worker.stop()
            self.worker = None

    async def run_claimed(self, payload: dict, job_id: int):
        """Execute a job this instance's worker claimed.

        The row was written by whichever instance enqueued it — the payload is
        validated again before anything acts on it (audit OS-09)."""
        try:
            payload = validate_structure(payload)
        except InvalidJobPayload as exc:
            log.error(f"WORKER: job #{job_id} rejected at claim — invalid payload: {exc}")
            await asyncio.to_thread(self._reject_claimed, job_id, str(exc))
            return
        await self.ensure_runtime_paths()
        await self.engine.run_pipeline(payload, job_id=job_id)

    def _reject_claimed(self, job_id: int, reason: str) -> None:
        step = f"Rejected: invalid job payload ({reason})"[:250]
        try:
            self.queue.append_log(job_id, [f"[SYSTEM] {step} — nothing was executed."], self.queue.last_seq(job_id))
        except Exception as exc:
            log.debug(f"reject log line for #{job_id} not written: {exc}")
        self.db.update_progress(job_id, progress=None, current_step=step)
        self.db.update_job(job_id, "FAILED")

    async def submit_job(self, payload: dict, *, entitlements=(), requested_by: str | None = None) -> int | None:
        """The one way a trigger becomes a job.

        ``entitlements`` names the privileged fields the calling code path may
        set (``job_payload.ENT_*``); everything else is stripped, then the
        payload is validated. Returns the job id, ``None`` when the same target
        is already queued/running. Raises :class:`InvalidJobPayload`.
        """
        clean, dropped = strip_unentitled(payload or {}, entitlements)
        if dropped:
            log.warning(f"ENQUEUE: dropped privileged field(s) {dropped} from a "
                        f"'{clean.get('pipeline_type')}' trigger without entitlement.")
        clean = validate_structure(clean)
        who = requested_by or clean.get("trigger") or ("ui" if clean.get("manual") else "webhook")
        try:
            return await asyncio.to_thread(self.enqueue_pipeline, clean, requested_by=who)
        except DuplicateJob as exc:
            target = clean.get("service_name") or clean.get("host_name") or clean.get("limit") or clean.get("pipeline_type")
            log.warning(f"ENQUEUE: duplicate pipeline ignored for '{target}' (job #{exc.existing_id} active).")
            try:
                self.engine._notify("Pipeline Ignored",
                                    f"A run for {target} is already queued or running (job #{exc.existing_id}).",
                                    "warning")
            except Exception:
                pass
            return None

    def submit_from_ui(self, payload: dict) -> None:
        """Trigger from the plugin's own in-process (NiceGUI) pages: sync wrapper
        around :meth:`submit_job` with the page's entitlements."""
        async def _go():
            try:
                await self.submit_job(payload, entitlements=ALL_ENTITLEMENTS)
            except InvalidJobPayload as exc:
                log.warning(f"ENQUEUE: UI trigger rejected: {exc}")
            except Exception as exc:
                log.error(f"ENQUEUE: UI trigger failed: {exc}")

        self.ctx.create_task(_go(), name="iac:ui_submit")

    async def enqueue_from_event(self, payload: dict) -> int | None:
        """``iac:webhook_verified`` handler body. The bus has no sender identity —
        any plugin can emit this topic — so the payload is UNTRUSTED: no
        ``approve``, no ``force``, no self-declared trigger, strict validation.
        The plugin's own triggers do not come this way (they call submit_job)."""
        try:
            return await self.submit_job(payload or {}, entitlements=())
        except InvalidJobPayload as exc:
            log.warning(f"ENQUEUE: bus trigger rejected — {exc}")
            return None
        except DuplicateJob as exc:
            target = payload.get("service_name") or payload.get("host_name") or payload.get("limit") or payload.get("pipeline_type")
            log.warning(f"ENQUEUE: duplicate pipeline ignored for '{target}' (job #{exc.existing_id} active).")
            try:
                self.engine._notify("Pipeline Ignored",
                                    f"A run for {target} is already queued or running (job #{exc.existing_id}).",
                                    "warning")
            except Exception:
                pass
            return None
        except Exception as exc:
            log.error(f"ENQUEUE: failed to queue pipeline: {exc}")
            return None

    async def publish_snapshots_on_boot(self):
        """Runner boot: publish the repo-derived read models once the repos are there."""
        await asyncio.sleep(5)
        try:
            from . import api as _api

            counts = await asyncio.to_thread(_api.publish_repo_snapshots)
            if counts:
                log.info(f"IaC Orchestrator: repo snapshots published {counts}.")
        except Exception as exc:
            log.debug(f"IaC Orchestrator: snapshot publish on boot skipped: {exc}")

    #: Back-off between boot publish attempts while the Entity Registry is still booting.
    PUBLISH_RETRY_DELAYS = (15, 30, 60, 120, 300)

    async def publish_inventory_chain(self, *, retry_delays=None, sleep=asyncio.sleep):
        """Runner boot: the same ordered publish as ``NativeGenerateStage`` — Entity
        Registry ids (hosts + services) first, then the monitoring sync carrying them,
        then the effective maintenance windows (``maintenance:windows_published``).

        The registry is usually a few seconds behind this plugin at boot (``ok: False,
        "database not ready"``); without ci_ids nothing useful can be published, so
        the chain is repeated with back-off until the registry answered or the
        delays are exhausted (then the next deploy/sync publishes)."""
        delays = list(self.PUBLISH_RETRY_DELAYS if retry_delays is None else retry_delays)
        attempt = 0
        while True:
            try:
                await self.engine.emit_iac_inventory_updated()
                await self.engine.emit_monitoring_inventory_sync()
                await self.engine.publish_maintenance()
            except Exception as exc:
                log.error(f"IaC Orchestrator: inventory publish on boot failed: {exc}")
                return
            if getattr(self.engine, "_registry_ok", None) is not False:
                return
            if not delays:
                log.warning(
                    f"IaC Orchestrator: Entity Registry still unavailable after {attempt} retries — "
                    "ci_ids and maintenance windows are published with the next deploy or sync."
                )
                return
            delay = delays.pop(0)
            attempt += 1
            log.info(f"IaC Orchestrator: Entity Registry not ready — retrying the inventory publish in {delay}s.")
            await sleep(delay)

    def promote_scheduled(self, job_id: int) -> bool:
        """"Run now" for a parked deploy: SCHEDULED → QUEUED with ``force`` and wake the
        runners (the override is announced when the job actually runs)."""
        if not self.queue.promote(job_id, override=True):
            return False
        try:
            self.ctx.emit("iac:job_queued", {"job_id": job_id}, cluster=True)
        except TypeError:
            self.ctx.emit("iac:job_queued", {"job_id": job_id})
        except Exception as exc:
            log.debug(f"iac:job_queued not emitted: {exc}")
        if self.worker is not None:
            self.worker.wake()
        return True

    # -- webhook auto-sync: one instance (lease) -----------------------------
    def start_webhook_sync(self) -> None:
        if getattr(self, "_webhook_sync_task", None) is None:
            self._webhook_sync_task = self.ctx.create_task(self.run_webhook_sync_loop(), name="iac:webhook_sync_loop")

    def stop_webhook_sync(self) -> None:
        task = getattr(self, "_webhook_sync_task", None)
        if task is not None:
            task.cancel()
            self._webhook_sync_task = None

    async def watch_runner_presence(self, interval: float = 30.0, settle: float = 15.0):
        """Surface 'no runner' as a non-blocking degraded status — after TWO
        consecutive ``absent`` checks, never for ``restarting``/``unknown`` — and
        keep the orphan-sweep lease on the right kind of instance. Only a status
        this watcher set is reset."""
        set_status = getattr(self.ctx, "set_status", None)
        degraded = False
        misses = 0
        await asyncio.sleep(settle)  # let the coordinator learn the live instances first
        while True:
            try:
                self.review_sweep_lease()
                state = self.runner_state()
                misses = misses + 1 if state == "absent" else 0
                if callable(set_status):
                    if misses >= 2 and not degraded:
                        set_status("degraded", message="No runner instance online — pipelines are queued, not executed",
                                   blocking=False)
                        degraded = True
                    elif state in ("online", "restarting") and degraded:
                        set_status("ready")
                        degraded = False
            except Exception as exc:
                log.debug(f"runner watch: {exc}")
            await asyncio.sleep(interval)

    async def on_abort_requested(self, payload: dict | None = None):
        """``iac:job_abort_requested`` (local or relayed): abort the jobs this instance runs."""
        ids = [int(j) for j in ((payload or {}).get("job_ids") or []) if str(j).isdigit()]
        if not ids:
            return
        for job_id in ids:
            owned = self.worker.owns(job_id) if self.worker else False
            if owned:
                await self.worker.cancel(job_id)
            if owned or any(
                isinstance(t, dict) and t.get("job_id") == job_id
                for t in (self.state.get("active_tasks") or {}).values()
            ):
                try:
                    await self.engine.abort_job(job_id)
                except Exception as exc:
                    log.warning(f"abort of job #{job_id} failed: {exc}")

    # ------------------------------------------------------------------
    # DB bootstrap
    # ------------------------------------------------------------------

    def bootstrap_db(self, base=None):
        """Create ORM tables if the DB is connected. Safe to call multiple times."""
        if base is None:
            base = Base
        if not db_instance.is_connected or not db_instance.engine:
            return
        try:
            log.info("IaC Orchestrator: Verifying database tables...")
            base.metadata.create_all(bind=db_instance.engine, checkfirst=True)
            self.queue.ensure_schema()

            # Restore the last deployment status for the UI on boot
            recent = self.db.get_recent_jobs(1)
            if recent:
                self.state["last_deployment"] = recent[0]["status"]
        except Exception as exc:
            log.error(f"Failed to create tables: {exc}")

    # ------------------------------------------------------------------
    # API route wiring
    # ------------------------------------------------------------------

    def register_api_routes(self, fastapi_app, router):
        """Ensure the orchestrator API routes exist ahead of NiceGUI's catch-all mount."""
        api_prefix = "/api/iac"
        routes = list(fastapi_app.router.routes)
        existing_api_routes = [
            route for route in routes if getattr(route, "path", "").startswith(api_prefix)
        ]

        if not existing_api_routes:
            fastapi_app.include_router(router)
            routes = list(fastapi_app.router.routes)
            existing_api_routes = [
                route for route in routes if getattr(route, "path", "").startswith(api_prefix)
            ]

        if not existing_api_routes:
            return

        remaining_routes = [route for route in routes if route not in existing_api_routes]
        root_mount_index = next(
            (index for index, route in enumerate(remaining_routes) if getattr(route, "path", None) == ""),
            len(remaining_routes),
        )
        reordered_routes = (
            remaining_routes[:root_mount_index]
            + existing_api_routes
            + remaining_routes[root_mount_index:]
        )
        fastapi_app.router.routes = reordered_routes
        fastapi_app.openapi_schema = None

    # ------------------------------------------------------------------
    # Pipeline / background tasks
    # ------------------------------------------------------------------

    async def run_pipeline(self, payload: dict):
        """Delegate to the engine."""
        await self.ensure_runtime_paths()
        await self.engine.run_pipeline(payload)

    async def run_startup_reconciliation(self):
        """Settle the RUNNING jobs a previous incarnation of this runner left behind.

        Runner instances only, and only rows this instance id owned (plus legacy
        rows without an owner) that were NOT claimed by the current incarnation:
        another runner's jobs are alive, and a job the current worker claimed is
        executing (audit OC-04). Each row is adopted first (new claim token,
        conditional on the old one), so everything written afterwards is fenced
        and a row somebody else closed meanwhile is skipped. Nothing is resumed.
        """
        if not self.is_runner:
            return
        me = getattr(self.ctx, "instance_id", None)
        # Listed before anything else happens in this process.
        previous = await asyncio.to_thread(self.queue.previous_incarnation_jobs, me, self.incarnation)
        await self.ensure_runtime_paths()
        await asyncio.sleep(2)  # Give the DB a moment to wake up
        log.info("IaC Orchestrator: Checking for surviving Docker runners...")
        try:
            await asyncio.wait_for(self.engine.reconcile_orphaned_runners(), timeout=20)
        except asyncio.TimeoutError:
            log.warning("IaC Orchestrator: Runner reconciliation timed out after 20s, continuing startup.")
        except Exception as exc:
            log.error(f"IaC Orchestrator: Runner reconciliation failed: {exc}")
        finally:
            log.info("IaC Orchestrator: Startup reconciliation scan finished.")

        for row in previous:
            job_id = row["id"]
            token = await asyncio.to_thread(self.queue.adopt, job_id, me, self.incarnation, row["claim_token"])
            if token is None:
                continue  # closed or taken over meanwhile — not ours to touch
            self.db.claims[job_id] = token
            if job_id in self.engine.attended_job_ids():
                continue  # containers re-attached: their watchers close the job
            # The runner process died mid-job (restart / self-deploy). Its containers
            # may have finished meanwhile — close the job from their exit codes
            # before giving up on it.
            try:
                if await self.engine.finalize_from_containers(job_id, reason="restarted"):
                    continue
            except Exception as exc:
                log.debug(f"IaC Orchestrator: container recovery for job #{job_id} failed: {exc}")
            log.warning(
                f"IaC Orchestrator: Job #{job_id} is RUNNING but has no active runners. Marking as FAILED."
            )
            reason = await self.engine.interrupted_reason(job_id) or "System Restart (Aborted)"
            self.engine._log_line(job_id, f"[SYSTEM] {reason}.")
            await self.engine._close_log_writer(job_id)
            await asyncio.to_thread(self.db.update_progress, job_id, None, reason[:250])
            await asyncio.to_thread(self.db.update_job, job_id, "FAILED")
            self.db.claims.pop(job_id, None)
            self.engine.end_override_for(job_id)
            self.state["last_deployment"] = "FAILED"

    async def emit_monitoring_inventory_sync(self):
        """Delegate to the engine."""
        await self.engine.emit_monitoring_inventory_sync()

    async def emit_iac_inventory_updated(self):
        """Publish the host inventory (bus event + Server Manager registration + CI ids)."""
        await self.engine.emit_iac_inventory_updated()

    async def run_webhook_sync_loop(self):
        """Self-healing GitLab webhook registration.

        Runs the idempotent group upsert shortly after boot and then on a fixed
        interval, so a newly created service repo gets its merge-request hook
        without anyone clicking "Webhooks registrieren" in Settings. iac-controller
        CI can additionally hit POST /api/iac/webhook/sync for an immediate run.
        """
        if str(self.ctx.get_secret("iac_webhook_autosync_enabled") or "true").lower() == "false":
            self.ctx.log.info("IaC Orchestrator: webhook auto-sync disabled.")
            return

        raw_interval = self.ctx.get_secret("iac_webhook_sync_interval_seconds") or ""
        try:
            interval = max(300, int(str(raw_interval).strip() or "1800"))
        except ValueError:
            interval = 1800

        await asyncio.sleep(30)  # let Vault hydrate and boot settle
        while True:
            try:
                result = await asyncio.to_thread(sync_group_webhooks_from_ctx, self.ctx)
                self.ctx.log.info(
                    f"IaC Orchestrator: webhook auto-sync ok "
                    f"(projects={result['projects_total']}, created={result['created']}, "
                    f"updated={result['updated']}, failed={result['failed']})."
                )
            except WebhookConfigError as exc:
                self.ctx.log.debug(f"IaC Orchestrator: webhook auto-sync skipped — {exc}")
            except Exception as exc:
                self.ctx.log.warning(f"IaC Orchestrator: webhook auto-sync failed — {exc}")
            await asyncio.sleep(interval)

    async def ensure_runtime_paths(self):
        """Resolve host bind mounts from the core socket manager once per boot."""
        if getattr(self, "_runtime_paths_resolved", False):
            return
        try:
            mounts = await self.socket_client.resolve_runtime_mounts(
                [
                    "/data/storage/git_repos",
                    "/data/storage/services",
                    "/data/storage/terraform-providers",
                    "/data/security",
                ]
            )
            if mounts:
                self.config.apply_runtime_mount_paths(mounts)
                self.ctx.log.info("IaC Orchestrator: Resolved runtime host mounts via socket manager.")
        except Exception as exc:
            self.ctx.log.warning(f"IaC Orchestrator: Runtime mount resolution failed, using configured paths: {exc}")
        finally:
            self._runtime_paths_resolved = True
