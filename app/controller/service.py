"""
IaCService — single shared service object for the IaC Orchestrator plugin.

Composes IaCConfig, JobDatabase, and DeploymentEngine into one place so that
entrypoint.py and the UI/API layers only ever deal with this object.
"""
import asyncio
import logging
from core.api import db_instance

from ..model.models import Base
from ..model.database import JobDatabase
from .config import IaCConfig
from .engine import DeploymentEngine
from .queue import JobQueue, JobWorker, DuplicateJob
from .socket_client import SocketBusClient
from .gitlab_webhooks import sync_group_webhooks_from_ctx, WebhookConfigError

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
        self.engine = DeploymentEngine(ctx, self.state, self.db, self.config, self.socket_client)
        self.engine.queue = self.queue
        self.worker: JobWorker | None = None

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

    def runner_online(self) -> bool:
        """Is any live instance a runner (this one included)? Older cores: assume yes."""
        probe = getattr(self.ctx, "has_live_role", None)
        if callable(probe):
            try:
                return bool(probe("runner"))
            except Exception:
                pass
        return self.is_runner

    def enqueue_pipeline(self, payload: dict, *, requested_by: str | None = None) -> int:
        """Queue a pipeline for a runner. Raises :class:`DuplicateJob` when the same
        target is already queued or running. Wakes the runners via the cluster bus."""
        job_id = self.queue.enqueue(payload, requested_by=requested_by)
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

    def start_runner(self) -> None:
        """Start the job worker (runner instances only)."""
        if self.worker is not None or not self.is_runner:
            return
        try:
            max_parallel = int(str(self.ctx.get_secret("iac_max_parallel_jobs") or "3").strip() or "3")
        except (ValueError, Exception):
            max_parallel = 3
        self.worker = JobWorker(self.ctx, self.queue, self.run_claimed, max_parallel=max_parallel)
        self.worker.start()
        log.info(f"IaC Orchestrator: job worker started on '{getattr(self.ctx, 'instance_id', 'local')}' "
                 f"(max {max_parallel} parallel).")

    async def stop_runner(self) -> None:
        if self.worker is not None:
            await self.worker.stop()
            self.worker = None

    async def run_claimed(self, payload: dict, job_id: int):
        """Execute a job this instance's worker claimed."""
        await self.ensure_runtime_paths()
        await self.engine.run_pipeline(payload, job_id=job_id)

    async def enqueue_from_event(self, payload: dict) -> int | None:
        """``iac:webhook_verified`` handler body: queue the pipeline, tell the user
        when the same target is already queued/running (the old engine guard did
        this silently with a toast — keep the toast)."""
        try:
            return await asyncio.to_thread(self.enqueue_pipeline, payload,
                                           requested_by=payload.get("trigger") or ("ui" if payload.get("manual") else "webhook"))
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

    # -- webhook auto-sync: one instance (lease) -----------------------------
    def start_webhook_sync(self) -> None:
        if getattr(self, "_webhook_sync_task", None) is None:
            self._webhook_sync_task = self.ctx.create_task(self.run_webhook_sync_loop(), name="iac:webhook_sync_loop")

    def stop_webhook_sync(self) -> None:
        task = getattr(self, "_webhook_sync_task", None)
        if task is not None:
            task.cancel()
            self._webhook_sync_task = None

    async def watch_runner_presence(self, interval: float = 30.0):
        """Cluster: surface 'no runner online' as a non-blocking degraded status."""
        set_status = getattr(self.ctx, "set_status", None)
        if not callable(set_status):
            return
        degraded = False
        await asyncio.sleep(15)  # let the coordinator learn the live instances first
        while True:
            try:
                online = self.runner_online()
                if not online and not degraded:
                    set_status("degraded", message="No runner instance online — pipelines are queued, not executed",
                               blocking=False)
                    degraded = True
                elif online and degraded:
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
        """Reconcile orphaned runners and resume interrupted jobs after a restart.

        Runner instances only, and only for jobs THIS instance owned (plus legacy
        rows without an owner): in a cluster another runner's RUNNING jobs are
        alive and must not be touched — the orphan sweep handles dead owners.
        """
        if not self.is_runner:
            return
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

        # Resume any pending tasks in the database queue — own jobs + ownerless legacy rows.
        me = getattr(self.ctx, "instance_id", None)
        interrupted_jobs = [
            j for j in self.db.get_jobs_by_status("RUNNING")
            if getattr(j, "owner_instance", None) in (None, "", me)
        ]
        for job in interrupted_jobs:
            remaining_services = self.db.get_pending_tasks(job.id)
            if remaining_services:
                self.ctx.create_task(
                    self.engine.resume_bulk_rollout(job.id, remaining_services),
                    name=f"iac:resume:{job.id}",
                )
            elif not any(
                t.get("job_id") == job.id
                for t in self.engine.state.get("active_tasks", {}).values()
            ):
                log.warning(
                    f"IaC Orchestrator: Job #{job.id} is RUNNING but has no active runners. Marking as FAILED."
                )
                self.db.update_job(job.id, "FAILED")
                self.db.update_progress(job.id, progress=None, current_step="System Restart (Aborted)")
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
