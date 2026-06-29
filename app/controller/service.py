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
from .socket_client import SocketBusClient
from .gitlab_webhooks import sync_group_webhooks_from_ctx, WebhookConfigError

log = logging.getLogger("IaC:Service")


class IaCService:
    """Facade that composes all controller objects and owns the shared plugin state."""

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
        self.engine = DeploymentEngine(ctx, self.state, self.db, self.config, self.socket_client)

        # Restore the Auto-Apply setting from Vault/Config on boot
        self.state["auto_apply_enabled"] = self.config.auto_apply

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
        """Reconcile orphaned runners and resume interrupted jobs after a restart."""
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

        # Resume any pending tasks in the database queue
        interrupted_jobs = self.db.get_jobs_by_status("RUNNING")
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

    def emit_iac_inventory_updated(self):
        """Emit generic host inventory event so subscribed plugins can self-register."""
        self.engine.emit_iac_inventory_updated()

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
