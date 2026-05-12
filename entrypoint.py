import asyncio
from nicegui import ui
from ui.layout import main_layout
from core.api import ModuleManifest, db_instance
from .api import iac_api_router, init_api
from .engine import DeploymentEngine
from .ui_dashboard import render_dashboard
from .ui_settings import render_settings_ui as modular_settings_ui
from .database import JobDatabase
from .models import IaCJob, Base
from .config import IaCConfig

# ==========================================
# 1. MANIFEST
# ==========================================
manifest = ModuleManifest(
    id="lyndrix.plugin.iac_orchestrator",
    name="IaC Orchestrator",
    version="0.2.9",
    description="Standalone GitOps controller for executing Terraform and Ansible pipelines.",
    author="Lyndrix",
    icon="rocket_launch", 
    type="PLUGIN",
    min_core_version="0.0.6",
    auto_enable_on_install=False,
    repo_url="https://github.com/marvin1309/lyndrix-iac-orchestrator",
    ui_route="/iac",
    permissions={
        "subscribe": ["vault:ready_for_data", "iac:webhook_verified", "git:status_update", "db:connected"], 
        "emit": ["iac:pipeline_started", "iac:webhook_verified", "git:sync", "git:commit_push", "system:notify", "user:notify", "monitoring:inventory_sync"]
    }
)


# ==========================================
# 2. SHARED PLUGIN STATE
# ==========================================
plugin_state = {
    "auto_apply_enabled": False,
    "last_deployment": "Never",
    "latest_logs": [],
    "is_running": False,
    "active_tasks": {}
}


def _register_api_routes(fastapi_app):
    """Ensure the orchestrator API routes exist ahead of NiceGUI's catch-all mount."""
    api_prefix = "/api/iac"
    routes = list(fastapi_app.router.routes)
    existing_api_routes = [
        route for route in routes if getattr(route, "path", "").startswith(api_prefix)
    ]

    if not existing_api_routes:
        fastapi_app.include_router(iac_api_router)
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

# ==========================================
# 3. SETTINGS INJECTION
# ==========================================
def render_settings_ui(ctx):
    """Glue function for settings injection."""
    modular_settings_ui(ctx, plugin_state)

# --- NEW: WIDGET INJECTION ---
def render_dashboard_widget(ctx):
    """Renders a status widget for the main dashboard."""
    with ui.column().classes('gap-2 w-full'):
        ui.label("IaC Orchestrator").classes("text-base font-bold text-slate-200")
        ui.separator().classes('my-1 opacity-20')
        with ui.row().classes('w-full justify-between items-center'):
            ui.label("Last Deployment:").classes("text-xs text-slate-400")
            ui.label().classes("text-xs font-mono").bind_text_from(plugin_state, 'last_deployment')
        with ui.row().classes('w-full justify-between items-center'):
            ui.label("Pipeline Active:").classes("text-xs text-slate-400")
            with ui.row().classes('items-center gap-2'):
                ui.spinner('dots', color='indigo').bind_visibility_from(plugin_state, 'is_running')
                ui.label().classes("text-xs font-mono").bind_text_from(plugin_state, 'is_running', lambda v: "Yes" if v else "No")

# ==========================================
# 4. PLUGIN BOOT SEQUENCE
# ==========================================
def setup(ctx):
    ctx.log.info("IaC Orchestrator: Executing async setup sequence...")
    
    config = IaCConfig(ctx)
    job_db = JobDatabase()
    engine = DeploymentEngine(ctx, plugin_state, job_db, config)
    
    # Restore the Auto-Apply setting from Vault/Config on boot
    plugin_state["auto_apply_enabled"] = config.auto_apply

    # Initialize the API with the engine instance
    init_api(ctx, engine)
    
    # Attach API routes to the underlying FastAPI app before NiceGUI's root mount.
    from main import app as fastapi_app
    _register_api_routes(fastapi_app)
    
    def init_db_tables():
        if db_instance.is_connected and db_instance.engine:
            ctx.log.info("IaC Orchestrator: Verifying database tables...")
            try:
                Base.metadata.create_all(bind=db_instance.engine, checkfirst=True)
                
                # Restore the last deployment status for the UI on boot
                recent = job_db.get_recent_jobs(1)
                if recent:
                    plugin_state["last_deployment"] = recent[0]["status"]
            except Exception as e:
                ctx.log.error(f"Failed to create tables: {e}")

    # Initial DB Check
    init_db_tables()

    # --- EVENT SUBSCRIPTIONS ---
    @ctx.subscribe('db:connected')
    async def on_db_connected(payload):
        init_db_tables()
    
    @ctx.subscribe('iac:webhook_verified')
    async def on_webhook(payload):
        ctx.create_task(engine.run_pipeline(payload), name="iac:run_pipeline")
        
    # --- START RECONCILIATION ---
    async def run_reconciliation():
        await asyncio.sleep(2) # Give the DB a moment to wake up
        ctx.log.info("IaC Orchestrator: Checking for surviving Docker runners...")
        try:
            await asyncio.wait_for(engine.reconcile_orphaned_runners(), timeout=20)
        except asyncio.TimeoutError:
            ctx.log.warning("IaC Orchestrator: Runner reconciliation timed out after 20s, continuing startup.")
        except Exception as e:
            ctx.log.error(f"IaC Orchestrator: Runner reconciliation failed: {e}")
        finally:
            ctx.log.info("IaC Orchestrator: Startup reconciliation scan finished.")

        # Resume any pending tasks in the database queue
        interrupted_jobs = job_db.get_jobs_by_status("RUNNING")
        for job in interrupted_jobs:
            remaining_services = job_db.get_pending_tasks(job.id)
            if remaining_services:
                ctx.create_task(
                    engine.resume_bulk_rollout(job.id, remaining_services),
                    name=f"iac:resume:{job.id}"
                )
            elif not any(t.get("job_id") == job.id for t in engine.state.get("active_tasks", {}).values()):
                # If the job has no pending tasks AND no orphaned runners reattached to it,
                # the container was completely destroyed. We must close it out so it doesn't hang.
                ctx.log.warning(f"IaC Orchestrator: Job #{job.id} is RUNNING but has no active runners. Marking as FAILED.")
                job_db.update_job(job.id, "FAILED")
                job_db.update_progress(job.id, progress=None, current_step="System Restart (Aborted)")
                plugin_state["last_deployment"] = "FAILED"

    # Spawn reconciliation instantly in the background
    ctx.create_task(run_reconciliation(), name="iac:startup_reconciliation")
    ctx.create_task(engine.emit_monitoring_inventory_sync(), name="iac:monitoring_inventory_seed")
        
    # --- REGISTER UI ROUTE ---
    # Because this is inside setup(), it acts as a closure and permanently locks 
    # the 'ctx' and 'engine' instances to the route, surviving any hot-reloads.
    @ui.page('/iac')
    @main_layout('IaC Orchestrator')
    async def dashboard_page():
        await render_dashboard(ctx, plugin_state, engine, config)