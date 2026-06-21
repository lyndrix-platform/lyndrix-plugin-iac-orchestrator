from nicegui import ui
from ui.layout import main_layout
from core.api import ModuleManifest, NotificationEndpoint, db_instance

from .app.controller.service import IaCService
from .app.controller.api import iac_api_router, init_api
from .app.model.models import Base
from .app.ui.dashboard import render_dashboard
from .app.ui.settings import render_settings_ui as modular_settings_ui
from .app.ui.widget import render_dashboard_widget as modular_widget

# ==========================================
# 1. MANIFEST
# ==========================================
manifest = ModuleManifest(
    id="lyndrix.plugin.iac_orchestrator",
    name="IaC Orchestrator",
    version="0.5.5",
    description="Standalone GitOps controller for executing Terraform and Ansible pipelines.",
    author="Lyndrix",
    icon="rocket_launch",
    type="PLUGIN",
    min_core_version="0.1.1",
    auto_enable_on_install=False,
    repo_url="https://github.com/lyndrix-platform/lyndrix-plugin-iac-orchestrator",
    ui_route="/iac",
    permissions={
        "subscribe": ["vault:ready_for_data", "iac:webhook_verified", "git:status_update", "db:connected", "socket:response"],
        "emit": ["iac:pipeline_started", "iac:webhook_verified", "git:sync", "git:commit_push",
                 "system:notify", "user:notify", "monitoring:inventory_sync", "socket:request", "messaging:outbound"],
    },
    notification_endpoints=[
        NotificationEndpoint(
            name="deployment_started",
            description="Pipeline run has been queued or has begun.",
            default_active=True,
            internal_toast=False,
            internal_persist=True,
            external_default=True,
        ),
        NotificationEndpoint(
            name="deployment_succeeded",
            description="A pipeline finished successfully.",
            default_active=True,
            internal_toast=True,
            internal_persist=True,
            external_default=True,
        ),
        NotificationEndpoint(
            name="deployment_failed",
            description="A pipeline finished with errors.",
            default_active=True,
            internal_toast=True,
            internal_persist=True,
            external_default=True,
        ),
        NotificationEndpoint(
            name="webhook_verified",
            description="Incoming webhook accepted and dispatched to the engine.",
            default_active=False,
            internal_toast=False,
            internal_persist=False,
            external_default=False,
        ),
        NotificationEndpoint(
            name="drift_detected",
            description="Drift detection found differences during a rollout.",
            default_active=True,
            internal_toast=True,
            internal_persist=True,
            external_default=True,
        ),
    ],
)

_service: IaCService | None = None


# ==========================================
# 2. SETTINGS INJECTION
# ==========================================
def render_settings_ui(ctx):
    modular_settings_ui(ctx, _service)


# ==========================================
# 3. WIDGET INJECTION
# ==========================================
def render_dashboard_widget(ctx):
    modular_widget(ctx, _service)


# ==========================================
# 4. PLUGIN BOOT SEQUENCE
# ==========================================
def setup(ctx):
    global _service
    ctx.log.info("IaC Orchestrator: Executing async setup sequence...")

    _service = IaCService(ctx)

    # API wiring
    init_api(ctx, _service)
    from main import app as fastapi_app
    _service.register_api_routes(fastapi_app, iac_api_router)

    # DB bootstrap
    _service.bootstrap_db(Base)

    @ctx.subscribe('db:connected')
    async def _on_db(_):
        _service.bootstrap_db(Base)

    # Event wiring
    @ctx.subscribe('iac:webhook_verified')
    async def _on_webhook(payload):
        ctx.create_task(_service.run_pipeline(payload), name="iac:run_pipeline")

    # Startup tasks
    ctx.create_task(_service.run_startup_reconciliation(), name="iac:startup_reconciliation")
    ctx.create_task(_service.emit_monitoring_inventory_sync(), name="iac:monitoring_inventory_seed")

    # UI route
    @ui.page('/iac')
    @main_layout('IaC Orchestrator')
    async def _dashboard_page():
        await render_dashboard(ctx, _service)


def teardown(ctx):
    global _service
    _service = None
