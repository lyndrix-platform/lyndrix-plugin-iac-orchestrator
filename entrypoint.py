import asyncio
import time

from nicegui import ui
from ui.layout import main_layout
from core.api import ModuleManifest, NotificationEndpoint, PluginHealthStatus, db_instance

from .app.controller.service import IaCService
from .app.controller.api import init_api
from .app.controller.webhook_router import build_webhook_router
from .app.api import build_plugin_router, build_stream_router
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
    version="1.4.2",
    description="Standalone GitOps controller for executing Terraform and Ansible pipelines.",
    author="Lyndrix",
    icon="rocket_launch",
    type="PLUGIN",
    min_core_version="1.4.0",
    # Cluster placement (core API ≥ 1.7): the CONTROL PLANE — API, UI data, webhook
    # ingest, job queue, logs — runs on every instance from the shared database.
    # EXECUTION (git working trees, Docker runner containers, ansible/terraform)
    # happens only on instances whose LYNDRIX_INSTANCE_ROLES include "runner": they
    # claim QUEUED jobs (app/controller/queue.py). Single mode has every role.
    placement="any",
    auto_enable_on_install=True,
    repo_url="https://github.com/lyndrix-platform/lyndrix-plugin-iac-orchestrator",
    # Identity 2.0: fine-grained infra-apply permission (replaces the old
    # hardcoded core builtin "iac:infra_apply") + roles auto-mapped onto the
    # default groups. INT_ADMIN receives the qualified infra_apply once via the ledger.
    custom_permissions=[
        {"id": "api:infra_apply", "label": "Infrastruktur anwenden",
         "description": "Trigger IaC infrastructure apply (terraform/ansible apply)",
         "icon": "bolt"},
    ],
    # NB: the core's role qualifier (`_qualify` in role_registry.py) passes any
    # permission already starting with "api:"/"feature:"/"route:"/"plugin:" through
    # UNCHANGED — it only namespaces bare suffixes. A bare "api:read"/"api:write"
    # here would therefore land as a GLOBAL permission on the mapped group (every
    # plugin's routes, not just ours), not one scoped to this plugin. Fully qualify
    # every entry with this plugin's id, same as the existing infra_apply grant.
    roles=[
        {"id": "admin", "label": "IaC Administrator",
         "permissions": [
             "plugin:lyndrix.plugin.iac_orchestrator:api:read",
             "plugin:lyndrix.plugin.iac_orchestrator:api:write",
             "plugin:lyndrix.plugin.iac_orchestrator:api:infra_apply",
         ],
         "auto_map_groups": ["INT_ADMIN"],
         "description": "Full IaC control including infrastructure apply."},
        {"id": "operator", "label": "IaC Operator",
         "permissions": [
             "plugin:lyndrix.plugin.iac_orchestrator:api:read",
             "plugin:lyndrix.plugin.iac_orchestrator:api:write",
         ],
         "auto_map_groups": [],
         "description": "Trigger pipelines and edit configs, no infra apply."},
        {"id": "viewer", "label": "IaC Viewer",
         "permissions": ["plugin:lyndrix.plugin.iac_orchestrator:api:read"],
         "auto_map_groups": [],
         "description": "Read-only dashboard and job history."},
    ],
    ui_route="/iac",
    react_ui=True,
    # i18next-shaped namespace served to the React UI; core auto-registers
    # locales/iac.<locale>.json and adds "iac" to the client allowlist.
    i18n_namespace="iac",
    react_routes=[
        {
            "path": "/iac",
            "label": "IaC Orchestrator",
            "icon": "rocket_launch",
            "sidebar_visible": True,
        },
        {
            "path": "/iac/settings",
            "label": "IaC Orchestrator Settings",
            "icon": "settings",
            "sidebar_visible": False,
        },
    ],
    settings_ui_route="/iac/settings",
    permissions={
        "subscribe": ["vault:ready_for_data", "iac:webhook_verified", "git:status_update", "db:connected", "socket:response",
                      "entity_registry:identified", "iac:job_queued", "iac:job_abort_requested"],
        "emit": ["iac:pipeline_started", "iac:webhook_verified", "iac:inventory_updated",
                 "entity_registry:identify", "server_manager:register_server",
                 "git:sync", "git:commit_push",
                 "system:notify", "user:notify", "socket:request", "messaging:outbound",
                 "iac:job_queued", "iac:job_abort_requested"],
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
# 4. HEALTH — functional liveness probe
# ==========================================
async def health(ctx) -> PluginHealthStatus:
    """Functional health probe.

    This is the orchestrator's real liveness, not the ``setup() ran`` proxy the
    old ``/health`` route returned (which checked a non-existent
    ``engine.engine`` attribute and so always reported db_connected=False). Here
    we verify the DB the job/state tables live in is actually connected, that a
    job query succeeds, and we grade on the most recent pipeline outcomes: a
    fresh FAILED/ERROR job means the GitOps controller is unhealthy even while
    the process is up. The sync DB/Vault calls are offloaded so the probe never
    blocks the event loop.
    """
    start = time.perf_counter()

    if _service is None or getattr(_service, "engine", None) is None:
        return PluginHealthStatus(status="error", details={"reason": "not_initialized"})

    if not db_instance.is_connected:
        return PluginHealthStatus(status="error", details={"reason": "db_unavailable"})

    state = getattr(_service, "state", None) or {}
    active_workers = len(state.get("active_tasks", {}) or {})
    last_deployment = state.get("last_deployment")
    runner_online = _service.runner_online()

    # Vault holds the auto-apply flag (env fallback). A reachable Vault returns a
    # value; unreachable/unset returns None — informational, never fatal here.
    try:
        vault_ready = await asyncio.to_thread(
            lambda: ctx.get_secret("iac_auto_apply") is not None
        )
    except Exception:
        vault_ready = False

    try:
        recent = await asyncio.to_thread(_service.db.get_recent_jobs, 5)
    except Exception as exc:
        return PluginHealthStatus(
            status="error",
            details={"reason": "db_query_failed", "error": str(exc)},
            latency_ms=round((time.perf_counter() - start) * 1000, 1),
        )

    latency = round((time.perf_counter() - start) * 1000, 1)
    return PluginHealthStatus(
        # Control plane without any live runner: the API works, jobs queue up but
        # nothing executes them — degraded, not an error.
        status="ok" if runner_online else "degraded",
        details={
            "db_connected": True,
            "vault_ready": vault_ready,
            "active_workers": active_workers,
            "last_deployment": last_deployment,
            "recent_jobs": len(recent or []),
            "is_runner": _service.is_runner,
            "runner_online": runner_online,
        },
        latency_ms=latency,
    )


# ==========================================
# 5. PLUGIN BOOT SEQUENCE
# ==========================================
def setup(ctx):
    global _service
    ctx.log.info("IaC Orchestrator: Executing async setup sequence...")

    _service = IaCService(ctx)

    # API wiring
    init_api(ctx, _service)
    # TODO(agent): these reach into core internals (`main.app`, `core.api.route_order`)
    # to mount the auth-exempt public webhook + SSE routers directly on the app.
    # The stable fix is a sanctioned `core.api` helper for "mount a public
    # (auth-exempt) router"; it does not exist yet, so this coupling stays for now
    # (audit findings -010, -011 structural move also deferred — needs core support).
    from main import app as fastapi_app
    from core.api.route_order import move_routes_before_catchall

    # Public webhook router (/api/iac) — mounted DIRECTLY on the app. It must NOT
    # go through ctx.register_routes(): the registry wraps every route with
    # require_api_auth, which would reject external GitLab/CI callers that carry no
    # Lyndrix user token. Each handler validates the x_gitlab_token itself.
    fastapi_app.include_router(build_webhook_router(_service))
    move_routes_before_catchall(fastapi_app, "/api/iac")

    # Live SSE job stream — also mounted directly, because EventSource cannot send
    # an Authorization header. The handler validates the ?token= query param. (The
    # registry's header auth would otherwise break the stream.)
    fastapi_app.include_router(build_stream_router(_service))
    move_routes_before_catchall(
        fastapi_app, "/api/plugins/lyndrix.plugin.iac_orchestrator/stream"
    )

    # Auth'd plugin router — mounted via the registry, which adds require_api_auth
    # automatically. Routes add require_permission(api:read|api:write) themselves.
    ctx.register_routes(build_plugin_router(_service))

    # DB bootstrap
    _service.bootstrap_db(Base)

    @ctx.subscribe('db:connected')
    async def _on_db(_):
        _service.bootstrap_db(Base)

    # Event wiring — every trigger (UI, webhook, CI, NiceGUI, the engine's own
    # host-rollout hand-off) emits iac:webhook_verified on the local bus; it is
    # ENQUEUED here and executed by whichever runner instance claims it.
    @ctx.subscribe('iac:webhook_verified')
    async def _on_webhook(payload):
        if (payload or {}).get("_origin"):
            return  # relayed copy — the origin instance already queued it
        await _service.enqueue_from_event(payload or {})

    # A peer queued a job → poke this instance's worker (runner instances only).
    @ctx.subscribe('iac:job_queued')
    async def _on_job_queued(payload):
        if _service.worker is not None:
            _service.worker.wake()

    # Abort requested on any instance → the owning runner cancels + kills containers.
    @ctx.subscribe('iac:job_abort_requested')
    async def _on_abort(payload):
        await _service.on_abort_requested(payload or {})

    # Execution plane (runner role): worker, startup reconciliation, the
    # repo-derived publishes (they read the git working trees of this host).
    if _service.is_runner:
        _service.start_runner()
        ctx.create_task(_service.run_startup_reconciliation(), name="iac:startup_reconciliation")
        ctx.create_task(_service.emit_monitoring_inventory_sync(), name="iac:monitoring_inventory_seed")
        ctx.create_task(_service.emit_iac_inventory_updated(), name="iac:inventory_publish")
        ctx.create_task(_service.publish_snapshots_on_boot(), name="iac:snapshots_publish")
    else:
        ctx.log.info("IaC Orchestrator: control plane only on this instance (no 'runner' role).")

    # Exactly one instance keeps the GitLab webhooks registered (needs only the
    # GitLab API, not the working trees). Older cores without leases: run it here.
    on_lease = getattr(ctx, "on_lease", None)
    if callable(on_lease):
        on_lease("iac:webhook_sync", acquired=_service.start_webhook_sync, lost=_service.stop_webhook_sync)
    else:
        _service.start_webhook_sync()

    # Cluster: tell the operator when no runner is online (jobs would only queue).
    ctx.create_task(_service.watch_runner_presence(), name="iac:runner_watch")

    # UI route
    @ui.page('/iac')
    @main_layout('IaC Orchestrator')
    async def _dashboard_page():
        await render_dashboard(ctx, _service)


def teardown(ctx):
    global _service
    svc, _service = _service, None
    if svc is not None:
        try:
            svc.stop_webhook_sync()
            ctx.create_task(svc.stop_runner(), name="iac:worker_stop")
        except Exception:
            pass
