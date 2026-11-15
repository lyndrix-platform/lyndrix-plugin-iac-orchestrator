import asyncio
import time
import uuid

from nicegui import ui
from core.api import ModuleManifest, NotificationEndpoint, PluginHealthStatus, db_instance

from .app.controller.service import IaCService
from .app.controller.offloop import off_loop, shutdown_pool
from .app.controller.api import init_api
from .app.controller import api as _api_module
from .app.controller.webhook_router import build_webhook_router
from .app.controller.public_mount import mount_direct_routes, unmount_direct_routes
from .app.api import build_plugin_router, build_stream_router
from .app.model.models import Base
from .app.ui.access import PAGE_PERMISSION, page_layout
from .app.ui.dashboard import render_dashboard
from .app.ui.settings import render_settings_ui as modular_settings_ui
from .app.ui.widget import render_dashboard_widget as modular_widget

# ==========================================
# 1. MANIFEST
# ==========================================
manifest = ModuleManifest(
    id="lyndrix.plugin.iac_orchestrator",
    name="IaC Orchestrator",
    version="1.6.1",
    description="Standalone GitOps controller for executing Terraform and Ansible pipelines.",
    author="Lyndrix",
    icon="rocket_launch",
    type="PLUGIN",
    # core.api.maintenance (API 1.13) is imported unconditionally (maintenance windows).
    min_core_version="1.13.0",   # core.api.maintenance (core API 1.13 = core 1.11.0)
    # Cluster placement (core API ≥ 1.7): the CONTROL PLANE — API, UI data, webhook
    # ingest, job queue, logs — runs on every instance from the shared database.
    # EXECUTION (git working trees, Docker runner containers, ansible/terraform)
    # happens only on instances whose LYNDRIX_INSTANCE_ROLES include "runner": they
    # claim QUEUED jobs (app/controller/queue.py). Single mode has every role.
    placement="any",
    # AI assistant (core API 1.12): short purpose here, the full operating guide is
    # served live from ai/INSTRUCTIONS.md at /ai/instructions (single source of truth).
    ai_instructions=(
        "GitOps control plane for infrastructure: Terraform/OpenTofu plans, applies and state "
        "adoption for Proxmox/Hetzner containers plus Ansible service rollouts, all rendered from "
        "the iac-controller SSoT. Reach for it to inspect Terraform state, adopt existing containers, "
        "plan or apply infrastructure and roll out a single AAC service — never to hand-edit live infra."
    ),
    ai_instructions_route="/ai/instructions",
    auto_enable_on_install=True,
    repo_url="https://github.com/lyndrix-platform/lyndrix-plugin-iac-orchestrator",
    # Identity 2.0: fine-grained infra-apply permission (replaces the old
    # hardcoded core builtin "iac:infra_apply") + roles auto-mapped onto the
    # default groups. INT_ADMIN receives the qualified infra_apply once via the ledger.
    custom_permissions=[
        {"id": "api:infra_apply", "label": "Infrastruktur anwenden",
         "description": "Trigger IaC infrastructure apply (terraform/ansible apply)",
         "icon": "bolt"},
        # Deliberately NOT ending in api:read/api:write: the core grants those to
        # every INT_USER and lets the global api:write satisfy them (audit OS-02).
        {"id": "iac:settings_admin", "label": "IaC-Einstellungen verwalten",
         "description": "Change security-relevant settings: runner images, registry, GitLab/webhook URLs, "
                        "host allowlists, webhook sync",
         "icon": "admin_panel_settings"},
        {"id": "iac:logs_read", "label": "IaC-Job-Logs lesen",
         "description": "Read job output: log viewer, raw log download and the log lines of the live stream",
         "icon": "article"},
        {"id": "iac:credentials_admin", "label": "IaC-Zugangsdaten verwalten",
         "description": "Store or remove Git credentials, SSH keys and passwords and choose which "
                        "credential a setting uses",
         "icon": "key"},
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
             "plugin:lyndrix.plugin.iac_orchestrator:iac:settings_admin",
             "plugin:lyndrix.plugin.iac_orchestrator:iac:credentials_admin",
             "plugin:lyndrix.plugin.iac_orchestrator:iac:logs_read",
         ],
         "auto_map_groups": ["INT_ADMIN"],
         "description": "Full IaC control including infrastructure apply."},
        {"id": "operator", "label": "IaC Operator",
         "permissions": [
             "plugin:lyndrix.plugin.iac_orchestrator:api:read",
             "plugin:lyndrix.plugin.iac_orchestrator:api:write",
             "plugin:lyndrix.plugin.iac_orchestrator:iac:logs_read",
         ],
         "auto_map_groups": [],
         "description": "Trigger pipelines and read their logs, no infra apply, no admin settings."},
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
                      "entity_registry:identified", "iac:job_queued", "iac:job_abort_requested",
                      # core API >= 1.14 (local event); never fires on older cores
                      "cluster:instance_lost",
                      # this instance got SIGTERM: detach running jobs cleanly
                      "system:draining"],
        "emit": ["iac:pipeline_started", "iac:webhook_verified", "iac:inventory_updated",
                 "entity_registry:identify", "entity_registry:relate", "server_manager:register_server",
                 "git:sync", "git:commit_push",
                 "system:notify", "user:notify", "socket:request", "messaging:outbound",
                 "iac:job_queued", "iac:job_abort_requested",
                 # Maintenance windows (core API 1.13) — emitted LOCALLY, never cluster=True.
                 "maintenance:windows_published", "maintenance:override_started",
                 "maintenance:override_ended", "maintenance:window_missing"],
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
            name="job_orphaned",
            description="A running pipeline was closed as FAILED because its runner instance is gone.",
            default_active=True,
            internal_toast=True,
            internal_persist=True,
            external_default=True,
        ),
        NotificationEndpoint(
            name="job_timed_out",
            description="A running pipeline exceeded the maximum runtime and was stopped.",
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
# The service of every plugin load that is not torn down yet, keyed by the
# context of that load. teardown() stops the service ITS setup() created: on a
# core that runs the async teardown as a task (< 1.12) the successor's setup()
# may already have replaced ``_service`` — tearing "the current service" down
# then stopped the new load's worker, sweeper and webhook sync.
_services: dict[int, IaCService] = {}
# Token of the directly mounted routes per plugin load, keyed by the context of
# that load: an async teardown may run after the next setup() (core < 1.12) and
# must remove ITS routes, not the successor's (review R7).
_mount_tokens: dict[int, str] = {}
_mount_token: str | None = None   # the latest mount — fallback when teardown gets another ctx object


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

    # One reference for the whole probe: a reload may replace the global between awaits.
    svc = _service
    if svc is None or getattr(svc, "engine", None) is None:
        return PluginHealthStatus(status="error", details={"reason": "not_initialized"})

    if not db_instance.is_connected:
        return PluginHealthStatus(status="error", details={"reason": "db_unavailable"})

    state = getattr(svc, "state", None) or {}
    active_workers = len(state.get("active_tasks", {}) or {})
    last_deployment = state.get("last_deployment")
    # online | restarting | absent | unknown. Only a runner that is really gone
    # degrades the plugin: a restarting runner (core rollout) or a view that is
    # not built yet must not turn every instance's health red.
    runner_state = svc.runner_state()
    runner_online = runner_state == "online"

    # Vault holds the auto-apply flag (env fallback). A reachable Vault returns a
    # value; unreachable/unset returns None — informational, never fatal here.
    try:
        vault_ready = await off_loop(
            lambda: ctx.get_secret("iac_auto_apply") is not None
        )
    except Exception:
        vault_ready = False

    try:
        sweep = ((await off_loop(svc.db.get_state, "orphan_sweep")) or {}).get("data") or {}
    except Exception:
        sweep = {}

    try:
        recent = await off_loop(svc.db.get_recent_jobs, 5)
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
        status="degraded" if runner_state == "absent" else "ok",
        details={
            "db_connected": True,
            "vault_ready": vault_ready,
            "active_workers": active_workers,
            "last_deployment": last_deployment,
            "recent_jobs": len(recent or []),
            "is_runner": svc.is_runner,
            "runner_online": runner_online,
            "runner_state": runner_state,
            # Rejected calls on the token routes of this instance — a rotated
            # webhook token that CI still uses shows up here, not only in CI.
            "webhook_auth_failures": dict(_api_module.webhook_auth_failures),
            # Orphan sweep (lease-held by one instance): visible on every instance.
            "orphan_sweep": {
                "enabled": bool(await off_loop(lambda: svc.config.orphan_sweep_enabled)),
                "running_here": svc.sweeper.running,
                "holder": sweep.get("holder"),
                "last_sweep_at": sweep.get("last_sweep_at"),
                "orphans_failed_total": sweep.get("orphans_failed_total", 0),
                # Jobs the sweep closed itself after the maximum runtime (review C4).
                "timed_out_total": sweep.get("timed_out_total", 0),
            },
        },
        latency_ms=latency,
    )


# ==========================================
# 5. PLUGIN BOOT SEQUENCE
# ==========================================
def setup(ctx):
    global _service, _mount_token
    ctx.log.info("IaC Orchestrator: Executing async setup sequence...")

    # ``service`` — this load's own object. Every handler below closes over it,
    # never over the module global (which a later load replaces).
    service = _service = _services[id(ctx)] = IaCService(ctx)

    # API wiring
    init_api(ctx, _service)

    # Auth'd plugin router — mounted via the registry, which adds require_api_auth
    # automatically. Routes add require_permission(...) themselves. Registered
    # FIRST: on a re-enable the registry replaces its router by removing every
    # route under /api/plugins/<id> — that must not hit the stream routes below.
    ctx.register_routes(build_plugin_router(_service))

    # TODO(agent): these reach into core internals (`main.app`, `core.api.route_order`)
    # to mount the auth-exempt public webhook + SSE routers directly on the app.
    # The stable fix is a sanctioned `core.api` helper for "mount a public
    # (auth-exempt) router with its own prefix"; it does not exist yet.
    from main import app as fastapi_app
    from core.api.route_order import move_routes_before_catchall

    # * Public webhook router (/api/iac): external GitLab/CI callers carry no
    #   Lyndrix user token; each handler validates the x_gitlab_token itself.
    # * Live SSE job stream + raw log: EventSource/browser downloads cannot send
    #   an Authorization header; the handlers validate a ?ticket=/?token=.
    # public_mount removes the copies an earlier incarnation left behind first
    # and teardown() removes ours again (audit OS-15 / OC-17).
    _mount_token = _mount_tokens[id(ctx)] = uuid.uuid4().hex
    stale = mount_direct_routes(
        fastapi_app, manifest.id,
        [build_webhook_router(_service), build_stream_router(_service)],
        reorder=move_routes_before_catchall, token=_mount_token,
    )
    if stale:
        ctx.log.info(f"IaC Orchestrator: replaced {stale} stale directly mounted route(s).")

    # DB bootstrap — off the event loop (DDL and the bootstrap advisory lock can
    # take most of a minute while several instances start together). The runner
    # boot and the trigger path wait for it (IaCService.wait_bootstrapped).
    service.schedule_bootstrap(Base)

    @ctx.subscribe('db:connected')
    async def _on_db(_):
        await service.bootstrap_async(Base)

    # Event wiring — every trigger (UI, webhook, CI, NiceGUI, the engine's own
    # host-rollout hand-off) emits iac:webhook_verified on the local bus; it is
    # ENQUEUED here and executed by whichever runner instance claims it.
    @ctx.subscribe('iac:webhook_verified')
    async def _on_webhook(payload):
        if (payload or {}).get("_origin"):
            return  # relayed copy — the origin instance already queued it
        await service.enqueue_from_event(payload or {})

    # A peer queued a job → poke this instance's worker (runner instances only).
    @ctx.subscribe('iac:job_queued')
    async def _on_job_queued(payload):
        if service.worker is not None:
            service.worker.wake()

    # Abort requested on any instance → the owning runner cancels + kills containers.
    @ctx.subscribe('iac:job_abort_requested')
    async def _on_abort(payload):
        await service.on_abort_requested(payload or {})

    # Execution plane (runner role): worker, startup reconciliation, the
    # repo-derived publishes (they read the git working trees of this host).
    if service.is_runner:
        # Reconcile what a previous incarnation left behind, THEN start claiming.
        ctx.create_task(service.run_runner_boot(), name="iac:runner_boot")
        # One ordered chain (maintenance windows, 1.6): registry ci_ids → monitoring
        # sync with ci_ids → maintenance:windows_published. Same order as NativeGenerateStage.
        ctx.create_task(service.publish_inventory_chain(), name="iac:inventory_publish")
        ctx.create_task(service.publish_snapshots_on_boot(), name="iac:snapshots_publish")
    else:
        ctx.log.info("IaC Orchestrator: control plane only on this instance (no 'runner' role).")

    # Exactly one instance keeps the GitLab webhooks registered (needs only the
    # GitLab API, not the working trees). Older cores without leases: run it here.
    on_lease = getattr(ctx, "on_lease", None)
    if callable(on_lease):
        on_lease("iac:webhook_sync", acquired=service.start_webhook_sync, lost=service.stop_webhook_sync)
    else:
        service.start_webhook_sync()

    # Orphan sweep (rD §3): one instance under a lease — a control-plane instance
    # when there is one, so a hung or dead runner is noticed by somebody else.
    service.review_sweep_lease()
    try:
        @ctx.subscribe("cluster:instance_lost")
        async def _on_instance_lost(_payload):
            service.sweeper.poke()      # a peer is gone: sweep now, not at the next tick
    except Exception as exc:            # topic unknown to an older core
        ctx.log.debug(f"IaC Orchestrator: cluster:instance_lost not subscribed: {exc}")

    # SIGTERM gives plugins no teardown: detach running jobs when the instance
    # starts draining (flush logs, mark the step, leave the containers alone).
    try:
        @ctx.subscribe("system:draining")
        async def _on_draining(payload):
            if (payload or {}).get("_origin"):
                return  # a peer is draining, not this instance
            await service.shutdown(reason="instance draining", budget=8)
    except Exception as exc:
        ctx.log.debug(f"IaC Orchestrator: system:draining not subscribed: {exc}")

    # Cluster: tell the operator when no runner is online (jobs would only queue)
    # and keep the sweep lease on the right kind of instance.
    ctx.create_task(service.watch_runner_presence(), name="iac:runner_watch")

    # UI route (audit OS-17). The page triggers pipelines up to a fleet apply
    # with ``approve`` and shows job logs: it needs the strictest permission it
    # exposes (api:infra_apply — IaC admin role), not just a login. The React UI
    # is the canonical one and checks per route.
    # TODO(agent): gate per action and relax to api:read once core.api offers a
    # session permission helper (see app/ui/access.py).
    @ui.page('/iac')
    @page_layout('IaC Orchestrator', PAGE_PERMISSION)
    async def _dashboard_page():
        await render_dashboard(ctx, service)


async def teardown(ctx):
    """Plugin reload / disable / upgrade. Async on purpose: the core (>= 1.12)
    awaits it (bounded, 15 s) before it cancels the plugin's tasks and releases
    its leases; older cores run it as a task next to dispose(). Either way a job
    in flight is DETACHED, never closed — see IaCService.shutdown."""
    global _service
    # The service of THIS load (by its context). A context the registry does not
    # know: the oldest load still registered — a teardown belongs to an earlier
    # load, never to one that was set up after it.
    svc = _services.pop(id(ctx), None)
    if svc is None and _services:
        svc = _services.pop(next(iter(_services)))
    if svc is None:
        svc = _service
    if _service is svc:
        _service = None                 # nothing took over: the plugin is off
    # First: stop answering. The registry unmounts its own routes; the directly
    # mounted ones (/api/iac/*, stream) are ours to remove — a disabled plugin
    # must not keep accepting webhooks and CI triggers. Only the routes THIS load
    # mounted: on core < 1.12 this coroutine runs as a task, in the auto-revert
    # path after the restored version has already mounted its own (review R7).
    try:
        from main import app as fastapi_app

        token = _mount_tokens.pop(id(ctx), None) or _mount_token
        removed = unmount_direct_routes(fastapi_app, manifest.id, token=token) if token else 0
        ctx.log.info(f"IaC Orchestrator: removed {removed} directly mounted route(s).")
    except Exception as exc:
        ctx.log.warning(f"IaC Orchestrator: could not remove the public routes: {exc}")
    if svc is not None:
        try:
            await svc.shutdown(reason="plugin reload", budget=12)
        except Exception as exc:
            ctx.log.warning(f"IaC Orchestrator: teardown incomplete: {exc}")
    # The plugin's thread pool — unless a successor load is using it already.
    # Calls still queued or running finish; a late caller gets a new pool.
    if _service is None:
        shutdown_pool()
