"""Auth'd REST + SSE surface for the IaC Orchestrator.

Two routers live here:

``build_plugin_router(svc)``
    Mounted via ``ctx.register_routes()``. The plugin registry already wraps every
    route with ``require_api_auth`` (a route can never be served anonymously), so
    we do NOT add it again — we only add ``Depends(require_permission(...))`` for
    authorization (``api:read`` on reads, ``api:write`` on actions). Core mounts it
    at ``/api/plugins/lyndrix.plugin.iac_orchestrator/``.

``build_stream_router(svc)``
    The live job SSE stream. It is mounted DIRECTLY on the app (NOT via the
    registry) because the registry's header auth would break ``EventSource``, which
    cannot send an Authorization header. The handler validates a ``?token=`` query
    parameter in-handler instead. It carries the full plugin prefix so the bundle's
    ``/api/plugins/<id>/stream/jobs`` URL resolves.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request

from core.api import ApiIdentity, require_permission

from .controller import api as _api

PLUGIN_ID = "lyndrix.plugin.iac_orchestrator"


def build_plugin_router(service) -> APIRouter:
    """The single auth'd IaC Orchestrator router — core mounts it at /api/plugins/<id>/."""
    del service  # state is shared via api.init_api()
    router = APIRouter(tags=["IaC Orchestrator"])

    # ── Reads (api:read) ─────────────────────────────────────────────────────
    @router.get("/catalog")
    async def get_catalog(identity: ApiIdentity = Depends(require_permission("api:read"))):
        return await _api.do_get_catalog()

    @router.get("/jobs")
    async def list_jobs(
        limit: int = 20,
        identity: ApiIdentity = Depends(require_permission("api:read")),
    ):
        return await _api.do_list_jobs(limit)

    @router.get("/jobs/{job_id}/logs")
    async def job_logs(
        job_id: int,
        tail: int = 200,
        grep: str | None = None,
        offset: int = 0,
        identity: ApiIdentity = Depends(require_permission("api:read")),
    ):
        return await _api.do_job_logs(job_id, tail=tail, grep=grep, offset=offset)

    @router.get("/jobs/{job_id}/runners")
    async def job_runners(
        job_id: int,
        identity: ApiIdentity = Depends(require_permission("api:read")),
    ):
        return await _api.do_job_runners(job_id)

    @router.get("/deploy/service/{service_name}/status")
    async def service_status(
        service_name: str,
        since_epoch: int = 0,
        identity: ApiIdentity = Depends(require_permission("api:read")),
    ):
        return await _api.do_get_service_status(service_name, since_epoch=since_epoch)

    @router.get("/stats")
    async def stats(identity: ApiIdentity = Depends(require_permission("api:read"))):
        return await _api.do_get_stats()

    @router.get("/infrastructure/assignments")
    async def infra_assignments(
        identity: ApiIdentity = Depends(require_permission("api:read")),
    ):
        return await _api.do_load_assignments()

    @router.get("/infrastructure/terraform-hosts")
    async def infra_terraform_hosts(
        identity: ApiIdentity = Depends(require_permission("api:read")),
    ):
        return await _api.do_terraform_hosts()

    @router.get("/service/{service_name}/history")
    async def service_history(
        service_name: str,
        identity: ApiIdentity = Depends(require_permission("api:read")),
    ):
        return await _api.do_service_history(service_name)

    # NB: core auto-registers a generic ``/settings`` schema endpoint on every plugin
    # router, which would shadow an exact ``/settings`` route here. We therefore expose
    # the orchestrator settings surface under ``/settings/general`` (sub-paths do not
    # collide with core's exact-path route).
    @router.get("/settings/general")
    async def get_settings(identity: ApiIdentity = Depends(require_permission("api:read"))):
        return await _api.do_get_settings()

    # Schema-driven, comprehensive settings surface (pipeline + webhooks + ansible +
    # terraform + repo roles). The React UI renders generically from /settings/schema.
    @router.get("/settings/schema")
    async def get_settings_schema(
        identity: ApiIdentity = Depends(require_permission("api:read")),
    ):
        return await _api.do_get_settings_schema()

    @router.get("/settings/values")
    async def get_settings_values(
        identity: ApiIdentity = Depends(require_permission("api:read")),
    ):
        return await _api.do_get_settings_values()

    @router.get("/settings/credentials")
    async def list_credentials(
        identity: ApiIdentity = Depends(require_permission("api:read")),
    ):
        return await _api.do_list_credentials()

    @router.get("/settings/webhook-token")
    async def get_webhook_token(
        identity: ApiIdentity = Depends(require_permission("api:read")),
    ):
        return await _api.do_get_webhook_token()

    # ── Actions (api:write) ──────────────────────────────────────────────────
    @router.post("/pipeline")
    async def run_pipeline(
        payload: _api.PipelineRequest,
        identity: ApiIdentity = Depends(require_permission("api:write")),
    ):
        return await _api.do_run_pipeline(payload)

    @router.post("/abort")
    async def abort(identity: ApiIdentity = Depends(require_permission("api:write"))):
        return await _api.do_abort()

    @router.post("/settings/general")
    async def save_settings(
        payload: _api.SettingsRequest,
        identity: ApiIdentity = Depends(require_permission("api:write")),
    ):
        return await _api.do_save_settings(payload)

    @router.post("/settings/values")
    async def save_settings_values(
        payload: _api.SettingsValuesRequest,
        identity: ApiIdentity = Depends(require_permission("api:write")),
    ):
        return await _api.do_save_settings_values(payload)

    @router.post("/settings/credentials")
    async def add_credential(
        payload: _api.CredentialRequest,
        identity: ApiIdentity = Depends(require_permission("api:write")),
    ):
        return await _api.do_add_credential(payload)

    @router.delete("/settings/credentials/{alias}")
    async def delete_credential(
        alias: str,
        identity: ApiIdentity = Depends(require_permission("api:write")),
    ):
        return await _api.do_delete_credential(alias)

    @router.post("/settings/webhook-token/generate")
    async def generate_webhook_token(
        identity: ApiIdentity = Depends(require_permission("api:write")),
    ):
        return await _api.do_generate_webhook_token()

    @router.post("/settings/webhooks/sync")
    async def sync_webhooks(
        identity: ApiIdentity = Depends(require_permission("api:write")),
    ):
        return await _api.do_sync_webhooks_authed()

    @router.post("/maintenance/clear-stats")
    async def clear_stats(identity: ApiIdentity = Depends(require_permission("api:write"))):
        return await _api.do_clear_stats()

    @router.post("/maintenance/clear-failed-jobs")
    async def clear_failed_jobs(identity: ApiIdentity = Depends(require_permission("api:write"))):
        return await _api.do_clear_failed_jobs()

    @router.post("/maintenance/sync-repos")
    async def sync_repos(identity: ApiIdentity = Depends(require_permission("api:write"))):
        return await _api.do_sync_repos()
    @router.post("/deploy/service/{service_name}")
    async def deploy_service(
        service_name: str,
        payload: _api.DeployRequest,
        identity: ApiIdentity = Depends(require_permission("api:write")),
    ):
        return await _api.do_trigger_service_deployment(service_name, payload)

    @router.post("/deploy/test-host/{host_name}")
    async def deploy_test_host(
        host_name: str,
        payload: _api.TestHostDeployRequest,
        identity: ApiIdentity = Depends(require_permission("api:write")),
    ):
        return await _api.do_trigger_test_host_deployment(host_name, payload)

    @router.post("/bootstrap/{host_name}")
    async def bootstrap_host(
        host_name: str,
        payload: _api.TestHostDeployRequest,
        identity: ApiIdentity = Depends(require_permission("api:write")),
    ):
        return await _api.do_trigger_host_bootstrap(host_name, payload)

    @router.post("/infra/plan")
    async def infra_plan(identity: ApiIdentity = Depends(require_permission("api:write"))):
        return await _api.do_trigger_infra_plan()

    # Fleet-wide `tofu apply` can create/change/DESTROY real infrastructure, so it
    # requires a dedicated high-privilege permission in addition to generic write —
    # operating a single service must not implicitly grant destroying the fleet.
    # (The master system key and superadmin role bypass both, as everywhere.)
    @router.post("/infra/apply")
    async def infra_apply(
        identity: ApiIdentity = Depends(require_permission("api:write")),
        _apply: ApiIdentity = Depends(require_permission("iac:infra_apply")),
    ):
        return await _api.do_trigger_infra_apply()

    # Mint a short-lived ticket for the SSE/raw-log endpoints. Requires api:read
    # (same scope the stream itself needs), so the long-lived bearer token never
    # has to travel in a query string. See build_stream_router below.
    @router.post("/stream/ticket")
    async def stream_ticket(identity: ApiIdentity = Depends(require_permission("api:read"))):
        return {"ticket": _api.issue_stream_ticket("api:read"), "expires_in": 60}

    return router


def build_stream_router(service) -> APIRouter:
    """The live SSE job stream, mounted directly on the app (not via the registry).

    EventSource cannot send a Bearer header, so this route validates the
    ``lyndrix_token`` passed as ``?token=`` in-handler. It is given the full plugin
    prefix so the URL matches the registry-style path the React client uses.
    """
    del service
    router = APIRouter(prefix=f"/api/plugins/{PLUGIN_ID}", tags=["IaC Orchestrator (stream)"])

    @router.get("/stream/jobs")
    async def stream_jobs(request: Request, token: str | None = None, ticket: str | None = None):
        return await _api.stream_jobs(request, token, ticket)

    # Full-log download/view. Lives on the stream router (direct-on-app, no registry
    # header auth) because the browser opens this URL in a new tab / as a download
    # and cannot send a Bearer header — it authenticates via a short-lived ?ticket=
    # (preferred) or ?token= like the SSE, and both must carry api:read.
    @router.get("/jobs/{job_id}/logs/raw")
    async def job_log_raw(
        job_id: int,
        token: str | None = None,
        ticket: str | None = None,
        download: bool = False,
    ):
        from fastapi import HTTPException
        if not _api._authorize_stream_request(token, ticket, "api:read"):
            raise HTTPException(status_code=401, detail="Unauthorized")
        return await _api.do_job_log_raw(job_id, download=download)

    return router
