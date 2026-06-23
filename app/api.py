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
        identity: ApiIdentity = Depends(require_permission("api:read")),
    ):
        return await _api.do_job_logs(job_id, tail=tail, grep=grep)

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

    # ── Actions (api:write) ──────────────────────────────────────────────────
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

    @router.post("/infra/apply")
    async def infra_apply(identity: ApiIdentity = Depends(require_permission("api:write"))):
        return await _api.do_trigger_infra_apply()

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
    async def stream_jobs(request: Request, token: str | None = None):
        return await _api.stream_jobs(request, token)

    return router
