"""Public IaC Orchestrator webhook router (``/api/iac``).

These endpoints are genuinely public/external: GitLab and CI runners call them
over the network. They CANNOT be mounted via ``ctx.register_routes()`` — the
plugin registry wraps every registered route with ``require_api_auth``, which
would reject a GitLab caller that has no Lyndrix user token. Instead this router
is mounted DIRECTLY on the FastAPI app (the sanctioned exception for public
endpoints) and every handler validates the GitLab ``x_gitlab_token`` itself,
in-handler, via :func:`_require_gitlab_token`.

Mounted from ``entrypoint.setup()`` with::

    fastapi_app.include_router(build_webhook_router(svc))
    move_routes_before_catchall(fastapi_app, "/api/iac")
"""
from __future__ import annotations

from fastapi import APIRouter, Request, Header

from . import api as _api


def build_webhook_router(service) -> APIRouter:
    """Assemble the public ``/api/iac`` webhook router.

    ``service`` is accepted for symmetry with the auth'd plugin router; the shared
    handler logic resolves the live engine/ctx via the module-level handles set in
    :func:`api.init_api`.
    """
    del service  # state is shared via api.init_api()
    router = APIRouter(prefix="/api/iac", tags=["IaC Orchestrator (public)"])

    @router.get("/health")
    async def health_check():
        return await _api.do_health()

    @router.post("/webhook/gitlab")
    async def gitlab_webhook(request: Request, x_gitlab_token: str = Header(None)):
        return await _api.do_gitlab_webhook(request, x_gitlab_token)

    @router.post("/webhook/sync")
    async def sync_webhooks(x_gitlab_token: str = Header(None)):
        return await _api.do_sync_webhooks(x_gitlab_token)

    @router.post("/deploy/service/{service_name}/gitlab")
    async def trigger_service_deployment_gitlab(
        service_name: str,
        payload: _api.DeployRequest,
        x_gitlab_token: str = Header(None),
    ):
        _api._require_gitlab_token(x_gitlab_token)
        return await _api.do_trigger_service_deployment(service_name, payload)

    @router.get("/deploy/service/{service_name}/gitlab/status")
    async def get_service_deployment_status_gitlab(
        service_name: str,
        since_epoch: int = 0,
        x_gitlab_token: str = Header(None),
    ):
        _api._require_gitlab_token(x_gitlab_token)
        return await _api.do_get_service_status(service_name, since_epoch=since_epoch)

    @router.post("/webhook/gitlab/test-host/{host_name}")
    async def gitlab_test_host_webhook(
        host_name: str,
        payload: _api.TestHostDeployRequest,
        x_gitlab_token: str = Header(None),
    ):
        _api._require_gitlab_token(x_gitlab_token)
        return await _api.do_trigger_test_host_deployment(host_name, payload)

    @router.post("/infra/plan/gitlab")
    async def trigger_infra_plan_gitlab(x_gitlab_token: str = Header(None)):
        _api._require_gitlab_token(x_gitlab_token)
        return await _api.do_trigger_infra_plan()

    @router.post("/infra/apply/gitlab")
    async def trigger_infra_apply_gitlab(x_gitlab_token: str = Header(None)):
        _api._require_gitlab_token(x_gitlab_token)
        return await _api.do_trigger_infra_apply()

    return router
