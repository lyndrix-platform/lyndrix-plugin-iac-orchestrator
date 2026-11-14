"""OS-15 + OC-17: the directly mounted routes (/api/iac/*, stream) are replaced on
every setup and removed on teardown — never duplicated, never left behind."""
import asyncio
from unittest.mock import MagicMock

from fastapi import APIRouter, FastAPI

from core.api.route_order import move_routes_before_catchall
from iac_plugin.app.api import PLUGIN_ID, build_plugin_router, build_stream_router
from iac_plugin.app.controller.public_mount import mount_direct_routes, unmount_direct_routes
from iac_plugin.app.controller.webhook_router import build_webhook_router


def _paths(app):
    return [getattr(r, "path", "") for r in app.router.routes]


def _app():
    app = FastAPI()
    other = APIRouter()

    @other.get("/api/other/ping")
    async def ping():
        return {"ok": True}

    app.include_router(other)
    # what the core's registry mounts for this plugin (must survive our unmount)
    app.include_router(build_plugin_router(None), prefix=f"/api/plugins/{PLUGIN_ID}")
    return app


def _mount(app):
    return mount_direct_routes(app, PLUGIN_ID, [build_webhook_router(None), build_stream_router(None)],
                               reorder=move_routes_before_catchall)


def test_reload_never_duplicates_and_the_newest_handlers_answer():
    app = _app()
    assert _mount(app) == 0
    first = {r.path: r.endpoint for r in app.router.routes if getattr(r, "path", "").startswith("/api/iac")}
    stale = _mount(app)                                   # plugin reload: setup() runs again
    assert stale == len(first) + 2                         # public routes + the two stream routes
    paths = _paths(app)
    for path in ("/api/iac/health", "/api/iac/webhook/gitlab", "/api/iac/infra/apply/gitlab",
                 f"/api/plugins/{PLUGIN_ID}/stream/jobs", f"/api/plugins/{PLUGIN_ID}/jobs/{{job_id}}/logs/raw"):
        assert paths.count(path) == 1, path
    second = {r.path: r.endpoint for r in app.router.routes if getattr(r, "path", "").startswith("/api/iac")}
    assert all(second[p] is not first[p] for p in first)   # the old module's handlers are gone


def test_teardown_removes_the_public_routes_and_nothing_else():
    app = _app()
    before = _paths(app)
    _mount(app)
    removed = unmount_direct_routes(app, PLUGIN_ID)
    assert removed > 0 and _paths(app) == before
    assert "/api/other/ping" in before and f"/api/plugins/{PLUGIN_ID}/jobs/{{job_id}}/logs" in before
    assert not [p for p in _paths(app) if p.startswith("/api/iac")]
    assert unmount_direct_routes(app, PLUGIN_ID) == 0      # idempotent


def test_upgrade_from_a_version_that_never_unmounted():
    """1.6.1 left its /api/iac routes on the app: the new setup replaces them."""
    app = _app()
    app.include_router(build_webhook_router(None))         # stale copy without any marker
    app.include_router(build_stream_router(None))
    assert _mount(app) > 0
    assert _paths(app).count("/api/iac/health") == 1


def test_entrypoint_teardown_unmounts_before_it_shuts_down(monkeypatch):
    import sys
    import types

    from iac_plugin import entrypoint

    app = _app()
    _mount(app)
    monkeypatch.setitem(sys.modules, "main", types.SimpleNamespace(app=app))
    order = []

    class _Svc:
        async def shutdown(self, reason="", budget=0):
            order.append(("shutdown", "/api/iac/health" in _paths(app)))

    monkeypatch.setattr(entrypoint, "_service", _Svc())
    asyncio.run(entrypoint.teardown(MagicMock()))
    assert order == [("shutdown", False)]                  # routes were gone before the service stopped
    assert entrypoint._service is None
