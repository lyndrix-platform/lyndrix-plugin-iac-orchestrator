"""Routes this plugin mounts directly on the FastAPI app (audit OS-15 / OC-17).

Two routers cannot go through ``ctx.register_routes``: the public ``/api/iac``
webhook router (GitLab and CI call it without a Lyndrix identity, and the path
is part of deployed CI templates) and the stream router (EventSource and browser
downloads cannot send an Authorization header). The core's registry does not
know them, so nothing removed them on disable/reload: a disabled plugin kept
answering "accepted" on ``/api/iac/*`` while nothing enqueued, and every reload
appended another copy — Starlette serves the FIRST match, i.e. the handlers of
the unloaded module with its stopped service.

This module owns those routes: mounting first removes whatever an earlier
incarnation (or plugin version) left, teardown removes them again.

TODO(agent): drop this once the core offers a sanctioned public mount with a
custom prefix that is unmounted with the plugin.
"""
from __future__ import annotations

PUBLIC_PREFIX = "/api/iac"


def _stream_paths(plugin_id: str) -> tuple[str, ...]:
    base = f"/api/plugins/{plugin_id}"
    return (f"{base}/stream/jobs", f"{base}/jobs/{{job_id}}/logs/raw")


def _is_ours(path: str, plugin_id: str) -> bool:
    return path == PUBLIC_PREFIX or path.startswith(PUBLIC_PREFIX + "/") or path in _stream_paths(plugin_id)


def unmount_direct_routes(app, plugin_id: str) -> int:
    """Remove every directly mounted route of this plugin from ``app`` — whoever
    mounted it (this incarnation, an earlier one, an older plugin version).
    Returns the number of removed routes."""
    routes = app.router.routes
    kept = [route for route in routes if not _is_ours(getattr(route, "path", "") or "", plugin_id)]
    removed = len(routes) - len(kept)
    if removed:
        routes[:] = kept
        app.openapi_schema = None
    return removed


def mount_direct_routes(app, plugin_id: str, routers, reorder=None) -> int:
    """Mount ``routers`` directly on ``app`` exactly once: stale copies go first.
    ``reorder(app, prefix)`` moves freshly added routes ahead of the UI
    catch-all (``core.api.route_order.move_routes_before_catchall``).
    Returns the number of stale routes that were replaced."""
    stale = unmount_direct_routes(app, plugin_id)
    for router in routers:
        app.include_router(router)
    if reorder is not None:
        reorder(app, PUBLIC_PREFIX)
        reorder(app, f"/api/plugins/{plugin_id}")
    app.openapi_schema = None
    return stale
