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

Teardown removes only what ITS incarnation mounted (review R7). Each mount tags
its routes with a token; on core < 1.12 an async ``teardown`` runs as a task
next to the unload, and in the auto-revert path (unload → load of the previous
version without an await in between) it runs AFTER the restored version has
mounted its routes. Removing "everything under /api/iac" there deleted the
restored version's routes — CI and webhooks got 404 until the next reload.

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


#: Attribute on a route object naming the mount (plugin incarnation) it belongs to.
MOUNT_TOKEN_ATTR = "_iac_mount_token"


def unmount_direct_routes(app, plugin_id: str, token: str | None = None) -> int:
    """Remove directly mounted routes of this plugin from ``app``.

    With ``token``: only the routes that mount tagged — what a teardown may
    remove. Without: every one, whoever mounted it (this incarnation, an earlier
    one, a plugin version that did not tag) — what a fresh mount replaces.
    Returns the number of removed routes."""
    routes = app.router.routes

    def _goes(route) -> bool:
        if not _is_ours(getattr(route, "path", "") or "", plugin_id):
            return False
        return token is None or getattr(route, MOUNT_TOKEN_ATTR, None) == token

    kept = [route for route in routes if not _goes(route)]
    removed = len(routes) - len(kept)
    if removed:
        routes[:] = kept
        app.openapi_schema = None
    return removed


def mount_direct_routes(app, plugin_id: str, routers, reorder=None, token: str | None = None) -> int:
    """Mount ``routers`` directly on ``app`` exactly once: stale copies go first.
    ``reorder(app, prefix)`` moves freshly added routes ahead of the UI
    catch-all (``core.api.route_order.move_routes_before_catchall``).
    ``token`` tags the new routes so ``unmount_direct_routes(…, token)`` finds
    exactly them later. Returns the number of stale routes that were replaced."""
    stale = unmount_direct_routes(app, plugin_id)
    for router in routers:
        app.include_router(router)
    if token:
        # Everything of ours on the app right now was added by the lines above.
        for route in app.router.routes:
            if _is_ours(getattr(route, "path", "") or "", plugin_id):
                setattr(route, MOUNT_TOKEN_ATTR, token)
    if reorder is not None:
        reorder(app, PUBLIC_PREFIX)
        reorder(app, f"/api/plugins/{plugin_id}")
    app.openapi_schema = None
    return stale
