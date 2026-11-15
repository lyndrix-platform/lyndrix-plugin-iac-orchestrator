"""Who may do what on the plugin's NiceGUI surfaces (audit OS-17).

The NiceGUI pages used to check nothing: any logged-in user could reach the
dashboard, trigger a fleet apply with ``approve`` or rewrite the settings. The
API routes carry ``require_permission`` — these surfaces now follow them:

* the ``/iac`` page is gated on the strictest permission it exposes
  (``api:infra_apply``, IaC admin role) through ``main_layout(permission=…)``;
* the settings panel the core embeds renders only for a session that holds
  every admin permission of the plugin;
* a trigger from these pages gets the entitlements of the SESSION USER, not
  all of them, and records who triggered it.

``core.api`` offers plugins no session permission check yet, so the per-action
part uses the check behind ``main_layout`` (``ui.layout``) when the running
core has it. Where the core cannot tell, the page gate is all there is — it
already requires the permission the privileged actions need.

# TODO(agent): switch to a ``core.api`` session-permission helper once the core
# offers one, gate per action (viewer/operator) and relax the page gate to
# ``api:read``.
"""
from __future__ import annotations

import logging

from nicegui import app, ui

from ..controller.api import (
    _APPLY_CAPABLE_TYPES, PERM_CREDENTIALS_ADMIN, PERM_INFRA_APPLY, PERM_SETTINGS_ADMIN,
)
from ..controller.job_payload import ENT_APPROVE, ENT_FORCE, ENT_TRIGGER

log = logging.getLogger("IaC:UI")

PLUGIN_ID = "lyndrix.plugin.iac_orchestrator"
PERM_WRITE = f"plugin:{PLUGIN_ID}:api:write"
#: The ``/iac`` page: the strictest permission an action on it needs.
PAGE_PERMISSION = PERM_INFRA_APPLY
#: The embedded settings panel writes settings, credentials and the apply gates.
SETTINGS_PERMISSIONS = (PERM_SETTINGS_ADMIN, PERM_CREDENTIALS_ADMIN, PERM_INFRA_APPLY)


def session_may(permission: str) -> bool | None:
    """Does the user of the current NiceGUI session hold ``permission``?
    ``None`` when this core offers no way to tell."""
    try:
        from ui import layout as _layout
    except Exception:
        return None
    check = getattr(_layout, "_session_has_permission", None)
    if not callable(check):
        return None
    try:
        return bool(check(permission))
    except Exception as exc:
        log.warning("UI: permission check for '%s' failed: %s", permission, exc)
        return False


def session_username() -> str | None:
    try:
        name = app.storage.user.get("username")
    except Exception:
        return None
    return str(name)[:100] if name else None


def page_layout(title: str, permission: str = PAGE_PERMISSION):
    """``main_layout`` with the page gate. A core whose layout takes no
    ``permission`` still gets a gated page: the check runs inside it and a
    session that cannot be vouched for is turned away."""
    from ui.layout import main_layout

    try:
        return main_layout(title, permission=permission)
    except TypeError:
        plain = main_layout(title)

    def decorator(fn):
        async def page(*args, **kwargs):
            if session_may(permission) is not True:
                ui.notify("Missing permission for the IaC Orchestrator page.", type="negative")
                ui.navigate.to("/dashboard")
                return None
            return await fn(*args, **kwargs)

        return plain(page)

    return decorator


def may_use_settings() -> bool:
    """May this session see and change the plugin's settings panel? Needs every
    admin permission the panel exposes; a core that cannot tell is a "no"."""
    return all(session_may(permission) is True for permission in SETTINGS_PERMISSIONS)


def ui_entitlements(payload: dict) -> frozenset | None:
    """Entitlements for a trigger of the current session (``None`` = refuse).

    Mirrors the API routes: triggering needs ``api:write``; ``approve`` and the
    pipelines that can apply infrastructure (``init_host``, ``host_provision``;
    not as a dry-run) need ``api:infra_apply``. When the core cannot tell
    (``None``) the page gate — which requires ``api:infra_apply`` — vouches.
    """
    may_write, may_apply = session_may(PERM_WRITE), session_may(PERM_INFRA_APPLY)
    if may_write is False:
        return None
    applies = bool(payload.get("approve")) or (
        payload.get("pipeline_type") in _APPLY_CAPABLE_TYPES and not payload.get("dry_run"))
    if applies and may_apply is False:
        return None
    granted = {ENT_TRIGGER, ENT_FORCE}
    if may_apply is not False:
        granted.add(ENT_APPROVE)
    return frozenset(granted)


def ui_submit(service, payload: dict) -> bool:
    """Queue a trigger from a NiceGUI handler with the session user's
    entitlements. False (and a notification) when the user may not."""
    entitlements = ui_entitlements(payload or {})
    if entitlements is None:
        log.warning("UI: trigger '%s' refused — user '%s' lacks the permission.",
                    (payload or {}).get("pipeline_type"), session_username())
        ui.notify("You do not have the permission for this action.", type="negative")
        return False
    service.submit_from_ui(payload, entitlements=entitlements, requested_by=session_username())
    return True
