"""OS-17: the NiceGUI surfaces follow the permissions of the API routes.

They used to check nothing — any logged-in user could open ``/iac``, queue a
fleet apply with ``approve`` (the page handed every trigger ALL entitlements)
or rewrite the settings.
"""
import asyncio
import inspect
import json
import sys
import types
from unittest.mock import MagicMock

import pytest

from iac_plugin import entrypoint
from iac_plugin.app.controller import queue as queue_mod
from iac_plugin.app.controller.job_payload import ENT_APPROVE, ENT_FORCE, ENT_TRIGGER
from iac_plugin.app.model.models import IaCJob
from iac_plugin.app.ui import access, dashboard, settings as settings_ui

PID = "plugin:lyndrix.plugin.iac_orchestrator:"
WRITE, APPLY = PID + "api:write", PID + "api:infra_apply"
ADMIN = {WRITE, APPLY, PID + "iac:settings_admin", PID + "iac:credentials_admin"}


def _session(monkeypatch, granted):
    """The session user holds ``granted`` (``None``: this core cannot tell)."""
    monkeypatch.setattr(access, "session_may", lambda permission: None if granted is None else permission in granted)
    monkeypatch.setattr(access, "session_username", lambda: "alice")
    monkeypatch.setattr(access, "ui", MagicMock())


# --- page gate --------------------------------------------------------------------------------------

def test_the_page_requires_the_strictest_permission_it_exposes(monkeypatch):
    assert access.PAGE_PERMISSION == APPLY
    calls = []
    layout = types.SimpleNamespace(main_layout=lambda title, **kw: calls.append((title, kw)) or (lambda fn: fn))
    monkeypatch.setitem(sys.modules, "ui.layout", layout)
    access.page_layout("IaC Orchestrator", access.PAGE_PERMISSION)
    assert calls == [("IaC Orchestrator", {"permission": APPLY})]
    source = inspect.getsource(entrypoint.setup)
    assert "@page_layout('IaC Orchestrator', PAGE_PERMISSION)" in source and "@main_layout(" not in source


@pytest.mark.parametrize("verdict,served", [(True, True), (False, False), (None, False)])
def test_a_core_whose_layout_takes_no_permission_still_gets_a_gated_page(monkeypatch, verdict, served):
    """Fail closed: without the layout's gate the check runs inside the page,
    and a session nobody can vouch for is turned away."""
    def old_main_layout(title, **kwargs):
        if kwargs:
            raise TypeError("main_layout() got an unexpected keyword argument 'permission'")
        return lambda fn: fn

    monkeypatch.setitem(sys.modules, "ui.layout", types.SimpleNamespace(main_layout=old_main_layout))
    monkeypatch.setattr(access, "session_may", lambda permission: verdict)
    monkeypatch.setattr(access, "ui", MagicMock())
    rendered = []

    async def page():
        rendered.append(True)

    asyncio.run(access.page_layout("IaC Orchestrator")(page)())
    assert bool(rendered) is served
    assert access.ui.navigate.to.called is (not served)


# --- entitlements of a trigger ------------------------------------------------------------------------

APPLY_FLEET = {"pipeline_type": "infra_apply", "approve": True, "manual": True}
DEPLOY = {"pipeline_type": "single_service", "service_name": "aac-traefik", "manual": True}


@pytest.mark.parametrize("granted,payload,expected", [
    (ADMIN, APPLY_FLEET, {ENT_TRIGGER, ENT_FORCE, ENT_APPROVE}),
    ({WRITE}, DEPLOY, {ENT_TRIGGER, ENT_FORCE}),                      # operator: deploy, never approve
    ({WRITE}, APPLY_FLEET, None),                                     # … and no fleet apply
    ({WRITE}, {"pipeline_type": "init_host", "host_name": "h1"}, None),
    ({WRITE}, {"pipeline_type": "init_host", "host_name": "h1", "dry_run": True}, {ENT_TRIGGER, ENT_FORCE}),
    (set(), DEPLOY, None),                                            # a plain login: nothing
    ({APPLY}, DEPLOY, None),                                          # no api:write, no trigger
    (None, APPLY_FLEET, {ENT_TRIGGER, ENT_FORCE, ENT_APPROVE}),       # core cannot tell: the page gate vouches
])
def test_ui_entitlements_follow_the_session_user(monkeypatch, granted, payload, expected):
    _session(monkeypatch, granted)
    result = access.ui_entitlements(payload)
    assert result == (None if expected is None else frozenset(expected))


def test_ui_submit_refuses_without_permission_and_records_the_user_otherwise(monkeypatch):
    service = MagicMock()
    _session(monkeypatch, {WRITE})
    assert access.ui_submit(service, dict(APPLY_FLEET)) is False
    service.submit_from_ui.assert_not_called()
    assert access.ui.notify.call_args.kwargs == {"type": "negative"}

    assert access.ui_submit(service, dict(DEPLOY)) is True
    kwargs = service.submit_from_ui.call_args.kwargs
    assert kwargs == {"entitlements": frozenset({ENT_TRIGGER, ENT_FORCE}), "requested_by": "alice"}


def test_every_nicegui_trigger_goes_through_ui_submit():
    for module in (dashboard, settings_ui, sys.modules["iac_plugin.app.ui.terraform"]):
        source = inspect.getsource(module)
        assert "submit_from_ui(" not in source and "ui_submit(service, " in source, module.__name__


def test_submit_from_ui_no_longer_grants_everything(iac_service):
    """The service used to stamp ALL_ENTITLEMENTS on whatever a page handed it."""
    svc = iac_service

    async def scenario():
        svc.submit_from_ui({**DEPLOY, "approve": True, "force": True, "trigger": "ui"})
        svc.submit_from_ui(dict(APPLY_FLEET), entitlements={ENT_APPROVE, ENT_TRIGGER}, requested_by="alice")
        for _ in range(300):
            await asyncio.sleep(0.01)
            with queue_mod.db_instance.SessionLocal() as s:
                if s.query(IaCJob).count() == 2:
                    return

    asyncio.run(scenario())
    with queue_mod.db_instance.SessionLocal() as s:
        rows = {r.requested_by: json.loads(r.payload) for r in s.query(IaCJob).all()}
    untrusted = next(p for who, p in rows.items() if who != "alice")
    assert "approve" not in untrusted and "force" not in untrusted and untrusted["trigger"] == "bus"
    assert rows["alice"]["approve"] is True


# --- settings panel and abort ------------------------------------------------------------------------------

def test_the_settings_panel_renders_only_for_an_iac_administrator(monkeypatch):
    fake_ui = MagicMock()
    monkeypatch.setattr(settings_ui, "ui", fake_ui)
    ctx, service = MagicMock(), MagicMock()
    for granted in ({WRITE}, ADMIN - {APPLY}, None):                  # operator, half an admin, unknown
        monkeypatch.setattr(access, "session_may",
                            lambda permission, g=granted: None if g is None else permission in g)
        settings_ui.render_settings_ui(ctx, service)
    ctx.get_secret.assert_not_called()                                # nothing read, nothing rendered
    assert fake_ui.label.call_count == 3 and not fake_ui.button.called
    assert "settings_admin" in fake_ui.label.call_args.args[0]


def test_the_nicegui_abort_uses_the_stop_path_of_the_api():
    source = inspect.getsource(dashboard.render_dashboard)
    handler = source[source.index("async def abort_execution"):source.index("# Live viewer state")]
    assert "await _api.do_abort()" in handler and "unstopped_jobs" in handler
    for own_path in ("create_subprocess_exec", "update_job", 'state["active_tasks"] = {}'):
        assert own_path not in handler
