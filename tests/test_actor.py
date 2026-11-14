"""OS-13: who triggered a job is recorded; deleting history needs the admin permission."""
import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

from iac_plugin.app import api as routes_mod
from iac_plugin.app.controller import api as api_mod
from iac_plugin.app.controller import queue as queue_mod
from iac_plugin.app.model.models import IaCJob
from tests.test_settings_security import _route_permissions


def _row(job_id):
    with queue_mod.db_instance.SessionLocal() as s:
        return s.get(IaCJob, job_id)


def test_route_triggered_job_carries_the_username(iac_service, monkeypatch):
    svc = iac_service
    monkeypatch.setattr(api_mod, "_ctx", MagicMock())
    monkeypatch.setattr(api_mod, "_service", svc)
    monkeypatch.setattr(api_mod, "_engine", svc.engine)
    alice = SimpleNamespace(username="alice", allows=lambda p: True)

    async def scenario():
        ui = await api_mod.do_trigger_service_deployment("aac-x", api_mod.DeployRequest(), identity=alice)
        ci = await api_mod.do_trigger_service_deployment("aac-y", api_mod.DeployRequest(), trigger="gitlab-ci")
        plan = await api_mod.do_trigger_infra_plan(alice)
        sync = await api_mod.do_sync_repos(alice)
        return ui["job_id"], ci["job_id"], plan["job_id"], sync["job_id"]

    ui, ci, plan, sync = asyncio.run(scenario())
    assert _row(ui).requested_by == "alice" and _row(ui).current_step == "Queued by alice"
    assert _row(ci).requested_by == "gitlab-ci"            # token route: the trigger name, there is no user
    assert _row(plan).requested_by == "alice" and _row(sync).requested_by == "alice"
    jobs = {j["id"]: j for j in svc.db.get_recent_jobs(10)}
    assert jobs[ui]["requested_by"] == "alice" and jobs[ci]["requested_by"] == "gitlab-ci"


def test_deleting_history_needs_the_admin_permission_and_is_logged(monkeypatch):
    perms = _route_permissions(routes_mod.build_plugin_router(None))
    assert api_mod.PERM_SETTINGS_ADMIN in perms[("POST", "/maintenance/clear-stats")]
    assert api_mod.PERM_SETTINGS_ADMIN in perms[("POST", "/maintenance/clear-failed-jobs")]
    assert api_mod.PERM_SETTINGS_ADMIN not in perms[("POST", "/maintenance/sync-repos")]

    ctx = MagicMock()
    db = SimpleNamespace(clear_all_jobs=lambda keep: 12, clear_failed_jobs=lambda: 3)
    monkeypatch.setattr(api_mod, "_ctx", ctx)
    monkeypatch.setattr(api_mod, "_engine", SimpleNamespace(db=db))
    monkeypatch.setattr(api_mod, "_service", SimpleNamespace(state={}))
    bob = SimpleNamespace(username="bob")
    assert asyncio.run(api_mod.do_clear_stats(bob))["deleted"] == 12
    assert asyncio.run(api_mod.do_clear_failed_jobs(bob))["deleted"] == 3
    logged = [c.args[0] for c in ctx.log.warning.call_args_list]
    assert len(logged) == 2 and all("bob" in line for line in logged)
