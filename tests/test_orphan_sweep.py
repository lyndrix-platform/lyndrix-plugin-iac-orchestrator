"""OC-09 + OC-11: lease-based orphan sweep (rD §3) and a runner-presence view
that tolerates a restarting runner."""
import asyncio
import json
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlalchemy import update

from iac_plugin.app.controller import api as api_mod
from iac_plugin.app.controller import queue as queue_mod
from iac_plugin.app.controller.orphan_sweep import LEASE_NAME, WAITING_STEP, OrphanSweeper, owner_state
from iac_plugin.app.model.models import IaCJob

RUNNER, POD = "docker-auto-runner", "k3s-pod-1"


def _row(job_id):
    with queue_mod.db_instance.SessionLocal() as s:
        return s.get(IaCJob, job_id)


def _age(job_id, seconds, **values):
    """Make the job's heartbeat ``seconds`` old on the database clock (SQLite: UTC)."""
    with queue_mod.db_instance.SessionLocal() as s:
        s.execute(update(IaCJob).where(IaCJob.id == job_id)
                  .values(heartbeat_utc=datetime.utcnow() - timedelta(seconds=seconds), **values))
        s.commit()


def _view(*alive, dead=()):
    return ([{"instance_id": i, "alive": True, "roles": ["runner"] if "runner" in i else ["api", "leader"]} for i in alive]
            + [{"instance_id": i, "alive": False, "roles": ["runner"]} for i in dead])


def _sweeper(svc, *, instances=None, instance_state=None, **cfg):
    """The sweeper as it runs on a control-plane pod, on the service's shared DB."""
    ctx = SimpleNamespace(instance_id=POD, cluster_instances=instances if instances is not None else _view(POD),
                          notify=MagicMock())
    if instance_state is not None:
        ctx.instance_state = instance_state
    config = SimpleNamespace(orphan_sweep_enabled=True, orphan_sweep_grace_seconds=90,
                             orphan_sweep_container_grace_seconds=1800)
    for key, value in cfg.items():
        setattr(config, key, value)
    notified = []
    sweeper = OrphanSweeper(ctx, svc.queue, svc.db, config, notify=lambda job_id, reason, row: notified.append((job_id, reason)))
    return sweeper, notified


def _claimed(svc, incarnation="core-a:plugin-1", **payload):
    job_id = svc.queue.enqueue({"pipeline_type": "connectivity", **payload})
    claim = svc.queue.claim_next(RUNNER, incarnation)
    return job_id, claim.token


# --- the dead criterion -------------------------------------------------------------------

def test_live_owner_is_never_failed_however_stale_the_heartbeat(iac_service):
    job_id, _ = _claimed(iac_service)
    _age(job_id, 3700)                                   # e.g. the runner's loop is blocked for an hour
    sweeper, notified = _sweeper(iac_service, instances=_view(POD, RUNNER))
    assert asyncio.run(sweeper.sweep_once()) == [] and notified == []
    assert _row(job_id).status == "RUNNING"


def test_dead_owner_without_progress_and_container_is_failed_exactly_once(iac_service):
    svc = iac_service
    job_id, token = _claimed(svc)
    _age(job_id, 200)
    ended = []
    svc.queue.on_terminal = ended.append
    sweeper, notified = _sweeper(svc, instances=_view(POD, dead=[RUNNER]))

    assert asyncio.run(sweeper.sweep_once()) == [job_id]
    row = _row(job_id)
    assert row.status == "FAILED" and row.current_step.startswith(f"runner_lost: {RUNNER} last heartbeat ")
    assert row.end_time is not None
    assert notified == [(job_id, row.current_step)] and ended == [job_id]
    log_lines = svc.queue.read_log(job_id)["lines"]
    assert len(log_lines) == 1 and "runner_lost" in log_lines[0] and "re-trigger" in log_lines[0]

    assert asyncio.run(sweeper.sweep_once()) == []       # idempotent: a second pass / second sweeper
    assert len(notified) == 1 and ended == [job_id]
    state = svc.db.get_state("orphan_sweep")["data"]
    assert state["holder"] == POD and state["orphans_failed_total"] == 1 and state["last_sweep_at"]

    # fencing: the runner comes back and tries to finish the job
    svc.db.claims[job_id] = token
    assert svc.db.update_job(job_id, "SUCCESS") is False and _row(job_id).status == "FAILED"
    assert svc.queue.heartbeat(RUNNER, [job_id]) == [job_id]


def test_progress_within_the_grace_period_protects_the_job(iac_service):
    svc = iac_service
    fresh, _ = _claimed(svc)                              # heartbeat just now
    logging_job, _ = _claimed(svc)
    _age(fresh, 30)
    _age(logging_job, 500)
    svc.queue.append_log(logging_job, ["TASK [still working]"], 0)      # a log line a moment ago
    sweeper, _ = _sweeper(svc, instances=_view(POD, dead=[RUNNER]))
    assert asyncio.run(sweeper.sweep_once()) == []
    assert _row(fresh).status == "RUNNING" and _row(logging_job).status == "RUNNING"


def test_reported_container_gets_the_long_grace(iac_service):
    svc = iac_service
    job_id, token = _claimed(svc)
    svc.queue.claims[job_id] = token
    svc.queue.set_runners(job_id, {"aac-traefik": {"status": "running_ansible", "logs": [], "job_id": job_id}})
    _age(job_id, 600)
    sweeper, notified = _sweeper(svc, instances=_view(POD, dead=[RUNNER]))
    assert asyncio.run(sweeper.sweep_once()) == []
    assert _row(job_id).status == "RUNNING" and _row(job_id).current_step == WAITING_STEP
    _age(job_id, 1900)                                    # past orphan_sweep_container_grace_seconds
    assert asyncio.run(sweeper.sweep_once()) == [job_id] and len(notified) == 1


def test_untrusted_view_and_disabled_sweep_do_nothing(iac_service):
    job_id, _ = _claimed(iac_service)
    _age(job_id, 5000)
    for instances in ([], _view("someone-else"), None):   # empty / does not list this instance
        sweeper, _ = _sweeper(iac_service, instances=instances if instances is not None else [])
        assert asyncio.run(sweeper.sweep_once()) == []
    sweeper, _ = _sweeper(iac_service, instances=_view(POD, dead=[RUNNER]), orphan_sweep_enabled=False)
    assert asyncio.run(sweeper.sweep_once()) == [] and _row(job_id).status == "RUNNING"


def test_a_heartbeat_between_listing_and_closing_wins(iac_service):
    svc = iac_service
    job_id, token = _claimed(svc)
    _age(job_id, 200)
    svc.queue.claims[job_id] = token
    assert svc.queue.heartbeat(RUNNER, [job_id]) == []     # the runner is back
    assert svc.queue.fail_orphan(job_id, token, "runner_lost: x", 90) is False
    _age(job_id, 200)
    assert svc.queue.fail_orphan(job_id, "another-claim", "runner_lost: x", 90) is False    # adopted meanwhile
    assert _row(job_id).status == "RUNNING"


# --- owner liveness from core facts ---------------------------------------------------------

@pytest.mark.parametrize("info,expected", [
    ({"state": "alive", "incarnation": "core-a"}, "alive"),
    ({"state": "draining", "incarnation": "core-a"}, "alive"),
    ({"state": "alive", "incarnation": "core-b"}, "dead"),          # restarted under the fixed instance id
    ({"state": "restarting"}, "restarting"),
    ({"state": "unknown"}, "unknown"),
    ({"state": "dead"}, "dead"),
    ({"state": "something-new"}, "unknown"),
])
def test_owner_state_prefers_instance_state(info, expected):
    ctx = SimpleNamespace(instance_id=POD, instance_state=lambda owner: info,
                          cluster_instances=_view(POD, RUNNER))
    assert owner_state(ctx, RUNNER, "core-a:plugin-1") == expected


def test_owner_state_falls_back_to_the_heartbeat_view():
    assert owner_state(SimpleNamespace(instance_id=POD, cluster_instances=_view(POD, RUNNER)), RUNNER, None) == "alive"
    assert owner_state(SimpleNamespace(instance_id=POD, cluster_instances=_view(POD, dead=[RUNNER])), RUNNER, None) == "dead"
    assert owner_state(SimpleNamespace(instance_id=POD, cluster_instances=_view(POD)), RUNNER, None) == "dead"   # row gone
    assert owner_state(SimpleNamespace(instance_id=POD, cluster_instances=_view(RUNNER)), RUNNER, None) == "unknown"
    assert owner_state(SimpleNamespace(instance_id=POD), RUNNER, None) == "unknown"


def test_restarting_owner_is_spared_dead_one_is_not(iac_service):
    job_id, _ = _claimed(iac_service)
    _age(job_id, 400)
    state = {"state": "restarting"}
    sweeper, _ = _sweeper(iac_service, instance_state=lambda owner: state)
    assert asyncio.run(sweeper.sweep_once()) == [] and _row(job_id).status == "RUNNING"
    state["state"] = "dead"
    assert asyncio.run(sweeper.sweep_once()) == [job_id]


# --- lease placement (rD §3.1) ----------------------------------------------------------------

def _lease_ctx(svc, *, roles, instances):
    svc.ctx.roles = roles
    svc.ctx.cluster_instances = instances
    svc.ctx.on_lease = MagicMock()
    svc.ctx.release_lease = MagicMock()
    return svc.ctx


def test_control_plane_instances_contend_runners_only_when_alone(iac_service):
    svc = iac_service
    ctx = _lease_ctx(svc, roles=["api", "leader"], instances=_view(POD, RUNNER))
    svc.review_sweep_lease()
    svc.review_sweep_lease()
    assert ctx.on_lease.call_count == 1 and ctx.on_lease.call_args.args[0] == LEASE_NAME
    assert ctx.on_lease.call_args.kwargs["acquired"] == svc._start_sweeper

    ctx = _lease_ctx(svc, roles=["runner"], instances=_view(RUNNER, POD))
    svc._sweep_lease_wanted = False
    svc.review_sweep_lease()
    ctx.on_lease.assert_not_called()                      # a control-plane instance is alive

    ctx.cluster_instances = _view(RUNNER)                 # single mode / every pod is gone
    svc.review_sweep_lease()
    assert ctx.on_lease.call_count == 1
    ctx.cluster_instances = _view(RUNNER, POD)            # a pod is back: hand the sweep over
    svc.review_sweep_lease()
    ctx.release_lease.assert_called_once_with(LEASE_NAME)


# --- OC-11: runner presence ---------------------------------------------------------------------

def test_runner_state_tolerates_a_restart(iac_service, monkeypatch):
    svc = iac_service
    svc.ctx.roles = ["api"]
    del svc.ctx.role_state                                # core 1.11.1: only has_live_role
    clock = {"t": 1000.0}
    monkeypatch.setattr("iac_plugin.app.controller.service.time.monotonic", lambda: clock["t"])
    svc._started_at = 1000.0
    svc.ctx.has_live_role = lambda role: False
    assert svc.runner_state() == "unknown"                # view not built yet
    clock["t"] += 61
    assert svc.runner_state() == "absent"
    svc.ctx.has_live_role = lambda role: True
    assert svc.runner_state() == "online" and svc.runner_online() is True
    svc.ctx.has_live_role = lambda role: False            # graceful stop deletes the row at once
    clock["t"] += 30
    assert svc.runner_state() == "restarting" and svc.runner_online() is False
    clock["t"] += 100
    assert svc.runner_state() == "absent"


def test_runner_state_uses_the_core_role_state_when_offered(iac_service):
    svc = iac_service
    svc.ctx.has_live_role = lambda role: False
    for core, mine in (("alive", "online"), ("draining", "online"), ("restarting", "restarting"),
                       ("dead", "absent"), ("unknown", "unknown")):
        svc.ctx.role_state = lambda role, core=core: {"role": role, "state": core}
        assert svc.runner_state() == mine


def test_presence_watcher_degrades_only_after_two_absent_checks(iac_service, monkeypatch):
    svc = iac_service
    svc.ctx.set_status = MagicMock()
    monkeypatch.setattr(svc, "review_sweep_lease", lambda: None)
    script = iter(["restarting", "absent", "online", "absent", "absent", "absent", "restarting", "online"])
    calls_after = []

    def scripted():
        state = next(script)
        return state

    monkeypatch.setattr(svc, "runner_state", scripted)

    async def scenario():
        task = asyncio.get_running_loop().create_task(svc.watch_runner_presence(interval=0.01, settle=0))
        for _ in range(40):
            await asyncio.sleep(0.01)
            calls_after.append(svc.ctx.set_status.call_count)
            if next(iter([c for c in svc.ctx.set_status.call_args_list if c.args[0] == "ready"]), None):
                break
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    asyncio.run(scenario())
    phases = [c.args[0] for c in svc.ctx.set_status.call_args_list]
    assert phases == ["degraded", "ready"]                # one miss (2nd tick) did not degrade; restarting clears it
    assert svc.ctx.set_status.call_args_list[0].kwargs["blocking"] is False


def test_queued_jobs_say_they_wait_for_a_runner():
    jobs = [{"id": 2, "status": "QUEUED", "current_step": "Queued by gitlab-ci"},
            {"id": 1, "status": "RUNNING", "current_step": "Stage: x"}]
    assert api_mod._with_wait_hints(jobs, "online") == jobs
    hinted = api_mod._with_wait_hints(jobs, "absent")
    assert hinted[0]["waiting_for"] == "runner" and hinted[0]["current_step"].startswith("Waiting for runner — none online")
    assert "Queued by gitlab-ci" in hinted[0]["current_step"] and hinted[1] == jobs[1]
    assert "restarting" in api_mod._with_wait_hints(jobs, "restarting")[0]["current_step"]
    assert jobs[0]["current_step"] == "Queued by gitlab-ci"            # input untouched


def test_health_reports_sweep_and_degrades_only_for_an_absent_runner(iac_service, monkeypatch):
    from iac_plugin import entrypoint

    svc = iac_service
    monkeypatch.setattr(entrypoint, "_service", svc)
    svc.db.update_state("orphan_sweep", {"holder": POD, "last_sweep_at": "2026-11-14T10:00:00+00:00",
                                         "orphans_failed_total": 3}, "")
    results = {}
    for state in ("online", "restarting", "unknown", "absent"):
        monkeypatch.setattr(svc, "runner_state", lambda state=state: state)
        results[state] = asyncio.run(entrypoint.health(svc.ctx))
    assert [results[s].status for s in ("online", "restarting", "unknown", "absent")] == ["ok", "ok", "ok", "degraded"]
    details = results["restarting"].details
    assert details["runner_state"] == "restarting" and details["runner_online"] is False
    assert details["orphan_sweep"]["holder"] == POD and details["orphan_sweep"]["orphans_failed_total"] == 3
    assert details["orphan_sweep"]["enabled"] is True and details["orphan_sweep"]["running_here"] is False
