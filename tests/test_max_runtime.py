"""Review C4: a job cannot stay RUNNING for ever while its owner INSTANCE is
alive but the plugin part that executes it is not (plugin disabled, a version
that does not load, a watcher stuck on ``docker logs -f``, a runner whose
Docker never answers after a restart — review C1).

The sweep bounds every RUNNING job by ``job_max_runtime_seconds``: it asks the
runner to stop the job, and closes the row itself when the runner does not —
keeping the job's locks reserved, because nobody confirmed that its containers
are gone.
"""
import asyncio
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlalchemy import update

from iac_plugin.app.controller import engine as engine_mod
from iac_plugin.app.controller import orphan_sweep as sweep_mod
from iac_plugin.app.controller import queue as queue_mod
from iac_plugin.app.controller.config import IaCConfig
from iac_plugin.app.controller.engine import ContainerStateUnknown
from iac_plugin.app.controller.orphan_sweep import OVERDUE_STEP, OVERDUE_STOP_WAIT_SECONDS, OrphanSweeper
from iac_plugin.app.controller.settings_schema import OrchestratorSettings, SettingsValidationError
from iac_plugin.app.controller.utils import StageResult
from iac_plugin.app.model.models import IaCJob

RUNNER, POD = "docker-auto-runner", "k3s-pod-1"
TF = "tf:onprem_prod"
SIX_HOURS = 21600


def _row(job_id):
    with queue_mod.db_instance.SessionLocal() as s:
        return s.get(IaCJob, job_id)


def _set(job_id, **values):
    with queue_mod.db_instance.SessionLocal() as s:
        s.execute(update(IaCJob).where(IaCJob.id == job_id).values(**values))
        s.commit()


def _ago(seconds):
    return datetime.utcnow() - timedelta(seconds=seconds)      # SQLite's clock is UTC


def _claimed(svc, *, running_for=0, heartbeat_age=0, host="h1"):
    job_id = svc.queue.enqueue({"pipeline_type": "state_check", "host_name": host})
    claim = svc.queue.claim_next(RUNNER, "core-a:plugin-1")
    _set(job_id, claimed_utc=_ago(running_for), heartbeat_utc=_ago(heartbeat_age))
    return job_id, claim.token


class _Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def _sweeper(svc, *, alive=True, **cfg):
    """The sweeper on a control-plane pod; the runner INSTANCE is alive."""
    ctx = SimpleNamespace(
        instance_id=POD, emit=MagicMock(),
        cluster_instances=[{"instance_id": POD, "alive": True, "roles": ["api"]},
                           {"instance_id": RUNNER, "alive": alive, "roles": ["runner"]}])
    config = SimpleNamespace(orphan_sweep_enabled=True, orphan_sweep_grace_seconds=90,
                             orphan_sweep_container_grace_seconds=1800, job_max_runtime_seconds=SIX_HOURS)
    for key, value in cfg.items():
        setattr(config, key, value)
    told, clock = [], _Clock()
    sweeper = OrphanSweeper(ctx, svc.queue, svc.db, config, clock=clock,
                            notify_overdue=lambda job_id, text, row, closed: told.append((job_id, closed, text)))
    return sweeper, ctx, told, clock


# --- the sweep ------------------------------------------------------------------------------------

def test_a_job_within_the_limit_or_without_one_is_left_alone(iac_service):
    svc = iac_service
    young, _ = _claimed(svc, running_for=SIX_HOURS - 60, heartbeat_age=5000)     # plugin part dead for a while
    sweeper, ctx, told, _ = _sweeper(svc)
    assert asyncio.run(sweeper.sweep_once()) == []
    old, _ = _claimed(svc, running_for=10 * SIX_HOURS, host="h2")
    off, ctx_off, told_off, _ = _sweeper(svc, job_max_runtime_seconds=0)          # 0 = no limit
    assert asyncio.run(off.sweep_once()) == []
    _set(old, claimed_utc=None)                                                    # claimed by a version before C4
    assert asyncio.run(sweeper.sweep_once()) == []
    assert _row(young).status == _row(old).status == "RUNNING"
    ctx.emit.assert_not_called() and ctx_off.emit.assert_not_called()
    assert told == told_off == []


def test_an_overdue_job_is_reported_its_runner_asked_and_then_closed(iac_service):
    """Owner instance alive, heartbeat fresh (a watcher hangs on ``docker logs
    -f``): the orphan rules never fire. After the limit the runner is asked
    once; when the row is still RUNNING two minutes later the sweep closes it."""
    svc = iac_service
    job_id, token = _claimed(svc, running_for=SIX_HOURS + 125)
    ended = []
    svc.queue.on_terminal = ended.append
    sweeper, ctx, told, clock = _sweeper(svc)

    assert asyncio.run(sweeper.sweep_once()) == []                 # pass 1: report + ask
    row = _row(job_id)
    assert row.status == "RUNNING" and row.current_step == OVERDUE_STEP
    topic, payload = ctx.emit.call_args.args
    assert topic == "iac:job_abort_requested" and ctx.emit.call_args.kwargs == {"cluster": True}
    assert payload["job_ids"] == [job_id] and payload["final_status"] == "FAILED"
    assert payload["reason"] == "max_runtime: running for 6h 02m, limit 6h 00m"
    assert told == [(job_id, False, told[0][2])] and "was asked to stop" in told[0][2]
    assert "Maximum runtime exceeded (running for 6h 02m, limit 6h 00m)" in svc.queue.read_log(job_id)["lines"][0]

    clock.now += OVERDUE_STOP_WAIT_SECONDS - 1
    assert asyncio.run(sweeper.sweep_once()) == []                 # the runner still has time
    assert ctx.emit.call_count == 1 and len(told) == 1 and _row(job_id).status == "RUNNING"

    clock.now += 2
    assert asyncio.run(sweeper.sweep_once()) == [job_id]           # nobody closed it: the sweep does
    row = _row(job_id)
    assert row.status == "FAILED" and row.end_time is not None
    assert row.current_step.startswith("max_runtime: running for 6h 0")
    assert f"closed without confirmation from runner '{RUNNER}'" in row.current_step
    closing_line = svc.queue.read_log(job_id)["lines"][-1]
    assert "MAY STILL BE RUNNING" in closing_line and "stay reserved for up to 1800s" in closing_line
    assert told[-1][:2] == (job_id, True) and "may still be running" in told[-1][2]
    assert ended == [job_id]                                       # the maintenance override ends
    state = svc.db.get_state("orphan_sweep")["data"]
    assert state["timed_out_total"] == 1 and state["last_timed_out"] == [job_id]

    # fencing: whatever the runner still writes for the job is a no-op …
    svc.db.claims[job_id] = token
    assert svc.db.update_job(job_id, "SUCCESS") is False and _row(job_id).status == "FAILED"
    # … and its next heartbeat tells it to stop the containers.
    assert svc.queue.heartbeat(RUNNER, [job_id]) == [job_id]
    assert asyncio.run(sweeper.sweep_once()) == []                 # idempotent


def test_a_job_its_runner_closed_in_time_is_not_touched_again(iac_service):
    svc = iac_service
    job_id, token = _claimed(svc, running_for=SIX_HOURS + 60)
    sweeper, ctx, told, clock = _sweeper(svc)
    asyncio.run(sweeper.sweep_once())
    svc.db.claims[job_id] = token
    assert svc.db.update_job(job_id, "FAILED")                     # the runner stopped and closed it
    clock.now += OVERDUE_STOP_WAIT_SECONDS + 5
    assert asyncio.run(sweeper.sweep_once()) == []
    assert len(told) == 1 and not sweeper._stop_asked


def test_the_limit_holds_while_the_orphan_sweep_is_switched_off(iac_service):
    svc = iac_service
    job_id, _ = _claimed(svc, running_for=SIX_HOURS + 60)
    sweeper, ctx, _told, clock = _sweeper(svc, orphan_sweep_enabled=False)
    asyncio.run(sweeper.sweep_once())
    clock.now += OVERDUE_STOP_WAIT_SECONDS + 1
    assert asyncio.run(sweeper.sweep_once()) == [job_id]


def test_a_dead_runner_with_a_container_keeps_following_the_orphan_rules(iac_service):
    """The container grace of the orphan rules bounds that case already; the
    runtime limit does not cut it short."""
    svc = iac_service
    job_id, _ = _claimed(svc, running_for=SIX_HOURS + 60, heartbeat_age=600)
    assert svc.queue.acquire_lock(TF, job_id, RUNNER) == (True, job_id)
    sweeper, ctx, told, clock = _sweeper(svc, alive=False)
    assert asyncio.run(sweeper.sweep_once()) == []
    assert _row(job_id).current_step == sweep_mod.WAITING_STEP
    ctx.emit.assert_not_called()
    assert told == []


# --- locks: never free while a container may still run ------------------------------------------------

def test_the_locks_of_a_job_closed_by_the_limit_stay_reserved_for_the_container_grace(iac_service):
    """The plugin part died hours ago while tofu was applying: the heartbeat is
    ancient. Closing the row must not make the state lock takeable at once —
    the container may still be writing the state."""
    svc = iac_service
    job_id, _ = _claimed(svc, running_for=SIX_HOURS + 60, heartbeat_age=5 * 3600)
    assert svc.queue.acquire_lock(TF, job_id, RUNNER) == (True, job_id)
    waiter, _ = _claimed(svc, host="h2")
    sweeper, _ctx, _told, clock = _sweeper(svc)
    asyncio.run(sweeper.sweep_once())
    clock.now += OVERDUE_STOP_WAIT_SECONDS + 1
    assert asyncio.run(sweeper.sweep_once()) == [job_id]

    assert svc.queue.acquire_lock(TF, waiter, RUNNER) == (False, job_id)       # reserved from the close on
    _set(job_id, heartbeat_utc=_ago(1700))
    assert svc.queue.acquire_lock(TF, waiter, RUNNER) == (False, job_id)
    _set(job_id, heartbeat_utc=_ago(1801))                                      # the container grace is over
    assert svc.queue.acquire_lock(TF, waiter, RUNNER) == (True, waiter)


# --- the runner's side of the request ------------------------------------------------------------------

REQUEST = {"final_status": "FAILED", "reason": "max_runtime: running for 6h 02m, limit 6h 00m"}


def test_a_runner_that_can_stops_the_containers_closes_the_job_as_failed_and_frees_the_locks(iac_service):
    """The job is attended by this runner (re-attached, or still waiting for
    Docker after a restart): the request is carried out like an abort."""
    svc = iac_service
    job_id, token = _claimed(svc, running_for=SIX_HOURS + 60)
    svc.db.claims[job_id] = token
    svc._unsettled.add(job_id)
    assert svc.queue.acquire_lock(TF, job_id, RUNNER) == (True, job_id)
    stopped = []

    async def stop(jid):
        stopped.append(jid)
        return 1

    svc.engine.stop_job_containers = stop
    assert asyncio.run(svc.on_abort_requested({"job_ids": [job_id], **REQUEST})) == []
    row = _row(job_id)
    assert stopped == [job_id] and row.status == "FAILED" and row.current_step == REQUEST["reason"]
    assert svc.queue.lock_holder(TF) is None                       # Docker confirmed: free at once
    assert job_id not in svc._unsettled


def test_docker_never_answering_is_bounded_by_the_same_limit(iac_service):
    """Review C1 left "Docker did not answer after a restart" unbounded: the job
    stayed RUNNING and heartbeated. With the limit the runner cannot stop it
    either — the sweep closes it, says so, and keeps the state lock reserved."""
    svc = iac_service
    job_id, token = _claimed(svc, running_for=SIX_HOURS + 60)
    svc.db.claims[job_id] = token
    svc._unsettled.add(job_id)                                     # adopted, container state unknown
    assert svc.queue.acquire_lock(TF, job_id, RUNNER) == (True, job_id)
    waiter, _ = _claimed(svc, host="h2")

    async def no_docker(jid):
        raise ContainerStateUnknown("'docker ps' did not answer within 20s")

    svc.engine.stop_job_containers = no_docker
    svc._docker_retry_delay = lambda attempt: 3600                 # the background retry stays asleep
    sweeper, ctx, told, clock = _sweeper(svc)

    async def scenario():
        await sweeper.sweep_once()
        payload = ctx.emit.call_args.args[1]
        unstopped = await svc.on_abort_requested(payload)          # what the runner does with the request
        assert unstopped == [job_id] and _row(job_id).status == "RUNNING"
        assert svc.queue.heartbeat(RUNNER, [job_id]) == []          # still attended, still heartbeated
        clock.now += OVERDUE_STOP_WAIT_SECONDS + 1
        closed = await sweeper.sweep_once()
        for task in list(svc._stop_retries.values()):
            task.cancel()
        return closed

    assert asyncio.run(scenario()) == [job_id]
    row = _row(job_id)
    assert row.status == "FAILED" and "closed without confirmation" in row.current_step
    assert any("could not be stopped" in line for line in svc.queue.read_log(job_id)["lines"])
    assert "MAY STILL BE RUNNING" in svc.queue.read_log(job_id)["lines"][-1]
    assert svc.queue.acquire_lock(TF, waiter, RUNNER) == (False, job_id)       # held for the container grace
    assert svc.queue.heartbeat(RUNNER, [job_id]) == [job_id]                    # fenced: the runner lets go


class _Gate:
    name = "container running"

    def __init__(self):
        self.started, self.gate = asyncio.Event(), asyncio.Event()

    async def run(self, engine, context):
        self.started.set()
        await self.gate.wait()
        return StageResult(True, "ok")


def test_a_running_pipeline_stopped_for_the_limit_ends_failed_with_the_reason(iac_service, monkeypatch, tmp_path):
    svc = iac_service
    eng = svc.engine

    async def ok(*a, **k):
        return True

    async def nothing(*a, **k):
        return None

    eng.execute_git_sync = ok
    for name in ("_generate_inventory_and_snapshot", "emit_iac_inventory_updated", "emit_monitoring_inventory_sync",
                 "publish_maintenance", "_publish_snapshots"):
        setattr(eng, name, nothing)
    monkeypatch.setenv("PLUGIN_IAC_ORCHESTRATOR_INTERNAL_LOGS_DIR", str(tmp_path))
    monkeypatch.setenv("PLUGIN_IAC_ORCHESTRATOR_INTERNAL_GIT_REPOS_DIR", str(tmp_path / "git"))
    stage = None

    class _Strategy:
        sync_infra_engine = commit_state = organizes_tasks = False

        def db_tag(self, payload):
            return "connectivity"

        def build_stages(self, engine, payload, context):
            return [stage]

    monkeypatch.setattr(engine_mod, "get_pipeline_strategy", lambda ptype: _Strategy())
    job_id = svc.queue.enqueue({"pipeline_type": "connectivity"})
    svc.db.claims[job_id] = svc.queue.claim_next(RUNNER, svc.incarnation).token

    async def scenario():
        nonlocal stage
        stage = _Gate()
        task = asyncio.ensure_future(eng.run_pipeline({"pipeline_type": "connectivity"}, job_id=job_id))
        await stage.started.wait()
        eng.request_abort(job_id, status="FAILED", reason=REQUEST["reason"])
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())
    row = _row(job_id)
    assert row.status == "FAILED" and row.current_step == REQUEST["reason"]
    assert eng._stop_verdict(job_id) == ("ABORTED", None)          # nothing left behind for a later job


def test_a_plain_abort_still_ends_aborted(iac_service):
    svc = iac_service
    job_id, token = _claimed(svc)
    svc.db.claims[job_id] = token
    svc._unsettled.add(job_id)

    async def stop(jid):
        return 0

    svc.engine.stop_job_containers = stop
    assert asyncio.run(svc.on_abort_requested({"job_ids": [job_id], "final_status": "SUCCESS", "reason": "x"})) == []
    row = _row(job_id)
    assert row.status == "ABORTED" and row.current_step == "Aborted by User"


# --- the setting -----------------------------------------------------------------------------------------

def test_the_limit_setting_is_validated_and_never_stops_jobs_after_seconds(monkeypatch):
    vault = {}
    ctx = SimpleNamespace(get_secret=vault.get, set_secret=lambda k, v: vault.__setitem__(k, v) or True, log=MagicMock())
    mgr = OrchestratorSettings(ctx, SimpleNamespace(state={}))
    for bad in ("30", "599", "-1", "2592001", "soon"):
        with pytest.raises(SettingsValidationError):
            mgr.save_values({"job_max_runtime_seconds": bad})
    assert "iac_job_max_runtime_seconds" not in vault
    assert mgr.save_values({"job_max_runtime_seconds": "0"})["saved"] == ["job_max_runtime_seconds"]
    assert mgr.save_values({"job_max_runtime_seconds": "600"})["saved"] == ["job_max_runtime_seconds"]

    config = IaCConfig(ctx)
    assert config.job_max_runtime_seconds == 600
    vault.clear()
    assert config.job_max_runtime_seconds == SIX_HOURS             # the default: 6 h
    vault["iac_job_max_runtime_seconds"] = "0"
    assert config.job_max_runtime_seconds == 0
    vault["iac_job_max_runtime_seconds"] = "45"                    # written past the API: raised, not honoured
    assert config.job_max_runtime_seconds == 600
    monkeypatch.setenv("PLUGIN_IAC_ORCHESTRATOR_JOB_MAX_RUNTIME_SECONDS", "7200")
    assert config.job_max_runtime_seconds == 7200                  # env > Vault


def test_a_requeued_job_starts_its_clock_again(iac_service):
    svc = iac_service
    job_id, token = _claimed(svc, running_for=SIX_HOURS + 60)
    assert svc.queue.release(job_id, token, "Queued — waiting")
    assert _row(job_id).claimed_utc is None
    svc.queue.claim_next(RUNNER, "core-a:plugin-1")
    runtime = [r for r in svc.queue.sweep_candidates() if r["id"] == job_id][0]["runtime"]
    assert 0 <= runtime < 60
    # … while an adoption by the restarted runner keeps it: the job goes on.
    _set(job_id, claimed_utc=_ago(1000))
    assert svc.queue.adopt(job_id, RUNNER, "core-b:plugin-2", _row(job_id).claim_token)
    assert [r for r in svc.queue.sweep_candidates() if r["id"] == job_id][0]["runtime"] >= 990   # SQLite clock: whole seconds
