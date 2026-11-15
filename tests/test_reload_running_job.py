"""OC-10: a plugin reload / runner restart with a job in flight.

* teardown detaches the job: no terminal status, containers untouched, the
  maintenance override stays, the log is flushed;
* only an operator's abort makes a cancellation terminal;
* the next incarnation re-attaches by label, does not store output twice, and
  closes the job from ALL its containers and the stage it had reached.
"""
import asyncio
import inspect
import json
from unittest.mock import MagicMock

import pytest
from sqlalchemy import update

from iac_plugin.app.controller import engine as engine_mod
from iac_plugin.app.controller import queue as queue_mod
from iac_plugin.app.controller.engine import DeploymentEngine
from iac_plugin.app.controller.service import IaCService
from iac_plugin.app.controller.utils import StageResult
from iac_plugin.app.model.models import IaCJob

ME = "docker-auto-runner"


def _row(job_id):
    with queue_mod.db_instance.SessionLocal() as s:
        return s.get(IaCJob, job_id)


class _Stage:
    def __init__(self, name, gate=None, started=None):
        self.name, self.gate, self.started = name, gate, started

    async def run(self, engine, context):
        if self.started is not None:
            self.started.set()
        if self.gate is not None:
            await self.gate.wait()            # "the container is running"
        return StageResult(True, "ok")


class _Strategy:
    sync_infra_engine = False
    commit_state = False
    organizes_tasks = False

    def __init__(self, stages):
        self.stages = stages

    def db_tag(self, payload):
        return "connectivity"

    def build_stages(self, engine, payload, context):
        return self.stages


def _wire(svc, monkeypatch, tmp_path, stages):
    """Real engine/worker; the shared prefix (git sync, generation) is a no-op."""
    async def ok(*a, **k):
        return True

    async def nothing(*a, **k):
        return None

    eng = svc.engine
    eng.execute_git_sync = ok
    eng._generate_inventory_and_snapshot = nothing
    eng.emit_iac_inventory_updated = nothing
    eng.emit_monitoring_inventory_sync = nothing
    eng.publish_maintenance = nothing
    eng._publish_snapshots = nothing
    monkeypatch.setenv("PLUGIN_IAC_ORCHESTRATOR_INTERNAL_LOGS_DIR", str(tmp_path / "logs"))
    monkeypatch.setenv("PLUGIN_IAC_ORCHESTRATOR_INTERNAL_GIT_REPOS_DIR", str(tmp_path / "git"))
    monkeypatch.setattr(engine_mod, "get_pipeline_strategy", lambda ptype: _Strategy(stages))


async def _until(predicate, timeout=3.0):
    for _ in range(int(timeout / 0.01)):
        if predicate():
            return True
        await asyncio.sleep(0.01)
    return False


def _start_job(svc, started):
    job_id = svc.queue.enqueue({"pipeline_type": "connectivity"})
    svc.start_runner(1)
    svc.worker.poll_seconds = 0.02
    return job_id


# --- teardown / detach ---------------------------------------------------------------------

def test_teardown_is_async_and_awaits_the_shutdown():
    from iac_plugin import entrypoint

    assert inspect.iscoroutinefunction(entrypoint.teardown)


def test_reload_detaches_the_job_instead_of_closing_it(iac_service, monkeypatch, tmp_path):
    svc = iac_service

    async def scenario():
        gate, started = asyncio.Event(), asyncio.Event()
        _wire(svc, monkeypatch, tmp_path, [_Stage("Clone"), _Stage("Single Service: aac-x", gate, started)])
        job_id = _start_job(svc, started)
        await asyncio.wait_for(started.wait(), 3)
        svc.queue.set_override(job_id, f"iac-job-{job_id}", ["u-1"])          # the deploy announced maintenance
        ended = []
        svc.queue.on_terminal = ended.append
        stops = []

        async def stop_containers(jid):
            stops.append(jid)

        svc.engine.stop_job_containers = stop_containers
        await svc.shutdown(reason="plugin reload", budget=3)
        return job_id, ended, stops

    job_id, ended, stops = asyncio.run(scenario())
    row = _row(job_id)
    assert row.status == "RUNNING" and row.end_time is None                    # no ABORTED, no terminal state
    assert row.current_step.startswith("Runner reloading — detached at stage 3/3 (Single Service: aac-x)")
    assert json.loads(row.exec_state) == {"stage": 3, "stages": 3, "stage_name": "Single Service: aac-x", "last": True}
    assert row.override_id == f"iac-job-{job_id}" and ended == []              # the override is still on
    assert stops == []                                                          # containers were not touched
    lines = svc.queue.read_log(job_id)["lines"]
    assert any("pipeline detached at stage 3/3" in line for line in lines)      # the log was flushed
    assert svc.worker is None and svc.db.claims == {}


def test_only_an_operator_abort_is_terminal(iac_service, monkeypatch, tmp_path):
    svc = iac_service

    async def scenario():
        gate, started = asyncio.Event(), asyncio.Event()
        _wire(svc, monkeypatch, tmp_path, [_Stage("Single Service: aac-x", gate, started)])
        job_id = _start_job(svc, started)
        await asyncio.wait_for(started.wait(), 3)
        stops = []

        async def stop_containers(jid):
            stops.append(jid)
            return 1

        svc.engine.stop_job_containers = stop_containers
        await svc.on_abort_requested({"job_ids": [job_id]})
        await svc.worker.stop()
        return job_id, stops

    job_id, stops = asyncio.run(scenario())
    assert _row(job_id).status == "ABORTED" and stops == [job_id]


# --- next incarnation -------------------------------------------------------------------------

def _successor(svc, monkeypatch):
    """A new plugin load on the same database (same instance id, new incarnation)."""
    real_sleep = asyncio.sleep

    async def fast_sleep(seconds, *a, **k):
        await real_sleep(0 if seconds >= 1 else seconds)

    monkeypatch.setattr(asyncio, "sleep", fast_sleep)
    new = IaCService(svc.ctx)
    new._runtime_paths_resolved = True
    return new


def _detached_job(svc, *, last, task="Single Service: aac-x", lines=()):
    job_id = svc.queue.enqueue({"pipeline_type": "connectivity"})
    svc.queue.claim_next(ME, "core-a:previous")
    with queue_mod.db_instance.SessionLocal() as s:
        s.execute(update(IaCJob).where(IaCJob.id == job_id).values(
            override_id=f"iac-job-{job_id}", window_ci_ids="[]",
            exec_state=json.dumps({"stage": 3 if last else 2, "stages": 3, "stage_name": task, "last": last})))
        s.commit()
    if lines:
        svc.queue.append_log(job_id, list(lines), 0)
    return job_id


def _container(job_id, task="Single Service: aac-x", name="aac-runner-x", kind="ansible"):
    return {"id": name, "name": name, "state": "running", "job_id": job_id, "task_name": task, "kind": kind}


def _recover(new, containers, results, monkeypatch):
    """Run the successor's reconciliation with scripted container watchers."""
    seen = {}

    async def list_containers(job_id=None):
        return containers

    async def watch(name, task_name, job_id, **kwargs):
        seen[name] = kwargs
        ok, gate = results[name]
        if gate is not None:
            await gate.wait()
        return ok, {"exit_code": 0 if ok else 2}

    new.engine._list_job_containers = list_containers
    new.engine._watch_detached_runner = watch
    new.engine._notify = MagicMock()
    ended = []
    new.queue.on_terminal = None
    new.engine.end_override_for = lambda job_id: ended.append(job_id) or True
    return seen, ended


def test_last_stage_container_ok_closes_the_job_as_success_without_duplicate_lines(iac_service, monkeypatch):
    task = "Single Service: aac-x"
    job_id = _detached_job(iac_service, last=True, lines=[
        "[Engine] --- STAGE: Single Service ---", f"[{task}] PLAY [all] ***", f"[{task}] TASK [deploy] ***",
        f"[SYSTEM] [{task}] [WARNING] a remark of the engine about this task"])
    new = _successor(iac_service, monkeypatch)
    seen, ended = _recover(new, [_container(job_id)], {"aac-runner-x": (True, None)}, monkeypatch)

    async def scenario():
        await new.run_startup_reconciliation()
        # The close (status, locks, claim, override) is one thread call: wait for its end.
        await _until(lambda: _row(job_id).status != "RUNNING" and ended and not new.db.claims)

    asyncio.run(scenario())
    row = _row(job_id)
    assert row.status == "SUCCESS" and row.current_step == "Recovered after runner restart" and row.progress == 100
    assert seen["aac-runner-x"]["skip_lines"] == 2            # the two stored output lines, not the engine's remark
    assert seen["aac-runner-x"]["kind"] == "ansible"
    assert ended == [job_id]                                   # the override ends exactly once
    assert row.owner_incarnation == new.incarnation and new.db.claims == {}


def test_green_container_in_a_middle_stage_is_not_a_green_pipeline(iac_service, monkeypatch):
    """host_provision restarted during the compliance bootstrap: the bootstrap
    container exits 0, but service deploy, rollout hand-off and rules never ran."""
    job_id = _detached_job(iac_service, last=False, task="Compliance Bootstrap: docker-new")
    new = _successor(iac_service, monkeypatch)
    _seen, ended = _recover(new, [_container(job_id, task="Compliance Bootstrap: docker-new")],
                            {"aac-runner-x": (True, None)}, monkeypatch)

    async def scenario():
        await new.run_startup_reconciliation()
        await _until(lambda: _row(job_id).status != "RUNNING" and ended)   # the whole close, see above

    asyncio.run(scenario())
    row = _row(job_id)
    assert row.status == "FAILED" and ended == [job_id]
    assert row.current_step.startswith("Interrupted by a runner restart at stage 2/3 (Compliance Bootstrap: docker-new)")
    assert "re-trigger" in row.current_step


def test_job_is_closed_from_all_containers_not_from_the_first(iac_service, monkeypatch):
    job_id = _detached_job(iac_service, last=True)
    new = _successor(iac_service, monkeypatch)

    async def scenario():
        slow = asyncio.Event()
        containers = [_container(job_id, task="aac-a", name="aac-runner-a"), _container(job_id, task="aac-b", name="aac-runner-b")]
        _recover(new, containers, {"aac-runner-a": (False, None), "aac-runner-b": (True, slow)}, monkeypatch)
        await new.run_startup_reconciliation()
        await asyncio.sleep(0.05)
        during = _row(job_id).status              # the failing container is done, the other one still runs
        attended = new.engine.attended_job_ids()
        slow.set()
        await _until(lambda: _row(job_id).status != "RUNNING")
        return during, attended

    during, attended = asyncio.run(scenario())
    assert during == "RUNNING" and attended == {job_id}
    row = _row(job_id)
    assert row.status == "FAILED" and row.current_step == "Recovered after runner restart (failed)"


def test_containers_of_a_job_closed_elsewhere_are_stopped(iac_service, monkeypatch):
    """rD §3.4: the sweep failed the job while the runner was away — the returning
    runner stops the container instead of letting it finish (or rewriting the row)."""
    job_id = _detached_job(iac_service, last=True)
    assert iac_service.queue.mark_failed(job_id, "runner_lost: docker-auto-runner", token=_row(job_id).claim_token)
    new = _successor(iac_service, monkeypatch)
    seen, _ended = _recover(new, [_container(job_id)], {"aac-runner-x": (True, None)}, monkeypatch)
    stopped = []

    async def stop(jid):
        stopped.append(jid)

    new.engine.stop_job_containers = stop
    asyncio.run(new.run_startup_reconciliation())
    assert stopped == [job_id] and seen == {}
    assert _row(job_id).status == "FAILED" and _row(job_id).current_step == "runner_lost: docker-auto-runner"


def test_job_without_stage_info_keeps_the_old_exit_code_rule(iac_service, monkeypatch):
    job_id = _detached_job(iac_service, last=True)
    with queue_mod.db_instance.SessionLocal() as s:           # a row written by plugin <= 1.6.1
        s.execute(update(IaCJob).where(IaCJob.id == job_id).values(exec_state=None))
        s.commit()
    new = _successor(iac_service, monkeypatch)
    _recover(new, [_container(job_id)], {"aac-runner-x": (True, None)}, monkeypatch)

    async def scenario():
        await new.run_startup_reconciliation()
        await _until(lambda: _row(job_id).status != "RUNNING")

    asyncio.run(scenario())
    assert _row(job_id).status == "SUCCESS"


# --- the watcher itself: skipped lines still count for the verdict ---------------------------------

def test_watcher_skips_stored_lines_but_parses_them(monkeypatch):
    lines = ["TASK [deploy] ***", "PLAY RECAP ***", "h1 : ok=3 changed=1 unreachable=0 failed=0", "late line"]

    class _Stream:
        def __init__(self):
            self._lines = [(line + "\n").encode() for line in lines]

        async def readline(self):
            return self._lines.pop(0) if self._lines else b""

    class _Proc:
        def __init__(self, out=b""):
            self.stdout, self._out = _Stream(), out

        async def communicate(self):
            return self._out, b""

    async def fake_exec(*cmd, **kw):
        return _Proc(b"0" if cmd[:2] == ("docker", "wait") else b"")

    monkeypatch.setattr(engine_mod.asyncio, "create_subprocess_exec", fake_exec)
    eng = DeploymentEngine.__new__(DeploymentEngine)
    eng.state = {}
    eng.db = MagicMock()
    eng._log_line = MagicMock()
    ok, stats = asyncio.run(eng._watch_detached_runner("aac-runner-x", "x", 5, kind="ansible", skip_lines=3))
    assert ok and stats["successful_hosts"] == 1              # the recap was among the skipped lines
    assert [c.args[1] for c in eng._log_line.call_args_list] == ["[x] late line"]
    eng.db.update_progress.assert_not_called()                # no progress replay for old lines
