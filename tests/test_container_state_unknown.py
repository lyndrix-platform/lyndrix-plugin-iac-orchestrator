"""Review C1: "Docker did not answer" is never "there are no containers".

* the container listing raises on a non-zero exit code and on a timeout;
* a runner restart with an unreachable Docker daemon leaves the adopted jobs
  RUNNING, heartbeated and in possession of their locks — the listing is retried
  and the jobs are settled once Docker answers;
* a container stop that could not look (abort, fencing, leftovers) is reported
  and retried instead of being a silent no-op.
"""
import asyncio
import json
from unittest.mock import MagicMock

import pytest
from sqlalchemy import update

from iac_plugin.app.controller import engine as engine_mod
from iac_plugin.app.controller import queue as queue_mod
from iac_plugin.app.controller.engine import ContainerStateUnknown, DeploymentEngine
from iac_plugin.app.controller.service import IaCService
from iac_plugin.app.model.models import IaCJob

ME = "docker-auto-runner"
DOWN = "Cannot connect to the Docker daemon at unix:///var/run/docker.sock"


def _row(job_id):
    with queue_mod.db_instance.SessionLocal() as s:
        return s.get(IaCJob, job_id)


async def _until(predicate, timeout=3.0):
    for _ in range(int(timeout / 0.01)):
        if predicate():
            return True
        await asyncio.sleep(0.01)
    return False


# --- engine: the listing itself ---------------------------------------------------------------

class _Proc:
    def __init__(self, code=0, out=b"", err=b"", hang=False):
        self.returncode, self._out, self._err, self._hang, self.killed = code, out, err, hang, False

    async def communicate(self, input=None):
        if self._hang:
            await asyncio.Event().wait()
        return self._out, self._err

    def kill(self):
        self.killed = True


def _engine(monkeypatch, procs):
    """Engine whose docker calls answer with ``procs[<subcommand>]``; calls are recorded."""
    calls = []

    async def fake_exec(*cmd, **kwargs):
        calls.append(cmd[1:])
        return procs.get(cmd[1], _Proc())

    monkeypatch.setattr(engine_mod.asyncio, "create_subprocess_exec", fake_exec)
    monkeypatch.setattr(engine_mod.shutil, "which", lambda _: "/usr/bin/docker")
    eng = DeploymentEngine.__new__(DeploymentEngine)
    eng.state = {"active_tasks": {"Terraform Apply: onprem_prod": {"status": "running_terraform", "job_id": 7}}}
    eng.db = MagicMock()
    eng._sync_runners = MagicMock()
    eng.end_override_for = MagicMock()
    return eng, calls


PS = b"aaa111\tiac-tf-j7-Terraform-Apply--onprem_prod\trunning\t7\tTerraform Apply: onprem_prod\tterraform\n"


def test_listing_raises_on_a_docker_error_instead_of_returning_nothing(monkeypatch):
    eng, _calls = _engine(monkeypatch, {"ps": _Proc(1, b"", f"{DOWN}\n".encode())})
    with pytest.raises(ContainerStateUnknown) as err:
        asyncio.run(eng._list_job_containers())
    assert "exit code 1" in str(err.value) and "Cannot connect to the Docker daemon" in str(err.value)


def test_listing_raises_on_a_timeout_and_kills_the_hanging_call(monkeypatch):
    hanging = _Proc(hang=True)
    eng, _calls = _engine(monkeypatch, {"ps": hanging})
    eng._DOCKER_QUERY_TIMEOUT_SECONDS = 0.05
    with pytest.raises(ContainerStateUnknown) as err:
        asyncio.run(eng._list_job_containers(7))
    assert "did not answer" in str(err.value) and hanging.killed


def test_an_empty_answer_of_docker_still_means_no_containers(monkeypatch):
    eng, _calls = _engine(monkeypatch, {"ps": _Proc(0, b"")})
    assert asyncio.run(eng._list_job_containers()) == []
    assert asyncio.run(eng.stop_job_containers(7)) == 0


def test_stop_and_abort_surface_a_failed_listing(monkeypatch):
    eng, calls = _engine(monkeypatch, {"ps": _Proc(1, b"", DOWN.encode())})
    with pytest.raises(ContainerStateUnknown):
        asyncio.run(eng.stop_job_containers(7))
    with pytest.raises(ContainerStateUnknown):
        asyncio.run(eng.abort_job(7))
    assert [c[0] for c in calls] == ["ps", "ps"]                       # nothing was stopped or removed
    assert eng.state["active_tasks"]["Terraform Apply: onprem_prod"]["status"] == "running_terraform"
    eng.db.update_job.assert_not_called()                              # no ABORTED for a job that may still run


def test_stop_surfaces_a_failed_removal(monkeypatch):
    eng, calls = _engine(monkeypatch, {"ps": _Proc(0, PS), "rm": _Proc(1, b"", b"Error response from daemon")})
    with pytest.raises(ContainerStateUnknown) as err:
        asyncio.run(eng.stop_job_containers(7))
    assert "docker rm" in str(err.value)
    assert [c[0] for c in calls] == ["ps", "stop", "rm"]
    assert eng.state["active_tasks"]["Terraform Apply: onprem_prod"]["status"] == "running_terraform"



def test_a_removal_already_in_progress_counts_as_stopped(monkeypatch):
    """Staging 2026-11-15 (max_runtime, job #27): the abort relay and the worker's
    fencing removed the same container 12 ms apart; the loser's "removal …
    already in progress" was logged as "could not be stopped — may still be
    running" and retried. A container that is gone or going is stopped."""
    for err in (b"Error response from daemon: removal of container 2f1f5735a646 is already in progress",
                b"Error response from daemon: No such container: 2f1f5735a646"):
        eng, calls = _engine(monkeypatch, {"ps": _Proc(0, PS), "rm": _Proc(1, b"", err)})
        assert asyncio.run(eng.stop_job_containers(7)) == 1
        assert eng.state["active_tasks"]["Terraform Apply: onprem_prod"]["status"] == "aborted"


def test_a_real_removal_error_next_to_a_gone_container_still_raises(monkeypatch):
    err = (b"Error response from daemon: No such container: a\n"
           b"Error response from daemon: cannot remove container b: device or resource busy")
    eng, calls = _engine(monkeypatch, {"ps": _Proc(0, PS), "rm": _Proc(1, b"", err)})
    with pytest.raises(ContainerStateUnknown):
        asyncio.run(eng.stop_job_containers(7))


# --- runner restart while Docker does not answer -----------------------------------------------

def _successor(svc, monkeypatch):
    """A new plugin load on the same database (same instance id, new incarnation)."""
    real_sleep = asyncio.sleep

    async def fast_sleep(seconds, *a, **k):
        await real_sleep(0.01 if seconds >= 1 else seconds)

    monkeypatch.setattr(asyncio, "sleep", fast_sleep)
    new = IaCService(svc.ctx)
    new._runtime_paths_resolved = True
    new.engine._notify = MagicMock()
    new.engine.end_override_for = lambda job_id: True
    return new


def _previous_job(svc, *, lock="tf:onprem_prod"):
    job_id = svc.queue.enqueue({"pipeline_type": "connectivity"})
    svc.queue.claim_next(ME, "core-a:previous")
    with queue_mod.db_instance.SessionLocal() as s:
        s.execute(update(IaCJob).where(IaCJob.id == job_id).values(
            exec_state=json.dumps({"stage": 3, "stages": 3, "stage_name": "tf", "last": True})))
        s.commit()
    if lock:
        assert svc.queue.acquire_lock(lock, job_id, ME)[0]
    return job_id


class _Docker:
    """Scripted ``_list_job_containers``: raises while ``down``, else ``containers``."""

    def __init__(self, containers=()):
        self.down, self.containers, self.calls = True, list(containers), 0

    async def list(self, job_id=None):
        self.calls += 1
        if self.down:
            raise ContainerStateUnknown(DOWN)
        return [c for c in self.containers if job_id is None or c["job_id"] == job_id]


def _container(job_id, name="iac-tf-x", task="Terraform Apply: onprem_prod", kind="terraform"):
    return {"id": name, "name": name, "state": "running", "job_id": job_id, "task_name": task, "kind": kind}


def test_restart_with_unreachable_docker_keeps_the_job_and_its_lock(iac_service, monkeypatch):
    """The review's repro R1, asserting the correct outcome."""
    svc = iac_service
    job_id = _previous_job(svc)
    new = _successor(svc, monkeypatch)
    docker = _Docker()
    new.engine._list_job_containers = docker.list
    stopped = []

    async def stop(jid):
        stopped.append(jid)
        return 0

    new.engine.stop_job_containers = stop

    async def scenario():
        await new.run_startup_reconciliation()
        await _until(lambda: docker.calls >= 4)                  # the listing is retried in the background
        row = _row(job_id)
        other = svc.queue.enqueue({"pipeline_type": "state_check", "host_name": "h1"})
        facts = (row.status, row.current_step, row.owner_incarnation, new.attended_job_ids(),
                 svc.queue.acquire_lock("tf:onprem_prod", other, ME))
        new._reconcile_task.cancel()
        return facts

    status, step, incarnation, attended, lock = asyncio.run(scenario())
    assert status == "RUNNING" and "container state unknown" in step       # not FAILED "no active runners"
    assert incarnation == new.incarnation and new.db.claims[job_id]         # adopted: heartbeat + fencing work
    assert attended == {job_id}                                             # heartbeated, not "unattended"
    assert lock == (False, job_id)                                          # no second tofu on that state
    assert stopped == []
    assert any("container state unknown" in line for line in svc.queue.read_log(job_id)["lines"])
    new.engine._notify.assert_called_once()


def test_job_is_reattached_once_docker_answers(iac_service, monkeypatch):
    svc = iac_service
    job_id = _previous_job(svc)
    new = _successor(svc, monkeypatch)
    docker = _Docker([_container(job_id)])
    new.engine._list_job_containers = docker.list
    watched = []

    async def watch(name, task_name, jid, **kwargs):
        watched.append((name, jid))
        return True, {"exit_code": 0}

    new.engine._watch_detached_runner = watch

    async def scenario():
        await new.run_startup_reconciliation()
        await _until(lambda: docker.calls >= 3)
        during = _row(job_id).status
        docker.down = False
        await _until(lambda: _row(job_id).status != "RUNNING")
        await _until(lambda: new._reconcile_task.done())
        return during

    assert asyncio.run(scenario()) == "RUNNING"
    row = _row(job_id)
    assert row.status == "SUCCESS" and row.current_step == "Recovered after runner restart"
    assert watched == [("iac-tf-x", job_id)] and new._unsettled == set()


def test_job_without_containers_is_failed_only_after_docker_said_so(iac_service, monkeypatch):
    svc = iac_service
    job_id = _previous_job(svc, lock=None)
    new = _successor(svc, monkeypatch)
    docker = _Docker()
    new.engine._list_job_containers = docker.list

    async def scenario():
        await new.run_startup_reconciliation()
        await _until(lambda: docker.calls >= 3)
        during = _row(job_id).status
        docker.down = False
        await _until(lambda: new._reconcile_task.done())
        return during

    assert asyncio.run(scenario()) == "RUNNING"
    assert _row(job_id).status == "FAILED" and _row(job_id).current_step == "System Restart (Aborted)"
    assert new._unsettled == set() and job_id not in new.db.claims


def test_worker_does_not_close_a_job_that_waits_for_docker(iac_service, monkeypatch):
    """The adopted row carries THIS incarnation and has a log: without being
    attended the worker's unattended-claim check would close it as FAILED."""
    svc = iac_service
    job_id = _previous_job(svc)
    svc.queue.append_log(job_id, ["[Engine] --- STAGE: Terraform ---"], 0)
    new = _successor(svc, monkeypatch)
    new.engine._list_job_containers = _Docker().list

    async def scenario():
        await new.run_startup_reconciliation()
        new.start_runner(1)
        with queue_mod.db_instance.SessionLocal() as s:           # the claim is "old"
            s.execute(update(IaCJob).where(IaCJob.id == job_id).values(claimed_at=None))
            s.commit()
        await new.worker._recover_unattended()
        fenced = await asyncio.to_thread(new.queue.heartbeat, ME, sorted(new.worker._attended()))
        await new.shutdown(budget=1)
        return fenced

    assert asyncio.run(scenario()) == []                           # heartbeat accepted under the new token
    assert _row(job_id).status == "RUNNING"


def test_abort_while_docker_is_down_is_reported_and_retried(iac_service, monkeypatch):
    svc = iac_service
    job_id = _previous_job(svc)
    new = _successor(svc, monkeypatch)
    docker = _Docker([_container(job_id)])
    new.engine._list_job_containers = docker.list
    removed = []

    async def fake_docker(*args, timeout=None):
        removed.append(args)
        return 0, b"", b""

    new.engine._docker = fake_docker

    async def scenario():
        await new.run_startup_reconciliation()
        unstopped = await new.on_abort_requested({"job_ids": [job_id]})
        during = _row(job_id).status
        docker.down = False
        await _until(lambda: _row(job_id).status != "RUNNING")
        await _until(lambda: not new._stop_retries and new._reconcile_task.done())
        return unstopped, during

    unstopped, during = asyncio.run(scenario())
    assert unstopped == [job_id] and during == "RUNNING"           # no ABORTED while the container may run
    assert _row(job_id).status == "ABORTED"
    assert ("rm", "-f", "iac-tf-x") in removed                      # stopped, not re-attached
    lines = svc.queue.read_log(job_id)["lines"]
    assert any("could not be stopped" in line for line in lines) and any("were stopped" in line for line in lines)


def test_fenced_stop_is_retried_and_gives_up_loudly(iac_service, monkeypatch):
    svc = iac_service
    new = _successor(svc, monkeypatch)
    new.CONTAINER_STOP_RETRY_ATTEMPTS = 3
    job_id = svc.queue.enqueue({"pipeline_type": "connectivity"})
    docker = _Docker()
    new.engine._list_job_containers = docker.list

    async def scenario():
        await new._on_fenced(job_id)                               # what JobWorker._handle_fenced calls
        await new._on_fenced(job_id)                               # a second request does not double the retry
        await _until(lambda: not new._stop_retries)

    asyncio.run(scenario())
    assert docker.calls == 2 + 3
    new.engine._notify.assert_called_once()
    assert "may still be running" in new.engine._notify.call_args.args[1]
    assert any("Giving up" in line for line in svc.queue.read_log(job_id)["lines"])


def test_leftover_containers_of_a_closed_job_are_stopped_after_docker_recovers(iac_service, monkeypatch):
    svc = iac_service
    job_id = _previous_job(svc, lock=None)
    assert svc.queue.mark_failed(job_id, "runner_lost: docker-auto-runner", token=_row(job_id).claim_token)
    new = _successor(svc, monkeypatch)
    docker = _Docker([_container(job_id)])
    new.engine._list_job_containers = docker.list
    stops = []

    async def stop(jid):
        stops.append(jid)
        if len(stops) < 3:
            raise ContainerStateUnknown(DOWN)
        return 1

    new.engine.stop_job_containers = stop

    async def scenario():
        await new.run_startup_reconciliation()
        docker.down = False
        await _until(lambda: len(stops) >= 3 and not new._stop_retries)

    asyncio.run(scenario())
    assert stops == [job_id] * 3
    assert _row(job_id).status == "FAILED" and _row(job_id).current_step == "runner_lost: docker-auto-runner"


def test_job_failed_at_restart_leaves_no_snapshot_or_scratch_behind(iac_service, monkeypatch, tmp_path):
    """Staging 2026-11-15: jobs closed as "System Restart (Aborted)" kept
    ``.job_snapshots/job_<id>`` (inventory snapshot + the 1.7 scratch dir) on
    the runner's disk for good; every other terminal path removes it."""
    svc = iac_service
    job_id = _previous_job(svc, lock=None)
    new = _successor(svc, monkeypatch)
    docker = _Docker()
    docker.down = False                                   # Docker answers: no container for the job
    new.engine._list_job_containers = docker.list
    new.engine.base_git_dir = tmp_path
    snap = new.engine.base_git_dir / ".job_snapshots" / f"job_{job_id}"
    (snap / "scratch").mkdir(parents=True, exist_ok=True)
    (snap / "inventory_state").mkdir(parents=True, exist_ok=True)
    (snap / "scratch" / "left-by-a-playbook").write_text("x")

    asyncio.run(new.run_startup_reconciliation())

    assert _row(job_id).status == "FAILED" and _row(job_id).current_step == "System Restart (Aborted)"
    assert not snap.exists()
