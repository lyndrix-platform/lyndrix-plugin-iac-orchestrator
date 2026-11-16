"""Leftovers of the abort endpoint and of teardown.

* ``POST /abort`` ended with ``state["active_tasks"] = {}`` on the instance that
  served it. On the runner that also forgot the tasks of jobs whose containers
  could NOT be stopped — the tasks are what keeps a re-attached job attended
  and heartbeated.
* ``teardown`` stopped the module-global service: on a core that runs the async
  teardown as a task, a late teardown of the previous load shut down the
  service the next load had just created.
"""
import asyncio
from unittest.mock import MagicMock

from iac_plugin import entrypoint
from iac_plugin.app.controller import api as api_mod
from iac_plugin.app.controller import queue as queue_mod
from iac_plugin.app.controller.engine import ContainerStateUnknown
from iac_plugin.app.model.models import IaCJob

ME = "docker-auto-runner"


def _row(job_id):
    with queue_mod.db_instance.SessionLocal() as s:
        return s.get(IaCJob, job_id)


def _reattached(svc, name, host):
    """A job this runner re-attached to: no worker task, a live runner task."""
    job_id = svc.queue.enqueue({"pipeline_type": "state_check", "host_name": host})
    svc.db.claims[job_id] = svc.queue.claim_next(ME, svc.incarnation).token
    svc.state["active_tasks"][name] = {"status": "running_terraform", "logs": [], "job_id": job_id}
    return job_id


def test_forget_stopped_tasks_keeps_everything_else():
    state = {"running_jobs": 0, "active_tasks": {
        "stopped": {"status": "aborted", "job_id": 1},
        "docker down": {"status": "running_terraform", "job_id": 2},
        "started meanwhile": {"status": "running_ansible", "job_id": 3},
        "legacy": {"status": "success", "job_id": 0},
        "broken": "not a task",
    }}
    tasks = state["active_tasks"]
    api_mod._forget_stopped_tasks(state, [1])
    assert sorted(state["active_tasks"]) == ["docker down", "started meanwhile"]
    assert state["active_tasks"] is tasks and state["is_running"] is True      # the engine keeps its dict
    api_mod._forget_stopped_tasks(state, [2, 3])
    assert state["active_tasks"] == {} and state["is_running"] is False


def test_abort_on_the_runner_keeps_jobs_attended_whose_containers_were_not_stopped(iac_service, monkeypatch):
    svc = iac_service
    svc.ctx.cluster_instances = [{"instance_id": ME, "alive": True, "roles": ["api", "runner"]}]
    stoppable = _reattached(svc, "Terraform Plan: dev", "h1")
    stuck = _reattached(svc, "Terraform Apply: prod", "h2")

    async def stop(job_id):
        if job_id == stuck:
            raise ContainerStateUnknown("'docker ps' did not answer within 20s")
        for task in svc.state["active_tasks"].values():
            if task.get("job_id") == job_id:
                task["status"] = "aborted"
        return 1

    svc.engine.stop_job_containers = stop
    svc._docker_retry_delay = lambda attempt: 3600                 # the background retry stays asleep
    monkeypatch.setattr(api_mod, "_ctx", svc.ctx)
    monkeypatch.setattr(api_mod, "_engine", svc.engine)
    monkeypatch.setattr(api_mod, "_service", svc)

    async def scenario():
        result = await api_mod.do_abort()
        attended = svc.attended_job_ids()
        fenced = await asyncio.to_thread(svc.queue.heartbeat, ME, sorted(attended))
        for task in list(svc._stop_retries.values()):
            task.cancel()
        return result, attended, fenced

    result, attended, fenced = asyncio.run(scenario())
    assert result["aborted_jobs"] == [stoppable] and result["unstopped_jobs"] == [stuck]
    assert _row(stoppable).status == "ABORTED" and _row(stuck).status == "RUNNING"
    assert attended == {stuck} and fenced == []                    # still attended and heartbeated
    assert list(svc.state["active_tasks"]) == ["Terraform Apply: prod"]
    assert svc.state["is_running"] is True


# --- teardown belongs to its own load ------------------------------------------------------------

class _Service:
    def __init__(self, name):
        self.name, self.stopped = name, []

    async def shutdown(self, reason="", budget=0):
        self.stopped.append(reason)


def _loads(monkeypatch):
    """Two plugin loads as setup() registers them: the old one, then its successor."""
    old_ctx, new_ctx = MagicMock(name="old ctx"), MagicMock(name="new ctx")
    old, new = _Service("old"), _Service("new")
    monkeypatch.setattr(entrypoint, "_services", {id(old_ctx): old, id(new_ctx): new})
    monkeypatch.setattr(entrypoint, "_service", new)               # the successor's setup() ran already
    monkeypatch.setattr(entrypoint, "_mount_tokens", {})
    monkeypatch.setattr(entrypoint, "_mount_token", None)
    monkeypatch.setattr(entrypoint, "shutdown_pool", MagicMock())
    return old_ctx, new_ctx, old, new


def test_a_late_teardown_stops_its_own_service_not_the_successors(monkeypatch):
    old_ctx, new_ctx, old, new = _loads(monkeypatch)
    asyncio.run(entrypoint.teardown(old_ctx))
    assert old.stopped == ["plugin reload"] and new.stopped == []
    assert entrypoint._service is new                              # the new load keeps running
    entrypoint.shutdown_pool.assert_not_called()                   # … and keeps its thread pool
    asyncio.run(entrypoint.teardown(new_ctx))
    assert new.stopped == ["plugin reload"] and entrypoint._service is None
    entrypoint.shutdown_pool.assert_called_once()
    assert entrypoint._services == {}


def test_a_teardown_with_an_unknown_context_takes_the_oldest_load(monkeypatch):
    _old_ctx, _new_ctx, old, new = _loads(monkeypatch)
    asyncio.run(entrypoint.teardown(MagicMock(name="another ctx object")))
    assert old.stopped == ["plugin reload"] and new.stopped == []
    assert entrypoint._service is new
