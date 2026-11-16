"""Review C2: a held lock is evidence of a container, and a closed job's lock is
not free before its runner had the chance to stop that container.

The runner snapshot (``runners``) the orphan sweep used to rely on alone is
written fire-and-forget: it can be stale or lost exactly when the database
flaps — the moment the sweep looks at the job.
"""
import asyncio
import json
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

from sqlalchemy import delete, update

from iac_plugin.app.controller import queue as queue_mod
from iac_plugin.app.controller.orphan_sweep import WAITING_STEP, OrphanSweeper
from iac_plugin.app.controller.service import IaCService
from iac_plugin.app.model.models import IaCJob

ME, POD = "docker-auto-runner", "k3s-pod-1"
TF = "tf:onprem_prod"


def _row(job_id):
    with queue_mod.db_instance.SessionLocal() as s:
        return s.get(IaCJob, job_id)


def _set(job_id, **values):
    with queue_mod.db_instance.SessionLocal() as s:
        s.execute(update(IaCJob).where(IaCJob.id == job_id).values(**values))
        s.commit()


def _age(job_id, seconds):
    _set(job_id, heartbeat_utc=datetime.utcnow() - timedelta(seconds=seconds))


def _claimed(queue, incarnation="core-a:plugin-1", host="h1"):
    job_id = queue.enqueue({"pipeline_type": "state_check", "host_name": host})
    queue.claim_next(ME, incarnation)
    return job_id


def _sweeper(svc):
    ctx = SimpleNamespace(instance_id=POD, notify=MagicMock(),
                          cluster_instances=[{"instance_id": POD, "alive": True, "roles": ["api"]},
                                             {"instance_id": ME, "alive": False, "roles": ["runner"]}])
    cfg = SimpleNamespace(orphan_sweep_enabled=True, orphan_sweep_grace_seconds=90,
                          orphan_sweep_container_grace_seconds=1800)
    return OrphanSweeper(ctx, svc.queue, svc.db, cfg)


# --- sweep ---------------------------------------------------------------------------------------

def test_sweep_spares_a_lock_holder_whose_snapshot_is_stale(iac_service):
    """The review's repro R2, asserting the correct outcome: the write that would
    have reported the new tofu container was lost, the runner is cut off for
    longer than the short grace — and its tofu is still applying."""
    svc = iac_service
    a = _claimed(svc.queue, host="h1")
    assert svc.queue.acquire_lock(TF, a, ME)[0]
    _set(a, runners=json.dumps({"Ansible: x": {"status": "success", "job_id": a}}))
    _age(a, 120)
    b = _claimed(svc.queue, host="h2")
    sweeper = _sweeper(svc)

    assert asyncio.run(sweeper.sweep_once()) == []
    assert _row(a).status == "RUNNING" and _row(a).current_step == WAITING_STEP
    assert svc.queue.acquire_lock(TF, b, ME) == (False, a)                # no second tofu on that state

    _age(a, 1900)                                                          # the container grace is over
    assert asyncio.run(sweeper.sweep_once()) == [a]
    assert svc.queue.acquire_lock(TF, b, ME) == (True, b)                  # … and so is the lock's


def test_a_rules_lock_counts_like_a_state_lock_and_no_lock_keeps_the_short_grace(iac_service):
    svc = iac_service
    with_lock, without = _claimed(svc.queue, host="h1"), _claimed(svc.queue, host="h2")
    assert svc.queue.acquire_lock("rules", with_lock, ME)[0]
    _age(with_lock, 120)
    _age(without, 120)
    rows = {r["id"]: r for r in svc.queue.sweep_candidates()}
    assert rows[with_lock]["holds_lock"] and rows[with_lock]["has_container"]
    assert not rows[without]["holds_lock"] and not rows[without]["has_container"]
    assert asyncio.run(_sweeper(svc).sweep_once()) == [without]


# --- lock takeover ---------------------------------------------------------------------------------

def test_lock_of_a_job_closed_from_outside_waits_for_the_grace(sqlite_queue):
    """An operator's abort for a runner that looks dead, ``mark_failed``: the row
    is closed at once, the runner learns it with its next heartbeat at best."""
    q = sqlite_queue
    a, b = _claimed(q, host="h1"), _claimed(q, host="h2")
    assert q.acquire_lock(TF, a, ME)[0]
    _set(a, status="ABORTED", end_time=datetime.now())
    assert q.acquire_lock(TF, b, ME) == (False, a)
    _age(a, q.lock_takeover_grace - 60)
    assert q.acquire_lock(TF, b, ME) == (False, a)
    _age(a, q.lock_takeover_grace + 1)
    assert q.acquire_lock(TF, b, ME) == (True, b)


def test_takeover_without_a_heartbeat_ages_the_end_time_and_a_deleted_holder_is_free(sqlite_queue):
    q = sqlite_queue
    a, b, c = _claimed(q, host="h1"), _claimed(q, host="h2"), _claimed(q, host="h3")
    assert q.acquire_lock(TF, a, ME)[0] and q.acquire_lock("tf:onprem_dev", c, ME)[0]
    _set(a, status="FAILED", heartbeat_utc=None, end_time=datetime.now())
    assert q.acquire_lock(TF, b, ME) == (False, a)
    _set(a, end_time=datetime.now() - timedelta(seconds=q.lock_takeover_grace + 1))
    assert q.acquire_lock(TF, b, ME) == (True, b)
    with queue_mod.db_instance.SessionLocal() as s:                         # job history was deleted
        s.execute(delete(IaCJob).where(IaCJob.id == c))
        s.commit()
    assert q.acquire_lock("tf:onprem_dev", b, ME) == (True, b)


def test_grace_follows_the_sweeps_container_grace_setting(iac_service):
    svc = iac_service
    assert svc.queue._takeover_grace() == 1800.0                           # the setting's default
    svc.queue._lock_grace_cache = None
    svc.ctx.get_secret = MagicMock(side_effect=lambda key: "600" if key == "iac_orphan_sweep_container_grace_seconds" else None)
    assert svc.queue._takeover_grace() == 600.0
    svc.ctx.get_secret = MagicMock(return_value="90")
    assert svc.queue._takeover_grace() == 600.0                            # cached: not one Vault read per poll
    a, b = _claimed(svc.queue, host="h1"), _claimed(svc.queue, host="h2")
    assert svc.queue.acquire_lock(TF, a, ME)[0]
    _set(a, status="FAILED")
    _age(a, 601)
    assert svc.queue.acquire_lock(TF, b, ME) == (True, b)


# --- positive knowledge: the runner stopped / followed the containers --------------------------------

def test_stopping_the_containers_frees_the_locks_at_once(iac_service):
    svc = iac_service
    a, b = _claimed(svc.queue, host="h1"), _claimed(svc.queue, host="h2")
    assert svc.queue.acquire_lock(TF, a, ME)[0] and svc.queue.acquire_lock("rules", a, ME)[0]
    _set(a, status="FAILED")                                                # closed by the sweep; we are fenced

    async def stop(job_id):
        return 1

    svc.engine.stop_job_containers = stop
    assert asyncio.run(svc._on_fenced(a)) is None
    assert svc.queue.acquire_lock(TF, b, ME) == (True, b) and svc.queue.lock_holder("rules") is None


def test_a_failed_stop_keeps_the_locks(iac_service):
    from iac_plugin.app.controller.engine import ContainerStateUnknown

    svc = iac_service
    svc.CONTAINER_STOP_RETRY_ATTEMPTS = 0
    svc.engine._notify = MagicMock()
    a, b = _claimed(svc.queue, host="h1"), _claimed(svc.queue, host="h2")
    assert svc.queue.acquire_lock(TF, a, ME)[0]
    _set(a, status="FAILED")

    async def stop(job_id):
        raise ContainerStateUnknown("Cannot connect to the Docker daemon")

    svc.engine.stop_job_containers = stop

    async def scenario():
        await svc._on_fenced(a)
        await asyncio.gather(*svc._stop_retries.values(), return_exceptions=True)

    asyncio.run(scenario())
    assert svc.queue.acquire_lock(TF, b, ME) == (False, a)


def _recovered(svc, monkeypatch, exit_code):
    """Successor incarnation that re-attaches to one tofu container of job ``a``."""
    a, b = _claimed(svc.queue, "core-a:previous", host="h1"), _claimed(svc.queue, "core-b:other", host="h2")
    _set(a, exec_state=json.dumps({"stage": 3, "stages": 3, "stage_name": "tf", "last": True}))
    assert svc.queue.acquire_lock(TF, a, ME)[0]
    real_sleep = asyncio.sleep

    async def fast_sleep(seconds, *args, **kwargs):
        await real_sleep(0 if seconds >= 1 else seconds)

    monkeypatch.setattr(asyncio, "sleep", fast_sleep)
    new = IaCService(svc.ctx)
    new._runtime_paths_resolved = True
    new.engine._notify = MagicMock()
    new.engine.end_override_for = lambda job_id: True

    async def listing(job_id=None):
        return [{"id": "c1", "name": "iac-tf-x", "state": "running", "job_id": a,
                 "task_name": "Terraform Apply: onprem_prod", "kind": "terraform"}]

    async def watch(name, task_name, job_id, **kwargs):
        return exit_code == 0, {"exit_code": exit_code}

    new.engine._list_job_containers = listing
    new.engine._watch_detached_runner = watch

    async def scenario():
        await new.run_startup_reconciliation()
        for _ in range(300):
            if _row(a).status != "RUNNING":
                break
            await real_sleep(0.01)
        await real_sleep(0.05)

    asyncio.run(scenario())
    return a, b


def test_recovery_frees_the_lock_when_every_container_was_followed_to_its_exit(iac_service, monkeypatch):
    a, b = _recovered(iac_service, monkeypatch, exit_code=0)
    assert _row(a).status == "SUCCESS"
    assert iac_service.queue.acquire_lock(TF, b, ME) == (True, b)


def test_recovery_keeps_the_lock_when_it_lost_track_of_a_container(iac_service, monkeypatch):
    a, b = _recovered(iac_service, monkeypatch, exit_code=None)
    assert _row(a).status == "FAILED"
    assert iac_service.queue.acquire_lock(TF, b, ME) == (False, a)
