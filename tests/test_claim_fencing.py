"""OC-03 + OC-04: a claimed job is either executing or released; startup
reconciliation never touches a job of the current incarnation; every state
write is conditional on the expected state and the claim token (fencing)."""
import asyncio
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

from sqlalchemy import update
from sqlalchemy.exc import OperationalError

from iac_plugin.app.controller import queue as queue_mod
from iac_plugin.app.controller.engine import DeploymentEngine
from iac_plugin.app.controller.queue import JobQueue, JobWorker
from iac_plugin.app.model.models import IaCJob

ME = "docker-auto-runner"
LOST = OperationalError("COMMIT", {}, Exception("(2013, 'Lost connection to MySQL server during query')"))


def _row(job_id):
    with queue_mod.db_instance.SessionLocal() as s:
        return s.get(IaCJob, job_id)


class FlakyQueue(JobQueue):
    """``lose_ack``: the claim's COMMIT is applied but the client sees an error.
    ``fail_update``: the claim's UPDATE fails before anything is committed.
    ``blind``: the follow-up check cannot tell either (connection still broken)."""
    lose_ack = fail_update = blind = False

    def _session(self):
        s = super()._session()
        q, commit, execute = self, s.commit, s.execute

        def flaky_commit():
            commit()
            if q.lose_ack:
                q.lose_ack = False
                raise LOST

        def flaky_execute(stmt, *a, **kw):
            if q.fail_update and getattr(stmt, "is_update", False):
                q.fail_update = False
                raise LOST
            return execute(stmt, *a, **kw)

        s.commit, s.execute = flaky_commit, flaky_execute
        return s

    def _claim_is_mine(self, job_id, token):
        return False if self.blind else super()._claim_is_mine(job_id, token)


class _Ctx:
    instance_id = ME
    cluster_instances = [{"instance_id": ME, "roles": ["api", "runner"], "alive": True, "is_self": True}]
    log = logging.getLogger("test")

    def create_task(self, coro, name=None):
        return asyncio.get_running_loop().create_task(coro)


def _worker(q, run_job, **kw):
    kw.setdefault("poll_seconds", 0.02)
    kw.setdefault("heartbeat_seconds", 0.02)
    return JobWorker(_Ctx(), q, run_job, incarnation="core:this", **kw)


async def _drive(worker, until, timeout=3.0):
    worker.start()
    try:
        for _ in range(int(timeout / 0.02)):
            await asyncio.sleep(0.02)
            if until():
                return
    finally:
        await worker.stop()
        await asyncio.sleep(0.03)


# --- OC-03: claim atomicity -------------------------------------------------------------

def test_claim_survives_a_lost_commit_ack(sqlite_queue):
    q = FlakyQueue()
    job_id = q.enqueue({"pipeline_type": "single_service", "service_name": "svc-a"})
    q.lose_ack = True
    claim = q.claim_next(ME, "core:this")
    assert claim is not None and claim[0] == job_id and claim[1]["service_name"] == "svc-a"
    row = _row(job_id)
    assert row.status == "RUNNING" and row.claim_token == claim.token and row.owner_incarnation == "core:this"


def test_job_runs_exactly_once_when_the_ack_is_lost(sqlite_queue):
    q = FlakyQueue()
    job_id = q.enqueue({"pipeline_type": "single_service", "service_name": "svc-a"})
    q.lose_ack = True
    ran = []

    async def run_job(payload, jid):
        ran.append(jid)
        with queue_mod.db_instance.SessionLocal() as s:
            s.execute(update(IaCJob).where(IaCJob.id == jid).values(status="SUCCESS"))
            s.commit()

    w = _worker(q, run_job)
    asyncio.run(_drive(w, lambda: _row(job_id).status == "SUCCESS"))
    assert ran == [job_id] and _row(job_id).status == "SUCCESS"


def test_error_before_the_commit_leaves_the_job_claimable(sqlite_queue):
    q = FlakyQueue()
    job_id = q.enqueue({"pipeline_type": "connectivity"})
    q.fail_update = True
    assert q.claim_next(ME, "core:this") is None
    assert _row(job_id).status == "QUEUED" and _row(job_id).owner_instance is None
    assert q.claim_next(ME, "core:this")[0] == job_id


def test_unattended_claim_is_requeued_and_then_executed_once(sqlite_queue):
    """Worst case: the UPDATE landed, the client saw an error AND the re-check
    could not tell. The row is RUNNING/ours with nobody executing it — before, it
    stayed like that until the runner restarted and blocked its target."""
    q = FlakyQueue()
    job_id = q.enqueue({"pipeline_type": "single_service", "service_name": "svc-a"})
    q.lose_ack = q.blind = True
    ran = []

    async def run_job(payload, jid):
        ran.append(jid)
        with queue_mod.db_instance.SessionLocal() as s:
            s.execute(update(IaCJob).where(IaCJob.id == jid).values(status="SUCCESS"))
            s.commit()

    w = _worker(q, run_job)
    asyncio.run(_drive(w, lambda: _row(job_id).status == "SUCCESS"))
    assert ran == [job_id]


def test_unattended_claim_with_output_is_failed_not_rerun(sqlite_queue):
    q = sqlite_queue
    job_id = q.enqueue({"pipeline_type": "connectivity"})
    q.claim_next(ME, "core:this")
    q.append_log(job_id, ["[SYSTEM] Pipeline Started"], 0)
    ran = []

    async def run_job(payload, jid):
        ran.append(jid)

    w = _worker(q, run_job)
    asyncio.run(_recover(w))
    row = _row(job_id)
    assert ran == [] and row.status == "FAILED" and "lost track" in row.current_step


async def _recover(worker):
    worker.heartbeat_seconds = 0.0
    await worker._recover_unattended()


def test_recovery_never_touches_executing_or_foreign_jobs(sqlite_queue):
    q = sqlite_queue
    mine = q.enqueue({"pipeline_type": "connectivity"})
    old = q.enqueue({"pipeline_type": "state_check", "host_name": "h1"})
    other = q.enqueue({"pipeline_type": "state_check", "host_name": "h2"})
    q.claim_next(ME, "core:this")
    q.claim_next(ME, "core:previous")
    q.claim_next("runner-2", "core:x")

    async def scenario():
        w = _worker(q, None, extra_attended=lambda: {mine})
        await _recover(w)

    asyncio.run(scenario())
    assert [_row(j).status for j in (mine, old, other)] == ["RUNNING", "RUNNING", "RUNNING"]


# --- OC-04: startup reconciliation --------------------------------------------------------

def _reconcile(svc, monkeypatch):
    real_sleep = asyncio.sleep

    async def fast_sleep(_s, *a, **k):
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", fast_sleep)
    async def no_containers(job_id=None):
        return []

    svc.engine._list_job_containers = no_containers

    async def scenario():
        await svc.run_startup_reconciliation()
        await real_sleep(0.05)

    asyncio.run(scenario())


def test_reconciliation_spares_jobs_of_the_current_incarnation(iac_service, monkeypatch):
    """p03: the new worker claimed a queued job; the reconciliation of the same
    process must not FAIL it as 'System Restart (Aborted)'."""
    svc = iac_service
    fresh = svc.queue.enqueue({"pipeline_type": "single_service", "service_name": "svc-a"})
    stale = svc.queue.enqueue({"pipeline_type": "single_service", "service_name": "svc-b"})
    foreign = svc.queue.enqueue({"pipeline_type": "single_service", "service_name": "svc-c"})
    legacy = svc.queue.enqueue({"pipeline_type": "single_service", "service_name": "svc-d"})
    svc.queue.claim_next(ME, svc.incarnation)                 # claimed by the current worker
    old_token = svc.queue.claim_next(ME, "core:previous").token
    svc.queue.claim_next("runner-2", "core:other")
    svc.queue.claim_next(ME, None)                            # row of a pre-fencing plugin version

    _reconcile(svc, monkeypatch)
    assert _row(fresh).status == "RUNNING" and _row(foreign).status == "RUNNING"
    assert _row(fresh).current_step == f"Claimed by {ME}"      # not even looked at
    for job_id in (stale, legacy):
        assert _row(job_id).status == "FAILED" and _row(job_id).current_step == "System Restart (Aborted)"
    assert _row(stale).claim_token != old_token and _row(stale).owner_incarnation == svc.incarnation
    assert svc.db.claims == {}                                # fences released again


def test_runner_boot_reconciles_before_the_worker_claims(iac_service, monkeypatch):
    svc = iac_service
    order = []

    async def reconcile():
        order.append("reconcile")

    monkeypatch.setattr(svc, "run_startup_reconciliation", reconcile)
    monkeypatch.setattr(svc, "start_runner", lambda max_parallel=None: order.append(("worker", max_parallel)))
    svc.ctx.get_secret = MagicMock(return_value="5")
    asyncio.run(svc.run_runner_boot())
    assert order == ["reconcile", ("worker", 5)]

    async def boom():
        raise RuntimeError("db down")

    order.clear()
    monkeypatch.setattr(svc, "run_startup_reconciliation", boom)
    asyncio.run(svc.run_runner_boot())
    assert order == [("worker", 5)]                           # the runner still comes up


# --- fencing ------------------------------------------------------------------------------

def test_a_job_closed_elsewhere_is_never_overwritten(iac_service):
    svc = iac_service
    job_id = svc.queue.enqueue({"pipeline_type": "connectivity"})
    claim = svc.queue.claim_next(ME, svc.incarnation)
    svc.db.claims[job_id] = claim.token
    assert svc.db.update_progress(job_id, 10, "Stage: x") is True
    # the orphan sweep (or an abort on another instance) closes the row
    assert svc.queue.mark_failed(job_id, "runner_lost: docker-auto-runner", token=_row(job_id).claim_token) is True
    # the returning runner's writes are no-ops
    assert svc.db.update_job(job_id, "SUCCESS") is False
    assert svc.db.update_progress(job_id, 100, "Completed Successfully") is False
    svc.db.update_pending_tasks(job_id, [])
    row = _row(job_id)
    assert row.status == "FAILED" and row.current_step == "runner_lost: docker-auto-runner" and row.progress == 10
    assert svc.queue.heartbeat(ME, [job_id]) == [job_id]      # … and it is told so


def test_writes_of_a_superseded_claim_are_no_ops(iac_service):
    svc = iac_service
    job_id = svc.queue.enqueue({"pipeline_type": "connectivity"})
    old = svc.queue.claim_next(ME, "core:previous").token
    new = svc.queue.adopt(job_id, ME, svc.incarnation, old)
    assert new and new != old
    assert svc.queue.adopt(job_id, ME, "core:third", old) is None       # the old token is spent
    svc.db.claims[job_id] = old                                         # the previous incarnation, still alive
    assert svc.db.update_job(job_id, "ABORTED") is False
    assert svc.queue.heartbeat(ME, [job_id]) == [job_id]
    svc.queue.set_runners(job_id, {"t": {"status": "failed"}})
    assert _row(job_id).status == "RUNNING" and _row(job_id).runners is None
    svc.db.claims[job_id] = new
    assert svc.queue.heartbeat(ME, [job_id]) == []
    assert svc.db.update_job(job_id, "SUCCESS") is True


def test_terminal_writes_need_a_running_row(iac_service):
    """Also without a claim token (API instance, legacy row): expected previous state."""
    svc = iac_service
    job_id = svc.queue.enqueue({"pipeline_type": "connectivity"})
    assert svc.db.update_job(job_id, "ABORTED") is False and _row(job_id).status == "QUEUED"
    svc.queue.claim_next(ME, svc.incarnation)
    assert svc.db.update_job(job_id, "SUCCESS") is True
    assert svc.db.update_job(job_id, "FAILED") is False and _row(job_id).status == "SUCCESS"


def test_worker_stops_a_fenced_job_and_its_containers(sqlite_queue):
    q = sqlite_queue
    job_id = q.enqueue({"pipeline_type": "connectivity"})
    stopped, cancelled = [], []

    async def run_job(payload, jid):
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            cancelled.append(jid)
            raise

    async def on_fenced(jid):
        stopped.append(jid)

    w = _worker(q, run_job, on_fenced=on_fenced)

    async def scenario():
        w.start()
        for _ in range(100):
            await asyncio.sleep(0.02)
            if w.owns(job_id):
                break
        q_other = JobQueue()                                    # another instance: no claim registered
        assert q_other.mark_failed(job_id, "runner_lost: docker-auto-runner", token=_row(job_id).claim_token)
        for _ in range(100):
            await asyncio.sleep(0.02)
            if stopped:
                break
        await w.stop()

    asyncio.run(scenario())
    assert cancelled == [job_id] and stopped == [job_id]
    assert _row(job_id).status == "FAILED" and job_id not in q.claims


def test_recovered_container_result_never_rewrites_a_closed_job(iac_service):
    """The sweep FAILED the job while its container was still running; the
    container then exits 0 — the row stays FAILED (no FAILED→SUCCESS any more)."""
    svc = iac_service
    job_id = svc.queue.enqueue({"pipeline_type": "connectivity"})
    token = svc.queue.claim_next(ME, svc.incarnation).token
    svc.db.claims[job_id] = token
    assert svc.queue.mark_failed(job_id, "runner_lost: docker-auto-runner", token=_row(job_id).claim_token)
    svc.engine._notify = MagicMock()

    async def watched(*a, **k):
        return True, {}

    svc.engine._watch_detached_runner = watched
    container = {"id": "c1", "name": "aac-runner-x", "task_name": "x", "kind": "ansible", "state": "running", "job_id": job_id}
    assert asyncio.run(svc.engine.recover_job(job_id, [container])) is None
    assert _row(job_id).status == "FAILED" and _row(job_id).current_step == "runner_lost: docker-auto-runner"
    svc.engine._notify.assert_not_called()
