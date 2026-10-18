"""Cluster job queue (plugin ≥ 1.4): enqueue on any instance, atomic claim by a
runner, heartbeat/orphan handling, runner snapshots and the shared log store.

Runs against an in-memory SQLite engine patched into the core's db_instance.
    PYTHONPATH=/app pytest -q tests/test_queue.py
"""
import asyncio
from datetime import datetime, timedelta

import pytest
from sqlalchemy import create_engine, update
from sqlalchemy.orm import sessionmaker

from iac_plugin.app.controller import queue as queue_mod
from iac_plugin.app.controller.queue import (
    DuplicateJob, JobQueue, JobLogWriter, JobWorker, dedupe_key_for, normalize_payload, _conflicts,
)
from iac_plugin.app.model.models import Base, IaCJob


@pytest.fixture
def q(monkeypatch):
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False},
                           poolclass=__import__("sqlalchemy.pool", fromlist=["StaticPool"]).StaticPool)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    monkeypatch.setattr(queue_mod.db_instance, "is_connected", True, raising=False)
    monkeypatch.setattr(queue_mod.db_instance, "SessionLocal", Session, raising=False)
    monkeypatch.setattr(queue_mod.db_instance, "engine", engine, raising=False)
    monkeypatch.setattr(queue_mod.db_instance, "advisory_lock", None, raising=False)
    jq = JobQueue()
    jq.ensure_schema()  # no-op on a fresh schema, must not raise
    return jq


# --- payload normalisation / keys ----------------------------------------------

def test_normalize_and_keys():
    p = normalize_payload({"object_kind": "push", "project": {"name": "aac-x"}, "ref": "refs/heads/main"})
    assert p["pipeline_type"] == "single_service" and p["service_name"] == "aac-x"
    assert dedupe_key_for(p) == "single_service:aac-x"
    assert dedupe_key_for(normalize_payload({"pipeline_type": "terraform_provision", "host_name": "H1"})) == "host_provision:h1"
    assert dedupe_key_for({"pipeline_type": "rollout"}) == "rollout:all"
    assert dedupe_key_for({"pipeline_type": "rollout", "limit": "docker-dev"}) == "rollout:docker-dev"
    assert dedupe_key_for({"pipeline_type": "infra_apply"}) == "infra"
    assert dedupe_key_for({"pipeline_type": "connectivity"}) is None


def test_rollout_conflict_rules():
    assert _conflicts("rollout:all", {"rollout:docker-dev"})
    assert _conflicts("rollout:docker-dev", {"rollout:all"})
    assert not _conflicts("rollout:docker-dev", {"rollout:other"})
    assert _conflicts("single_service:a", {"single_service:a"})
    assert not _conflicts("single_service:a", {"single_service:b"})


# --- enqueue / claim ------------------------------------------------------------

def test_enqueue_claim_and_duplicate(q):
    jid = q.enqueue({"pipeline_type": "single_service", "service_name": "Svc-A", "manual": True}, requested_by="ui")
    assert jid > 0
    with pytest.raises(DuplicateJob) as exc:
        q.enqueue({"pipeline_type": "single_service", "service_name": "svc-a"})
    assert exc.value.existing_id == jid

    claimed = q.claim_next("runner-1")
    assert claimed is not None
    cid, payload = claimed
    assert cid == jid and payload["service_name"] == "svc-a" and payload["manual"] is True
    assert q.claim_next("runner-2") is None  # nothing left

    with queue_mod.db_instance.SessionLocal() as s:
        row = s.get(IaCJob, jid)
        assert row.status == "RUNNING" and row.owner_instance == "runner-1"
        assert row.pipeline_type.startswith("single_service")
        assert row.heartbeat_at is not None

    # still a duplicate while RUNNING, free again once it finished
    with pytest.raises(DuplicateJob):
        q.enqueue({"pipeline_type": "single_service", "service_name": "svc-a"})
    with queue_mod.db_instance.SessionLocal() as s:
        s.execute(update(IaCJob).where(IaCJob.id == jid).values(status="SUCCESS"))
        s.commit()
    assert q.enqueue({"pipeline_type": "single_service", "service_name": "svc-a"}) > jid


def test_claim_is_fifo_and_exclusive(q):
    a = q.enqueue({"pipeline_type": "connectivity"})
    b = q.enqueue({"pipeline_type": "connectivity"})
    assert q.claim_next("r1")[0] == a
    assert q.claim_next("r2")[0] == b
    assert q.claim_next("r1") is None


def test_abort_queued_and_running_summary(q):
    a = q.enqueue({"pipeline_type": "connectivity"})
    b = q.enqueue({"pipeline_type": "connectivity"})
    q.claim_next("r1")  # a is RUNNING
    assert q.abort_queued() == [b]
    running = q.running_jobs()
    assert [r["id"] for r in running] == [a] and running[0]["owner_instance"] == "r1"


# --- heartbeat / orphans --------------------------------------------------------

def test_heartbeat_and_orphan_sweep(q):
    a = q.enqueue({"pipeline_type": "connectivity"})
    b = q.enqueue({"pipeline_type": "connectivity"})
    q.claim_next("r1")
    q.claim_next("r2")
    # r2 is dead (not alive) → b fails; a keeps running
    assert q.fail_orphans({"r1"}) == [b]
    # stale heartbeat without instance knowledge → fails too
    with queue_mod.db_instance.SessionLocal() as s:
        s.execute(update(IaCJob).where(IaCJob.id == a).values(heartbeat_at=datetime.now() - timedelta(minutes=10)))
        s.commit()
    q.heartbeat("r1", [a])  # fresh heartbeat protects it
    assert q.fail_orphans(None, stale_seconds=180) == []
    with queue_mod.db_instance.SessionLocal() as s:
        s.execute(update(IaCJob).where(IaCJob.id == a).values(heartbeat_at=datetime.now() - timedelta(minutes=10)))
        s.commit()
    assert q.fail_orphans(None, stale_seconds=180) == [a]


# --- runner snapshots -----------------------------------------------------------

def test_runner_snapshot_feeds_live_summary(q):
    a = q.enqueue({"pipeline_type": "connectivity"})
    q.claim_next("r1")
    q.set_runners(a, {"docker-dev": {"status": "running_ansible", "logs": ["l1", "l2"], "job_id": a}})
    live = q.live_summary()
    assert live["is_running"] is True and live["queued"] == 0
    assert live["active_tasks"]["docker-dev"]["logs"] == ["l1", "l2"]
    assert q.runners_for(a)["docker-dev"]["status"] == "running_ansible"


# --- log store ------------------------------------------------------------------

def test_log_store_tail_offset_grep_and_iter(q):
    a = q.enqueue({"pipeline_type": "connectivity"})
    seq = q.append_log(a, [f"line {i}" for i in range(10)], 0)
    assert seq == 10 and q.has_log(a)
    tail = q.read_log(a, tail=3)
    assert tail["lines"] == ["line 7", "line 8", "line 9"] and tail["offset"] == 10 and tail["size"] == 10
    more = q.read_log(a, offset=tail["offset"])
    assert more["lines"] == []
    q.append_log(a, ["line 10 ERROR", "line 11"], seq)
    inc = q.read_log(a, offset=tail["offset"])
    assert inc["lines"] == ["line 10 ERROR", "line 11"] and inc["offset"] == 12
    assert q.read_log(a, grep="error")["lines"] == ["line 10 ERROR"]
    assert list(q.iter_log(a, chunk=5)) == [f"line {i}" for i in range(10)] + ["line 10 ERROR", "line 11"]
    q.delete_logs([a])
    assert not q.has_log(a)


def test_log_writer_batches_and_flushes_on_close(q):
    a = q.enqueue({"pipeline_type": "connectivity"})

    async def run():
        w = JobLogWriter(q, a, interval=0.05)
        w.start(lambda coro, name=None: asyncio.get_running_loop().create_task(coro))
        for i in range(5):
            w.append(f"w{i}")
        await asyncio.sleep(0.15)
        w.append("late")
        await w.close()

    asyncio.run(run())
    assert q.read_log(a, tail=100)["lines"] == ["w0", "w1", "w2", "w3", "w4", "late"]


# --- worker -----------------------------------------------------------------------

class _Ctx:
    instance_id = "runner-x"
    cluster_instances = [{"instance_id": "runner-x", "roles": ["api", "runner"], "alive": True, "is_self": True}]

    def __init__(self):
        import logging
        self.log = logging.getLogger("test")

    def create_task(self, coro, name=None):
        return asyncio.get_running_loop().create_task(coro)


def test_worker_claims_and_runs_jobs(q):
    ran = []

    async def run_job(payload, job_id):
        ran.append((job_id, payload["pipeline_type"]))
        with queue_mod.db_instance.SessionLocal() as s:
            s.execute(update(IaCJob).where(IaCJob.id == job_id).values(status="SUCCESS"))
            s.commit()

    a = q.enqueue({"pipeline_type": "connectivity"})
    b = q.enqueue({"pipeline_type": "state_check", "host_name": "h1"})

    async def scenario():
        w = JobWorker(_Ctx(), q, run_job, max_parallel=1, poll_seconds=0.05, heartbeat_seconds=0.05)
        w.start()
        for _ in range(60):
            await asyncio.sleep(0.05)
            if len(ran) == 2 and not w.running_ids:
                break
        await w.stop()
        await asyncio.sleep(0.05)

    asyncio.run(scenario())
    assert [j for j, _ in ran] == [a, b]
    with queue_mod.db_instance.SessionLocal() as s:
        assert {s.get(IaCJob, a).status, s.get(IaCJob, b).status} == {"SUCCESS"}
        assert s.get(IaCJob, a).owner_instance == "runner-x"


# --- orphan sweep: never own jobs, finalize hook first --------------------------

def test_orphan_candidates_skip_own_instance_and_mark_failed(q):
    a = q.enqueue({"pipeline_type": "connectivity"})
    b = q.enqueue({"pipeline_type": "connectivity"})
    q.claim_next("me")
    q.claim_next("other")
    # "other" is not alive; "me" is excluded by self_instance even though not in the alive set
    cands = q.orphan_candidates({"someone-else"}, 180, self_instance="me")
    assert [c["id"] for c in cands] == [b]
    assert q.mark_failed(b, "Runner 'other' lost") is True
    assert q.mark_failed(b, "again") is False  # no longer RUNNING
    with queue_mod.db_instance.SessionLocal() as s:
        assert s.get(IaCJob, a).status == "RUNNING" and s.get(IaCJob, b).status == "FAILED"


def test_worker_sweep_uses_finalize_hook_before_failing(q):
    a = q.enqueue({"pipeline_type": "connectivity"})
    b = q.enqueue({"pipeline_type": "connectivity"})
    q.claim_next("dead-1")
    q.claim_next("dead-2")
    finalized = []

    async def finalize(job_id, owner):
        finalized.append((job_id, owner))
        if job_id == a:  # pretend a's containers were found and closed
            with queue_mod.db_instance.SessionLocal() as s:
                s.execute(update(IaCJob).where(IaCJob.id == a).values(status="SUCCESS"))
                s.commit()
            return True
        return False

    async def noop(payload, job_id):
        return None

    async def scenario():
        w = JobWorker(_Ctx(), q, noop, finalize_orphan=finalize)
        return await w.sweep_orphans()

    failed = asyncio.run(scenario())
    assert failed == [b] and sorted(finalized) == sorted([(a, "dead-1"), (b, "dead-2")])
    with queue_mod.db_instance.SessionLocal() as s:
        assert s.get(IaCJob, a).status == "SUCCESS" and s.get(IaCJob, b).status == "FAILED"


def test_alive_set_untrusted_until_it_lists_this_instance(q):
    class _Ctx2(_Ctx):
        cluster_instances = [{"instance_id": "someone", "roles": ["api"], "alive": True, "is_self": False}]

    async def noop(payload, job_id):
        return None

    assert JobWorker(_Ctx2(), q, noop)._alive_instances() is None
    assert JobWorker(_Ctx(), q, noop)._alive_instances() == {"runner-x"}


# --- maintenance windows (1.6): SCHEDULED, park/promote, coalesce, overrides ------

from iac_plugin.app.controller.queue import from_queue_clock, to_queue_clock  # noqa: E402
from iac_plugin.app.model.database import JobDatabase  # noqa: E402

WEBHOOK = {"pipeline_type": "single_service", "service_name": "svc-a", "service_branch": "main", "trigger": "gitlab-ci"}


def _row(jid):
    with queue_mod.db_instance.SessionLocal() as s:
        return s.get(IaCJob, jid)


def test_clock_helpers_roundtrip():
    from datetime import timezone
    aware = datetime(2026, 10, 25, 2, 0, tzinfo=timezone.utc)
    naive = to_queue_clock(aware)
    assert naive.tzinfo is None and from_queue_clock(naive) == aware
    assert from_queue_clock(None) is None and to_queue_clock(naive) is naive


def test_park_releases_claim_and_due_claim_takes_it_again(q):
    jid = q.enqueue(WEBHOOK)
    assert q.claim_next("r1")[0] == jid
    future = datetime.now() + timedelta(days=3)
    assert q.park(jid, not_before=future, window_ci_ids=["u-s", "u-h"], reason="window later", instance_id="r1")
    row = _row(jid)
    assert row.status == "SCHEDULED" and row.owner_instance is None and row.claimed_at is None
    assert row.heartbeat_at is None and row.progress == 0 and row.current_step == "Scheduled"
    assert row.schedule_reason == "window later" and "u-s" in row.window_ci_ids
    assert q.park(jid, not_before=future, window_ci_ids=[], reason="again") is False  # not RUNNING any more
    assert q.claim_next("r2") is None                       # not due
    assert q.scheduled_count() == 1 and q.queued_count() == 0
    assert q.orphan_candidates(set(), 0) == []              # no owner → never an orphan
    sched = q.job_schedule(jid)
    assert sched["status"] == "SCHEDULED" and sched["window_ci_ids"] == ["u-s", "u-h"]
    assert sched["not_before"] == from_queue_clock(to_queue_clock(from_queue_clock(future)))

    # make it due → the claim takes it, not_before stays on the row
    with queue_mod.db_instance.SessionLocal() as s:
        s.execute(update(IaCJob).where(IaCJob.id == jid).values(not_before=datetime.now() - timedelta(minutes=1)))
        s.commit()
    claimed = q.claim_next("r2")
    assert claimed and claimed[0] == jid and claimed[1]["service_name"] == "svc-a"
    row = _row(jid)
    assert row.status == "RUNNING" and row.owner_instance == "r2" and row.not_before is not None


def test_due_scheduled_comes_before_plain_queue(q):
    q.enqueue({"pipeline_type": "connectivity"})
    b = q.enqueue(WEBHOOK)
    q.claim_next("r1")  # a RUNNING
    q.claim_next("r1")  # b RUNNING
    assert q.park(b, not_before=datetime.now() - timedelta(minutes=5), window_ci_ids=None, reason="due")
    c = q.enqueue({"pipeline_type": "state_check", "host_name": "h1"})
    assert q.claim_next("r1")[0] == b   # due scheduled first
    assert q.claim_next("r1")[0] == c


def test_promote_runs_now_with_force(q):
    jid = q.enqueue(WEBHOOK)
    q.claim_next("r1")
    q.park(jid, not_before=datetime.now() + timedelta(days=1), window_ci_ids=["u"], reason="later")
    before = _row(jid).start_time
    assert q.promote(jid, override=True) is True
    row = _row(jid)
    assert row.status == "QUEUED" and row.not_before is None and row.start_time >= before
    assert q.promote(jid) is False  # no longer SCHEDULED
    claimed = q.claim_next("r1")
    assert claimed[0] == jid and claimed[1]["force"] is True


def test_newer_webhook_is_coalesced_into_scheduled_job(q):
    jid = q.enqueue(WEBHOOK)
    q.claim_next("r1")
    q.park(jid, not_before=datetime.now() + timedelta(days=1), window_ci_ids=None, reason="later")
    with queue_mod.db_instance.SessionLocal() as s:
        s.execute(update(IaCJob).where(IaCJob.id == jid).values(start_time=datetime.now() - timedelta(days=2)))
        s.commit()
    res = q.enqueue_ex({**WEBHOOK, "service_branch": "release-2"}, requested_by="webhook")
    assert res.job_id == jid and res.coalesced and not res.promoted
    row = _row(jid)
    assert row.status == "SCHEDULED" and row.not_before is not None
    assert "release-2" in row.payload and row.start_time >= datetime.now() - timedelta(minutes=1)
    assert q.scheduled_count() == 1  # still exactly one job for the service


def test_ui_deploy_promotes_scheduled_job(q):
    jid = q.enqueue(WEBHOOK)
    q.claim_next("r1")
    q.park(jid, not_before=datetime.now() + timedelta(days=1), window_ci_ids=None, reason="later")
    res = q.enqueue_ex({"pipeline_type": "single_service", "service_name": "svc-a", "manual": True, "trigger": "ui"},
                       requested_by="ui")
    assert res.job_id == jid and res.promoted and not res.coalesced
    row = _row(jid)
    assert row.status == "QUEUED" and row.not_before is None and '"force": true' in row.payload
    # QUEUED now → a further trigger is a duplicate again (first wins, as before)
    with pytest.raises(DuplicateJob):
        q.enqueue(WEBHOOK)


def test_abort_queued_keeps_scheduled_unless_asked(q):
    a = q.enqueue(WEBHOOK)
    q.claim_next("r1")
    q.park(a, not_before=datetime.now() + timedelta(days=1), window_ci_ids=None, reason="later")
    b = q.enqueue({"pipeline_type": "connectivity"})
    assert q.abort_queued() == [b]
    assert _row(a).status == "SCHEDULED"
    assert q.abort_queued(include_scheduled=True) == [a]
    assert _row(a).status == "ABORTED" and _row(a).not_before is None


def test_abort_scheduled_and_listing(q):
    a = q.enqueue(WEBHOOK)
    q.claim_next("r1")
    nb = datetime.now() + timedelta(days=1)
    q.park(a, not_before=nb, window_ci_ids=["u-s"], reason="later")
    rows = q.scheduled_jobs()
    assert len(rows) == 1 and rows[0]["id"] == a and rows[0]["service"] == "svc-a" and rows[0]["branch"] == "main"
    assert rows[0]["trigger"] == "gitlab-ci" and rows[0]["window_ci_ids"] == ["u-s"]
    assert rows[0]["not_before"].tzinfo is not None and rows[0]["schedule_reason"] == "later"
    assert q.live_summary()["scheduled"] == 1
    assert q.abort_scheduled(a) is True and q.abort_scheduled(a) is False
    assert _row(a).status == "ABORTED" and q.scheduled_jobs() == []


def test_clear_all_jobs_keeps_scheduled_and_running(q):
    a = q.enqueue(WEBHOOK)
    q.claim_next("r1")
    q.park(a, not_before=datetime.now() + timedelta(days=1), window_ci_ids=None, reason="later")
    b = q.enqueue({"pipeline_type": "connectivity"})
    q.claim_next("r1")  # b RUNNING
    c = q.enqueue({"pipeline_type": "state_check", "host_name": "h1"})
    with queue_mod.db_instance.SessionLocal() as s:
        s.execute(update(IaCJob).where(IaCJob.id == c).values(status="SUCCESS"))
        s.commit()
    assert JobDatabase().clear_all_jobs(keep_running=True) == 1
    assert _row(a).status == "SCHEDULED" and _row(b).status == "RUNNING" and _row(c) is None


def test_latest_job_prefers_scheduled_regardless_of_time_window(q):
    a = q.enqueue(WEBHOOK)
    q.claim_next("r1")
    q.park(a, not_before=datetime.now() + timedelta(days=1), window_ci_ids=None, reason="later")
    with queue_mod.db_instance.SessionLocal() as s:  # parked long ago
        s.execute(update(IaCJob).where(IaCJob.id == a).values(start_time=datetime.now() - timedelta(days=10)))
        s.commit()
    job = JobDatabase().get_latest_job_for_pipeline_type("single_service:svc-a", since_epoch=int(datetime.now().timestamp()))
    assert job and job["id"] == a and job["status"] == "SCHEDULED" and job["not_before"] is not None


def test_override_bookkeeping_and_terminal_hook(q):
    a = q.enqueue(WEBHOOK)
    q.claim_next("r1")
    q.set_override(a, "iac-job-%d" % a, ["u-s", "u-h"])
    assert _row(a).override_id == f"iac-job-{a}"
    live = q.running_overrides()
    assert len(live) == 1 and live[0]["override_id"] == f"iac-job-{a}" and live[0]["window_ci_ids"] == ["u-s", "u-h"]
    assert live[0]["claimed_at"].tzinfo is not None
    assert q.clear_override(a) == f"iac-job-{a}"
    assert q.clear_override(a) is None          # exactly once
    assert _row(a).override_id is None

    ended = []
    q.on_terminal = ended.append
    q.set_override(a, "iac-job-x", [])
    assert q.mark_failed(a, "Runner 'r1' lost") is True
    assert ended == [a]
    assert q.mark_failed(a, "again") is False and ended == [a]  # hook only on the real transition
