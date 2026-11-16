"""Review C7: the small robustness items of the queue.

* the job-log writer keeps lines the database did not take, retries them, and
  marks the place when its bounded buffer overflowed — it used to drop every
  batch that hit a database error, silently;
* log lines are stamped and aged on the database clock in UTC;
* ``enqueue_ex`` retries a deadlock / lock wait / snapshot conflict instead of
  losing an accepted trigger.
"""
import asyncio
from datetime import datetime, timedelta

import pytest
from sqlalchemy import literal, update
from sqlalchemy.exc import OperationalError

from iac_plugin.app.controller import queue as queue_mod
from iac_plugin.app.controller.queue import DuplicateJob, JobLogWriter, JobQueue
from iac_plugin.app.model.models import IaCJob, IaCJobLog


@pytest.fixture
def q(sqlite_queue):
    return sqlite_queue


def _spawn(coro, name=None):
    return asyncio.get_running_loop().create_task(coro)


def _lines(q, job_id):
    return q.read_log(job_id, tail=100000)["lines"]


class _Outage:
    """``append_log`` that fails like a database error while ``down``."""

    def __init__(self, q):
        self.q, self.down, self.calls, self.real = q, False, 0, q.append_log
        q.append_log = self

    def __call__(self, job_id, lines, start_seq):
        self.calls += 1
        if self.down:
            return start_seq                    # what append_log returns after a rollback
        return self.real(job_id, lines, start_seq)


# --- job-log writer ---------------------------------------------------------------------------

def test_lines_survive_a_database_outage_in_order(q):
    job_id = q.enqueue({"pipeline_type": "connectivity"})
    outage = _Outage(q)

    async def run():
        w = JobLogWriter(q, job_id, interval=0.01)
        w.MAX_RETRY_PAUSE = 0.02
        w.start(_spawn)
        w.append("before")
        while w.pending:
            await asyncio.sleep(0.01)
        outage.down = True
        for n in range(5):
            w.append(f"during-{n}")
        calls = outage.calls
        while outage.calls < calls + 3:                     # tried more than once …
            await asyncio.sleep(0.01)
        assert w.pending == 5                               # … and kept
        outage.down = False
        w.append("after")
        await w.close()
        return w

    w = asyncio.run(run())
    assert _lines(q, job_id) == ["before", *[f"during-{n}" for n in range(5)], "after"]
    assert w.lost_total == 0


def test_a_full_buffer_drops_the_oldest_lines_and_marks_the_gap(q):
    job_id = q.enqueue({"pipeline_type": "connectivity"})
    outage = _Outage(q)

    async def run():
        w = JobLogWriter(q, job_id, interval=0.01)
        w.MAX_BUFFERED_LINES, w.MAX_RETRY_PAUSE = 10, 0.02
        w.start(_spawn)
        w.append("stored")
        while w.pending:                                    # in the database before the outage starts
            await asyncio.sleep(0.01)
        outage.down = True
        for n in range(25):
            w.append(f"line-{n}")
        await asyncio.sleep(0.05)
        assert w.pending <= 10                              # bounded while the database is away
        outage.down = False
        await w.close()
        return w

    w = asyncio.run(run())
    stored = _lines(q, job_id)
    assert stored[0] == "stored"
    assert stored[1].startswith("[SYSTEM] [WARNING] 15 log line(s) are missing here")
    assert f"job_{job_id}.log" in stored[1]
    assert stored[2:] == [f"line-{n}" for n in range(15, 25)]    # the newest ten, in order
    assert w.lost_total == 15


def test_a_batch_whose_commit_ack_was_lost_is_not_written_twice(q):
    job_id = q.enqueue({"pipeline_type": "connectivity"})
    real = q.append_log
    state = {"lose_ack": True}

    def lossy(job_id, lines, start_seq):
        seq = real(job_id, lines, start_seq)
        if state["lose_ack"]:
            state["lose_ack"] = False
            return start_seq                    # applied on the server, error on the client
        return seq

    q.append_log = lossy

    async def run():
        w = JobLogWriter(q, job_id, interval=0.01)
        w.start(_spawn)
        w.append("one")
        w.append("two")
        await asyncio.sleep(0.05)
        w.append("three")
        await w.close()

    asyncio.run(run())
    assert _lines(q, job_id) == ["one", "two", "three"]


def test_close_is_bounded_and_the_rest_is_stored_late(q, caplog):
    job_id = q.enqueue({"pipeline_type": "connectivity"})
    outage = _Outage(q)
    outage.down = True

    async def run():
        w = JobLogWriter(q, job_id, interval=0.01)
        w.CLOSE_RETRY_PAUSE, w.LATE_FLUSH_SECONDS, w.MAX_RETRY_PAUSE = 0.01, 0.05, 0.02
        w.start(_spawn)
        w.append("result line")
        started = asyncio.get_running_loop().time()
        await w.close()                                     # the job's end does not wait for the database
        assert asyncio.get_running_loop().time() - started < 1.0
        assert w.pending == 1 and _lines(q, job_id) == []
        outage.down = False
        await asyncio.wait_for(w._late_task, timeout=2)

    with caplog.at_level("ERROR"):
        asyncio.run(run())
    assert _lines(q, job_id) == ["result line"]
    assert any("could not be stored when the job ended" in r.message for r in caplog.records)


# --- log age on the UTC database clock ------------------------------------------------------------

def test_log_lines_are_stamped_and_aged_on_the_utc_database_clock(q, monkeypatch):
    """Not ``NOW()`` (the database's local time, which jumps with DST): the
    stamp and the "now" of the sweep come from the same UTC clock."""
    db_utc = datetime(2031, 3, 30, 1, 30, 0)                # any moment — never the host's clock
    monkeypatch.setattr(JobQueue, "_utc_now", staticmethod(lambda session: literal(db_utc)))
    job_id = q.enqueue({"pipeline_type": "connectivity"})
    q.claim_next("runner-x", "core:1")
    q.append_log(job_id, ["TASK [x]"], 0)
    with queue_mod.db_instance.SessionLocal() as s:
        assert s.query(IaCJobLog).filter_by(job_id=job_id).one().ts.replace(tzinfo=None) == db_utc
        s.execute(update(IaCJobLog).where(IaCJobLog.job_id == job_id).values(ts=db_utc - timedelta(seconds=300)))
        s.commit()
    row = [r for r in q.sweep_candidates() if r["id"] == job_id][0]
    assert row["log_age"] == 300


# --- enqueue retry ---------------------------------------------------------------------------------

def _db_error(code, text="Deadlock found when trying to get lock; try restarting transaction"):
    return OperationalError("INSERT", {}, Exception(code, text))


def _failing_insert(monkeypatch, errors):
    real = JobQueue._insert
    calls = []

    def insert(session, payload, key, requested_by, step):
        calls.append(dict(payload))
        if errors:
            raise errors.pop(0)
        return real(session, payload, key, requested_by, step)

    monkeypatch.setattr(JobQueue, "_insert", staticmethod(insert))
    monkeypatch.setattr(queue_mod.time, "sleep", lambda s: None)
    return calls


@pytest.mark.parametrize("code", [1213, 1205, 1020])
def test_enqueue_retries_a_retryable_database_error(q, monkeypatch, code):
    calls = _failing_insert(monkeypatch, [_db_error(code), _db_error(code)])
    res = q.enqueue_ex({"pipeline_type": "single_service", "service_name": "aac-traefik"})
    assert res.job_id > 0 and len(calls) == 3
    with queue_mod.db_instance.SessionLocal() as s:
        assert s.query(IaCJob).count() == 1                 # exactly one job, not one per attempt


def test_enqueue_gives_up_after_bounded_attempts_and_never_retries_other_errors(q, monkeypatch):
    calls = _failing_insert(monkeypatch, [_db_error(1213)] * 10)
    with pytest.raises(OperationalError):
        q.enqueue_ex({"pipeline_type": "connectivity"})
    assert len(calls) == 4

    calls = _failing_insert(monkeypatch, [_db_error(1062, "Duplicate entry '1213' for key 'PRIMARY'")])
    with pytest.raises(OperationalError):
        q.enqueue_ex({"pipeline_type": "connectivity"})
    assert len(calls) == 1                                   # "1213" in the text is not the error code

    calls = _failing_insert(monkeypatch, [RuntimeError("job #1213 looks odd")])
    with pytest.raises(RuntimeError):
        q.enqueue_ex({"pipeline_type": "connectivity"})
    assert len(calls) == 1


def test_a_retried_enqueue_decides_again_and_does_not_carry_fields_over(q, monkeypatch):
    """The follow-up decision of the failed attempt (``followup_of``) must not
    leak into the next one when the target is free by then."""
    running = q.enqueue({"pipeline_type": "single_service", "service_name": "aac-traefik"})
    token = q.claim_next("runner-x", "core:1").token
    real = JobQueue._insert
    seen = []

    def insert(session, payload, key, requested_by, step):
        seen.append(payload.get("followup_of"))
        if len(seen) == 1:
            session.rollback()
            q.claims[running] = token
            with queue_mod.db_instance.SessionLocal() as s:  # the running job ends meanwhile
                s.execute(update(IaCJob).where(IaCJob.id == running).values(status="SUCCESS"))
                s.commit()
            raise _db_error(1213)
        return real(session, payload, key, requested_by, step)

    monkeypatch.setattr(JobQueue, "_insert", staticmethod(insert))
    monkeypatch.setattr(queue_mod.time, "sleep", lambda s: None)
    res = q.enqueue_ex({"pipeline_type": "single_service", "service_name": "aac-traefik"})
    assert seen == [running, None] and res.waiting_for is None


def test_a_duplicate_is_reported_not_retried(q, monkeypatch):
    q.enqueue({"pipeline_type": "single_service", "service_name": "aac-traefik"})
    monkeypatch.setattr(queue_mod.time, "sleep", lambda s: pytest.fail("a DuplicateJob must not be retried"))
    with pytest.raises(DuplicateJob):
        q.enqueue_ex({"pipeline_type": "single_service", "service_name": "aac-traefik", "dry_run": True})
