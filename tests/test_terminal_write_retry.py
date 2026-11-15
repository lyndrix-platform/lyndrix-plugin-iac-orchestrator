"""Review C5: the result of a finished job survives a database blip, and the
worker's "lost track" close is bound to the claim it looked at.

Before: the final ``update_job`` of a SUCCESSFUL job failed once on error 2013
(no retry), the row stayed RUNNING with nothing executing it, and the worker's
unattended-claim check closed it as FAILED twenty seconds later — unfenced.
"""
import asyncio
import logging
import time

import pytest
from sqlalchemy.exc import OperationalError

from iac_plugin.app.controller import queue as queue_mod
from iac_plugin.app.controller.queue import JobWorker
from iac_plugin.app.model import database as database_mod
from iac_plugin.app.model.database import JobDatabase, is_connection_error
from iac_plugin.app.model.models import IaCJob

ME = "docker-auto-runner"


def _lost(code=2013, text="Lost connection to MySQL server during query"):
    return OperationalError("UPDATE", {}, Exception(f"({code}, '{text}')"))


def _row(job_id):
    with queue_mod.db_instance.SessionLocal() as s:
        return s.get(IaCJob, job_id)


class _Flaky:
    """Database sessions that fail like a broken connection. ``before``: the next
    n UPDATEs fail before anything is sent; ``lose_ack``: the next COMMIT is
    applied but the client sees an error; ``down``: every statement fails."""

    def __init__(self, monkeypatch):
        self.before, self.lose_ack, self.down, self.updates = 0, False, False, 0
        self.error = _lost()
        real = database_mod.read_committed_session

        def session():
            s = real()
            commit, execute = s.commit, s.execute

            def flaky_execute(stmt, *a, **kw):
                if self.down:
                    raise self.error
                if getattr(stmt, "is_update", False):
                    self.updates += 1
                    if self.before > 0:
                        self.before -= 1
                        raise self.error
                return execute(stmt, *a, **kw)

            def flaky_commit():
                commit()
                if self.lose_ack:
                    self.lose_ack = False
                    raise self.error

            s.execute, s.commit = flaky_execute, flaky_commit
            return s

        monkeypatch.setattr(database_mod, "read_committed_session", session)
        self.slept = []
        monkeypatch.setattr(time, "sleep", self.slept.append)


def _claimed(queue, db, incarnation="core:this"):
    job_id = queue.enqueue({"pipeline_type": "connectivity"})
    claim = queue.claim_next(ME, incarnation)
    db.claims[job_id] = claim.token
    return job_id, claim.token


@pytest.fixture
def db(sqlite_queue):
    """A JobDatabase wired to the queue the way the service does it."""
    jd = JobDatabase()
    sqlite_queue.claims = jd.claims
    sqlite_queue.unwritten_terminal = jd.unwritten_terminal
    sqlite_queue.finish_terminal = jd.finish_unwritten
    return jd


class _Ctx:
    instance_id = ME
    log = logging.getLogger("test")


def _worker(queue):
    return JobWorker(_Ctx(), queue, None, incarnation="core:this", heartbeat_seconds=0.0)


# --- which errors ------------------------------------------------------------------------------

def test_connection_errors_are_recognised_by_their_client_code():
    assert is_connection_error(_lost(2013)) and is_connection_error(_lost(2006, "MySQL server has gone away"))
    assert is_connection_error(_lost(2003, "Can't connect to MySQL server"))
    assert not is_connection_error(_lost(1062, "Duplicate entry '2013' for key"))
    assert not is_connection_error(ValueError("job 2013"))


# --- the terminal write ----------------------------------------------------------------------------

def test_terminal_write_is_retried_after_a_lost_connection(sqlite_queue, db, monkeypatch):
    job_id, _token = _claimed(sqlite_queue, db)
    flaky = _Flaky(monkeypatch)
    flaky.before = 2
    assert db.update_job(job_id, "SUCCESS") is True
    assert _row(job_id).status == "SUCCESS" and _row(job_id).progress == 100
    assert flaky.slept == [0.5, 1.0] and db.unwritten_terminal == {}          # short, growing pauses


def test_a_commit_whose_ack_was_lost_counts_as_written(sqlite_queue, db, monkeypatch):
    job_id, _token = _claimed(sqlite_queue, db)
    flaky = _Flaky(monkeypatch)
    flaky.lose_ack = True
    assert db.update_job(job_id, "SUCCESS") is True                            # not "closed elsewhere"
    assert _row(job_id).status == "SUCCESS" and flaky.updates == 2


def test_a_row_closed_elsewhere_is_still_reported_as_not_written(sqlite_queue, db, monkeypatch):
    job_id, token = _claimed(sqlite_queue, db)
    assert sqlite_queue.fail_orphan(job_id, token, "runner_lost: x", 0)
    flaky = _Flaky(monkeypatch)
    flaky.before = 1
    assert db.update_job(job_id, "SUCCESS") is False                           # the retry did not make it ours
    assert _row(job_id).status == "FAILED" and db.unwritten_terminal == {}


def test_other_errors_are_not_retried(sqlite_queue, db, monkeypatch):
    job_id, _token = _claimed(sqlite_queue, db)
    flaky = _Flaky(monkeypatch)
    flaky.error, flaky.before = OperationalError("UPDATE", {}, Exception("(1146, \"Table doesn't exist\")")), 1
    assert db.update_job(job_id, "SUCCESS") is False
    assert flaky.updates == 1 and flaky.slept == []


# --- the review's scenario: the database stays away while a successful job ends ---------------------

def test_successful_job_is_not_flipped_to_failed_after_a_database_outage(sqlite_queue, db, monkeypatch):
    q = sqlite_queue
    job_id, token = _claimed(q, db)
    q.append_log(job_id, ["[SYSTEM] Pipeline Started"], 0)                     # it ran: "has a log"
    flaky = _Flaky(monkeypatch)
    flaky.down = True
    assert db.update_job(job_id, "SUCCESS") is False                           # 5 attempts, then kept
    assert flaky.slept == [0.5, 1.0, 2.0, 4.0]
    assert db.unwritten_terminal == {job_id: ("SUCCESS", token)}
    db.claims.pop(job_id)                                                       # what JobWorker._run does next
    worker = _worker(q)

    async def recover():
        with queue_mod.db_instance.SessionLocal() as s:                        # the claim is older than the check
            s.query(IaCJob).filter(IaCJob.id == job_id).update({"claimed_at": None})
            s.commit()
        await worker._recover_unattended()

    flaky.down = False
    asyncio.run(recover())
    row = _row(job_id)
    assert row.status == "SUCCESS" and row.progress == 100                     # was: FAILED "lost track"
    assert db.unwritten_terminal == {}


def test_unwritten_result_waits_for_the_database_and_is_given_up_in_the_end(sqlite_queue, db, monkeypatch):
    q = sqlite_queue
    job_id, token = _claimed(q, db)
    flaky = _Flaky(monkeypatch)
    flaky.down = True
    assert db.update_job(job_id, "SUCCESS") is False
    db.UNWRITTEN_MAX_ATTEMPTS = 3
    assert db.finish_unwritten(job_id) is None and db.finish_unwritten(job_id) is None
    assert job_id in db.unwritten_terminal                                      # still pending
    assert db.finish_unwritten(job_id) is False and db.unwritten_terminal == {}  # bounded
    flaky.down = False
    assert _row(job_id).status == "RUNNING"                                     # → the worker's "lost track" close


def test_unwritten_result_never_overwrites_a_row_that_changed_hands(sqlite_queue, db, monkeypatch):
    q = sqlite_queue
    job_id, token = _claimed(q, db, incarnation="core:previous")
    flaky = _Flaky(monkeypatch)
    flaky.down = True
    assert db.update_job(job_id, "SUCCESS") is False
    flaky.down = False
    assert q.adopt(job_id, ME, "core:next", token)                              # a newer incarnation took it
    assert db.finish_unwritten(job_id) is False
    assert _row(job_id).status == "RUNNING" and db.unwritten_terminal == {}


# --- mark_failed is fenced ----------------------------------------------------------------------------

def test_mark_failed_is_bound_to_the_claim_it_was_decided_on(sqlite_queue):
    """The review's repro R3, asserting the correct outcome."""
    q = sqlite_queue
    job_id = q.enqueue({"pipeline_type": "connectivity"})
    old = q.claim_next(ME, "core-a:p1")
    new_token = q.adopt(job_id, ME, "core-a:p2", old.token)
    assert new_token
    # the old incarnation still runs its unattended check with its stale view
    assert q.mark_failed(job_id, "Runner lost track of the job", token=old.token) is False
    assert q.mark_failed(job_id, "Runner lost track of the job", token=None) is False
    assert _row(job_id).status == "RUNNING" and _row(job_id).claim_token == new_token
    assert q.mark_failed(job_id, "Runner lost track of the job", token=new_token) is True
    with pytest.raises(TypeError):
        q.mark_failed(job_id, "no token given")                                # there is no unfenced form


def test_unattended_check_passes_the_claim_token(sqlite_queue):
    q = sqlite_queue
    job_id = q.enqueue({"pipeline_type": "connectivity"})
    claim = q.claim_next(ME, "core:this")
    q.append_log(job_id, ["[SYSTEM] Pipeline Started"], 0)
    seen = []
    real = q.mark_failed

    def spy(jid, reason, *, token):
        seen.append((jid, token))
        return real(jid, reason, token=token)

    q.mark_failed = spy
    worker = _worker(q)

    async def recover():
        with queue_mod.db_instance.SessionLocal() as s:
            s.query(IaCJob).filter(IaCJob.id == job_id).update({"claimed_at": None})
            s.commit()
        await worker._recover_unattended()

    asyncio.run(recover())
    assert seen == [(job_id, claim.token)] and _row(job_id).status == "FAILED"
