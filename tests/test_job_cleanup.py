"""Job cleanup (Clear Failed Jobs / Clear Statistics) must delete the child rows in
iac_orchestrator_job_logs and _job_tasks first — MariaDB enforces the foreign keys
(error 1451), so SQLite runs with PRAGMA foreign_keys=ON here.
    PYTHONPATH=/app pytest -q tests/test_job_cleanup.py
"""
import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from iac_plugin.app.model import database as db_mod
from iac_plugin.app.model.database import JobDatabase
from iac_plugin.app.model.models import Base, IaCJob, IaCJobLog, IaCJobTask


@pytest.fixture
def db(monkeypatch):
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)

    @event.listens_for(engine, "connect")
    def _fk_on(dbapi_conn, _record):
        dbapi_conn.execute("PRAGMA foreign_keys=ON")

    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    monkeypatch.setattr(db_mod.db_instance, "is_connected", True, raising=False)
    monkeypatch.setattr(db_mod.db_instance, "SessionLocal", Session, raising=False)
    monkeypatch.setattr(db_mod, "_DELETE_BATCH", 2)  # exercise the batching
    return Session


def _job(Session, status: str) -> int:
    s = Session()
    job = IaCJob(pipeline_type="single_service", status=status)
    s.add(job)
    s.flush()
    s.add(IaCJobLog(job_id=job.id, seq=0, line="x"))
    s.add(IaCJobTask(job_id=job.id, task_num=1, task_name="t", task_label="T"))
    s.commit()
    jid = job.id
    s.close()
    return jid


def _statuses(Session) -> list:
    s = Session()
    try:
        return sorted(j.status for j in s.query(IaCJob).all())
    finally:
        s.close()


def test_clear_failed_jobs_removes_logs_and_tasks(db):
    for status in ("FAILED", "ERROR", "ABORTED", "SUCCESS", "RUNNING"):
        _job(db, status)
    assert JobDatabase().clear_failed_jobs() == 3
    assert _statuses(db) == ["RUNNING", "SUCCESS"]
    s = db()
    try:
        assert s.query(IaCJobLog).count() == 2
        assert s.query(IaCJobTask).count() == 2
    finally:
        s.close()


def test_clear_failed_jobs_nothing_to_do(db):
    _job(db, "SUCCESS")
    assert JobDatabase().clear_failed_jobs() == 0


def test_clear_all_jobs_keeps_running(db):
    for status in ("FAILED", "SUCCESS", "SUCCESS", "RUNNING"):
        _job(db, status)
    assert JobDatabase().clear_all_jobs(keep_running=True) == 3
    assert _statuses(db) == ["RUNNING"]
