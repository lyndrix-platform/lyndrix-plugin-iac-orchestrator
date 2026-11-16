import json
import logging
import re
from datetime import datetime
from core.api import db_instance
from .models import IaCJob, IaCJobLog, IaCState, IaCJobTask
from sqlalchemy import or_, select, update

log = logging.getLogger("IaC:Database")

# MariaDB ≥ 11.6 runs innodb_snapshot_isolation=ON: a REPEATABLE READ transaction that
# updates a row another transaction changed since its first read fails with error 1020
# instead of waiting. Several writers touch one job row at once (progress per Ansible
# task, the worker heartbeat, the runner snapshot) — so every session here runs READ
# COMMITTED and the short writers retry on 1020/1205/1213.
_RETRY_CODES = ("1020", "1205", "1213")


def _is_retryable(exc: Exception) -> bool:
    return any(code in str(getattr(exc, "orig", exc)) for code in _RETRY_CODES)


def read_committed_session():
    """A core session pinned to READ COMMITTED (no-op on SQLite). ``None`` when the
    core database is not connected."""
    if not db_instance.is_connected or not db_instance.SessionLocal:
        return None
    session = db_instance.SessionLocal()
    try:
        bind = session.get_bind()
        if bind is not None and getattr(bind.dialect, "name", "") in ("mysql", "mariadb"):
            session.connection(execution_options={"isolation_level": "READ COMMITTED"})
    except Exception as exc:  # isolation is an optimisation, never a reason to fail
        log.debug("read_committed_session: isolation not applied: %s", exc)
    return session


def retry_write(fn, *, attempts: int = 4, pause: float = 0.05):
    """Run ``fn()`` (a complete short transaction) again on 1020/1205/1213."""
    import time as _time

    for attempt in range(attempts):
        try:
            return fn()
        except Exception as exc:
            if attempt + 1 < attempts and _is_retryable(exc):
                _time.sleep(pause * (attempt + 1))
                continue
            raise


# Client-side connection errors of MariaDB/MySQL: cannot connect (2002/2003),
# "server has gone away" (2006), "lost connection during query" (2013) — what a
# VIP move, a pool connection the server dropped or a short network blip look
# like. Matched as the driver prints them, "(2013, '…')".
_CONNECTION_ERROR_RE = re.compile(r"\((?:2002|2003|2006|2013),")

#: Pauses before a job's terminal status is written again after a lost
#: connection (7.5 s in total — the write runs where the job ends).
TERMINAL_RETRY_PAUSES = (0.5, 1.0, 2.0, 4.0)


def is_connection_error(exc: Exception) -> bool:
    if getattr(exc, "connection_invalidated", False):
        return True
    return bool(_CONNECTION_ERROR_RE.search(str(getattr(exc, "orig", exc))))


def retry_terminal_write(tx, landed, *, pauses=TERMINAL_RETRY_PAUSES) -> bool:
    """Run ``tx()`` — one complete conditional "close the job" transaction that
    returns whether it changed the row — and try again after a lost connection
    (bounded by ``pauses``), on top of the 1020/1205/1213 retry of
    :func:`retry_write`.

    A connection that breaks during COMMIT leaves the outcome unknown: the
    server may have applied it. A later attempt then finds nothing to change —
    ``landed()`` tells whether that is because OUR write is in the row (review
    C5). Raises the last error when the database stays away.
    """
    import time as _time

    lost_connection = False
    for pause in (*pauses, None):
        try:
            done = bool(retry_write(tx))
            return done or (lost_connection and bool(landed()))
        except Exception as exc:
            if pause is None or not is_connection_error(exc):
                raise
            lost_connection = True
            log.warning("Terminal job write lost its connection (%s) — retrying in %.1fs.", exc, pause)
            _time.sleep(pause)
    return False  # not reached


_DELETE_BATCH = 100


def _delete_jobs(session, job_ids: list) -> int:
    """Delete jobs together with every child row, children first.

    MariaDB enforces the foreign keys of ``iac_orchestrator_job_logs`` and
    ``iac_orchestrator_job_tasks`` (error 1451 if the parent goes first). Jobs are
    removed in committed batches so a large log table stays well under the core's
    per-statement time cap; each batch retries on 1020/1205/1213.
    """
    deleted = 0
    for start in range(0, len(job_ids), _DELETE_BATCH):
        batch = job_ids[start:start + _DELETE_BATCH]

        def _tx(batch=batch):
            try:
                session.query(IaCJobLog).filter(IaCJobLog.job_id.in_(batch)).delete(synchronize_session=False)
                session.query(IaCJobTask).filter(IaCJobTask.job_id.in_(batch)).delete(synchronize_session=False)
                n = session.query(IaCJob).filter(IaCJob.id.in_(batch)).delete(synchronize_session=False)
                session.commit()
                return int(n or 0)
            except Exception:
                session.rollback()
                raise

        deleted += retry_write(_tx)
    return deleted


class JobDatabase:
    def __init__(self):
        # job id -> claim token of the jobs THIS process executes (shared with the
        # JobQueue by the service). While a token is registered, every write below
        # for that job is conditional on it: a job closed or taken over by someone
        # else meanwhile is never overwritten (fencing).
        self.claims: dict[int, str] = {}
        # job id -> (final status, claim token) of a claimed job whose terminal
        # write could not reach the database even after the retries (review C5).
        # The worker completes it when the database is back instead of closing
        # the finished job as FAILED "lost track" (shared with the JobQueue).
        self.unwritten_terminal: dict[int, tuple[str, str]] = {}
        self._unwritten_attempts: dict[int, int] = {}

    #: Later attempts to store an unwritten result before it is given up (the
    #: worker tries every ~20 s → about 10 minutes). After that the job is closed
    #: like any other job nothing executes: FAILED "Runner lost track of the job".
    UNWRITTEN_MAX_ATTEMPTS = 30

    def _get_session(self):
        """Safely retrieves a database session if the central engine is connected."""
        session = read_committed_session()
        if session is None:
            log.error("JobDatabase: Cannot get session, Core Database is disconnected.")
        return session

    def create_job(self, pipeline_type: str) -> int:
        """Creates a new job record and returns its ID."""
        session = self._get_session()
        if not session:
            return -1

        try:
            new_job = IaCJob(
                pipeline_type=pipeline_type,
                status="RUNNING",
                progress=0,                      # NEW
                current_step="Pending Start",    # NEW
                logs="[]",
                pending_tasks="[]" 
            )
            session.add(new_job)
            session.commit()
            session.refresh(new_job)
            return new_job.id
        except Exception as e:
            log.error(f"Failed to create job in DB: {e}")
            session.rollback()
            return -1
        finally:
            if session:
                session.close()

    def update_job(self, job_id: int, status: str) -> bool:
        """RUNNING → ``status`` (the final status of a job). Returns True when this
        call changed the row.

        Conditional on the expected previous state: only a RUNNING row is closed,
        and — when this process holds the job's claim — only while the claim token
        still matches. A job the orphan sweep failed, an abort closed or a newer
        incarnation adopted is left alone (the caller learns it through ``False``).

        A lost connection (2006/2013, …) is retried with a short bounded backoff
        (review C5): the result of a finished job must not depend on one database
        blip. If the database stays away, the status of a claimed job is kept in
        ``unwritten_terminal`` for the worker to write later.
        """
        if job_id in (None, -1):
            return False
        token = self.claims.get(job_id)
        try:
            done = retry_terminal_write(lambda: self._close_tx(job_id, status, token),
                                        lambda: self._closed_as(job_id, status, token))
            self.unwritten_terminal.pop(job_id, None)
            self._unwritten_attempts.pop(job_id, None)
            return done
        except Exception as e:
            log.error(f"Failed to update job {job_id} in DB: {e}")
            if token:
                self.unwritten_terminal[job_id] = (status, token)
            return False

    def _close_tx(self, job_id: int, status: str, token: str | None) -> bool:
        """One transaction: RUNNING (and still our claim) → ``status``."""
        session = self._get_session()
        if not session:
            return False
        try:
            cond = [IaCJob.id == job_id, IaCJob.status == "RUNNING"]
            if token:
                cond.append(IaCJob.claim_token == token)
            values: dict = {"status": status}
            if status in ("SUCCESS", "FAILED", "ERROR", "ABORTED"):
                values["end_time"] = datetime.now()
                if status == "SUCCESS":
                    values["progress"] = 100  # Snap to 100% on success
            res = session.execute(update(IaCJob).where(*cond).values(**values))
            session.commit()
            return res.rowcount == 1
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _closed_as(self, job_id: int, status: str, token: str | None) -> bool:
        """Does the row carry ``status`` under our claim — i.e. did a write whose
        acknowledgement got lost land after all?"""
        session = self._get_session()
        if not session:
            return False
        try:
            row = session.execute(select(IaCJob.status, IaCJob.claim_token).where(IaCJob.id == job_id)).first()
            return bool(row) and row[0] == status and (not token or row[1] == token)
        finally:
            session.close()

    def finish_unwritten(self, job_id: int) -> bool | None:
        """Write the terminal status a job could not store when it ended. ``True``
        written now, ``False`` nothing to do any more (the row is not RUNNING
        under that claim: the write had landed, or the job was closed elsewhere),
        ``None`` the database is still away — try again later."""
        pending = self.unwritten_terminal.get(job_id)
        if pending is None:
            return False
        status, token = pending
        try:
            done = bool(retry_write(lambda: self._close_tx(job_id, status, token)))
        except Exception as exc:
            attempts = self._unwritten_attempts.get(job_id, 0) + 1
            if attempts >= self.UNWRITTEN_MAX_ATTEMPTS:
                log.error(f"Job {job_id}: giving up writing its final status {status} after {attempts} attempts: {exc}")
                self.unwritten_terminal.pop(job_id, None)
                self._unwritten_attempts.pop(job_id, None)
                return False
            self._unwritten_attempts[job_id] = attempts
            log.warning(f"Job {job_id}: final status {status} still not written: {exc}")
            return None
        self.unwritten_terminal.pop(job_id, None)
        self._unwritten_attempts.pop(job_id, None)
        if done:
            log.warning(f"Job {job_id}: final status {status} written late (the database was unreachable when it ended).")
        return done

    def get_recent_jobs(self, limit: int = 20) -> list:
        """Fetches metadata for the UI table."""
        session = self._get_session()
        if not session:
            return []

        try:
            jobs = session.query(
                IaCJob.id,
                IaCJob.pipeline_type,
                IaCJob.start_time,
                IaCJob.end_time,
                IaCJob.status,
                IaCJob.progress,
                IaCJob.current_step,
                IaCJob.requested_by,
            ).order_by(IaCJob.id.desc()).limit(limit).all()

            return [
                {
                    "id": job.id,
                    "pipeline_type": job.pipeline_type,
                    "status": job.status,
                    "progress": job.progress or 0,
                    "current_step": job.current_step or "",
                    "requested_by": job.requested_by,
                    "start_time": job.start_time.strftime("%Y-%m-%d %H:%M:%S") if job.start_time else "N/A",
                    "end_time": job.end_time.strftime("%H:%M:%S") if job.end_time else "Running"
                }
                for job in jobs
            ]
        finally:
            if session:
                session.close()

    def get_latest_job_for_pipeline_type(self, pipeline_type: str, since_epoch: int = 0) -> dict | None:
        """Returns the newest job for an exact pipeline type, optionally after epoch seconds.

        A SCHEDULED (parked) job is the current state of that service regardless of the
        time window — a deploy planned days ahead must still be found by the CI poll.
        """
        session = self._get_session()
        if not session:
            return None

        try:
            columns = (
                IaCJob.id,
                IaCJob.pipeline_type,
                IaCJob.status,
                IaCJob.progress,
                IaCJob.current_step,
                IaCJob.start_time,
                IaCJob.end_time,
                IaCJob.not_before,
            )
            job = (
                session.query(*columns)
                .filter(IaCJob.pipeline_type == pipeline_type, IaCJob.status == "SCHEDULED")
                .order_by(IaCJob.id.desc())
                .first()
            )
            if not job:
                query = session.query(*columns).filter(IaCJob.pipeline_type == pipeline_type)

                if since_epoch and since_epoch > 0:
                    # Allow a small skew window so CI/Orchestrator clock drift does not
                    # hide the freshly created deployment job from the status endpoint.
                    since_dt = datetime.fromtimestamp(max(0, since_epoch - 60))
                    query = query.filter(
                        or_(
                            IaCJob.start_time >= since_dt,
                            IaCJob.end_time >= since_dt,
                        )
                    )

                job = query.order_by(IaCJob.id.desc()).first()
            if not job:
                return None

            return {
                "id": job.id,
                "pipeline_type": job.pipeline_type,
                "status": job.status,
                "progress": job.progress or 0,
                "current_step": job.current_step or "",
                "start_time": job.start_time,
                "end_time": job.end_time,
                "not_before": job.not_before,
            }
        finally:
            if session:
                session.close()

    def get_job_logs(self, job_id: int) -> list:
        """Fetches the raw log array for the popup window."""
        session = self._get_session()
        if not session:
            return ["Database connection lost."]

        try:
            job = session.query(IaCJob.logs).filter(IaCJob.id == job_id).first()
            if job and job.logs:
                return json.loads(job.logs)
            return ["No logs found."]
        finally:
            if session:
                session.close()

    def update_pending_tasks(self, job_id: int, pending_list: list):
        """Updates the queue of services yet to be deployed (fenced like update_job)."""
        session = self._get_session()
        if not session:
            return
        token = self.claims.get(job_id)
        try:
            cond = [IaCJob.id == job_id]
            if token:
                cond.append(IaCJob.claim_token == token)
            session.execute(update(IaCJob).where(*cond).values(pending_tasks=json.dumps(pending_list)))
            session.commit()
        except Exception as e:
            log.error(f"Failed to update pending tasks for {job_id}: {e}")
            session.rollback()
        finally:
            if session:
                session.close()

    def get_pending_tasks(self, job_id: int) -> list:
        """Retrieves the surviving queue list for a specific job."""
        session = self._get_session()
        if not session:
            return []

        try:
            job = session.query(IaCJob).filter(IaCJob.id == job_id).first()
            if job and job.pending_tasks:
                return json.loads(job.pending_tasks)
            return []
        finally:
            if session:
                session.close()

    def get_job_payload(self, job_id: int) -> dict | None:
        """The trigger payload persisted with the job (``None`` for a row without
        one, e.g. a legacy direct run). Raises when the row cannot be read — the
        caller must not guess a job's mode."""
        session = self._get_session()
        if not session:
            raise RuntimeError("job store unavailable")
        try:
            raw = session.query(IaCJob.payload).filter(IaCJob.id == job_id).scalar()
        finally:
            session.close()
        if not raw:
            return None
        data = json.loads(raw)
        return data if isinstance(data, dict) else None

    def get_exec_state(self, job_id: int) -> dict | None:
        """Stage progress recorded by the engine (``None`` for a row of a plugin
        version that did not record it)."""
        session = self._get_session()
        if not session:
            return None
        try:
            raw = session.query(IaCJob.exec_state).filter(IaCJob.id == job_id).scalar()
        finally:
            session.close()
        try:
            data = json.loads(raw) if raw else None
        except Exception:
            return None
        return data if isinstance(data, dict) else None

    def get_jobs_by_status(self, status: str) -> list:
        """Finds all jobs currently in a specific state (e.g., RUNNING)."""
        session = self._get_session()
        if not session:
            return []

        try:
            return session.query(IaCJob).filter(IaCJob.status == status).all()
        except Exception as e:
            log.error(f"Failed to fetch jobs by status '{status}': {e}")
            return []
        finally:
            if session:
                session.close()

    def clear_failed_jobs(self) -> int:
        """Deletes only job records with a failure status (FAILED, ERROR, ABORTED).

        Running and successful jobs are left untouched. Returns the number of
        deleted rows (-1 on failure).
        """
        session = self._get_session()
        if not session:
            return -1
        try:
            fail_statuses = ["FAILED", "ERROR", "ABORTED"]
            job_ids = [
                row.id for row in session.query(IaCJob.id)
                .filter(IaCJob.status.in_(fail_statuses))
                .all()
            ]
            deleted = _delete_jobs(session, job_ids)
            log.info(f"Cleared {deleted} failed job record(s) from statistics.")
            return deleted
        except Exception as e:
            log.error(f"Failed to clear failed job statistics: {e}")
            session.rollback()
            return -1
        finally:
            if session:
                session.close()

    def clear_all_jobs(self, keep_running: bool = True) -> int:
        """Deletes job history (the data behind the Overview statistics).

        By default keeps any currently RUNNING jobs so an active pipeline is not
        disrupted, and SCHEDULED jobs — a parked deploy waiting for its maintenance
        window is pending work, not history. Returns the number of deleted rows
        (-1 on failure).
        """
        session = self._get_session()
        if not session:
            return -1
        try:
            job_query = session.query(IaCJob)
            if keep_running:
                job_query = job_query.filter(IaCJob.status.notin_(("RUNNING", "SCHEDULED")))
            job_ids = [row.id for row in job_query.with_entities(IaCJob.id).all()]
            deleted = _delete_jobs(session, job_ids)
            log.info(f"Cleared {deleted} job record(s) from statistics (keep_running={keep_running}).")
            return deleted
        except Exception as e:
            log.error(f"Failed to clear job statistics: {e}")
            session.rollback()
            return -1
        finally:
            if session:
                session.close()

    # --- STATE MANAGEMENT METHODS ---

    def get_state(self, state_id: str) -> dict:
        """Fetches and decodes a state snapshot from the database."""
        session = self._get_session()
        if not session: return None
        try:
            state_record = session.query(IaCState).filter(IaCState.id == state_id).first()
            if state_record and state_record.state_data:
                return {
                    "data": json.loads(state_record.state_data),
                    "commit_hash": state_record.commit_hash
                }
            return None
        except Exception as e:
            log.error(f"Failed to get state '{state_id}': {e}")
            return None
        finally:
            if session: session.close()

    def update_state(self, state_id: str, new_state_data: dict, commit_hash: str):
        """Creates or updates a state snapshot in the database."""
        session = self._get_session()
        if not session: return
        try:
            state_record = session.query(IaCState).filter(IaCState.id == state_id).first()
            encoded_data = json.dumps(new_state_data)

            if state_record:
                state_record.state_data = encoded_data
                state_record.commit_hash = commit_hash
            else:
                new_record = IaCState(id=state_id, state_data=encoded_data, commit_hash=commit_hash)
                session.add(new_record)
            session.commit()
        except Exception as e:
            log.error(f"Failed to update state '{state_id}': {e}")
            session.rollback()
        finally:
            if session: session.close()
                
                
    # -- rule-engine marker (audit OC-06) ---------------------------------------------
    #: ``iac_orchestrator_state`` id of the marker.
    RULES_MARKER = "rules_applied"

    def rules_marker(self) -> dict | None:
        """The inventory_state commit up to which the rule engine has acted:
        ``{"sha", "failures", "last_error", "job_id"}``; ``None`` when none was
        recorded yet. ``failures`` counts the rule stages that failed since the
        marker last moved.

        RAISES on a database error (unlike :meth:`get_state`): "unknown" must
        never look like "none" — the caller would fall back to evaluating only
        its own commit and move the marker past changes nobody acted on.
        """
        session = self._get_session()
        if not session:
            raise RuntimeError("the job database is not connected")
        try:
            rec = session.query(IaCState).filter(IaCState.id == self.RULES_MARKER).first()
            if rec is None or not rec.state_data:
                return None
            data = json.loads(rec.state_data)
            sha = str((data or {}).get("sha") or "")
            if not sha:
                return None
            return {"sha": sha, "failures": int(data.get("failures") or 0),
                    "last_error": str(data.get("last_error") or ""), "job_id": data.get("job_id")}
        finally:
            session.close()

    #: ``set_rules_marker(expected=…)`` default: write whatever the marker is now.
    ANY_MARKER = object()

    def set_rules_marker(self, sha: str, *, job_id: int | None = None, failures: int = 0,
                         last_error: str = "", only_if_absent: bool = False, expected=ANY_MARKER) -> bool:
        """Write the marker; raises on a database error. Returns whether THIS
        call wrote it.

        ``expected`` makes the write a compare-and-set (review C6): the marker
        is only written while it still points at the commit the caller read —
        ``expected="<sha>"`` — or while there is none (``expected=None``, the
        same as ``only_if_absent``; atomic by the primary key). One conditional
        UPDATE: of two writers that read the same marker only one succeeds, and
        a writer whose view is outdated (its ``rules`` lock was taken over) never
        moves the marker back. Without ``expected`` the write is unconditional.
        """
        from sqlalchemy.exc import IntegrityError

        data = json.dumps({"sha": sha, "failures": int(failures), "last_error": str(last_error or "")[:500],
                           "job_id": job_id, "at": datetime.now().isoformat(timespec="seconds")})
        if expected is None:
            only_if_absent = True
        session = self._get_session()
        if not session:
            raise RuntimeError("the job database is not connected")
        try:
            if expected is not self.ANY_MARKER and expected is not None:
                # The row's commit_hash column always carries the marker's sha.
                res = session.execute(
                    update(IaCState)
                    .where(IaCState.id == self.RULES_MARKER, IaCState.commit_hash == str(expected)[:40])
                    .values(state_data=data, commit_hash=sha[:40]))
                session.commit()
                return res.rowcount == 1
            rec = session.query(IaCState).filter(IaCState.id == self.RULES_MARKER).first()
            if rec is not None:
                if only_if_absent:
                    return False
                rec.state_data, rec.commit_hash = data, sha[:40]
                session.commit()
                return True
            try:
                session.add(IaCState(id=self.RULES_MARKER, state_data=data, commit_hash=sha[:40]))
                session.commit()
                return True
            except IntegrityError:          # a peer created it between the read and the insert
                session.rollback()
                if only_if_absent:
                    return False
                session.query(IaCState).filter(IaCState.id == self.RULES_MARKER).update(
                    {"state_data": data, "commit_hash": sha[:40]}, synchronize_session=False)
                session.commit()
                return True
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def update_progress(self, job_id: int, progress: int = None, current_step: str = None, *,
                        fenced: bool = True, exec_state: dict | None = None) -> bool:
        """Live updates the progress bar and current action text.

        While this process holds the job's claim the write only lands on the
        RUNNING row with that claim token. ``fenced=False`` is for the one writer
        that describes a row it just gave up on purpose (parked as SCHEDULED).
        """
        if job_id in (None, -1):
            return False
        values: dict = {}
        if progress is not None:
            values["progress"] = progress
        if current_step:
            values["current_step"] = str(current_step)[:250]
        if exec_state is not None:
            values["exec_state"] = json.dumps(exec_state, default=str)
        if not values:
            return False
        token = self.claims.get(job_id) if fenced else None

        def _tx():
            session = self._get_session()
            if not session:
                return False
            try:
                cond = [IaCJob.id == job_id]
                if token:
                    cond += [IaCJob.claim_token == token, IaCJob.status == "RUNNING"]
                res = session.execute(update(IaCJob).where(*cond).values(**values))
                session.commit()
                return res.rowcount == 1
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()

        try:
            return bool(retry_write(_tx))
        except Exception as e:
            log.error(f"Failed to update progress for job {job_id}: {e}")
            return False

    def insert_job_task(self, job_id: int, task_num: int, task_name: str, task_label: str, 
                       start_time: float, end_time: float, duration_ms: int, status: str, error: str = None):
        """Records a completed task within a job."""
        session = self._get_session()
        if not session or job_id == -1:
            return

        try:
            from datetime import datetime as dt
            task = IaCJobTask(
                job_id=job_id,
                task_num=task_num,
                task_name=task_name,
                task_label=task_label,
                start_time=dt.fromtimestamp(start_time) if start_time else None,
                end_time=dt.fromtimestamp(end_time) if end_time else None,
                duration_ms=duration_ms,
                status=status,
                error=error
            )
            session.add(task)
            session.commit()
        except Exception as e:
            log.error(f"Failed to insert job task for job {job_id}: {e}")
            session.rollback()
        finally:
            if session:
                session.close()
                
    def get_job_tasks_for_stats(self, limit: int = 500) -> list:
        """
        Fetch IaCJobTask rows for the most recent host_provision jobs.

        Returns task-level records so the lifecycle dashboard can correctly
        attribute Configure and Deploy phase counts to their sub-tasks rather
        than rolling everything up into a single Provision entry.
        """
        session = self._get_session()
        if not session:
            return []
        try:
            rows = (
                session.query(
                    IaCJobTask.task_name,
                    IaCJobTask.status,
                    IaCJobTask.start_time,
                    IaCJobTask.end_time,
                    IaCJobTask.duration_ms,
                )
                .join(IaCJob, IaCJobTask.job_id == IaCJob.id)
                .filter(IaCJob.pipeline_type.like("host_provision%"))
                .order_by(IaCJobTask.id.desc())
                .limit(limit)
                .all()
            )
            return [
                {
                    "task_name": r.task_name,
                    "status": r.status,
                    "start_time": r.start_time,
                    "end_time": r.end_time,
                    "duration_ms": r.duration_ms or 0,
                }
                for r in rows
            ]
        finally:
            if session:
                session.close()

    def get_jobs_for_stats(self, limit: int = 500) -> list:
        """
        Fetch a lightweight, raw-typed slice of recent jobs for statistics.

        Unlike :meth:`get_recent_jobs` (which pre-formats timestamps for tables),
        this returns native datetimes so callers can compute durations and trends.
        """
        session = self._get_session()
        if not session:
            return []

        try:
            jobs = session.query(
                IaCJob.id,
                IaCJob.pipeline_type,
                IaCJob.status,
                IaCJob.progress,
                IaCJob.start_time,
                IaCJob.end_time,
            ).order_by(IaCJob.id.desc()).limit(limit).all()

            return [
                {
                    "id": job.id,
                    "pipeline_type": job.pipeline_type,
                    "status": job.status,
                    "progress": job.progress or 0,
                    "start_time": job.start_time,
                    "end_time": job.end_time,
                }
                for job in jobs
            ]
        finally:
            if session:
                session.close()

    def get_service_history(self, service_name: str, limit: int = 15) -> list:
        """Fetches recent jobs involving a specific service with strict filtering."""
        session = self._get_session()
        if not session: return []
        try:
            # Search for the service name in the type string OR inside the pending_tasks JSON blob
            search = f"%{service_name}%"
            jobs = session.query(IaCJob).filter(
                or_(IaCJob.pipeline_type.like(search), IaCJob.pending_tasks.like(search))
            ).order_by(IaCJob.id.desc()).limit(limit).all()

            return [{
                "id": j.id,
                "pipeline_type": j.pipeline_type,
                "status": j.status,
                "progress": j.progress or 0,
                "start_time": j.start_time.strftime("%Y-%m-%d %H:%M:%S") if j.start_time else "N/A"
            } for j in jobs]
        finally:
            if session: session.close()
