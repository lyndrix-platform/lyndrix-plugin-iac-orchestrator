"""Cluster-safe job queue for the IaC Orchestrator (plugin ≥ 1.4).

Control plane (every core instance, placement ``any``): triggers only *enqueue*
a job row (status ``QUEUED``) and every read — jobs, progress, runner state,
logs — comes from the shared database.

Execution plane (instances with the ``runner`` role): :class:`JobWorker`
claims queued jobs atomically, runs the engine, heartbeats its jobs, and
publishes runner state and log lines into the database so any instance can
show them. A job whose runner disappears is marked FAILED by the orphan sweep.

No in-memory state here is needed by another instance.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable

from sqlalchemy import inspect as sa_inspect, select, text, update

from core.api import db_instance

from ..model.models import IaCJob, IaCJobLog, Base

log = logging.getLogger("IaC:Queue")

STATUS_QUEUED = "QUEUED"
STATUS_RUNNING = "RUNNING"
STATUS_ABORTED = "ABORTED"
STATUS_FAILED = "FAILED"
# Maintenance windows (plugin 1.6): a deploy the runner parked until its window opens.
# No owner, no heartbeat — the claim picks it up again once ``not_before`` is reached.
STATUS_SCHEDULED = "SCHEDULED"

#: Statuses that block a second job with the same dedupe key (``DuplicateJob``).
_ACTIVE_STATUSES = (STATUS_QUEUED, STATUS_RUNNING)


def to_queue_clock(aware: datetime) -> datetime:
    """Aware datetime → the naive process-local clock every job column uses
    (``datetime.now()``), so ``not_before <= now`` compares like-for-like in SQL."""
    if aware.tzinfo is None:
        return aware
    return aware.astimezone().replace(tzinfo=None)


def from_queue_clock(naive: datetime | None) -> datetime | None:
    """Naive process-local column value → aware UTC (for ISO output on the bus/API)."""
    if naive is None:
        return None
    if naive.tzinfo is not None:
        return naive.astimezone(timezone.utc)
    local_tz = datetime.now().astimezone().tzinfo
    return naive.replace(tzinfo=local_tz).astimezone(timezone.utc)


@dataclass(frozen=True)
class EnqueueResult:
    """Outcome of :meth:`JobQueue.enqueue_ex`.

    ``coalesced``: an existing SCHEDULED job for the same target absorbed this trigger
    (payload replaced, ``start_time`` reset). ``promoted``: the existing SCHEDULED job
    was moved to QUEUED because this trigger must run now (UI deploy / ``force``).
    """

    job_id: int
    coalesced: bool = False
    promoted: bool = False

# Pipeline types that must not run twice for the same target at the same time.
_HOST_SCOPED = ("host_provision", "bootstrap_compliance", "init_host", "compliance", "state_check", "adopt_host")

# Errors MariaDB raises when two runners race for the same row under snapshot
# isolation / locking — the claim simply retries.
_RETRY_SQLSTATE_CODES = ("1020", "1205", "1213")


class DuplicateJob(Exception):
    """A job with the same concurrency key is already queued or running."""

    def __init__(self, key: str, existing_id: int | None = None):
        super().__init__(f"duplicate job for key '{key}'")
        self.key = key
        self.existing_id = existing_id


def normalize_payload(payload: dict) -> dict:
    """Apply the same normalisation the engine applies before dispatch."""
    payload = dict(payload or {})
    if not payload.get("pipeline_type") and payload.get("object_kind") == "push":
        name, ref = (payload.get("project") or {}).get("name"), payload.get("ref", "")
        if name and str(ref).startswith("refs/heads/"):
            payload.update({"pipeline_type": "single_service", "service_name": name,
                            "service_branch": str(ref).replace("refs/heads/", "")})
    ptype = payload.get("pipeline_type") or "connectivity"
    if ptype == "terraform_provision":
        ptype = "host_provision"
    payload["pipeline_type"] = ptype
    if ptype == "single_service" and payload.get("service_name"):
        payload["service_name"] = str(payload["service_name"]).strip().lower()
    if ptype in _HOST_SCOPED:
        host = str(payload.get("host_name") or payload.get("test_host") or payload.get("limit") or "").strip().lower()
        if host:
            payload["host_name"] = host
    return payload


def dedupe_key_for(payload: dict) -> str | None:
    """Concurrency key of a (normalised) payload, mirroring the engine's dispatch guard."""
    ptype = payload.get("pipeline_type") or "connectivity"
    if ptype == "rollout":
        limit = str(payload.get("limit") or "").strip().lower()
        return f"rollout:{limit}" if (limit and limit != "all") else "rollout:all"
    if ptype == "single_service":
        svc = str(payload.get("service_name") or "").strip().lower()
        return f"single_service:{svc}" if svc else None
    if ptype in _HOST_SCOPED:
        host = str(payload.get("host_name") or "").strip().lower()
        return f"{ptype}:{host}" if host else None
    if ptype in ("infra_plan", "infra_apply"):
        return "infra"
    if ptype == "repo_sync":
        return "repo_sync"
    return None


def _conflicts(key: str, active_keys: set[str]) -> bool:
    """Does ``key`` collide with any active (queued/running) key?"""
    if key in active_keys:
        return True
    if key.startswith("rollout:"):
        # A global rollout blocks on any rollout; a host rollout blocks on a global one.
        if key == "rollout:all":
            return any(k.startswith("rollout:") for k in active_keys)
        return "rollout:all" in active_keys
    return False


def db_tag_for(payload: dict) -> str:
    """The ``pipeline_type`` column value (strategy-specific tag, e.g. ``single_service:<svc>``)."""
    try:
        from .engine import get_pipeline_strategy  # lazy: engine imports are heavy

        return get_pipeline_strategy(payload.get("pipeline_type") or "connectivity").db_tag(payload)
    except Exception:
        return str(payload.get("pipeline_type") or "connectivity")


def _is_retryable(exc: Exception) -> bool:
    msg = str(getattr(exc, "orig", exc))
    return any(code in msg for code in _RETRY_SQLSTATE_CODES)


class JobQueue:
    """Database-backed queue + log/runner store. Every method is synchronous and
    cheap; callers off-load them with ``asyncio.to_thread`` where it matters."""

    #: Called with the job id after this queue closed a job on a terminal path it
    #: owns itself (``mark_failed`` → orphan sweep), so the engine can end the job's
    #: maintenance override. Set by the service; may run in a worker thread.
    on_terminal: Callable[[int], None] | None = None

    def _session(self):
        # READ COMMITTED: several writers (progress, heartbeat, runner snapshot, claim)
        # touch one job row concurrently — see database.read_committed_session.
        from ..model.database import read_committed_session

        return read_committed_session()

    @staticmethod
    def _retry(fn):
        from ..model.database import retry_write

        return retry_write(fn)

    # ------------------------------------------------------------------ schema
    def ensure_schema(self) -> None:
        """Add the queue columns and the log table (idempotent, bootstrap-locked)."""
        engine = getattr(db_instance, "engine", None)
        if not engine:
            return
        lock = getattr(db_instance, "advisory_lock", None)
        cm = lock("lyndrix_bootstrap") if callable(lock) else _nullcontext()
        with cm:
            Base.metadata.create_all(bind=engine, checkfirst=True)
            insp = sa_inspect(engine)
            existing = {c["name"] for c in insp.get_columns(IaCJob.__tablename__)}
            wanted = {
                "payload": "LONGTEXT NULL",
                "dedupe_key": "VARCHAR(255) NULL",
                "owner_instance": "VARCHAR(100) NULL",
                "claimed_at": "DATETIME NULL",
                "heartbeat_at": "DATETIME NULL",
                "runners": "LONGTEXT NULL",
                # maintenance windows (plugin 1.6)
                "not_before": "DATETIME NULL",
                "window_ci_ids": "LONGTEXT NULL",
                "override_id": "VARCHAR(64) NULL",
                "schedule_reason": "VARCHAR(255) NULL",
            }
            if engine.dialect.name == "sqlite":
                wanted = {k: v.replace("LONGTEXT", "TEXT").replace("VARCHAR(255)", "TEXT")
                          .replace("VARCHAR(100)", "TEXT").replace("VARCHAR(64)", "TEXT")
                          for k, v in wanted.items()}
            with engine.begin() as conn:
                for col, ddl in wanted.items():
                    if col not in existing:
                        try:
                            conn.execute(text(f"ALTER TABLE {IaCJob.__tablename__} ADD COLUMN {col} {ddl}"))
                            log.info("QUEUE: added column %s.%s", IaCJob.__tablename__, col)
                        except Exception as exc:  # a peer added it between inspect and ALTER
                            if "1060" not in str(exc) and "duplicate column" not in str(exc).lower():
                                raise

    # ----------------------------------------------------------------- enqueue
    def active_keys(self, session=None) -> set[str]:
        own = session is None
        session = session or self._session()
        if not session:
            return set()
        try:
            rows = session.execute(
                select(IaCJob.dedupe_key).where(IaCJob.status.in_(_ACTIVE_STATUSES))
            ).all()
            return {r[0] for r in rows if r[0]}
        finally:
            if own:
                session.close()

    def enqueue(self, payload: dict, *, requested_by: str | None = None) -> int:
        """Insert a QUEUED job for ``payload``; raises :class:`DuplicateJob` on a
        concurrency collision. Returns the job id (-1 without a database).

        A SCHEDULED job for the same target absorbs the trigger instead (see
        :meth:`enqueue_ex`); callers that need to know use ``enqueue_ex``.
        """
        return self.enqueue_ex(payload, requested_by=requested_by).job_id

    def enqueue_ex(self, payload: dict, *, requested_by: str | None = None) -> EnqueueResult:
        """:meth:`enqueue` with the coalesce/promote outcome.

        * QUEUED/RUNNING job with the same key → :class:`DuplicateJob` (first wins, as before).
        * SCHEDULED job with the same key → the newer trigger is **coalesced** into it
          (payload replaced, ``start_time = now`` so the CI finds it via ``since_epoch``);
          a trigger that must run now (UI deploy, ``force``) **promotes** it to QUEUED instead.
        """
        payload = normalize_payload(payload)
        key = dedupe_key_for(payload)
        session = self._session()
        if not session:
            return EnqueueResult(-1)
        try:
            if key and _conflicts(key, self.active_keys(session)):
                existing = session.execute(
                    select(IaCJob.id).where(IaCJob.dedupe_key == key,
                                            IaCJob.status.in_(_ACTIVE_STATUSES))
                    .order_by(IaCJob.id.desc())
                ).first()
                raise DuplicateJob(key, existing[0] if existing else None)
            if key:
                scheduled = session.execute(
                    select(IaCJob.id).where(IaCJob.dedupe_key == key, IaCJob.status == STATUS_SCHEDULED)
                    .order_by(IaCJob.id.desc())
                ).first()
                if scheduled:
                    from .maintenance_resolver import policy_applies

                    sched_id = int(scheduled[0])
                    now = datetime.now()
                    if policy_applies(payload):
                        session.execute(
                            update(IaCJob).where(IaCJob.id == sched_id, IaCJob.status == STATUS_SCHEDULED)
                            .values(payload=json.dumps(payload, default=str), start_time=now,
                                    current_step="Scheduled (updated by a newer trigger)")
                        )
                        session.commit()
                        log.info("QUEUE: coalesced trigger for '%s' into scheduled job #%s", key, sched_id)
                        return EnqueueResult(sched_id, coalesced=True)
                    payload["force"] = True
                    session.execute(
                        update(IaCJob).where(IaCJob.id == sched_id, IaCJob.status == STATUS_SCHEDULED)
                        .values(status=STATUS_QUEUED, not_before=None, start_time=now,
                                payload=json.dumps(payload, default=str),
                                current_step="Queued" + (f" by {requested_by}" if requested_by else "") + " (run now)")
                    )
                    session.commit()
                    log.info("QUEUE: promoted scheduled job #%s for '%s' (run now)", sched_id, key)
                    return EnqueueResult(sched_id, promoted=True)
            job = IaCJob(
                pipeline_type=db_tag_for(payload),
                status=STATUS_QUEUED,
                progress=0,
                current_step="Queued" + (f" by {requested_by}" if requested_by else ""),
                logs="[]",
                pending_tasks="[]",
                payload=json.dumps(payload, default=str),
                dedupe_key=key,
            )
            session.add(job)
            session.commit()
            session.refresh(job)
            return EnqueueResult(int(job.id))
        except DuplicateJob:
            session.rollback()
            raise
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    # ------------------------------------------------------------------- claim
    def claim_next(self, instance_id: str) -> tuple[int, dict] | None:
        """Atomically move the oldest QUEUED job — or a SCHEDULED job whose
        ``not_before`` is reached — to RUNNING owned by ``instance_id``.

        Two runners may race for the same row: the conditional UPDATE lets only
        one win (``rowcount == 1``); the loser retries with the next candidate.
        Due SCHEDULED jobs come first (``not_before`` ascending), then QUEUED by id.
        ``not_before`` stays on the claimed row so the policy stage knows the job
        is due and does not park it again.
        """
        for _ in range(5):
            session = self._session()
            if not session:
                return None
            try:
                now = datetime.now()
                cand = session.execute(
                    select(IaCJob.id)
                    .where(
                        (IaCJob.status == STATUS_QUEUED)
                        | ((IaCJob.status == STATUS_SCHEDULED) & (IaCJob.not_before <= now))
                    )
                    # NULLS FIRST portable across SQLite/MariaDB: due scheduled jobs
                    # (oldest window first) ahead of the plain FIFO queue.
                    .order_by(IaCJob.not_before.is_(None), IaCJob.not_before, IaCJob.id)
                    .limit(1)
                ).first()
                if not cand:
                    return None
                job_id = int(cand[0])
                res = session.execute(
                    update(IaCJob)
                    .where(IaCJob.id == job_id, IaCJob.status.in_((STATUS_QUEUED, STATUS_SCHEDULED)))
                    .values(status=STATUS_RUNNING, owner_instance=instance_id, claimed_at=now,
                            heartbeat_at=now, current_step=f"Claimed by {instance_id}")
                )
                session.commit()
                if res.rowcount != 1:
                    continue  # a peer won this row — try the next one
                raw = session.execute(select(IaCJob.payload).where(IaCJob.id == job_id)).scalar()
                try:
                    payload = json.loads(raw) if raw else {}
                except Exception:
                    payload = {}
                return job_id, payload
            except Exception as exc:
                session.rollback()
                if _is_retryable(exc):
                    time.sleep(0.1)
                    continue
                log.error("QUEUE: claim failed: %s", exc)
                return None
            finally:
                session.close()
        return None

    def queued_count(self) -> int:
        session = self._session()
        if not session:
            return 0
        try:
            return len(session.execute(select(IaCJob.id).where(IaCJob.status == STATUS_QUEUED)).all())
        finally:
            session.close()

    # ------------------------------------------------- scheduled deploys (1.6)
    def park(self, job_id: int, *, not_before: datetime, window_ci_ids: list[str] | None,
             reason: str, instance_id: str | None = None) -> bool:
        """RUNNING → SCHEDULED until ``not_before`` (aware or process-clock naive).

        Conditional on the row still being RUNNING and owned by this instance (or
        ownerless — a legacy direct ``run_pipeline`` call). Releases the claim so the
        orphan sweep never sees the parked job; the claim takes it again once due.
        """
        if job_id in (None, -1, 0):
            return False
        due = to_queue_clock(not_before)

        def _tx():
            session = self._session()
            if not session:
                return False
            try:
                cond = [IaCJob.id == job_id, IaCJob.status == STATUS_RUNNING]
                if instance_id:
                    cond.append((IaCJob.owner_instance == instance_id) | (IaCJob.owner_instance.is_(None)))
                res = session.execute(
                    update(IaCJob).where(*cond).values(
                        status=STATUS_SCHEDULED, not_before=due,
                        window_ci_ids=json.dumps(list(window_ci_ids or [])),
                        schedule_reason=str(reason or "")[:250],
                        owner_instance=None, claimed_at=None, heartbeat_at=None,
                        progress=0, current_step="Scheduled",
                    )
                )
                session.commit()
                return res.rowcount == 1
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()

        try:
            return bool(self._retry(_tx))
        except Exception as exc:
            log.error("QUEUE: park of job #%s failed: %s", job_id, exc)
            return False

    def promote(self, job_id: int, *, override: bool = True) -> bool:
        """SCHEDULED → QUEUED now ("run now"). ``override=True`` sets ``force`` in the
        payload so the policy stage lets the deploy through; the caller emits
        ``iac:job_queued`` to wake the runners."""
        session = self._session()
        if not session:
            return False
        try:
            row = session.execute(
                select(IaCJob.id, IaCJob.payload).where(IaCJob.id == job_id, IaCJob.status == STATUS_SCHEDULED)
            ).first()
            if not row:
                return False
            try:
                payload = json.loads(row[1]) if row[1] else {}
            except Exception:
                payload = {}
            if override:
                payload["force"] = True
            res = session.execute(
                update(IaCJob).where(IaCJob.id == job_id, IaCJob.status == STATUS_SCHEDULED)
                .values(status=STATUS_QUEUED, not_before=None, start_time=datetime.now(),
                        payload=json.dumps(payload, default=str), current_step="Queued (run now)")
            )
            session.commit()
            return res.rowcount == 1
        except Exception as exc:
            session.rollback()
            log.error("QUEUE: promote of job #%s failed: %s", job_id, exc)
            return False
        finally:
            session.close()

    def abort_scheduled(self, job_id: int) -> bool:
        """SCHEDULED → ABORTED (cancel a parked deploy)."""
        session = self._session()
        if not session:
            return False
        try:
            res = session.execute(
                update(IaCJob).where(IaCJob.id == job_id, IaCJob.status == STATUS_SCHEDULED)
                .values(status=STATUS_ABORTED, not_before=None, end_time=datetime.now(),
                        current_step="Scheduled deployment cancelled")
            )
            session.commit()
            return res.rowcount == 1
        except Exception as exc:
            session.rollback()
            log.error("QUEUE: abort of scheduled job #%s failed: %s", job_id, exc)
            return False
        finally:
            session.close()

    def scheduled_jobs(self) -> list[dict]:
        """Every SCHEDULED job (oldest window first) for the API/UI."""
        session = self._session()
        if not session:
            return []
        try:
            rows = session.execute(
                select(IaCJob.id, IaCJob.pipeline_type, IaCJob.payload, IaCJob.not_before,
                       IaCJob.window_ci_ids, IaCJob.schedule_reason, IaCJob.start_time)
                .where(IaCJob.status == STATUS_SCHEDULED)
                .order_by(IaCJob.not_before, IaCJob.id)
            ).all()
            out = []
            for job_id, ptype, raw, not_before, ci_raw, reason, start_time in rows:
                try:
                    payload = json.loads(raw) if raw else {}
                except Exception:
                    payload = {}
                try:
                    ci_ids = json.loads(ci_raw) if ci_raw else []
                except Exception:
                    ci_ids = []
                out.append({
                    "id": int(job_id),
                    "pipeline_type": ptype,
                    "service": payload.get("service_name"),
                    "branch": payload.get("service_branch"),
                    "trigger": payload.get("trigger") or ("ui" if payload.get("manual") else "webhook"),
                    "not_before": from_queue_clock(not_before),
                    "window_ci_ids": ci_ids,
                    "schedule_reason": reason,
                    "start_time": from_queue_clock(start_time),
                })
            return out
        finally:
            session.close()

    def scheduled_count(self) -> int:
        session = self._session()
        if not session:
            return 0
        try:
            return len(session.execute(select(IaCJob.id).where(IaCJob.status == STATUS_SCHEDULED)).all())
        finally:
            session.close()

    def job_schedule(self, job_id: int) -> dict | None:
        """``{status, not_before, window_ci_ids, override_id, claimed_at}`` of one job."""
        if job_id in (None, -1, 0):
            return None
        session = self._session()
        if not session:
            return None
        try:
            row = session.execute(
                select(IaCJob.status, IaCJob.not_before, IaCJob.window_ci_ids, IaCJob.override_id, IaCJob.claimed_at)
                .where(IaCJob.id == job_id)
            ).first()
            if not row:
                return None
            status, not_before, ci_raw, override_id, claimed_at = row
            try:
                ci_ids = json.loads(ci_raw) if ci_raw else []
            except Exception:
                ci_ids = []
            return {"status": status, "not_before": from_queue_clock(not_before), "window_ci_ids": ci_ids,
                    "override_id": override_id, "claimed_at": from_queue_clock(claimed_at)}
        finally:
            session.close()

    def set_override(self, job_id: int, override_id: str, window_ci_ids: list[str]) -> None:
        """Remember the maintenance override a running deploy announced."""
        if job_id in (None, -1, 0):
            return

        def _tx():
            session = self._session()
            if not session:
                return
            try:
                session.execute(
                    update(IaCJob).where(IaCJob.id == job_id)
                    .values(override_id=override_id, window_ci_ids=json.dumps(list(window_ci_ids or [])))
                )
                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()

        try:
            self._retry(_tx)
        except Exception as exc:
            log.debug("QUEUE: set_override failed: %s", exc)

    def clear_override(self, job_id: int) -> str | None:
        """Atomically take the job's ``override_id`` (``None`` when there is none or a
        peer already took it) — the caller emits ``override_ended`` exactly once."""
        if job_id in (None, -1, 0):
            return None
        session = self._session()
        if not session:
            return None
        try:
            current = session.execute(select(IaCJob.override_id).where(IaCJob.id == job_id)).scalar()
            if not current:
                return None
            res = session.execute(
                update(IaCJob).where(IaCJob.id == job_id, IaCJob.override_id == current).values(override_id=None)
            )
            session.commit()
            return str(current) if res.rowcount == 1 else None
        except Exception as exc:
            session.rollback()
            log.debug("QUEUE: clear_override failed: %s", exc)
            return None
        finally:
            session.close()

    def running_overrides(self) -> list[dict]:
        """RUNNING jobs that announced a maintenance override: ``[{id, override_id,
        window_ci_ids, claimed_at}]`` — for the read model on every instance."""
        session = self._session()
        if not session:
            return []
        try:
            rows = session.execute(
                select(IaCJob.id, IaCJob.override_id, IaCJob.window_ci_ids, IaCJob.claimed_at)
                .where(IaCJob.status == STATUS_RUNNING, IaCJob.override_id.isnot(None))
            ).all()
            out = []
            for job_id, override_id, ci_raw, claimed_at in rows:
                try:
                    ci_ids = json.loads(ci_raw) if ci_raw else []
                except Exception:
                    ci_ids = []
                out.append({"id": int(job_id), "override_id": override_id, "window_ci_ids": ci_ids,
                            "claimed_at": from_queue_clock(claimed_at)})
            return out
        finally:
            session.close()

    # --------------------------------------------------------------- heartbeat
    def heartbeat(self, instance_id: str, job_ids: list[int]) -> None:
        if not job_ids:
            return

        def _tx():
            session = self._session()
            if not session:
                return
            try:
                session.execute(
                    update(IaCJob)
                    .where(IaCJob.id.in_(job_ids), IaCJob.owner_instance == instance_id,
                           IaCJob.status == STATUS_RUNNING)
                    .values(heartbeat_at=datetime.now())
                )
                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()

        try:
            self._retry(_tx)
        except Exception as exc:
            log.debug("QUEUE: heartbeat failed: %s", exc)

    def orphan_candidates(self, alive_instances: set[str] | None, stale_seconds: int = 180,
                          self_instance: str | None = None) -> list[dict]:
        """RUNNING jobs whose runner looks gone: owner not among the live instances
        (when the caller knows them) or heartbeat older than ``stale_seconds``.

        Never this instance's own jobs (``self_instance``) — those are handled by
        its worker/startup reconciliation, and right after a restart the
        coordinator may not list this instance yet. Rows without an owner (pre-1.4
        or resumed legacy jobs) are left alone as well.
        """
        session = self._session()
        if not session:
            return []
        try:
            cutoff = datetime.now() - timedelta(seconds=stale_seconds)
            rows = session.execute(
                select(IaCJob.id, IaCJob.owner_instance, IaCJob.heartbeat_at)
                .where(IaCJob.status == STATUS_RUNNING, IaCJob.owner_instance.isnot(None))
            ).all()
            out = []
            for job_id, owner, hb in rows:
                if self_instance and owner == self_instance:
                    continue
                dead_owner = alive_instances is not None and owner not in alive_instances
                stale = hb is not None and hb.replace(tzinfo=None) < cutoff
                if dead_owner or stale:
                    out.append({"id": int(job_id), "owner_instance": owner, "heartbeat_at": hb})
            return out
        finally:
            session.close()

    def mark_failed(self, job_id: int, reason: str) -> bool:
        """RUNNING → FAILED with ``reason`` as the last step (no-op when not RUNNING)."""
        session = self._session()
        if not session:
            return False
        try:
            res = session.execute(
                update(IaCJob).where(IaCJob.id == job_id, IaCJob.status == STATUS_RUNNING)
                .values(status=STATUS_FAILED, end_time=datetime.now(), current_step=reason[:250])
            )
            session.commit()
            done = res.rowcount == 1
        except Exception as exc:
            session.rollback()
            log.debug("QUEUE: mark_failed failed: %s", exc)
            return False
        finally:
            session.close()
        if done and self.on_terminal is not None:
            try:
                self.on_terminal(job_id)
            except Exception as exc:
                log.debug("QUEUE: on_terminal hook failed for #%s: %s", job_id, exc)
        return done

    def fail_orphans(self, alive_instances: set[str] | None, stale_seconds: int = 180,
                     self_instance: str | None = None) -> list[int]:
        """Mark every orphan candidate FAILED ("Runner '<owner>' lost")."""
        failed = []
        for row in self.orphan_candidates(alive_instances, stale_seconds, self_instance):
            if self.mark_failed(row["id"], f"Runner '{row['owner_instance']}' lost"):
                failed.append(row["id"])
        if failed:
            log.warning("QUEUE: marked orphaned job(s) FAILED: %s", failed)
        return failed

    def own_running(self, instance_id: str) -> list[IaCJob]:
        session = self._session()
        if not session:
            return []
        try:
            return session.execute(
                select(IaCJob).where(IaCJob.status == STATUS_RUNNING, IaCJob.owner_instance == instance_id)
            ).scalars().all()
        finally:
            session.close()

    def owner_of(self, job_id: int) -> str | None:
        session = self._session()
        if not session:
            return None
        try:
            return session.execute(select(IaCJob.owner_instance).where(IaCJob.id == job_id)).scalar()
        finally:
            session.close()

    # ------------------------------------------------------------------- abort
    def abort_queued(self, *, include_scheduled: bool = False) -> list[int]:
        """QUEUED → ABORTED (nothing is executing yet). Parked SCHEDULED deploys are
        kept unless ``include_scheduled`` — the global abort button stops executions,
        it does not cancel planned maintenance deploys."""
        session = self._session()
        if not session:
            return []
        statuses = (STATUS_QUEUED, STATUS_SCHEDULED) if include_scheduled else (STATUS_QUEUED,)
        try:
            ids = [int(r[0]) for r in session.execute(select(IaCJob.id).where(IaCJob.status.in_(statuses))).all()]
            if ids:
                session.execute(
                    update(IaCJob).where(IaCJob.id.in_(ids), IaCJob.status.in_(statuses))
                    .values(status=STATUS_ABORTED, not_before=None, end_time=datetime.now(),
                            current_step="Aborted by User")
                )
                session.commit()
            return ids
        except Exception as exc:
            session.rollback()
            log.error("QUEUE: abort_queued failed: %s", exc)
            return []
        finally:
            session.close()

    def running_jobs(self) -> list[dict]:
        """``[{id, owner_instance, heartbeat_at}]`` of RUNNING jobs."""
        session = self._session()
        if not session:
            return []
        try:
            rows = session.execute(
                select(IaCJob.id, IaCJob.owner_instance, IaCJob.heartbeat_at).where(IaCJob.status == STATUS_RUNNING)
            ).all()
            return [{"id": int(i), "owner_instance": o, "heartbeat_at": h} for i, o, h in rows]
        finally:
            session.close()

    # ----------------------------------------------------------------- runners
    def set_runners(self, job_id: int, runners: dict) -> None:
        if job_id in (None, -1):
            return
        blob = json.dumps(runners, default=str)

        def _tx():
            session = self._session()
            if not session:
                return
            try:
                session.execute(update(IaCJob).where(IaCJob.id == job_id).values(runners=blob))
                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()

        try:
            self._retry(_tx)
        except Exception as exc:
            log.debug("QUEUE: set_runners failed: %s", exc)

    def runners_for(self, job_id: int) -> dict:
        session = self._session()
        if not session:
            return {}
        try:
            raw = session.execute(select(IaCJob.runners).where(IaCJob.id == job_id)).scalar()
            return json.loads(raw) if raw else {}
        except Exception:
            return {}
        finally:
            session.close()

    def live_summary(self, recent_minutes: int = 10) -> dict:
        """``{"active_tasks": {...}, "is_running": bool, "queued": n, "scheduled": n}``
        for the SSE snapshot — the same shape the in-memory state had, now from the
        database so every instance shows the same picture."""
        session = self._session()
        if not session:
            return {"active_tasks": {}, "is_running": False, "queued": 0, "scheduled": 0}
        try:
            since = datetime.now() - timedelta(minutes=recent_minutes)
            rows = session.execute(
                select(IaCJob.id, IaCJob.status, IaCJob.runners, IaCJob.end_time)
                .where(IaCJob.runners.isnot(None))
                .where((IaCJob.status == STATUS_RUNNING) | (IaCJob.end_time >= since))
                .order_by(IaCJob.id.desc()).limit(20)
            ).all()
            active: dict = {}
            for job_id, status, raw, _end in rows:
                try:
                    data = json.loads(raw) if raw else {}
                except Exception:
                    continue
                for task, info in (data or {}).items():
                    if isinstance(info, dict):
                        info = dict(info)
                        info.setdefault("job_id", int(job_id))
                        active.setdefault(task, info)
            n_running = len(session.execute(select(IaCJob.id).where(IaCJob.status == STATUS_RUNNING)).all())
            n_queued = len(session.execute(select(IaCJob.id).where(IaCJob.status == STATUS_QUEUED)).all())
            n_scheduled = len(session.execute(select(IaCJob.id).where(IaCJob.status == STATUS_SCHEDULED)).all())
            return {"active_tasks": active, "is_running": n_running > 0, "queued": n_queued,
                    "scheduled": n_scheduled}
        finally:
            session.close()

    # -------------------------------------------------------------------- logs
    def append_log(self, job_id: int, lines: list[str], start_seq: int) -> int:
        """Append ``lines`` with consecutive ``seq`` numbers; returns the next seq."""
        if not lines or job_id in (None, -1):
            return start_seq
        session = self._session()
        if not session:
            return start_seq
        try:
            seq = start_seq
            objs = []
            for ln in lines:
                objs.append(IaCJobLog(job_id=job_id, seq=seq, line=ln))
                seq += 1
            session.add_all(objs)
            session.commit()
            return seq
        except Exception as exc:
            session.rollback()
            log.debug("QUEUE: append_log failed: %s", exc)
            return start_seq
        finally:
            session.close()

    def last_seq(self, job_id: int) -> int:
        session = self._session()
        if not session:
            return 0
        try:
            from sqlalchemy import func as sa_func

            val = session.execute(select(sa_func.max(IaCJobLog.seq)).where(IaCJobLog.job_id == job_id)).scalar()
            return int(val) + 1 if val is not None else 0
        finally:
            session.close()

    def has_log(self, job_id: int) -> bool:
        session = self._session()
        if not session:
            return False
        try:
            return session.execute(select(IaCJobLog.id).where(IaCJobLog.job_id == job_id).limit(1)).first() is not None
        finally:
            session.close()

    def read_log(self, job_id: int, *, offset: int = 0, tail: int = 600, grep: str | None = None,
                 grep_max: int = 2000) -> dict:
        """Lines after ``offset`` (a seq), or the last ``tail`` lines when offset is 0,
        or the last ``grep_max`` lines containing ``grep``. ``offset`` in the result is
        the next seq to ask for."""
        session = self._session()
        if not session:
            return {"lines": [], "offset": 0, "size": 0}
        try:
            from sqlalchemy import func as sa_func

            total = session.execute(select(sa_func.count(IaCJobLog.id)).where(IaCJobLog.job_id == job_id)).scalar() or 0
            next_seq = session.execute(select(sa_func.max(IaCJobLog.seq)).where(IaCJobLog.job_id == job_id)).scalar()
            next_seq = int(next_seq) + 1 if next_seq is not None else 0
            q = select(IaCJobLog.seq, IaCJobLog.line).where(IaCJobLog.job_id == job_id)
            if grep:
                q = q.where(IaCJobLog.line.ilike(f"%{grep}%")).order_by(IaCJobLog.seq.desc()).limit(grep_max)
                rows = session.execute(q).all()[::-1]
            elif offset and offset > 0:
                q = q.where(IaCJobLog.seq >= offset).order_by(IaCJobLog.seq)
                rows = session.execute(q).all()
            else:
                q = q.order_by(IaCJobLog.seq.desc()).limit(max(1, tail))
                rows = session.execute(q).all()[::-1]
            return {"lines": [r[1] for r in rows], "offset": next_seq, "size": int(total)}
        finally:
            session.close()

    def iter_log(self, job_id: int, chunk: int = 2000):
        """Yield log lines in order, in chunks — for the raw download."""
        seq = 0
        while True:
            session = self._session()
            if not session:
                return
            try:
                rows = session.execute(
                    select(IaCJobLog.seq, IaCJobLog.line).where(IaCJobLog.job_id == job_id, IaCJobLog.seq >= seq)
                    .order_by(IaCJobLog.seq).limit(chunk)
                ).all()
            finally:
                session.close()
            if not rows:
                return
            for s, line in rows:
                yield line
            seq = int(rows[-1][0]) + 1
            if len(rows) < chunk:
                return

    def delete_logs(self, job_ids: list[int]) -> None:
        if not job_ids:
            return
        session = self._session()
        if not session:
            return
        try:
            session.query(IaCJobLog).filter(IaCJobLog.job_id.in_(job_ids)).delete(synchronize_session=False)
            session.commit()
        except Exception:
            session.rollback()
        finally:
            session.close()


class _nullcontext:
    def __enter__(self):
        return None

    def __exit__(self, *exc):
        return False


class JobLogWriter:
    """Buffers a job's log lines and flushes them to the database in batches.

    ``append`` is synchronous and cheap (called from a logging handler and from
    the container log reader); ``run`` flushes every ``interval`` seconds off
    the event loop; ``close`` flushes the rest.
    """

    def __init__(self, queue: JobQueue, job_id: int, *, interval: float = 0.5, batch: int = 200):
        self.queue = queue
        self.job_id = job_id
        self.interval = interval
        self.batch = batch
        self._buf: list[str] = []
        self._seq: int | None = None
        self._closed = False
        self._task: asyncio.Task | None = None

    def append(self, line: str) -> None:
        if self._closed:
            return
        self._buf.append(line)

    def start(self, create_task) -> None:
        if self._task is None:
            self._task = create_task(self.run(), name=f"iac:joblog:{self.job_id}")

    async def _flush(self) -> None:
        if not self._buf:
            return
        lines, self._buf = self._buf[: self.batch * 5], self._buf[self.batch * 5:]
        if self._seq is None:
            self._seq = await asyncio.to_thread(self.queue.last_seq, self.job_id)
        self._seq = await asyncio.to_thread(self.queue.append_log, self.job_id, lines, self._seq)

    async def run(self) -> None:
        try:
            while not self._closed:
                await asyncio.sleep(self.interval)
                await self._flush()
        except asyncio.CancelledError:
            pass

    async def close(self) -> None:
        self._closed = True
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        while self._buf:
            await self._flush()


class JobWorker:
    """Runner-side loop: claim → execute → heartbeat; orphan sweep; abort relay.

    One per instance with the ``runner`` role. ``run_job`` is the coroutine that
    executes a claimed job (``engine.run_pipeline(payload, job_id=...)``).
    """

    def __init__(self, ctx, queue: JobQueue, run_job, *, max_parallel: int = 3,
                 poll_seconds: float = 2.0, heartbeat_seconds: float = 10.0,
                 orphan_sweep_seconds: float = 60.0, stale_seconds: int = 180,
                 first_sweep_delay: float = 90.0, finalize_orphan=None):
        self.ctx = ctx
        self.queue = queue
        self.run_job = run_job
        self.max_parallel = max(1, int(max_parallel))
        self.poll_seconds = poll_seconds
        self.heartbeat_seconds = heartbeat_seconds
        self.orphan_sweep_seconds = orphan_sweep_seconds
        self.stale_seconds = stale_seconds
        # The first sweep waits: right after a (re)start the coordinator does not list
        # every live instance yet, and a job this host was running may still have a
        # container going — finalize it from its exit code instead of failing it.
        self.first_sweep_delay = first_sweep_delay
        # async (job_id, owner) -> bool: True when the job was finalized from a local
        # runner container (exit code), so it must NOT be marked "Runner lost".
        self.finalize_orphan = finalize_orphan
        self.tasks: dict[int, asyncio.Task] = {}
        self._wake = asyncio.Event()
        self._stopped = False
        self._tick_task: asyncio.Task | None = None
        self._hb_task: asyncio.Task | None = None
        self._started_at = time.monotonic()

    # -- lifecycle ---------------------------------------------------------
    def start(self) -> None:
        self._tick_task = self.ctx.create_task(self._loop(), name="iac:worker")
        self._hb_task = self.ctx.create_task(self._heartbeat_loop(), name="iac:worker:heartbeat")

    async def stop(self) -> None:
        self._stopped = True
        self._wake.set()
        for t in (self._tick_task, self._hb_task):
            if t:
                t.cancel()

    def wake(self, *_args) -> None:
        self._wake.set()

    @property
    def running_ids(self) -> list[int]:
        return [jid for jid, t in self.tasks.items() if not t.done()]

    # -- loops --------------------------------------------------------------
    async def _loop(self) -> None:
        last_sweep = self._started_at - self.orphan_sweep_seconds + self.first_sweep_delay
        while not self._stopped:
            try:
                self.tasks = {j: t for j, t in self.tasks.items() if not t.done()}
                while len(self.tasks) < self.max_parallel:
                    claimed = await asyncio.to_thread(self.queue.claim_next, self.ctx.instance_id)
                    if not claimed:
                        break
                    job_id, payload = claimed
                    self.ctx.log.info(f"WORKER: claimed job #{job_id} ({payload.get('pipeline_type')})")
                    self.tasks[job_id] = self.ctx.create_task(self._run(job_id, payload), name=f"iac:job:{job_id}")
                if time.monotonic() - last_sweep >= self.orphan_sweep_seconds:
                    last_sweep = time.monotonic()
                    await self.sweep_orphans()
            except Exception as exc:
                self.ctx.log.warning(f"WORKER: loop error: {exc}")
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self.poll_seconds)
            except asyncio.TimeoutError:
                pass
            self._wake.clear()

    async def _run(self, job_id: int, payload: dict) -> None:
        try:
            await self.run_job(payload, job_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.ctx.log.error(f"WORKER: job #{job_id} crashed: {exc}")

    async def _heartbeat_loop(self) -> None:
        while not self._stopped:
            await asyncio.sleep(self.heartbeat_seconds)
            ids = self.running_ids
            if ids:
                try:
                    await asyncio.to_thread(self.queue.heartbeat, self.ctx.instance_id, ids)
                except Exception as exc:
                    self.ctx.log.debug(f"WORKER: heartbeat failed: {exc}")

    async def sweep_orphans(self) -> list[int]:
        """RUNNING jobs of dead/stale runners: finalize from a local container when
        possible, otherwise FAILED "Runner '<owner>' lost". Returns the failed ids."""
        candidates = await asyncio.to_thread(
            self.queue.orphan_candidates, self._alive_instances(), self.stale_seconds, self.ctx.instance_id,
        )
        failed: list[int] = []
        for row in candidates:
            if self.owns(row["id"]):
                continue
            if self.finalize_orphan is not None:
                try:
                    if await self.finalize_orphan(row["id"], row["owner_instance"]):
                        continue
                except Exception as exc:
                    self.ctx.log.debug(f"WORKER: finalize of orphan #{row['id']} failed: {exc}")
            if await asyncio.to_thread(self.queue.mark_failed, row["id"], f"Runner '{row['owner_instance']}' lost"):
                failed.append(row["id"])
        if failed:
            self.ctx.log.warning(f"WORKER: marked orphaned job(s) FAILED: {failed}")
        return failed

    def _alive_instances(self) -> set[str] | None:
        """Live instance ids from the core (API ≥ 1.7.1), else ``None`` (heartbeat-only
        sweep). Also ``None`` while the coordinator does not list THIS instance yet —
        its view is not trustworthy right after a (re)start."""
        instances = getattr(self.ctx, "cluster_instances", None)
        if not instances:
            return None
        try:
            alive = {i["instance_id"] for i in instances if i.get("alive")}
        except Exception:
            return None
        if self.ctx.instance_id not in alive:
            return None
        return alive

    def owns(self, job_id: int) -> bool:
        return job_id in self.tasks and not self.tasks[job_id].done()

    async def cancel(self, job_id: int) -> bool:
        task = self.tasks.get(job_id)
        if not task or task.done():
            return False
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass
        return True
