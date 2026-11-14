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
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable

from sqlalchemy import func as sa_fn, inspect as sa_inspect, select, text, update

from core.api import db_instance

from ..model.models import IaCJob, IaCJobLog, IaCLock, Base

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


def _naive(value):
    """A database datetime without tzinfo (drivers differ)."""
    return value.replace(tzinfo=None) if isinstance(value, datetime) else value


def _seconds_between(later, earlier) -> float | None:
    if not isinstance(later, datetime) or not isinstance(earlier, datetime):
        return None
    return (_naive(later) - _naive(earlier)).total_seconds()


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

    def __init__(self):
        # job id -> claim token of the jobs THIS process executes. The service
        # shares one dict between the queue and the JobDatabase; while a token is
        # registered, the runner-side writes for that job are conditional on it.
        self.claims: dict[int, str] = {}

    def _token_cond(self, job_id: int) -> list:
        token = self.claims.get(job_id)
        return [IaCJob.claim_token == token] if token else []

    @staticmethod
    def _utc_now(session):
        """SQL expression for "now" on the database clock in UTC. Heartbeats are
        written and aged with it, so neither the clocks/time zones of the
        instances nor a DST switch can make a live job look stale."""
        name = getattr(session.get_bind().dialect, "name", "")
        return sa_fn.utc_timestamp() if name in ("mysql", "mariadb") else sa_fn.current_timestamp()

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
                # fencing (claim token + claimant incarnation)
                "claim_token": "VARCHAR(64) NULL",
                "owner_incarnation": "VARCHAR(64) NULL",
                # heartbeat on the DATABASE clock in UTC (orphan sweep, audit OC-09/OC-18)
                "heartbeat_utc": "DATETIME NULL",
                # stage progress for restart recovery
                "exec_state": "LONGTEXT NULL",
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
        # The single point where a payload becomes a job row: nothing unvalidated
        # is ever stored (audit OS-09). Raises InvalidJobPayload.
        from .job_payload import validate_structure

        payload = validate_structure(payload)
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
    def claim_next(self, instance_id: str, incarnation: str | None = None) -> "Claim | None":
        """Atomically move the oldest QUEUED job — or a SCHEDULED job whose
        ``not_before`` is reached — to RUNNING owned by ``instance_id``.

        Two runners may race for the same row: the conditional UPDATE lets only
        one win (``rowcount == 1``); the loser retries with the next candidate.
        Due SCHEDULED jobs come first (``not_before`` ascending), then QUEUED by id.
        ``not_before`` stays on the claimed row so the policy stage knows the job
        is due and does not park it again.

        A claim is either returned to the caller or not made (audit OC-03): the
        payload is read *with* the candidate, so nothing has to be read after the
        COMMIT, and when the COMMIT itself fails on the client (lost ack, 2013
        after the server applied it) a fresh session checks the claim token — a
        row that carries it is ours and is returned, never left behind as
        "Claimed by …" with nobody executing it.
        """
        for _ in range(5):
            session = self._session()
            if not session:
                return None
            job_id, payload, token, update_sent = None, {}, uuid.uuid4().hex, False
            try:
                now = datetime.now()
                cand = session.execute(
                    select(IaCJob.id, IaCJob.payload)
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
                try:
                    payload = json.loads(cand[1]) if cand[1] else {}
                except Exception:
                    payload = {}
                update_sent = True
                res = session.execute(
                    update(IaCJob)
                    .where(IaCJob.id == job_id, IaCJob.status.in_((STATUS_QUEUED, STATUS_SCHEDULED)))
                    .values(status=STATUS_RUNNING, owner_instance=instance_id, owner_incarnation=incarnation,
                            claim_token=token, claimed_at=now, heartbeat_at=now,
                            heartbeat_utc=self._utc_now(session),
                            current_step=f"Claimed by {instance_id}")
                )
                session.commit()
                if res.rowcount != 1:
                    continue  # a peer won this row — try the next one
                return Claim(job_id, payload, token)
            except Exception as exc:
                try:
                    session.rollback()
                except Exception:
                    pass
                if update_sent and job_id is not None and self._claim_is_mine(job_id, token):
                    log.warning("QUEUE: claim of job #%s was committed although the client saw '%s' — keeping it.",
                                job_id, exc)
                    return Claim(job_id, payload, token)
                if _is_retryable(exc):
                    time.sleep(0.1)
                    continue
                log.error("QUEUE: claim failed: %s", exc)
                return None
            finally:
                session.close()
        return None

    def _claim_is_mine(self, job_id: int, token: str) -> bool:
        """Does the row carry ``token`` (fresh session)? Retries briefly: the
        connection that just failed may need a moment (VIP move, pool recycle)."""
        for attempt in range(3):
            session = None
            try:
                session = self._session()
                if not session:
                    return False
                row = session.execute(
                    select(IaCJob.status, IaCJob.claim_token).where(IaCJob.id == job_id)
                ).first()
                return bool(row) and row[0] == STATUS_RUNNING and row[1] == token
            except Exception as exc:
                log.debug("QUEUE: claim re-check for #%s failed (%s), attempt %s", job_id, exc, attempt + 1)
                time.sleep(0.2 * (attempt + 1))
            finally:
                if session is not None:
                    session.close()
        # Unknown: the worker's unattended-claim check (own_unattended) recovers
        # the row if the UPDATE did land.
        return False

    def own_unattended(self, instance_id: str, incarnation: str | None, attended: set[int],
                       older_than_seconds: float) -> list[dict]:
        """RUNNING rows claimed by THIS incarnation that nothing here executes
        (``attended`` = ids of running job tasks and re-attached jobs) and whose
        claim is older than ``older_than_seconds``: ``[{id, claim_token, has_log}]``."""
        if not incarnation:
            return []
        session = self._session()
        if not session:
            return []
        try:
            cutoff = datetime.now() - timedelta(seconds=older_than_seconds)
            rows = session.execute(
                select(IaCJob.id, IaCJob.claim_token, IaCJob.claimed_at)
                .where(IaCJob.status == STATUS_RUNNING, IaCJob.owner_instance == instance_id,
                       IaCJob.owner_incarnation == incarnation)
            ).all()
            out = []
            for job_id, token, claimed_at in rows:
                if int(job_id) in attended:
                    continue
                if claimed_at is not None and claimed_at.replace(tzinfo=None) > cutoff:
                    continue
                has_log = session.execute(
                    select(IaCJobLog.id).where(IaCJobLog.job_id == job_id).limit(1)).first() is not None
                out.append({"id": int(job_id), "claim_token": token, "has_log": has_log})
            return out
        finally:
            session.close()

    def release(self, job_id: int, token: str | None, step: str) -> bool:
        """RUNNING → QUEUED for a claim that never started executing (conditional
        on the claim token): the job runs later instead of being lost."""
        session = self._session()
        if not session:
            return False
        try:
            cond = [IaCJob.id == job_id, IaCJob.status == STATUS_RUNNING]
            cond.append(IaCJob.claim_token == token if token else IaCJob.claim_token.is_(None))
            res = session.execute(
                update(IaCJob).where(*cond).values(
                    status=STATUS_QUEUED, owner_instance=None, owner_incarnation=None, claim_token=None,
                    claimed_at=None, heartbeat_at=None, heartbeat_utc=None, progress=0,
                    current_step=str(step)[:250])
            )
            session.commit()
            return res.rowcount == 1
        except Exception as exc:
            session.rollback()
            log.error("QUEUE: release of job #%s failed: %s", job_id, exc)
            return False
        finally:
            session.close()

    def previous_incarnation_jobs(self, instance_id: str | None, incarnation: str | None) -> list[dict]:
        """RUNNING rows this instance id owns (or legacy rows without an owner)
        that were NOT claimed by the current incarnation — what a restarted
        runner has to reconcile. Never a job the current worker claimed."""
        session = self._session()
        if not session:
            return []
        try:
            rows = session.execute(
                select(IaCJob.id, IaCJob.owner_instance, IaCJob.owner_incarnation, IaCJob.claim_token)
                .where(IaCJob.status == STATUS_RUNNING)
            ).all()
            return [
                {"id": int(job_id), "claim_token": token, "owner_incarnation": inc}
                for job_id, owner, inc, token in rows
                if owner in (None, "", instance_id) and (not inc or inc != incarnation)
            ]
        finally:
            session.close()

    def adopt(self, job_id: int, instance_id: str | None, incarnation: str | None,
              old_token: str | None) -> str | None:
        """Take over a RUNNING row of a previous incarnation: new claim token and
        incarnation, conditional on the token the row had when it was listed.
        Returns the new token, ``None`` when the row changed meanwhile."""
        token = uuid.uuid4().hex
        session = self._session()
        if not session:
            return None
        try:
            now = datetime.now()
            cond = [IaCJob.id == job_id, IaCJob.status == STATUS_RUNNING]
            cond.append(IaCJob.claim_token == old_token if old_token else IaCJob.claim_token.is_(None))
            res = session.execute(
                update(IaCJob).where(*cond).values(
                    owner_instance=instance_id, owner_incarnation=incarnation, claim_token=token, heartbeat_at=now,
                    heartbeat_utc=self._utc_now(session))
            )
            session.commit()
            return token if res.rowcount == 1 else None
        except Exception as exc:
            session.rollback()
            log.error("QUEUE: adopt of job #%s failed: %s", job_id, exc)
            return None
        finally:
            session.close()

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
                cond = [IaCJob.id == job_id, IaCJob.status == STATUS_RUNNING, *self._token_cond(job_id)]
                if instance_id:
                    cond.append((IaCJob.owner_instance == instance_id) | (IaCJob.owner_instance.is_(None)))
                res = session.execute(
                    update(IaCJob).where(*cond).values(
                        status=STATUS_SCHEDULED, not_before=due,
                        window_ci_ids=json.dumps(list(window_ci_ids or [])),
                        schedule_reason=str(reason or "")[:250],
                        owner_instance=None, owner_incarnation=None, claim_token=None,
                        claimed_at=None, heartbeat_at=None, heartbeat_utc=None,
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
                    update(IaCJob).where(IaCJob.id == job_id, *self._token_cond(job_id))
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

    # ------------------------------------------------------- named locks (OC-01)
    def acquire_lock(self, key: str, job_id: int, owner: str | None = None) -> tuple[bool, int | None]:
        """Try to take lock ``key`` for ``job_id``. Returns ``(acquired, holder)``.

        Atomic by the primary key: the INSERT wins or fails. A lock whose holder
        job is no longer RUNNING is stale and taken over with a conditional
        UPDATE (only one of several waiters succeeds). Re-entrant for the job
        that already holds it.
        """
        from sqlalchemy.exc import IntegrityError

        session = self._session()
        if not session:
            return False, None
        try:
            try:
                session.add(IaCLock(lock_key=key, job_id=int(job_id), token=uuid.uuid4().hex, owner_instance=owner))
                session.commit()
                return True, int(job_id)
            except IntegrityError:
                session.rollback()
            row = session.execute(
                select(IaCLock.job_id, IaCLock.token).where(IaCLock.lock_key == key)
            ).first()
            if row is None:
                return False, None            # released in between — the caller polls again
            holder, holder_token = int(row[0]), row[1]
            if holder == int(job_id):
                return True, holder
            status = session.execute(select(IaCJob.status).where(IaCJob.id == holder)).scalar()
            if status == STATUS_RUNNING:
                return False, holder
            res = session.execute(
                update(IaCLock).where(IaCLock.lock_key == key, IaCLock.job_id == holder, IaCLock.token == holder_token)
                .values(job_id=int(job_id), token=uuid.uuid4().hex, owner_instance=owner, acquired_at=datetime.now())
            )
            session.commit()
            if res.rowcount == 1:
                log.info("QUEUE: lock '%s' taken over from finished job #%s by job #%s", key, holder, job_id)
                return True, int(job_id)
            return False, holder
        except Exception as exc:
            session.rollback()
            log.error("QUEUE: acquire_lock('%s') failed: %s", key, exc)
            raise
        finally:
            session.close()

    def release_lock(self, key: str, job_id: int) -> bool:
        """Give lock ``key`` back (only when ``job_id`` holds it)."""
        session = self._session()
        if not session:
            return False
        try:
            n = session.query(IaCLock).filter(IaCLock.lock_key == key, IaCLock.job_id == int(job_id)).delete(
                synchronize_session=False)
            session.commit()
            return bool(n)
        except Exception as exc:
            session.rollback()
            log.debug("QUEUE: release_lock('%s') failed: %s", key, exc)
            return False
        finally:
            session.close()

    def lock_holder(self, key: str) -> int | None:
        session = self._session()
        if not session:
            return None
        try:
            val = session.execute(select(IaCLock.job_id).where(IaCLock.lock_key == key)).scalar()
            return int(val) if val is not None else None
        finally:
            session.close()

    def job_status(self, job_id: int) -> str | None:
        session = self._session()
        if not session:
            return None
        try:
            return session.execute(select(IaCJob.status).where(IaCJob.id == job_id)).scalar()
        finally:
            session.close()

    # --------------------------------------------------------------- heartbeat
    def heartbeat(self, instance_id: str, job_ids: list[int]) -> list[int]:
        """Refresh the heartbeat of the jobs this process executes. Returns the ids
        whose row is no longer ours (closed by the orphan sweep or an abort,
        adopted by someone else): the caller must stop working on them — its
        writes for those jobs are no-ops anyway (fencing). ``[]`` on a database
        error: unknown is not fenced."""
        if not job_ids:
            return []

        def _tx():
            session = self._session()
            if not session:
                return []
            try:
                now = datetime.now()
                fenced = []
                for job_id in job_ids:
                    res = session.execute(
                        update(IaCJob)
                        .where(IaCJob.id == job_id, IaCJob.owner_instance == instance_id,
                               IaCJob.status == STATUS_RUNNING, *self._token_cond(job_id))
                        .values(heartbeat_at=now, heartbeat_utc=self._utc_now(session))
                    )
                    if res.rowcount != 1:
                        fenced.append(int(job_id))
                session.commit()
                return fenced
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()

        try:
            return list(self._retry(_tx) or [])
        except Exception as exc:
            log.debug("QUEUE: heartbeat failed: %s", exc)
            return []

    #: runner snapshot statuses that mean "a container of this job may still run".
    _LIVE_RUNNER_STATUSES = ("pulling_image", "running_ansible", "running_terraform")

    def sweep_candidates(self) -> list[dict]:
        """RUNNING jobs with an owner, with their ages on the database clock —
        the facts the orphan sweep decides on (it never asks a runner's Docker
        socket): ``[{id, owner_instance, owner_incarnation, claim_token,
        heartbeat_utc, hb_age, log_age, has_container, current_step}]``.
        ``log_age`` is ``None`` for a job without log lines. Rows without a UTC
        heartbeat (claimed by a plugin version before the sweep) are skipped."""
        session = self._session()
        if not session:
            return []
        try:
            db_utc = session.execute(select(self._utc_now(session))).scalar()
            db_local = session.execute(select(sa_fn.now())).scalar()
            rows = session.execute(
                select(IaCJob.id, IaCJob.owner_instance, IaCJob.owner_incarnation, IaCJob.claim_token,
                       IaCJob.heartbeat_utc, IaCJob.runners, IaCJob.current_step)
                .where(IaCJob.status == STATUS_RUNNING, IaCJob.owner_instance.isnot(None),
                       IaCJob.heartbeat_utc.isnot(None))
            ).all()
            out = []
            for job_id, owner, incarnation, token, hb, runners_raw, step in rows:
                last_log = session.execute(
                    select(sa_fn.max(IaCJobLog.ts)).where(IaCJobLog.job_id == job_id)).scalar()
                try:
                    runners = json.loads(runners_raw) if runners_raw else {}
                except Exception:
                    runners = {}
                has_container = any(
                    isinstance(info, dict) and info.get("status") in self._LIVE_RUNNER_STATUSES
                    for info in (runners or {}).values()
                )
                out.append({
                    "id": int(job_id), "owner_instance": owner, "owner_incarnation": incarnation,
                    "claim_token": token, "heartbeat_utc": hb,
                    "hb_age": _seconds_between(db_utc, hb),
                    "log_age": _seconds_between(db_local, last_log) if last_log is not None else None,
                    "has_container": has_container, "current_step": step,
                })
            return out
        finally:
            session.close()

    def fail_orphan(self, job_id: int, token: str | None, reason: str, min_hb_age: float) -> bool:
        """RUNNING → FAILED for an orphaned job. Conditional on everything the
        decision was based on: still RUNNING, still the same claim, and no
        heartbeat since (database clock). A runner that came back in between wins;
        a second sweeper finds nothing left to do. Appends the reason to the job
        log and ends the maintenance override through ``on_terminal``."""
        def _tx():
            session = self._session()
            if not session:
                return False
            try:
                db_utc = session.execute(select(self._utc_now(session))).scalar()
                cutoff = _naive(db_utc) - timedelta(seconds=max(0.0, float(min_hb_age)))
                cond = [IaCJob.id == job_id, IaCJob.status == STATUS_RUNNING, IaCJob.heartbeat_utc <= cutoff]
                cond.append(IaCJob.claim_token == token if token else IaCJob.claim_token.is_(None))
                res = session.execute(
                    update(IaCJob).where(*cond)
                    .values(status=STATUS_FAILED, end_time=datetime.now(), current_step=str(reason)[:250])
                )
                session.commit()
                return res.rowcount == 1
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()

        try:
            done = bool(self._retry(_tx))
        except Exception as exc:
            log.warning("QUEUE: fail_orphan(#%s) failed: %s", job_id, exc)
            return False
        if not done:
            return False
        try:
            self.append_log(job_id, [f"[SYSTEM] {reason} — the job was closed as FAILED by the orphan sweep. "
                                     "Nothing is re-run automatically; re-trigger the pipeline."],
                            self.last_seq(job_id))
        except Exception as exc:
            log.debug("QUEUE: orphan log line for #%s not written: %s", job_id, exc)
        if self.on_terminal is not None:
            try:
                self.on_terminal(job_id)
            except Exception as exc:
                log.debug("QUEUE: on_terminal hook failed for #%s: %s", job_id, exc)
        return True

    def note_step(self, job_id: int, token: str | None, step: str) -> bool:
        """Set ``current_step`` of a RUNNING job on behalf of a party that does not
        own it (the sweeper's "runner lost — waiting for re-attach"), conditional
        on the claim it looked at."""
        session = self._session()
        if not session:
            return False
        try:
            cond = [IaCJob.id == job_id, IaCJob.status == STATUS_RUNNING, IaCJob.current_step != str(step)[:250]]
            cond.append(IaCJob.claim_token == token if token else IaCJob.claim_token.is_(None))
            res = session.execute(update(IaCJob).where(*cond).values(current_step=str(step)[:250]))
            session.commit()
            return res.rowcount == 1
        except Exception as exc:
            session.rollback()
            log.debug("QUEUE: note_step failed: %s", exc)
            return False
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
                session.execute(update(IaCJob).where(IaCJob.id == job_id, *self._token_cond(job_id))
                                .values(runners=blob))
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

    def count_task_lines(self, job_id: int, task_name: str) -> int:
        """How many output lines of runner task ``task_name`` the job log already
        holds (lines stored as ``[<task>] …``; the engine's own remarks about a
        task are stored as ``[SYSTEM] [<task>] …`` and do not count). A watcher
        that re-attaches to the container skips exactly that many lines."""
        prefix = f"[{task_name}] "
        session = self._session()
        if not session:
            return 0
        try:
            return int(session.execute(
                select(sa_fn.count(IaCJobLog.id))
                .where(IaCJobLog.job_id == job_id, sa_fn.substr(IaCJobLog.line, 1, len(prefix)) == prefix)
            ).scalar() or 0)
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


class Claim(tuple):
    """``(job_id, payload)`` of a claimed job plus its fencing ``token``."""

    def __new__(cls, job_id: int, payload: dict, token: str):
        obj = super().__new__(cls, (job_id, payload))
        obj.token = token
        return obj


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
    """Runner-side loop: claim → execute → heartbeat; abort relay.

    One per instance with the ``runner`` role. ``run_job`` is the coroutine that
    executes a claimed job (``engine.run_pipeline(payload, job_id=...)``).
    Orphaned jobs are NOT handled here any more: a runner cannot sweep its own
    hang or death — see ``orphan_sweep.OrphanSweeper`` (lease-based, control plane).
    """

    def __init__(self, ctx, queue: JobQueue, run_job, *, max_parallel: int = 3,
                 poll_seconds: float = 2.0, heartbeat_seconds: float = 10.0,
                 incarnation: str | None = None, on_fenced=None, extra_attended=None):
        self.ctx = ctx
        self.queue = queue
        self.run_job = run_job
        self.max_parallel = max(1, int(max_parallel))
        self.poll_seconds = poll_seconds
        self.heartbeat_seconds = heartbeat_seconds
        # Claims are stamped with this incarnation (one per plugin load): rows of
        # an earlier incarnation are never mistaken for jobs this worker runs.
        self.incarnation = incarnation
        # async (job_id) -> None: stop whatever this host still runs for a job that
        # was closed or taken over elsewhere (no status writes).
        self.on_fenced = on_fenced
        # () -> ids of jobs this process attends outside of ``tasks`` (re-attached).
        self.extra_attended = extra_attended
        self.fenced: set[int] = set()
        self._last_unattended_check = time.monotonic()
        self.tasks: dict[int, asyncio.Task] = {}
        self._wake = asyncio.Event()
        self._stopped = False
        self._tick_task: asyncio.Task | None = None
        self._hb_task: asyncio.Task | None = None

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

    async def shutdown(self, budget: float = 10.0) -> None:
        """Clean stop for a plugin reload / draining instance (audit OC-10): stop
        claiming, cancel the job tasks and wait — bounded — until they let go
        (they detach: no terminal status, containers untouched), heartbeat last."""
        self._stopped = True
        self._wake.set()
        if self._tick_task:
            self._tick_task.cancel()
        pending = [t for t in self.tasks.values() if not t.done()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.wait(pending, timeout=max(0.1, budget))
        if self._hb_task:
            self._hb_task.cancel()

    def wake(self, *_args) -> None:
        self._wake.set()

    @property
    def running_ids(self) -> list[int]:
        return [jid for jid, t in self.tasks.items() if not t.done()]

    # -- loops --------------------------------------------------------------
    async def _loop(self) -> None:
        while not self._stopped:
            try:
                self.tasks = {j: t for j, t in self.tasks.items() if not t.done()}
                while len(self.tasks) < self.max_parallel:
                    claimed = await asyncio.to_thread(self.queue.claim_next, self.ctx.instance_id, self.incarnation)
                    if not claimed:
                        break
                    job_id, payload = claimed
                    # From here on the job is either executing (a task below) or
                    # released again by _recover_unattended — never just "claimed".
                    token = getattr(claimed, "token", None)
                    if token:
                        self.queue.claims[job_id] = token
                    self.ctx.log.info(f"WORKER: claimed job #{job_id} ({payload.get('pipeline_type')})")
                    self.tasks[job_id] = self.ctx.create_task(self._run(job_id, payload), name=f"iac:job:{job_id}")
                if time.monotonic() - self._last_unattended_check >= 2 * self.heartbeat_seconds:
                    self._last_unattended_check = time.monotonic()
                    await self._recover_unattended()
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
        finally:
            self.queue.claims.pop(job_id, None)

    def _attended(self) -> set[int]:
        ids = set(self.running_ids)
        if self.extra_attended is not None:
            try:
                ids |= {int(i) for i in self.extra_attended()}
            except Exception as exc:
                self.ctx.log.debug(f"WORKER: extra_attended failed: {exc}")
        return ids

    async def _recover_unattended(self) -> None:
        """A row this incarnation claimed but nothing here executes (the claim's
        result got lost between the database and this loop): give it back to the
        queue when it never started, close it when it did (audit OC-03)."""
        rows = await asyncio.to_thread(
            self.queue.own_unattended, self.ctx.instance_id, self.incarnation,
            self._attended(), 2 * self.heartbeat_seconds,
        )
        for row in rows:
            job_id = row["id"]
            if job_id in self._attended():
                continue
            if not row["has_log"]:
                if await asyncio.to_thread(self.queue.release, job_id, row["claim_token"],
                                           "Re-queued: the claim was lost before the job started"):
                    self.ctx.log.warning(f"WORKER: job #{job_id} was claimed but never started — re-queued.")
                    self.wake()
            elif await asyncio.to_thread(
                    self.queue.mark_failed, job_id, "Runner lost track of the job — re-trigger the pipeline"):
                self.ctx.log.warning(f"WORKER: job #{job_id} is RUNNING without an executing task — closed as FAILED.")

    async def _heartbeat_loop(self) -> None:
        while not self._stopped:
            await asyncio.sleep(self.heartbeat_seconds)
            ids = sorted(self._attended())
            if not ids:
                continue
            try:
                fenced = await asyncio.to_thread(self.queue.heartbeat, self.ctx.instance_id, ids)
            except Exception as exc:
                self.ctx.log.debug(f"WORKER: heartbeat failed: {exc}")
                continue
            still_attended = self._attended()
            for job_id in fenced:
                # A job that finished between collecting the ids and the UPDATE is
                # simply done — only a job we are STILL working on was fenced.
                if job_id in still_attended:
                    await self._handle_fenced(job_id)

    async def _handle_fenced(self, job_id: int) -> None:
        """The row of a job we execute is no longer ours (the orphan sweep failed
        it, an abort closed it, a newer incarnation adopted it): stop working on
        it. Its writes are no-ops by the claim token; only the work has to end."""
        if job_id in self.fenced:
            return
        self.fenced.add(job_id)
        self.ctx.log.warning(f"WORKER: job #{job_id} was closed or taken over elsewhere — stopping it here.")
        await self.cancel(job_id)
        if self.on_fenced is not None:
            try:
                await self.on_fenced(job_id)
            except Exception as exc:
                self.ctx.log.warning(f"WORKER: stopping the containers of fenced job #{job_id} failed: {exc}")

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
