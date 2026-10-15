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
from datetime import datetime, timedelta

from sqlalchemy import inspect as sa_inspect, select, text, update

from core.api import db_instance

from ..model.models import IaCJob, IaCJobLog, Base

log = logging.getLogger("IaC:Queue")

STATUS_QUEUED = "QUEUED"
STATUS_RUNNING = "RUNNING"
STATUS_ABORTED = "ABORTED"
STATUS_FAILED = "FAILED"

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

    def _session(self):
        if not db_instance.is_connected or not db_instance.SessionLocal:
            return None
        return db_instance.SessionLocal()

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
            }
            if engine.dialect.name == "sqlite":
                wanted = {k: v.replace("LONGTEXT", "TEXT").replace("VARCHAR(255)", "TEXT").replace("VARCHAR(100)", "TEXT")
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
                select(IaCJob.dedupe_key).where(IaCJob.status.in_((STATUS_QUEUED, STATUS_RUNNING)))
            ).all()
            return {r[0] for r in rows if r[0]}
        finally:
            if own:
                session.close()

    def enqueue(self, payload: dict, *, requested_by: str | None = None) -> int:
        """Insert a QUEUED job for ``payload``; raises :class:`DuplicateJob` on a
        concurrency collision. Returns the job id (-1 without a database)."""
        payload = normalize_payload(payload)
        key = dedupe_key_for(payload)
        session = self._session()
        if not session:
            return -1
        try:
            if key and _conflicts(key, self.active_keys(session)):
                existing = session.execute(
                    select(IaCJob.id).where(IaCJob.dedupe_key == key,
                                            IaCJob.status.in_((STATUS_QUEUED, STATUS_RUNNING)))
                    .order_by(IaCJob.id.desc())
                ).first()
                raise DuplicateJob(key, existing[0] if existing else None)
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
            return int(job.id)
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
        """Atomically move the oldest QUEUED job to RUNNING owned by ``instance_id``.

        Two runners may race for the same row: the conditional UPDATE lets only
        one win (``rowcount == 1``); the loser retries with the next candidate.
        """
        for _ in range(5):
            session = self._session()
            if not session:
                return None
            try:
                cand = session.execute(
                    select(IaCJob.id).where(IaCJob.status == STATUS_QUEUED).order_by(IaCJob.id).limit(1)
                ).first()
                if not cand:
                    return None
                job_id = int(cand[0])
                now = datetime.now()
                res = session.execute(
                    update(IaCJob)
                    .where(IaCJob.id == job_id, IaCJob.status == STATUS_QUEUED)
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

    # --------------------------------------------------------------- heartbeat
    def heartbeat(self, instance_id: str, job_ids: list[int]) -> None:
        if not job_ids:
            return
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
        except Exception as exc:
            session.rollback()
            log.debug("QUEUE: heartbeat failed: %s", exc)
        finally:
            session.close()

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
            return res.rowcount == 1
        except Exception as exc:
            session.rollback()
            log.debug("QUEUE: mark_failed failed: %s", exc)
            return False
        finally:
            session.close()

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
    def abort_queued(self) -> list[int]:
        """QUEUED → ABORTED (nothing is executing yet)."""
        session = self._session()
        if not session:
            return []
        try:
            ids = [int(r[0]) for r in session.execute(select(IaCJob.id).where(IaCJob.status == STATUS_QUEUED)).all()]
            if ids:
                session.execute(
                    update(IaCJob).where(IaCJob.id.in_(ids), IaCJob.status == STATUS_QUEUED)
                    .values(status=STATUS_ABORTED, end_time=datetime.now(), current_step="Aborted by User")
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
        session = self._session()
        if not session or job_id in (None, -1):
            return
        try:
            session.execute(
                update(IaCJob).where(IaCJob.id == job_id).values(runners=json.dumps(runners, default=str))
            )
            session.commit()
        except Exception as exc:
            session.rollback()
            log.debug("QUEUE: set_runners failed: %s", exc)
        finally:
            session.close()

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
        """``{"active_tasks": {...}, "is_running": bool, "queued": n}`` for the SSE
        snapshot — the same shape the in-memory state had, now from the database
        so every instance shows the same picture."""
        session = self._session()
        if not session:
            return {"active_tasks": {}, "is_running": False, "queued": 0}
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
            return {"active_tasks": active, "is_running": n_running > 0, "queued": n_queued}
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
