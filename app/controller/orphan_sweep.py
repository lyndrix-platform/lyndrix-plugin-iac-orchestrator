"""Orphan sweep for jobs of lost runners (rD §3, audit OC-09).

A runner cannot notice its own hang or death, so this does not run in the
runner's worker: it runs under the lease ``iac_orchestrator:orphan_sweep`` on a
control-plane instance (a runner only contends while no other instance is alive;
single mode: the only instance).

A RUNNING job is orphaned when **all** of this holds:

a. its owner is *dead* by the core's facts — ``ctx.instance_state()`` (core API
   ≥ 1.14: ``dead``, or alive again under a new boot nonce), else the heartbeat
   view ``ctx.cluster_instances``. ``restarting`` and ``unknown`` are never dead;
b. it made no progress for the grace period: no job heartbeat **and** no log
   line, both aged on the database clock (never the clocks of two hosts);
c. no runner container is reported for it and it holds no lock — the sweeper
   only reads the database, never a runner's Docker socket. The runner snapshot
   is written fire-and-forget and may be stale, so a held ``tf:``/``rules``
   lock counts as a container too. With either, the job only gets the step
   "runner lost — waiting for re-attach" until the (long) container grace is
   over.

Action: FAILED ``runner_lost: <instance> last heartbeat <ts>`` (conditional on
the claim token and the heartbeat it judged), a job-log line, the maintenance
override ends, one ``job_orphaned`` notification. Nothing is re-run. A runner
that comes back finds its writes fenced by the claim token and stops the job's
containers (queue heartbeat → ``JobWorker._handle_fenced``).

**Maximum runtime (review C4).** Those rules never fire for a job whose owner
INSTANCE is alive while the plugin part that executes it is not — plugin
disabled, a new version that does not load, a watcher stuck on ``docker logs
-f``, a runner whose Docker never answers after a restart. Such a job stayed
RUNNING for ever, with its follow-ups waiting and its ``tf:``/``rules`` locks
held. So every RUNNING job is also bounded by ``job_max_runtime_seconds``
(default 6 h, 0 = off), measured on the database clock from its claim:

1. the job is reported (job log, step, ``job_timed_out`` notification) and its
   runner is asked — ``iac:job_abort_requested`` with ``final_status: FAILED`` —
   to stop the containers and close the job. A runner that can, does exactly
   what an abort does and gives the job's locks back once Docker confirmed;
2. if the row is still RUNNING :data:`OVERDUE_STOP_WAIT_SECONDS` later, the sweep
   closes it itself as FAILED ``max_runtime: …`` and says that its containers
   may still be running. Nobody confirmed they are gone, so its locks stay
   reserved for the container grace from that moment (``JobQueue.fail_overdue``)
   — a state lock is never free while a tofu container may still write.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import datetime, timezone

from .offloop import off_loop

log = logging.getLogger("IaC:OrphanSweep")

STATE_KEY = "orphan_sweep"
LEASE_NAME = "iac_orchestrator:orphan_sweep"
WAITING_STEP = "Runner lost — waiting for re-attach"
OVERDUE_STEP = "Maximum runtime exceeded — stopping"
ABORT_TOPIC = "iac:job_abort_requested"
#: Time the runner gets to stop the containers of an overdue job and close it
#: (Terraform stop grace 30 s + two Docker calls of 20 s, with room to spare)
#: before the sweep closes the row itself.
OVERDUE_STOP_WAIT_SECONDS = 120.0


def _fmt_span(seconds: float) -> str:
    minutes = int(max(0, seconds)) // 60
    return f"{minutes // 60}h {minutes % 60:02d}m"


def _lease_ttl() -> int:
    try:
        return max(1, int(os.getenv("LYNDRIX_LEASE_TTL_SECONDS", "30")))
    except ValueError:
        return 30


def owner_state(ctx, owner: str | None, owner_incarnation: str | None) -> str:
    """``alive`` | ``restarting`` | ``dead`` | ``unknown`` for the instance that
    claimed a job. Only ``dead`` lets the sweep act."""
    if not owner:
        return "unknown"
    probe = getattr(ctx, "instance_state", None)
    if callable(probe):
        try:
            info = probe(owner)
        except Exception as exc:
            log.debug("instance_state(%s) failed: %s", owner, exc)
            info = None
        if isinstance(info, dict) and isinstance(info.get("state"), str):
            state = info["state"]
            if state in ("alive", "draining"):
                # Same instance id, other process: the claimant is gone even
                # though "the instance" is alive (fixed ids such as
                # docker-auto-runner). Its successor adopts the rows it finds at
                # boot; one that is still stamped with the old nonce after the
                # grace period was left behind.
                claimed_nonce = str(owner_incarnation or "").split(":", 1)[0]
                current = info.get("incarnation")
                if claimed_nonce and isinstance(current, str) and current and current != claimed_nonce:
                    return "dead"
                return "alive"
            if state in ("restarting", "dead"):
                return state
            return "unknown"
    # Core < 1.14: the heartbeat view. It is only trusted once it lists THIS
    # instance as alive — right after a start it may be empty or fabricated.
    instances = getattr(ctx, "cluster_instances", None)
    if not isinstance(instances, (list, tuple)) or not instances:
        return "unknown"
    try:
        alive = {i["instance_id"] for i in instances if i.get("alive")}
    except Exception:
        return "unknown"
    me = getattr(ctx, "instance_id", None)
    if me not in alive:
        return "unknown"
    return "alive" if owner in alive else "dead"


class OrphanSweeper:
    """Lease-held loop; ``sweep_once`` is the unit under test."""

    def __init__(self, ctx, queue, db, config, *, interval: float = 30.0, notify=None, notify_overdue=None,
                 clock=time.monotonic):
        self.ctx = ctx
        self.queue = queue
        self.db = db
        self.config = config
        self.interval = interval
        self.notify = notify                    # (job_id, reason, row) -> None
        self.notify_overdue = notify_overdue    # (job_id, text, row, closed: bool) -> None
        self.clock = clock
        self.failed_total = 0
        self.timed_out_total = 0
        # job id -> (claim token, when its runner was asked to stop it).
        self._stop_asked: dict[int, tuple[str | None, float]] = {}
        self._task: asyncio.Task | None = None
        self._poke = asyncio.Event()

    # -- lifecycle (lease callbacks) --------------------------------------------------
    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = self.ctx.create_task(self.run(), name="iac:orphan_sweep")
            log.info("Orphan sweep started on this instance.")

    def stop(self) -> None:
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            log.info("Orphan sweep stopped on this instance.")

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    def poke(self) -> None:
        """Run a pass now (``cluster:instance_lost``)."""
        self._poke.set()

    async def run(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self._poke.wait(), timeout=self.interval)
            except asyncio.TimeoutError:
                pass
            self._poke.clear()
            try:
                await self.sweep_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("Orphan sweep pass failed: %s", exc)

    # -- one pass -----------------------------------------------------------------------
    def grace_seconds(self) -> int:
        return max(2 * _lease_ttl(), int(getattr(self.config, "orphan_sweep_grace_seconds", 90)))

    async def sweep_once(self) -> list[int]:
        """One pass. Returns the ids of the jobs this pass closed (orphans and
        jobs over the maximum runtime that their runner did not close)."""
        enabled, max_runtime, sweep_grace, container_grace = await off_loop(lambda: (
            bool(getattr(self.config, "orphan_sweep_enabled", True)),
            int(getattr(self.config, "job_max_runtime_seconds", 0) or 0),
            self.grace_seconds(),
            int(getattr(self.config, "orphan_sweep_container_grace_seconds", 1800))))
        if not enabled and max_runtime <= 0:
            return []
        grace = sweep_grace
        container_grace = max(grace, container_grace)
        candidates = await off_loop(self.queue.sweep_candidates)
        failed: list[int] = []
        timed_out: list[int] = []
        for row in candidates:
            verdict = await self._judge_orphan(row, grace, container_grace) if enabled else None
            if verdict == "failed":
                failed.append(row["id"])
            elif verdict is None and max_runtime > 0 and (row.get("runtime") or 0) >= max_runtime:
                if await self._stop_overdue(row, max_runtime, container_grace):
                    timed_out.append(row["id"])
        # A job that is gone from the candidates was closed (by its runner, by
        # an abort, by this sweep): nothing to remember about it.
        running = {row["id"] for row in candidates}
        for job_id in [j for j in self._stop_asked if j not in running or j in timed_out]:
            self._stop_asked.pop(job_id, None)
        await off_loop(self._record, failed, timed_out)
        return failed + timed_out

    async def _judge_orphan(self, row: dict, grace: float, container_grace: float) -> str | None:
        """``"failed"`` — closed as an orphan; ``"waiting"`` — orphaned, but a
        container may still run (long grace); ``None`` — not an orphan."""
        hb_age, log_age = row["hb_age"], row["log_age"]
        if hb_age is None or hb_age < grace or (log_age is not None and log_age < grace):
            return None                                     # (b) progress within the grace period
        state = owner_state(self.ctx, row["owner_instance"], row["owner_incarnation"])
        if state != "dead":
            return None                                     # (a) alive, restarting or unknown
        if row["has_container"] and hb_age < container_grace:
            # (c) a container may still be running on the runner's host: give
            # the runner time to come back and re-attach.
            await off_loop(self.queue.note_step, row["id"], row["claim_token"], WAITING_STEP)
            return "waiting"
        hb = row["heartbeat_utc"]
        stamp = hb.replace(tzinfo=timezone.utc).isoformat(timespec="seconds") if isinstance(hb, datetime) else "unknown"
        reason = f"runner_lost: {row['owner_instance']} last heartbeat {stamp}"
        if not await off_loop(self.queue.fail_orphan, row["id"], row["claim_token"], reason, grace):
            return "waiting"                                # the runner came back in this very moment
        self.failed_total += 1
        log.warning("Orphan sweep: job #%s closed as FAILED (%s).", row["id"], reason)
        if self.notify is not None:
            try:
                self.notify(row["id"], reason, row)
            except Exception as exc:
                log.debug("orphan notification failed: %s", exc)
        return "failed"

    async def _stop_overdue(self, row: dict, limit: int, container_grace: float) -> bool:
        """A RUNNING job over the maximum runtime (review C4): first ask its runner
        to stop it, close it here when that did not happen. True when THIS call
        closed the row."""
        job_id, token, owner = row["id"], row["claim_token"], row["owner_instance"]
        spans = f"running for {_fmt_span(row['runtime'])}, limit {_fmt_span(limit)}"
        asked = self._stop_asked.get(job_id)
        if asked is None or asked[0] != token:
            self._stop_asked[job_id] = (token, self.clock())
            reason = f"max_runtime: {spans}"
            log.warning("Orphan sweep: job #%s exceeded the maximum runtime (%s) — asking runner '%s' to stop it.",
                        job_id, spans, owner)
            await off_loop(self._announce_overdue, job_id, token, spans, owner)
            self._emit_stop(job_id, reason)
            self._tell(job_id, f"Maximum runtime exceeded ({spans}). Runner '{owner}' was asked to stop the "
                               "job's containers and close it.", row, closed=False)
            return False
        if self.clock() - asked[1] < OVERDUE_STOP_WAIT_SECONDS:
            return False
        reason = f"max_runtime: {spans} — closed without confirmation from runner '{owner}'"
        line = (f"Maximum runtime exceeded ({spans}) and runner '{owner}' did not close the job: closed as FAILED "
                f"by the sweep. Nobody confirmed that its containers were stopped — they MAY STILL BE RUNNING on "
                f"'{owner}'. Locks the job holds (Terraform state, rule engine) stay reserved for up to "
                f"{int(container_grace)}s unless the runner stops the containers earlier. Nothing is re-run "
                "automatically; check the runner host, then re-trigger the pipeline.")
        if not await off_loop(self.queue.fail_overdue, job_id, token, reason, line):
            return False                                    # closed elsewhere in this very moment
        self.timed_out_total += 1
        log.warning("Orphan sweep: job #%s closed as FAILED (%s).", job_id, reason)
        self._tell(job_id, f"Maximum runtime exceeded ({spans}); runner '{owner}' did not answer, the job was "
                           "closed as FAILED. Its containers may still be running on the runner — check the host. "
                           "Nothing is re-run automatically.", row, closed=True)
        return True

    def _announce_overdue(self, job_id: int, token: str | None, spans: str, owner: str | None) -> None:
        """Job-log line and step for an overdue job (sync — runs in a thread)."""
        try:
            self.queue.append_log(job_id, [
                f"[SYSTEM] Maximum runtime exceeded ({spans}) — asking runner '{owner}' to stop the job's "
                "containers and close it as FAILED."], self.queue.last_seq(job_id))
        except Exception as exc:
            log.debug("overdue log line for #%s not written: %s", job_id, exc)
        self.queue.note_step(job_id, token, OVERDUE_STEP)

    def _emit_stop(self, job_id: int, reason: str) -> None:
        """Ask the owning runner (every instance hears it; only the one that
        executes or attends the job acts) to stop the job like an abort, but
        close it as FAILED with ``reason``."""
        emit = getattr(self.ctx, "emit", None)
        if not callable(emit):
            return
        payload = {"job_ids": [job_id], "final_status": "FAILED", "reason": reason}
        try:
            try:
                emit(ABORT_TOPIC, payload, cluster=True)
            except TypeError:                               # older core without the cluster flag
                emit(ABORT_TOPIC, payload)
        except Exception as exc:
            log.warning("Orphan sweep: stop request for job #%s not sent: %s", job_id, exc)

    def _tell(self, job_id: int, text: str, row: dict, *, closed: bool) -> None:
        if self.notify_overdue is None:
            return
        try:
            self.notify_overdue(job_id, text, row, closed)
        except Exception as exc:
            log.debug("max-runtime notification failed: %s", exc)

    def _record(self, failed: list[int], timed_out: list[int] | None = None) -> None:
        """Publish the sweep's state for ``health()`` on every instance."""
        timed_out = timed_out or []
        try:
            previous = (self.db.get_state(STATE_KEY) or {}).get("data") or {}
            total = int(previous.get("orphans_failed_total") or 0) + len(failed)
            self.db.update_state(STATE_KEY, {
                "holder": getattr(self.ctx, "instance_id", None),
                "last_sweep_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "orphans_failed_total": total,
                "last_failed": (failed or previous.get("last_failed") or [])[-20:],
                "timed_out_total": int(previous.get("timed_out_total") or 0) + len(timed_out),
                "last_timed_out": (timed_out or previous.get("last_timed_out") or [])[-20:],
            }, "")
        except Exception as exc:
            log.debug("orphan sweep state not recorded: %s", exc)
