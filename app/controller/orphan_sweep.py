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
c. no runner container is reported for it — the sweeper only reads the job row,
   never a runner's Docker socket. With a container reported the job only gets
   the step "runner lost — waiting for re-attach" until the (long) container
   grace is over.

Action: FAILED ``runner_lost: <instance> last heartbeat <ts>`` (conditional on
the claim token and the heartbeat it judged), a job-log line, the maintenance
override ends, one ``job_orphaned`` notification. Nothing is re-run. A runner
that comes back finds its writes fenced by the claim token and stops the job's
containers (queue heartbeat → ``JobWorker._handle_fenced``).
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone

log = logging.getLogger("IaC:OrphanSweep")

STATE_KEY = "orphan_sweep"
LEASE_NAME = "iac_orchestrator:orphan_sweep"
WAITING_STEP = "Runner lost — waiting for re-attach"


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

    def __init__(self, ctx, queue, db, config, *, interval: float = 30.0, notify=None):
        self.ctx = ctx
        self.queue = queue
        self.db = db
        self.config = config
        self.interval = interval
        self.notify = notify                    # (job_id, reason, row) -> None
        self.failed_total = 0
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
        enabled = await asyncio.to_thread(lambda: bool(getattr(self.config, "orphan_sweep_enabled", True)))
        if not enabled:
            return []
        grace = await asyncio.to_thread(self.grace_seconds)
        container_grace = max(grace, int(await asyncio.to_thread(
            lambda: getattr(self.config, "orphan_sweep_container_grace_seconds", 1800))))
        candidates = await asyncio.to_thread(self.queue.sweep_candidates)
        failed: list[int] = []
        for row in candidates:
            hb_age, log_age = row["hb_age"], row["log_age"]
            if hb_age is None or hb_age < grace or (log_age is not None and log_age < grace):
                continue                                    # (b) progress within the grace period
            state = owner_state(self.ctx, row["owner_instance"], row["owner_incarnation"])
            if state != "dead":
                continue                                    # (a) alive, restarting or unknown
            if row["has_container"] and hb_age < container_grace:
                # (c) a container may still be running on the runner's host: give
                # the runner time to come back and re-attach.
                await asyncio.to_thread(self.queue.note_step, row["id"], row["claim_token"], WAITING_STEP)
                continue
            hb = row["heartbeat_utc"]
            stamp = hb.replace(tzinfo=timezone.utc).isoformat(timespec="seconds") if isinstance(hb, datetime) else "unknown"
            reason = f"runner_lost: {row['owner_instance']} last heartbeat {stamp}"
            if await asyncio.to_thread(self.queue.fail_orphan, row["id"], row["claim_token"], reason, grace):
                failed.append(row["id"])
                self.failed_total += 1
                log.warning("Orphan sweep: job #%s closed as FAILED (%s).", row["id"], reason)
                if self.notify is not None:
                    try:
                        self.notify(row["id"], reason, row)
                    except Exception as exc:
                        log.debug("orphan notification failed: %s", exc)
        await asyncio.to_thread(self._record, failed)
        return failed

    def _record(self, failed: list[int]) -> None:
        """Publish the sweep's state for ``health()`` on every instance."""
        try:
            previous = (self.db.get_state(STATE_KEY) or {}).get("data") or {}
            total = int(previous.get("orphans_failed_total") or 0) + len(failed)
            self.db.update_state(STATE_KEY, {
                "holder": getattr(self.ctx, "instance_id", None),
                "last_sweep_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "orphans_failed_total": total,
                "last_failed": (failed or previous.get("last_failed") or [])[-20:],
            }, "")
        except Exception as exc:
            log.debug("orphan sweep state not recorded: %s", exc)
