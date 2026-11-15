"""Blocking work off the event loop — in the plugin's OWN thread pool.

Every database, Vault and large-file call of this plugin is synchronous
(SQLAlchemy/pymysql, hvac, PyYAML). On the event loop one such call against a
connection whose peer went silent froze the whole instance, the core's cluster
heartbeat included: the runner was reported dead and every instance showed "No
runner instance online" (prod, 2026-11-11, 16 minutes).

``asyncio.to_thread`` alone only moves the problem: it uses the loop's default
executor (``min(32, cpus + 4)`` threads — six on a two-core runner), which the
core's own heartbeat and lease writes use as well. A handful of hanging plugin
calls would starve it and take the instance out the same way. So:

* :func:`off_loop` runs a call in a pool that belongs to this plugin — a hang
  can stall the plugin's own off-loop work, never the core's;
* :class:`CoalescedWriter` bounds the fire-and-forget writers (progress per
  Ansible task line, runner snapshot per second): at most ONE write in flight
  per job, newer values replace the waiting one, the last value is always
  written.
"""
from __future__ import annotations

import asyncio
import contextvars
import functools
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable

log = logging.getLogger("IaC:OffLoop")

#: Threads for the plugin's blocking calls. Enough for three parallel jobs (log
#: writer, progress, runner snapshot and the stage's own call each) plus the
#: worker/heartbeat/sweep loops; small enough that a database that hangs cannot
#: take more than this many pooled connections with it.
POOL_SIZE = 12

_pool: ThreadPoolExecutor | None = None
_pool_guard = threading.Lock()


def _executor() -> ThreadPoolExecutor:
    global _pool
    with _pool_guard:
        if _pool is None:
            _pool = ThreadPoolExecutor(max_workers=POOL_SIZE, thread_name_prefix="iac-offloop")
        return _pool


def shutdown_pool() -> None:
    """Let go of the pool (plugin teardown). Calls that are queued or running
    still finish; a late caller simply gets a fresh pool."""
    global _pool
    with _pool_guard:
        pool, _pool = _pool, None
    if pool is not None:
        pool.shutdown(wait=False)


async def off_loop(fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Any:
    """``await off_loop(fn, *args, **kwargs)`` — like ``asyncio.to_thread`` (the
    context variables travel along, a cancellation of the awaiting task does not
    stop the call), but in the plugin's own pool."""
    loop = asyncio.get_running_loop()
    call = functools.partial(contextvars.copy_context().run, fn, *args, **kwargs)
    return await loop.run_in_executor(_executor(), call)


def on_loop_thread() -> bool:
    """Is the calling thread running an event loop right now?"""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return False
    return True


def merge_latest(_old: Any, new: Any) -> Any:
    return new


class CoalescedWriter:
    """Latest-wins writer per key (job id), one write in flight per key.

    ``push(key, value)`` never blocks and never waits: it stores the value and
    makes sure a drain task is running. While a write is in flight, later
    pushes are merged into ONE waiting value (``merge(waiting, new)``; default:
    the newer one replaces it) — a job that prints a thousand task lines while
    the database hangs costs one thread and one connection, not a thousand.
    The value that is waiting when the pushes stop is always written.

    Ordering: all writes of one key are serialised by a per-key lock.
    :meth:`exclusive` gives a caller that lock — the terminal write of a job
    takes it, writes what is still waiting and closes the row, so no progress
    write can land after it; :meth:`seal` then drops whatever is pushed later.
    """

    #: Sealed keys that are remembered (job ids are never reused; the cap only
    #: keeps a long-lived runner from collecting them for ever).
    _SEALED_MAX = 4096

    def __init__(self, write: Callable[[Any, Any], Any], *, merge: Callable[[Any, Any], Any] = merge_latest,
                 name: str = "writer"):
        self._write = write
        self._merge = merge
        self._name = name
        self._guard = threading.Lock()          # protects the dicts below
        self._waiting: dict[Any, Any] = {}
        self._io: dict[Any, threading.RLock] = {}
        self._sealed: dict[Any, None] = {}
        self._drains: dict[Any, asyncio.Task] = {}

    # -- producers ------------------------------------------------------------------
    def push(self, key: Any, value: Any) -> None:
        with self._guard:
            if key in self._sealed:
                return
            self._waiting[key] = self._merge(self._waiting[key], value) if key in self._waiting else value
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # Called from a worker thread (or sync code without a loop): this
            # thread may block, so it writes itself.
            self.write_waiting(key)
            return
        drain = self._drains.get(key)
        if drain is None or drain.done():
            # Not ctx.create_task: the core cancels those when the plugin is
            # disposed, and the step "detached at stage i/n" is pushed exactly then.
            self._drains[key] = loop.create_task(self._drain(key), name=f"iac:{self._name}:{key}")

    async def _drain(self, key: Any) -> None:
        try:
            while True:
                with self._guard:
                    if key not in self._waiting:
                        return
                await off_loop(self.write_waiting, key)
        except Exception as exc:  # a failed write must never end as an unretrieved task error
            log.debug("%s for %s stopped: %s", self._name, key, exc)

    # -- writers (synchronous: call them in a thread) --------------------------------
    def _io_lock(self, key: Any) -> threading.RLock:
        with self._guard:
            lock = self._io.get(key)
            if lock is None:
                lock = self._io[key] = threading.RLock()
            return lock

    def write_waiting(self, key: Any) -> Any:
        """Write the waiting value of ``key`` (``None`` when there is none)."""
        with self._io_lock(key):
            with self._guard:
                if key not in self._waiting:
                    return None
                value = self._waiting.pop(key)
            return self._write(key, value)

    def exclusive(self, key: Any) -> threading.RLock:
        """The lock every write of ``key`` holds — ``with writer.exclusive(job):``
        runs after the write in flight and before any later one."""
        return self._io_lock(key)

    def seal(self, key: Any) -> None:
        """Nothing is written for ``key`` any more (its job is closed)."""
        with self._guard:
            self._waiting.pop(key, None)
            self._sealed[key] = None
            while len(self._sealed) > self._SEALED_MAX:
                self._sealed.pop(next(iter(self._sealed)))
            self._io.pop(key, None)

    def take(self, key: Any) -> Any:
        """Remove and return the waiting value of ``key`` (``None``: none) — for
        a caller that holds :meth:`exclusive` and writes it itself."""
        with self._guard:
            return self._waiting.pop(key, None)

    def discard(self, key: Any) -> None:
        """Drop the waiting value of ``key`` (it was superseded)."""
        self.take(key)

    # -- consumers --------------------------------------------------------------------
    def keys(self) -> list:
        return list(self._drains)

    async def flush(self, key: Any, timeout: float | None = None) -> bool:
        """Wait until nothing is waiting or in flight for ``key``. ``False`` when
        ``timeout`` ran out first (the write goes on in its thread)."""
        drain = self._drains.get(key)
        if drain is None:
            return True
        if not drain.done():
            _done, pending = await asyncio.wait({drain}, timeout=timeout)
            if pending:
                return False
        if self._drains.get(key) is drain:
            self._drains.pop(key, None)
        return True
