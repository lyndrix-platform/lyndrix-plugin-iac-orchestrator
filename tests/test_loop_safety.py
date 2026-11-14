"""OS-11 (+ OC-08 B1/B5): unauthenticated and per-second paths never block the
event loop — the webhook token check, the public health, the CI status poll and
the SSE snapshot run off the loop and are cached/shared."""
import asyncio
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from iac_plugin.app.controller import api as api_mod

TOKEN = "dummy-webhook-token-not-real"


class _SlowVault:
    """ctx whose Vault read takes ``delay`` seconds (a slow or hanging Vault)."""

    def __init__(self, token=TOKEN, delay=0.3):
        self.token, self.delay, self.reads = token, delay, 0
        self.log = MagicMock()

    def get_secret(self, key):
        self.reads += 1
        time.sleep(self.delay)
        return self.token if key == "gitlab_webhook_token" else None


async def _max_loop_lag(coro):
    """Run ``coro`` next to a 10 ms ticker; return (result, worst tick delay)."""
    lags, stop = [], asyncio.Event()

    async def ticker():
        while not stop.is_set():
            t = time.monotonic()
            await asyncio.sleep(0.01)
            lags.append(time.monotonic() - t - 0.01)

    tick = asyncio.create_task(ticker())
    try:
        result = await coro
    finally:
        stop.set()
        await tick
    return result, max(lags or [0.0])


def test_token_check_is_cached_and_off_the_loop(monkeypatch):
    ctx = _SlowVault()
    monkeypatch.setattr(api_mod, "_ctx", ctx)

    async def scenario():
        async def many():
            for _ in range(20):
                await api_mod._require_gitlab_token(TOKEN)

        return await _max_loop_lag(many())

    _, lag = asyncio.run(scenario())
    assert ctx.reads == 1                         # 20 requests, one Vault read
    assert lag < 0.2                              # … and that one did not stall the loop (0.3 s read)


def test_bogus_tokens_cost_at_most_one_read_per_interval(monkeypatch):
    ctx = _SlowVault(delay=0)
    monkeypatch.setattr(api_mod, "_ctx", ctx)

    async def hammer():
        for bad in ["x", "", None, "wrong-token-not-real", "tökén-nön-ascii"] * 20:
            with pytest.raises(HTTPException) as exc:
                await api_mod._require_gitlab_token(bad)
            assert exc.value.status_code == 401   # a non-ASCII header is a 401, not a 500

    asyncio.run(hammer())
    assert ctx.reads <= 2                         # initial read + one forced re-read, not 100
    assert api_mod.webhook_auth_failures["count"] == 100 and api_mod.webhook_auth_failures["last_at"]


def test_rotated_token_is_picked_up_without_waiting_for_the_ttl(monkeypatch):
    ctx = _SlowVault(delay=0)
    monkeypatch.setattr(api_mod, "_ctx", ctx)

    async def scenario():
        await api_mod._require_gitlab_token(TOKEN)
        ctx.token = "dummy-rotated-token-not-real"       # rotated on another instance
        await api_mod._require_gitlab_token("dummy-rotated-token-not-real")
        with pytest.raises(HTTPException):
            await api_mod._require_gitlab_token(TOKEN)   # the old one is out at once

    asyncio.run(scenario())


def test_missing_token_fails_closed(monkeypatch):
    monkeypatch.setattr(api_mod, "_ctx", _SlowVault(token=None, delay=0))
    with pytest.raises(HTTPException) as exc:
        asyncio.run(api_mod._require_gitlab_token("anything"))
    assert exc.value.status_code == 500


def _slow_engine(delay=0.3):
    calls = {"recent": 0, "latest": 0}

    def recent(limit=20):
        calls["recent"] += 1
        time.sleep(delay)
        return [{"id": 1, "status": "RUNNING", "current_step": "x"}]

    def latest(pipeline_type, since_epoch=0):
        calls["latest"] += 1
        time.sleep(delay)
        return {"id": 7, "status": "RUNNING", "progress": 10, "current_step": "x"}

    return SimpleNamespace(db=SimpleNamespace(get_recent_jobs=recent, get_latest_job_for_pipeline_type=latest),
                           state={}), calls


def test_public_health_is_minimal_cached_and_off_the_loop(monkeypatch):
    engine, calls = _slow_engine()
    monkeypatch.setattr(api_mod, "_ctx", MagicMock())
    monkeypatch.setattr(api_mod, "_engine", engine)
    monkeypatch.setattr(api_mod, "_service", SimpleNamespace(runner_online=lambda: True))

    async def scenario():
        async def many():
            return [await api_mod.do_health() for _ in range(10)]

        return await _max_loop_lag(many())

    results, lag = asyncio.run(scenario())
    assert results[0] == {"status": "healthy", "db_connected": True, "runner_online": True}
    assert calls["recent"] == 1 and lag < 0.2     # no version, instance id or queue depth; one probe


def test_ci_status_poll_and_job_list_stay_off_the_loop(monkeypatch):
    engine, calls = _slow_engine()
    monkeypatch.setattr(api_mod, "_ctx", MagicMock())
    monkeypatch.setattr(api_mod, "_engine", engine)
    monkeypatch.setattr(api_mod, "_service", SimpleNamespace(runner_state=lambda: "online"))

    async def scenario():
        async def both():
            status = await api_mod.do_get_service_status("aac-x")
            jobs = await api_mod.do_list_jobs(10 ** 9)
            return status, jobs

        return await _max_loop_lag(both())

    (status, jobs), lag = asyncio.run(scenario())
    assert status["job_id"] == 7 and len(jobs) == 1 and lag < 0.2


def test_job_list_and_log_tail_are_clamped(monkeypatch):
    seen = {}
    db = SimpleNamespace(get_recent_jobs=lambda limit: seen.setdefault("limit", limit) and [])
    queue = SimpleNamespace(has_log=lambda job_id: True,
                            read_log=lambda job_id, **kw: seen.setdefault("tail", kw["tail"]) and
                            {"lines": [], "offset": 0, "size": 0})
    monkeypatch.setattr(api_mod, "_engine", SimpleNamespace(db=db))
    monkeypatch.setattr(api_mod, "_service", SimpleNamespace(queue=queue, runner_state=lambda: "online"))
    asyncio.run(api_mod.do_list_jobs(100000000))
    asyncio.run(api_mod.do_job_logs(1, tail=10 ** 9))
    assert seen == {"limit": 500, "tail": 5000}


def test_sse_streams_share_one_snapshot_per_second_built_off_the_loop(monkeypatch):
    """p08: every open dashboard tab used to run 5 sync queries per second on the loop."""
    engine, calls = _slow_engine(delay=0.3)
    monkeypatch.setattr(api_mod, "_ctx", MagicMock())
    monkeypatch.setattr(api_mod, "_engine", engine)
    monkeypatch.setattr(api_mod, "_service", SimpleNamespace(queue=None, runner_state=lambda: "online"))
    real_sleep = asyncio.sleep

    async def quick_tick(seconds):                # the stream's 1 s tick, shortened
        await real_sleep(0.02 if seconds >= 1 else seconds)

    monkeypatch.setattr(api_mod.asyncio, "sleep", quick_tick)

    class _Req:
        async def is_disconnected(self):
            return False

    async def client(frames):
        resp = await api_mod.stream_jobs(_Req(), None, api_mod.issue_stream_ticket("api:read"))
        it = resp.body_iterator
        for _ in range(frames):
            await it.__anext__()
        await it.aclose()

    async def scenario():
        return await _max_loop_lag(asyncio.gather(*(client(3) for _ in range(5))))

    _, lag = asyncio.run(scenario())
    assert calls["recent"] <= 3                   # 5 tabs × 3 frames, but one snapshot per (shortened) window
    assert lag < 0.2                             # the 0.3 s query never ran on the loop
