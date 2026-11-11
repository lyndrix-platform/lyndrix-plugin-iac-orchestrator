"""Dispatch guard of DeploymentEngine.run_pipeline (1.5.7, port in 1.6.1).

* A job claimed from the queue is already RUNNING in the DB; the rollout guard
  counted that row and blocked itself ("rollout for '<host>' already running",
  ABORTED without logs) — every queued rollout, incl. the ones infra_apply hands
  off for freshly created hosts.
* In-memory dispatch keys must be released whatever happens after reservation.
"""
import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from iac_plugin.app.controller.engine import DeploymentEngine


def _engine(running_jobs, *, execute=None):
    """Engine with only the dispatch-guard state; ``running_jobs`` = [(id, tag)]."""
    eng = DeploymentEngine.__new__(DeploymentEngine)
    eng._pipeline_dispatch_lock = asyncio.Lock()
    eng._active_single_service_keys = set()
    eng._active_terraform_host_keys = set()
    eng._active_rollout_keys = set()
    eng._notify = MagicMock()
    eng.db = MagicMock()
    eng.db.get_jobs_by_status.side_effect = lambda status: [
        SimpleNamespace(id=i, pipeline_type=t) for i, t in running_jobs
    ] if status == "RUNNING" else []
    eng.executed = []

    async def _execute(payload, job_id, pipeline_type):
        # Snapshot the reserved keys while the pipeline "runs".
        eng.executed.append((job_id, pipeline_type, set(eng._active_rollout_keys)
                             | eng._active_single_service_keys | eng._active_terraform_host_keys))
        if execute:
            await execute()

    eng._execute_dispatched = _execute
    return eng


def _no_keys(eng):
    return not (eng._active_rollout_keys or eng._active_single_service_keys
                or eng._active_terraform_host_keys)


def _skipped(eng, job_id):
    return any(c.args[:2] == (job_id, "ABORTED") for c in eng.db.update_job.call_args_list)


def test_claimed_host_rollout_does_not_block_itself():
    """Prod #1353/#1354: rollout:docker-sec claimed (RUNNING) must run, not abort."""
    eng = _engine([(1354, "rollout:docker-sec")])
    asyncio.run(eng.run_pipeline({"pipeline_type": "rollout", "limit": "docker-sec"}, job_id=1354))
    assert eng.executed and eng.executed[0][:2] == (1354, "rollout")
    assert "rollout:docker-sec" in eng.executed[0][2]
    assert not _skipped(eng, 1354)
    assert _no_keys(eng)


def test_claimed_global_rollout_does_not_block_itself():
    eng = _engine([(10, "rollout")])
    asyncio.run(eng.run_pipeline({"pipeline_type": "rollout", "limit": "all"}, job_id=10))
    assert eng.executed and not _skipped(eng, 10)
    assert _no_keys(eng)


def test_two_new_hosts_roll_out_in_parallel_during_infra_apply():
    """infra_apply (still RUNNING) hands off one rollout per new host: none may be
    blocked by the apply nor by the sibling host's rollout."""
    running = [(1352, "infra_apply"), (1353, "rollout:docker-store"), (1354, "rollout:docker-sec")]
    eng = _engine(running)
    asyncio.run(eng.run_pipeline({"pipeline_type": "rollout", "limit": "docker-store"}, job_id=1353))
    asyncio.run(eng.run_pipeline({"pipeline_type": "rollout", "limit": "docker-sec"}, job_id=1354))
    assert [e[0] for e in eng.executed] == [1353, 1354]
    assert not _skipped(eng, 1353) and not _skipped(eng, 1354)


def test_real_duplicate_host_rollout_is_still_blocked():
    eng = _engine([(1, "rollout:docker-sec"), (2, "rollout:docker-sec")])
    asyncio.run(eng.run_pipeline({"pipeline_type": "rollout", "limit": "docker-sec"}, job_id=2))
    assert not eng.executed
    assert _skipped(eng, 2)
    assert _no_keys(eng)


def test_host_rollout_blocked_by_other_global_rollout():
    eng = _engine([(1, "rollout"), (2, "rollout:docker-sec")])
    asyncio.run(eng.run_pipeline({"pipeline_type": "rollout", "limit": "docker-sec"}, job_id=2))
    assert not eng.executed and _skipped(eng, 2)
    assert _no_keys(eng)


def test_global_rollout_blocked_by_other_host_rollout():
    eng = _engine([(1, "rollout:docker-sec"), (2, "rollout")])
    asyncio.run(eng.run_pipeline({"pipeline_type": "rollout"}, job_id=2))
    assert not eng.executed and _skipped(eng, 2)


@pytest.mark.parametrize("payload,job", [
    ({"pipeline_type": "rollout", "limit": "docker-sec"}, (5, "rollout:docker-sec")),
    ({"pipeline_type": "single_service", "service_name": "aac-x"}, (5, "single_service:aac-x")),
    ({"pipeline_type": "host_provision", "host_name": "docker-sec"}, (5, "host_provision:docker-sec")),
])
def test_keys_released_when_setup_raises(payload, job):
    """A failure before the pipeline's own try/finally (job row, strategy, log
    bridge) used to leak the key until the pod restarted."""
    async def boom():
        raise RuntimeError("setup failed")

    eng = _engine([job], execute=boom)
    with pytest.raises(RuntimeError):
        asyncio.run(eng.run_pipeline(dict(payload), job_id=5))
    assert eng.executed  # the key was reserved while running ...
    assert _no_keys(eng)  # ... and released afterwards

    # The next run for the same target is not blocked by a stale key.
    eng2_jobs = [(6, job[1])]
    eng.db.get_jobs_by_status.side_effect = lambda status: [
        SimpleNamespace(id=i, pipeline_type=t) for i, t in eng2_jobs
    ] if status == "RUNNING" else []
    eng._execute_dispatched = _engine([])._execute_dispatched
    asyncio.run(eng.run_pipeline(dict(payload), job_id=6))
    assert not _skipped(eng, 6)


def test_keys_released_on_cancel():
    async def cancelled():
        raise asyncio.CancelledError()

    eng = _engine([(7, "rollout:docker-sec")], execute=cancelled)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(eng.run_pipeline({"pipeline_type": "rollout", "limit": "docker-sec"}, job_id=7))
    assert _no_keys(eng)


def test_skipped_single_service_leaves_no_key():
    eng = _engine([(1, "single_service:aac-x"), (2, "single_service:aac-x")])
    asyncio.run(eng.run_pipeline({"pipeline_type": "single_service", "service_name": "aac-x"}, job_id=2))
    assert not eng.executed and _skipped(eng, 2)
    assert _no_keys(eng)


@pytest.mark.parametrize("payload", [
    {"pipeline_type": "rollout", "limit": "docker-sec"},
    {"pipeline_type": "single_service", "service_name": "aac-x"},
    {"pipeline_type": "host_provision", "host_name": "docker-sec"},
    {"pipeline_type": "infra_plan"},
])
def test_guard_reads_running_jobs_once_and_off_the_event_loop(payload):
    """Prod 2026-11-11 (#1387): the guard's sync query sat on the event loop for the
    whole TCP retransmit timeout and froze the cluster heartbeat with it. The RUNNING
    snapshot is read exactly once per dispatch, in a worker thread."""
    import threading

    loop_thread = threading.get_ident()
    seen: list[int] = []
    eng = _engine([])

    def _fetch(status):
        seen.append(threading.get_ident())
        return []

    eng.db.get_jobs_by_status.side_effect = _fetch
    asyncio.run(eng.run_pipeline(dict(payload), job_id=9))
    assert eng.executed and not _skipped(eng, 9)
    assert len(seen) == 1
    assert seen[0] != loop_thread


def test_guard_skips_the_query_for_unguarded_pipeline_types():
    eng = _engine([])
    asyncio.run(eng.run_pipeline({"pipeline_type": "connectivity"}, job_id=9))
    assert eng.executed
    eng.db.get_jobs_by_status.assert_not_called()
