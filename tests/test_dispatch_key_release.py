"""Dispatch keys are released as soon as the job's row is final (1.7.1, found on staging).

Staging 2026-11-16: job #45 (single_service aac-stgdummy) wrote its terminal status,
then synced runners, closed its log writer and cleaned up — and only then did run_pipeline's finally free the in-memory
key ``single_service:aac-stgdummy``. The queue claims the follow-up (#49, waiting_for
#45) the moment #45's row is final; its dispatch guard found the key still held,
bounced it back to the queue up to _MAX_GUARD_BOUNCES times and then ABORTED it
("Skipped: deployment for aac-stgdummy already starting").
"""
import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

from iac_plugin.app.controller import engine as engine_mod
from iac_plugin.app.controller.engine import DeploymentEngine
from iac_plugin.app.controller.utils import StageResult

SERVICE = "aac-stgdummy"
KEY = f"single_service:{SERVICE}"
PAYLOAD = {"pipeline_type": "single_service", "service_name": SERVICE}


class _Config(SimpleNamespace):
    maintenance_deploy_marks_maintenance = True
    maintenance_max_override_minutes = 240
    maintenance_linger_minutes = 15
    maintenance_schedule_horizon_days = 30
    maintenance_report_missing_window = True


class _OkStage:
    name = "DEPLOY"

    async def run(self, engine, context):
        return StageResult(True, "ok")


class _Strategy:
    commit_state = False
    sync_infra_engine = False
    organizes_tasks = False

    def db_tag(self, payload):
        return KEY

    def build_stages(self, engine, payload, context):
        return [_OkStage()]


class _Rows:
    """The job table as the dispatch guard sees it: claimed jobs are RUNNING until
    their terminal write (``update_job``)."""

    def __init__(self, *running):
        self.status = {job_id: "RUNNING" for job_id in running}

    def get_jobs_by_status(self, status):
        return [SimpleNamespace(id=j, pipeline_type=KEY) for j, s in self.status.items() if s == status]

    def update_job(self, job_id, status):
        self.status[job_id] = status
        return True


def _engine(tmp_path, monkeypatch, rows):
    ctx = MagicMock()
    ctx.subscribe = MagicMock(return_value=lambda fn: fn)
    db = MagicMock()
    db.get_jobs_by_status.side_effect = rows.get_jobs_by_status
    db.update_job.side_effect = rows.update_job
    db.unwritten_terminal = {}
    cfg = _Config(
        git_repos_dir=tmp_path / "git",
        services_dir=tmp_path / "services",
        get_log_path=lambda jid: tmp_path / f"job_{jid}.log",
    )
    eng = DeploymentEngine(ctx, {}, db, cfg)

    async def ok(self, engine, context):
        return StageResult(True, "ok")

    monkeypatch.setattr(engine_mod.SyncRepoStage, "run", ok)
    monkeypatch.setattr(engine_mod.NativeGenerateStage, "run", ok)
    monkeypatch.setattr(engine_mod, "get_pipeline_strategy", lambda ptype: _Strategy())
    return eng, db


def test_key_is_free_when_the_row_is_final_before_cleanup(tmp_path, monkeypatch):
    rows = _Rows(45)
    eng, db = _engine(tmp_path, monkeypatch, rows)
    seen = {}

    def close_job_row(job_id, status, **kwargs):
        seen["at_close"] = KEY in eng._active_single_service_keys
        return rows.update_job(job_id, status)

    def cleanup(job_id):
        seen["at_cleanup"] = KEY in eng._active_single_service_keys

    eng.close_job_row = close_job_row
    eng._cleanup_inventory_snapshot = cleanup
    asyncio.run(eng.run_pipeline(dict(PAYLOAD), job_id=45))

    assert rows.status[45] == "SUCCESS"
    # Free before the terminal write (the RUNNING row guards the target until then)
    # and therefore during the post-run cleanup.
    assert seen == {"at_close": False, "at_cleanup": False}
    assert KEY not in eng._active_single_service_keys
    assert eng.__dict__.get("_dispatch_keys_by_owner") == {}


def test_follow_up_claimed_during_post_run_work_is_not_aborted(tmp_path, monkeypatch):
    """#49 is claimed while #45 is still in its post-run work: it must run, not be
    skipped as 'already starting' (without a queue the guard ABORTs at once)."""
    rows = _Rows(45)
    eng, db = _engine(tmp_path, monkeypatch, rows)
    real_close_writer = eng._close_log_writer
    follow_up = {}

    async def close_log_writer(job_id):
        await real_close_writer(job_id)
        if job_id == 45 and not follow_up:
            # The queue claims #49 now: #45's row is final, its post-run work goes on.
            assert rows.status[45] == "SUCCESS"
            rows.status[49] = "RUNNING"
            follow_up["keys_before"] = set(eng._active_single_service_keys)
            await eng.run_pipeline(dict(PAYLOAD), job_id=49)
            follow_up["done"] = True

    eng._close_log_writer = close_log_writer
    asyncio.run(eng.run_pipeline(dict(PAYLOAD), job_id=45))

    assert rows.status == {45: "SUCCESS", 49: "SUCCESS"}            # before the fix: 49 ABORTED
    assert follow_up == {"keys_before": set(), "done": True}
    steps = [c.kwargs.get("current_step") or "" for c in db.update_progress.call_args_list]
    assert not any(s.startswith("Skipped:") for s in steps)
    assert not eng._active_single_service_keys


def test_release_is_idempotent_and_never_frees_the_next_owners_key():
    """run_pipeline's finally releases again (safety net). By then the next job for
    the target may hold the same key — it must stay reserved."""
    eng = DeploymentEngine.__new__(DeploymentEngine)
    eng._pipeline_dispatch_lock = asyncio.Lock()
    eng._active_single_service_keys = set()
    first, second = object(), object()

    async def scenario():
        eng._reserve_dispatch_key(eng._active_single_service_keys, KEY, first)
        await eng._release_dispatch_keys(first)          # row final
        eng._reserve_dispatch_key(eng._active_single_service_keys, KEY, second)
        await eng._release_dispatch_keys(first)          # first job's safety net
        held = KEY in eng._active_single_service_keys
        await eng._release_dispatch_keys(second)
        await eng._release_dispatch_keys(None)           # no owner: no-op
        return held

    assert asyncio.run(scenario()) is True
    assert not eng._active_single_service_keys


def test_set_up_failure_still_releases_the_keys(tmp_path, monkeypatch):
    """The set-up raises before _execute_dispatched's own finally: the outer one
    still frees the key."""
    rows = _Rows(45)
    eng, _db = _engine(tmp_path, monkeypatch, rows)
    monkeypatch.setattr(engine_mod, "get_pipeline_strategy",
                        lambda ptype: (_ for _ in ()).throw(RuntimeError("no strategy")))
    try:
        asyncio.run(eng.run_pipeline(dict(PAYLOAD), job_id=45))
    except RuntimeError:
        pass
    assert not eng._active_single_service_keys
    assert eng.__dict__.get("_dispatch_keys_by_owner") == {}
