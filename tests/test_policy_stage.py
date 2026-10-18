"""MaintenancePolicyStage (plugin 1.6): decide in the job — run, park as SCHEDULED,
or run with a warning + ``maintenance:window_missing`` — and announce the deploy
override. Fake clock, temporary controller tree, SQLite queue."""

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import yaml
from sqlalchemy import select

from core.api import maintenance as mw
from iac_plugin.app.controller import engine as engine_mod
from iac_plugin.app.controller import queue as queue_mod
from iac_plugin.app.controller.engine import (
    DeploymentEngine,
    JobParked,
    MaintenancePolicyStage,
)
from iac_plugin.app.controller.utils import StageResult
from iac_plugin.app.model.models import IaCJob

# Sunday 2026-10-18 10:00 UTC — outside the sun 02:00–04:00 window, 6 days before the next Saturday.
NOW = datetime(2026, 10, 18, 10, 0, tzinfo=timezone.utc)
IN_WINDOW = datetime(2026, 10, 18, 2, 30, tzinfo=timezone.utc)
SUN = {"weekly": {"days": ["sun"], "start": "02:00", "end": "04:00", "tz": "UTC"}}


def _write(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data), encoding="utf-8")


def _controller(root: Path, *, windows=None, stage="prod", policy=None):
    std = root / "environments" / "sites" / "onprem" / "stages" / stage
    maint = {}
    if windows is not None:
        maint["windows"] = windows
    if policy:
        maint["deploy_policy"] = policy
    svc_entry = {"name": "svc-a"}
    if maint:
        svc_entry["maintenance"] = maint
    _write(
        root / "environments" / "sites" / "onprem" / "site_vars.yml",
        {"site_vars": {"timezone": "UTC"}},
    )
    _write(std / "stage_vars.yml", {"stage_vars": {}})
    _write(
        std / "hosts.yml",
        {
            "hosts": {
                "docker-a": {
                    "hostname": "a.int",
                    "ansible_host": "10.0.0.1",
                    "services": [svc_entry],
                }
            }
        },
    )


class _Config(SimpleNamespace):
    maintenance_deploy_marks_maintenance = True
    maintenance_max_override_minutes = 240
    maintenance_linger_minutes = 15
    maintenance_schedule_horizon_days = 30
    maintenance_report_missing_window = True


@pytest.fixture
def env(tmp_path, sqlite_queue):
    ctrl = tmp_path / "git" / "iac_controller"
    services = tmp_path / "services"
    services.mkdir()
    eng = object.__new__(DeploymentEngine)
    eng.ctx = MagicMock()
    eng.ctx.instance_id = "runner-1"
    eng.config = _Config(services_dir=services, git_repos_dir=tmp_path / "git")
    eng.base_git_dir = tmp_path / "git"
    eng.queue = sqlite_queue
    eng._ci_map = {
        ("host", "docker-a"): "u-host",
        ("service", "docker-a", "svc-a"): "u-svc",
    }
    return SimpleNamespace(
        engine=eng, ctrl=ctrl, services=services, queue=sqlite_queue, tmp=tmp_path
    )


def _claimed_job(queue, payload):
    jid = queue.enqueue(payload)
    claimed = queue.claim_next("runner-1")
    assert claimed and claimed[0] == jid
    return jid


def _row(jid):
    with queue_mod.db_instance.SessionLocal() as s:
        return s.execute(select(IaCJob).where(IaCJob.id == jid)).scalar_one()


def _emits(ctx, topic):
    return [c.args[1] for c in ctx.emit.call_args_list if c.args[0] == topic]


def _run(stage, engine, context):
    return asyncio.run(stage.run(engine, context))


WEBHOOK = {
    "pipeline_type": "single_service",
    "service_name": "svc-a",
    "service_branch": "main",
    "trigger": "gitlab-ci",
}


def test_outside_window_parks_job_as_scheduled(env):
    _controller(env.ctrl, windows=[SUN], policy="window")
    jid = _claimed_job(env.queue, WEBHOOK)
    stage = MaintenancePolicyStage("svc-a", "main", WEBHOOK, clock=lambda: NOW)
    with pytest.raises(JobParked) as exc:
        _run(stage, env.engine, {"job_id": jid, "payload": WEBHOOK})
    assert exc.value.not_before == datetime(2026, 10, 25, 2, 0, tzinfo=timezone.utc)
    row = _row(jid)
    assert (
        row.status == "SCHEDULED"
        and row.owner_instance is None
        and row.heartbeat_at is None
    )
    assert queue_mod.from_queue_clock(row.not_before) == exc.value.not_before
    assert "svc-a @ docker-a" in (row.schedule_reason or "")
    assert sorted(__import__("json").loads(row.window_ci_ids)) == ["u-host", "u-svc"]
    # parked → no override, no window_missing
    assert _emits(env.engine.ctx, mw.TOPIC_OVERRIDE_STARTED) == []
    assert _emits(env.engine.ctx, mw.TOPIC_WINDOW_MISSING) == []
    # not due yet → the claim leaves it alone
    assert env.queue.claim_next("runner-2") is None


def test_inside_window_runs_and_marks_override(env):
    _controller(env.ctrl, windows=[SUN], policy="window")
    jid = _claimed_job(env.queue, WEBHOOK)
    ctx = {"job_id": jid, "payload": WEBHOOK}
    res = _run(
        MaintenancePolicyStage("svc-a", "main", WEBHOOK, clock=lambda: IN_WINDOW),
        env.engine,
        ctx,
    )
    assert res.success and "inside maintenance window" in res.message
    started = _emits(env.engine.ctx, mw.TOPIC_OVERRIDE_STARTED)
    assert len(started) == 1
    assert started[0]["override_id"] == f"iac-job-{jid}" and sorted(
        started[0]["ci_ids"]
    ) == ["u-host", "u-svc"]
    assert (
        started[0]["until"] == mw.iso(IN_WINDOW + timedelta(minutes=240))
        and started[0]["linger_minutes"] == 15
    )
    assert started[0]["source"] == "iac-orchestrator"
    assert (
        ctx["override_id"] == f"iac-job-{jid}"
        and _row(jid).override_id == f"iac-job-{jid}"
    )
    assert _row(jid).status == "RUNNING"
    # every emit is LOCAL (transport rule): no cluster=True anywhere
    assert all(not c.kwargs.get("cluster") for c in env.engine.ctx.emit.call_args_list)


def test_no_window_in_horizon_runs_with_warning_and_one_window_missing(env, caplog):
    _controller(
        env.ctrl, windows=None, policy="window"
    )  # prod → window by default, no windows anywhere
    jid = _claimed_job(env.queue, WEBHOOK)
    caplog.set_level("WARNING", logger="IaC:Engine")
    res = _run(
        MaintenancePolicyStage("svc-a", "main", WEBHOOK, clock=lambda: NOW),
        env.engine,
        {"job_id": jid, "payload": WEBHOOK},
    )
    assert res.success and "no maintenance window within 30 days" in res.message
    assert any("deploying NOW" in r.message for r in caplog.records)
    missing = _emits(env.engine.ctx, mw.TOPIC_WINDOW_MISSING)
    assert len(missing) == 1
    assert (
        missing[0]["event_id"] == f"window_missing:u-svc:{jid}"
        and missing[0]["ci_id"] == "u-svc"
    )
    assert missing[0]["host_ci_id"] == "u-host" and (
        missing[0]["site"],
        missing[0]["stage"],
    ) == ("onprem", "prod")
    assert (
        missing[0]["horizon_days"] == 30
        and missing[0]["job_id"] == jid
        and missing[0]["kind"] == "service"
    )
    assert (
        len(_emits(env.engine.ctx, mw.TOPIC_OVERRIDE_STARTED)) == 1
    )  # the deploy still marks maintenance
    assert _row(jid).status == "RUNNING"


def test_window_missing_not_emitted_when_setting_off_or_without_service_ci(env):
    _controller(env.ctrl, windows=None, policy="window")
    env.engine.config.maintenance_report_missing_window = False
    jid = _claimed_job(env.queue, WEBHOOK)
    _run(
        MaintenancePolicyStage("svc-a", "main", WEBHOOK, clock=lambda: NOW),
        env.engine,
        {"job_id": jid, "payload": WEBHOOK},
    )
    assert _emits(env.engine.ctx, mw.TOPIC_WINDOW_MISSING) == []

    env.engine.config.maintenance_report_missing_window = True
    env.engine._ci_map = {
        ("host", "docker-a"): "u-host"
    }  # registry does not know services yet
    env.engine.ctx.emit.reset_mock()
    with queue_mod.db_instance.SessionLocal() as s:  # finish the first job so the key is free
        s.execute(
            __import__("sqlalchemy")
            .update(IaCJob)
            .where(IaCJob.id == jid)
            .values(status="SUCCESS")
        )
        s.commit()
    jid2 = _claimed_job(env.queue, WEBHOOK)
    res = _run(
        MaintenancePolicyStage("svc-a", "main", WEBHOOK, clock=lambda: NOW),
        env.engine,
        {"job_id": jid2, "payload": WEBHOOK},
    )
    assert res.success and _emits(env.engine.ctx, mw.TOPIC_WINDOW_MISSING) == []
    # host CI alone still gets the override
    assert _emits(env.engine.ctx, mw.TOPIC_OVERRIDE_STARTED)[0]["ci_ids"] == ["u-host"]


def test_force_and_ui_triggers_skip_the_policy(env):
    _controller(env.ctrl, windows=[SUN], policy="window")
    forced = {**WEBHOOK, "force": True}
    jid = _claimed_job(env.queue, forced)
    res = _run(
        MaintenancePolicyStage("svc-a", "main", forced, clock=lambda: NOW),
        env.engine,
        {"job_id": jid, "payload": forced},
    )
    assert res.success and _row(jid).status == "RUNNING"
    assert len(_emits(env.engine.ctx, mw.TOPIC_OVERRIDE_STARTED)) == 1

    with queue_mod.db_instance.SessionLocal() as s:
        s.execute(
            __import__("sqlalchemy")
            .update(IaCJob)
            .where(IaCJob.id == jid)
            .values(status="SUCCESS")
        )
        s.commit()
    ui = {**WEBHOOK, "trigger": "ui", "manual": True}
    jid2 = _claimed_job(env.queue, ui)
    res = _run(
        MaintenancePolicyStage("svc-a", "main", ui, clock=lambda: NOW),
        env.engine,
        {"job_id": jid2, "payload": ui},
    )
    assert res.success and _row(jid2).status == "RUNNING"


def test_due_scheduled_job_runs_without_reparking(env):
    _controller(env.ctrl, windows=[SUN], policy="window")
    jid = _claimed_job(env.queue, WEBHOOK)
    # The queue compares not_before with the REAL process clock, so this test runs on it too.
    real_now = lambda: datetime.now(timezone.utc)  # noqa: E731
    past = real_now() - timedelta(hours=1)
    assert env.queue.park(
        jid,
        not_before=past,
        window_ci_ids=["u-svc"],
        reason="test",
        instance_id="runner-1",
    )
    claimed = env.queue.claim_next("runner-1")  # due → claimed again, not_before kept
    assert claimed and claimed[0] == jid
    res = _run(
        MaintenancePolicyStage("svc-a", "main", WEBHOOK, clock=real_now),
        env.engine,
        {"job_id": jid, "payload": WEBHOOK},
    )
    assert (
        res.success
        and "scheduled window reached" in res.message
        and _row(jid).status == "RUNNING"
    )


def test_immediate_policy_and_unknown_service(env):
    _controller(env.ctrl, windows=[SUN], policy="immediate")
    jid = _claimed_job(env.queue, WEBHOOK)
    res = _run(
        MaintenancePolicyStage("svc-a", "main", WEBHOOK, clock=lambda: NOW),
        env.engine,
        {"job_id": jid, "payload": WEBHOOK},
    )
    assert (
        res.success and "'immediate'" in res.message and _row(jid).status == "RUNNING"
    )

    other = {**WEBHOOK, "service_name": "ghost"}
    with queue_mod.db_instance.SessionLocal() as s:
        s.execute(
            __import__("sqlalchemy")
            .update(IaCJob)
            .where(IaCJob.id == jid)
            .values(status="SUCCESS")
        )
        s.commit()
    jid2 = _claimed_job(env.queue, other)
    env.engine.ctx.emit.reset_mock()
    res = _run(
        MaintenancePolicyStage("ghost", "main", other, clock=lambda: NOW),
        env.engine,
        {"job_id": jid2, "payload": other},
    )
    assert res.success and "skipped" in res.message
    assert env.engine.ctx.emit.call_args_list == []  # no CI → no override, no event


def test_deploy_marks_maintenance_setting_off(env):
    _controller(env.ctrl, windows=[SUN], policy="immediate")
    env.engine.config.maintenance_deploy_marks_maintenance = False
    jid = _claimed_job(env.queue, WEBHOOK)
    _run(
        MaintenancePolicyStage("svc-a", "main", WEBHOOK, clock=lambda: NOW),
        env.engine,
        {"job_id": jid, "payload": WEBHOOK},
    )
    assert (
        _emits(env.engine.ctx, mw.TOPIC_OVERRIDE_STARTED) == []
        and _row(jid).override_id is None
    )


def test_end_override_for_emits_exactly_once(env):
    _controller(env.ctrl, windows=[SUN], policy="immediate")
    jid = _claimed_job(env.queue, WEBHOOK)
    _run(
        MaintenancePolicyStage("svc-a", "main", WEBHOOK, clock=lambda: NOW),
        env.engine,
        {"job_id": jid, "payload": WEBHOOK},
    )
    assert env.engine.end_override_for(jid) is True
    ended = _emits(env.engine.ctx, mw.TOPIC_OVERRIDE_ENDED)
    assert (
        len(ended) == 1
        and ended[0]["override_id"] == f"iac-job-{jid}"
        and ended[0]["linger_minutes"] == 15
    )
    assert (
        env.engine.end_override_for(jid) is False
    )  # second terminal path: nothing left
    assert len(_emits(env.engine.ctx, mw.TOPIC_OVERRIDE_ENDED)) == 1
    # orphan sweep path: mark_failed → on_terminal hook → override_ended
    env.queue.on_terminal = env.engine.end_override_for
    jid2 = _claimed_job(env.queue, {**WEBHOOK, "service_name": "svc-b"})
    env.queue.set_override(jid2, f"iac-job-{jid2}", ["u-x"])
    assert env.queue.mark_failed(jid2, "Runner lost")
    assert (
        _emits(env.engine.ctx, mw.TOPIC_OVERRIDE_ENDED)[-1]["override_id"]
        == f"iac-job-{jid2}"
    )


# --- run_pipeline: JobParked leaves the row SCHEDULED (no terminal update_job) ----


class _Parking:
    name = "park"

    def __init__(self, exc):
        self.exc = exc

    async def run(self, engine, context):
        if self.exc:
            raise self.exc
        return StageResult(False, "boom")


class _Strategy:
    commit_state = False
    sync_infra_engine = False
    organizes_tasks = False

    def __init__(self, stage):
        self.stage = stage

    def db_tag(self, payload):
        return "single_service:svc-a"

    def build_stages(self, engine, payload, context):
        return [self.stage]


def _pipeline_engine(tmp_path, monkeypatch, stage):
    ctx = MagicMock()
    ctx.subscribe = MagicMock(return_value=lambda fn: fn)
    db = MagicMock()
    db.get_jobs_by_status.return_value = []
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
    monkeypatch.setattr(
        engine_mod, "get_pipeline_strategy", lambda ptype: _Strategy(stage)
    )
    return eng, db


def test_run_pipeline_parked_skips_terminal_status(tmp_path, monkeypatch):
    nb = NOW + timedelta(days=7)
    eng, db = _pipeline_engine(
        tmp_path, monkeypatch, _Parking(JobParked(nb, "window opens later"))
    )
    asyncio.run(
        eng.run_pipeline(
            {"pipeline_type": "single_service", "service_name": "svc-a"}, job_id=7
        )
    )
    db.update_job.assert_not_called()
    steps = [
        c.kwargs.get("current_step") or (c.args[2] if len(c.args) > 2 else None)
        for c in db.update_progress.call_args_list
    ]
    assert any(s and s.startswith("Scheduled for 2026-10-25") for s in steps)
    assert (
        eng.state["running_jobs"] == 0
        and "single_service:svc-a" not in eng._active_single_service_keys
    )
    titles = [c.kwargs.get("title", "") for c in eng.ctx.notify.call_args_list]
    assert any("⏳" in t for t in titles) and not any("❌" in t for t in titles)


def test_run_pipeline_failure_still_writes_terminal_status(tmp_path, monkeypatch):
    eng, db = _pipeline_engine(tmp_path, monkeypatch, _Parking(None))
    asyncio.run(
        eng.run_pipeline(
            {"pipeline_type": "single_service", "service_name": "svc-a"}, job_id=8
        )
    )
    db.update_job.assert_called_once_with(job_id=8, status="FAILED")
