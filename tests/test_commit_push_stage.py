"""CommitPushStage required/best-effort semantics and per-strategy wiring."""

import asyncio
from types import SimpleNamespace

from iac_plugin.app.controller.engine import PIPELINE_STRATEGIES
from iac_plugin.stages.git import CommitPushStage


def _engine(status):
    async def commit_push(role_slug, message):
        return status
    return SimpleNamespace(execute_git_commit_push=commit_push)


def test_required_push_failure_aborts():
    ctx = {}
    res = asyncio.run(CommitPushStage("inventory_state", "m").run(_engine("error"), ctx))
    assert res.success is False
    assert ctx["inventory_state_commit_status"] == "error"


def test_best_effort_push_failure_continues():
    ctx = {}
    stage = CommitPushStage("inventory_state", "m", required=False)
    res = asyncio.run(stage.run(_engine("timeout"), ctx))
    assert res.success is True
    assert ctx["inventory_state_commit_status"] == "timeout"


def test_push_success_ok_either_way():
    for required in (True, False):
        stage = CommitPushStage("inventory_state", "m", required=required)
        assert asyncio.run(stage.run(_engine("pushed"), {})).success is True


def test_single_service_persists_state_best_effort():
    s = PIPELINE_STRATEGIES["single_service"]
    assert s.commit_state is True
    assert s.commit_state_required is False
    # Full pipelines keep the hard requirement.
    assert PIPELINE_STRATEGIES["rollout"].commit_state_required is True
    # Read-only pipelines still never commit.
    for t in ("infra_plan", "adopt_host", "state_check"):
        assert PIPELINE_STRATEGIES[t].commit_state is False
