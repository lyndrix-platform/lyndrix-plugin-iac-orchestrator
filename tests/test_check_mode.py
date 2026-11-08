"""Ansible --check decision (1.5.6).

auto_apply gates only infrastructure applies (Terraform, rule-engine actions).
Explicit deploys (single_service, rollout, compliance, bootstrap) run for real
unless the job payload asks for ``dry_run``.
"""
import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

from iac_plugin.app.controller import engine as engine_mod
from iac_plugin.app.controller.engine import (
    DeploymentEngine,
    PersistStateStage,
    SingleServiceStrategy,
    _payload_dry_run,
)
from iac_plugin.stages.ansible import AnsiblePlaybookStage, resolve_check_mode
from iac_plugin.stages.rules import DynamicRuleExecutionStage


class _FakeEngine:
    def __init__(self, auto_apply=False):
        self.config = SimpleNamespace(auto_apply=auto_apply, services_dir="/svc")
        self.calls = []

    async def execute_ansible_docker(self, **kwargs):
        self.calls.append(kwargs)
        return True, {"successful_hosts": 1, "failed_hosts": 0}


def _run_stage(stage, context, auto_apply=False):
    eng = _FakeEngine(auto_apply=auto_apply)
    res = asyncio.run(stage.run(eng, context))
    return eng.calls[-1], res


def test_payload_dry_run_parsing():
    assert _payload_dry_run({}) is False
    assert _payload_dry_run({"dry_run": True}) is True
    assert _payload_dry_run({"dry_run": "true"}) is True
    assert _payload_dry_run({"dry_run": "0"}) is False
    assert _payload_dry_run(None) is False


def test_resolve_check_mode_explicit_wins():
    assert resolve_check_mode(None, {}) is False
    assert resolve_check_mode(None, {"dry_run": True}) is True
    assert resolve_check_mode(False, {"dry_run": True}) is False
    assert resolve_check_mode(True, {}) is True


def test_playbook_stage_runs_real_with_auto_apply_off():
    """The bug: auto_apply off turned every single_service deploy into --check."""
    stage = AnsiblePlaybookStage(playbook_path="pb.yml", inventory_path="inv.yml")
    call, res = _run_stage(stage, {"job_id": 1}, auto_apply=False)
    assert call["check_mode"] is False
    assert res.data["check_mode"] is False


def test_playbook_stage_follows_job_dry_run():
    stage = AnsiblePlaybookStage(playbook_path="pb.yml", inventory_path="inv.yml")
    call, res = _run_stage(stage, {"job_id": 1, "dry_run": True}, auto_apply=True)
    assert call["check_mode"] is True
    assert "dry-run" in res.message


def test_single_service_strategy_never_gated_by_auto_apply():
    eng = MagicMock()
    eng.config.services_dir = "/svc"
    eng.base_git_dir = MagicMock()
    stages = SingleServiceStrategy().build_stages(
        eng, {"service_name": "aac-kea-dhcp", "service_branch": "main"}, {}
    )
    ansible_stage = stages[-1]
    assert isinstance(ansible_stage, AnsiblePlaybookStage)
    assert ansible_stage.check_mode is None  # follows dry_run only


def test_rule_engine_ansible_actions_gated_by_auto_apply():
    stage = DynamicRuleExecutionStage("infra_plan")
    assert stage._ansible_check_mode(_FakeEngine(auto_apply=False), {}) is True
    assert stage._ansible_check_mode(_FakeEngine(auto_apply=True), {}) is False
    assert stage._ansible_check_mode(_FakeEngine(auto_apply=True), {"dry_run": True}) is True


def test_persist_state_skipped_on_dry_run():
    eng = MagicMock()
    res = asyncio.run(PersistStateStage().run(eng, {"dry_run": True, "current_desired_state": {"h": {}}}))
    assert res.success
    eng.db.update_state.assert_not_called()


def _capture_runner_cmd(monkeypatch, tmp_path, *, auto_apply, check_mode):
    """Run execute_ansible_docker with docker stubbed out; return the argv."""
    captured = {}

    class _Proc:
        returncode = 0

        async def communicate(self, input=None):
            return b"", b""

    async def fake_exec(*cmd, **kwargs):
        if len(cmd) > 2 and cmd[1] == "run":
            captured["cmd"] = list(cmd)
        return _Proc()

    monkeypatch.setattr(engine_mod.shutil, "which", lambda _: "/usr/bin/docker")
    monkeypatch.setattr(engine_mod.asyncio, "create_subprocess_exec", fake_exec)

    eng = DeploymentEngine.__new__(DeploymentEngine)
    eng.state = {}
    eng.ctx = MagicMock()
    eng.ctx.get_secret = lambda key: "KEY" if key == "ansible_ssh_key" else None
    eng.config = SimpleNamespace(
        auto_apply=auto_apply, services_dir=tmp_path, host_git_repos_dir="/g",
        host_services_dir="/s", ansible_docker_image="img",
    )
    eng._sync_runners = lambda *a, **k: None
    eng._job_inventory_snapshot_dir = lambda job_id: tmp_path / "none"

    async def fake_watch(*a, **k):
        return True, {"successful_hosts": 1, "failed_hosts": 0}

    eng._watch_detached_runner = fake_watch
    ok, _ = asyncio.run(eng.execute_ansible_docker(
        playbook_subpath="pb.yml", inventory_subpath="inv.yml", limit="h1",
        task_name="t", job_id=7, check_mode=check_mode,
    ))
    assert ok
    return captured["cmd"]


def test_runner_cmd_has_no_check_when_auto_apply_off(monkeypatch, tmp_path):
    cmd = _capture_runner_cmd(monkeypatch, tmp_path, auto_apply=False, check_mode=False)
    assert "--check" not in cmd


def test_runner_cmd_has_check_only_on_explicit_dry_run(monkeypatch, tmp_path):
    cmd = _capture_runner_cmd(monkeypatch, tmp_path, auto_apply=True, check_mode=True)
    assert "--check" in cmd
