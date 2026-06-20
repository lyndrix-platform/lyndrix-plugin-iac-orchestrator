"""Unit tests for the DynamicRuleExecutionStage baseline-role logic.

The plugin normally imports inside the lyndrix-core runtime package (relative
imports like ``from ..utils`` + ``core.logger`` writing to ``/app/logs``). To keep
these tests runnable standalone (CI *and* a dev box), we load the REAL
``stages/rules.py`` under a synthetic package with lightweight stubs for its
import dependencies. The code under test is the genuine file, unchanged.
"""
import asyncio
import importlib.util
import logging
import sys
import types
from pathlib import Path

PLUGIN_ROOT = Path(__file__).resolve().parents[1]


def _load_rules_module():
    # Stub `core.logger` (the runtime version writes to a hardcoded /app/logs).
    sys.modules.setdefault("core", types.ModuleType("core"))
    clog = types.ModuleType("core.logger")
    clog.get_logger = lambda name="iac": logging.getLogger(name)
    sys.modules["core.logger"] = clog

    # Synthetic package tree so rules.py's relative imports resolve to stubs.
    pkg = types.ModuleType("iac_orch")
    pkg.__path__ = [str(PLUGIN_ROOT)]
    sys.modules["iac_orch"] = pkg

    utils = types.ModuleType("iac_orch.utils")

    class StageResult:
        def __init__(self, success, message="", data=None):
            self.success, self.message, self.data = success, message, data

    utils.StageResult = StageResult
    sys.modules["iac_orch.utils"] = utils

    stages = types.ModuleType("iac_orch.stages")
    stages.__path__ = [str(PLUGIN_ROOT / "stages")]
    sys.modules["iac_orch.stages"] = stages

    base = types.ModuleType("iac_orch.stages.base")

    class BaseStage:
        def __init__(self, name):
            self.name = name

    base.BaseStage = BaseStage
    sys.modules["iac_orch.stages.base"] = base

    ansible = types.ModuleType("iac_orch.stages.ansible")

    class _StubStage:
        def __init__(self, *a, **k):
            self.kwargs = k

    ansible.AnsiblePlaybookStage = _StubStage
    ansible.AsyncBulkRolloutStage = _StubStage
    sys.modules["iac_orch.stages.ansible"] = ansible

    spec = importlib.util.spec_from_file_location(
        "iac_orch.stages.rules", PLUGIN_ROOT / "stages" / "rules.py"
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["iac_orch.stages.rules"] = mod
    spec.loader.exec_module(mod)
    return mod


rules = _load_rules_module()
Stage = rules.DynamicRuleExecutionStage


NESTED_INVENTORY = {
    "all": {
        "children": {
            "site_onprem": {
                "hosts": {
                    "docker-cerberus": {"baseline_roles": ["common", "docker", "active-directory"]},
                }
            }
        },
        "hosts": {
            "hydra": {"baseline_roles": ["common", "active-directory"]},
            "no_roles_host": {"ansible_host": "10.0.0.9"},
        },
    }
}


def test_extract_host_baseline_roles_walks_nested_groups():
    out = Stage._extract_host_baseline_roles(NESTED_INVENTORY)
    assert out["hydra"] == {"common", "active-directory"}
    assert out["docker-cerberus"] == {"common", "docker", "active-directory"}
    assert "no_roles_host" not in out  # hosts without baseline_roles are ignored


def test_extract_handles_empty_or_malformed():
    assert Stage._extract_host_baseline_roles({}) == {}
    assert Stage._extract_host_baseline_roles({"all": None}) == {}


def _yaml_dump(d):
    import yaml
    return yaml.safe_dump(d)


def test_changed_baseline_roles_detects_only_added_role_on_changed_host():
    """The user's scenario: one host gains one new baseline role -> that host/role only."""
    old = {"all": {"hosts": {
        "host-a": {"baseline_roles": ["common"]},
        "host-b": {"baseline_roles": ["common", "docker"]},
    }}}
    new = {"all": {"hosts": {
        "host-a": {"baseline_roles": ["common", "dhcp_server"]},  # gained dhcp_server
        "host-b": {"baseline_roles": ["common", "docker"]},        # unchanged
    }}}

    stage = Stage("rollout")
    inv = "global/ansible/inventory.yml"

    async def fake_git(engine, *args):
        if args == ("show", f"HEAD:{inv}"):
            return (0, _yaml_dump(new))
        if args == ("show", f"HEAD~1:{inv}"):
            return (0, _yaml_dump(old))
        return (1, "")

    stage._git = fake_git  # type: ignore[assignment]
    result = asyncio.run(stage._changed_baseline_roles(None, inv))
    assert result == {"host-a": ["dhcp_server"]}


def test_changed_baseline_roles_skips_brand_new_hosts():
    """A host absent from the previous inventory is provisioned via host_provision,
    not role-patched here — so a brand-new host yields no baseline_role work."""
    old = {"all": {"hosts": {"host-a": {"baseline_roles": ["common"]}}}}
    new = {"all": {"hosts": {
        "host-a": {"baseline_roles": ["common"]},                 # unchanged existing host
        "host-new": {"baseline_roles": ["common", "docker"]},     # brand-new host
    }}}
    stage = Stage("rollout")
    inv = "global/ansible/inventory.yml"

    async def fake_git(engine, *args):
        if args == ("show", f"HEAD:{inv}"):
            return (0, _yaml_dump(new))
        if args == ("show", f"HEAD~1:{inv}"):
            return (0, _yaml_dump(old))
        return (1, "")

    stage._git = fake_git  # type: ignore[assignment]
    assert asyncio.run(stage._changed_baseline_roles(None, inv)) == {}


def test_changed_baseline_roles_first_commit_returns_empty():
    """No HEAD~1 (initial commit) -> every host is 'new' -> no baseline_role work."""
    new = {"all": {"hosts": {"host-a": {"baseline_roles": ["common"]}}}}
    stage = Stage("rollout")
    inv = "global/ansible/inventory.yml"

    async def fake_git(engine, *args):
        if args == ("show", f"HEAD:{inv}"):
            return (0, _yaml_dump(new))
        return (1, "")  # HEAD~1 missing

    stage._git = fake_git  # type: ignore[assignment]
    assert asyncio.run(stage._changed_baseline_roles(None, inv)) == {}


def test_terraform_action_never_force_applies():
    """Guardrail: a rule must NEVER force-apply. Even a mode:apply dev rule passes
    force_apply=False, so apply stays gated behind the global auto_apply switch."""
    stage = Stage("rollout")
    captured = {}

    class FakeEngine:
        async def execute_terraform_provision(self, host_name, job_id, mode, force_apply, adopt_existing):
            captured.update(host_name=host_name, mode=mode, force_apply=force_apply,
                            adopt_existing=adopt_existing)
            return True, {}

    async def fake_changed(engine, f):
        return ["host-a"]

    stage._changed_terraform_hosts = fake_changed  # type: ignore[assignment]
    action = {"type": "terraform", "mode": "apply", "adopt_existing": True}
    res = asyncio.run(stage._run_terraform_action(
        FakeEngine(), {"job_id": 1}, action,
        ["onprem/dev/terraform/terraform.tfvars.json"], {},
    ))
    assert res.success
    assert captured["force_apply"] is False      # never bypass auto_apply from a rule
    assert captured["mode"] == "apply"           # mode intent preserved
    assert captured["adopt_existing"] is True


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-v"]))
