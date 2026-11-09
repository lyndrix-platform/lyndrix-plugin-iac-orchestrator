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


def test_rules_diff_the_pinned_publish_commit_not_head():
    """1.5.3: run() pins the diff to the commit this pipeline published, so a
    concurrent pipeline's later commit cannot change which diff the rules see."""
    stage = Stage("infra_plan")
    seen = []

    async def fake_git(engine, *args):
        seen.append(args)
        return (0, "")

    stage._git = fake_git  # type: ignore[assignment]
    stage._ref = "abc123"
    tfvars = "onprem/prod/terraform/terraform.tfvars.json"
    asyncio.run(stage._get_changed_files(None))
    assert seen[-1][-1] == "abc123"
    asyncio.run(stage._changed_terraform_hosts(None, tfvars))
    assert ("show", f"abc123:{tfvars}") in seen
    assert ("show", f"abc123~1:{tfvars}") in seen


def test_run_dirs_never_reach_the_rules():
    """1.5.4: .terraform_runs/ is versioned again but must never trigger a rule."""
    stage = Stage("infra_plan")

    async def fake_git(engine, *args):
        return (0, ".terraform_runs/onprem_prod__plan/prod/terraform/terraform.tfvars.json\n"
                   ".terraform_runs/aac-x__apply/main.tf\n"
                   "onprem/prod/terraform/terraform.tfvars.json\n")

    stage._git = fake_git  # type: ignore[assignment]
    changed = asyncio.run(stage._get_changed_files(None))
    assert changed == ["onprem/prod/terraform/terraform.tfvars.json"]


# ------------------------------------- service reconcile (1.5.8: detection + notify)
import json as _json  # noqa: E402
from types import SimpleNamespace  # noqa: E402


class _Ctx:
    def __init__(self):
        self.notifications = []
        self.emitted = []

    def notify(self, endpoint, **kw):
        self.notifications.append((endpoint, kw))

    def emit(self, topic, payload):
        self.emitted.append((topic, payload))


class _Engine:
    def __init__(self, mode="notify"):
        self.config = SimpleNamespace(service_reconcile_mode=mode, auto_apply=False)
        self.ctx = _Ctx()


SECRET = "sup3r-s3cret-value"
PROD = "onprem/prod/ansible/inventory.yml"
DEV = "onprem/dev/ansible/inventory.yml"
GLOBAL = "global/ansible/inventory.yml"
CHANGED = [PROD, DEV, GLOBAL, "onprem/prod/terraform/terraform.tfvars.json"]
CONTEXT = {"job_id": 7, "inventory_state_commit_sha": "abc123def4567"}


def _svc(name, manager="m.old", secret=SECRET):
    return {"name": name, "config": {"vars": {"WAZUH_MANAGER": manager}, "secrets": {"KEY": secret}}}


def _inv(hosts):
    return {"all": {"hosts": hosts, "children": {}}}


def _renders(manager_new="m.new", secret_new=SECRET, prod_hosts=("docker-a", "docker-b", "pve-1")):
    old_prod = _inv({h: {"services": [_svc("aac-wazuh-agent")]} for h in prod_hosts})
    new_prod = _inv({h: {"services": [_svc("aac-wazuh-agent", manager_new, secret_new)]} for h in prod_hosts})
    old_dev = _inv({"docker-dev": {"services": [_svc("aac-wazuh-agent")]}})
    new_dev = _inv({"docker-dev": {"services": [_svc("aac-wazuh-agent", manager_new, secret_new)]}})
    return {PROD: (old_prod, new_prod), DEV: (old_dev, new_dev), GLOBAL: (old_prod, new_prod)}


def _stage_with_renders(renders, pipeline_type="infra_plan", ref="abc123def4567"):
    stage = Stage(pipeline_type)
    stage._ref = ref

    async def fake_git(engine, *args):
        if args[0] == "show":
            ref_part, _, path = args[1].partition(":")
            if path in renders:
                old, new = renders[path]
                return (0, _yaml_dump(old if ref_part.endswith("~1") else new))
        return (1, "missing")

    stage._git = fake_git  # type: ignore[assignment]
    return stage


def _reconcile(engine, renders=None, pipeline_type="infra_plan"):
    stage = _stage_with_renders(renders or _renders(), pipeline_type)
    return asyncio.run(stage._run_service_reconcile_action(engine, dict(CONTEXT), {"type": "service_reconcile"}, CHANGED))


def test_reconcile_mode_off_detects_but_stays_silent():
    eng = _Engine("off")
    res = _reconcile(eng)
    assert res.success and "mode 'off'" in res.message
    assert eng.ctx.notifications == [] and eng.ctx.emitted == []


def test_reconcile_notify_reports_service_stage_hosts_and_key_names_only():
    eng = _Engine("notify")
    res = _reconcile(eng)
    assert res.success and "notified" in res.message
    assert len(eng.ctx.notifications) == 1
    endpoint, kw = eng.ctx.notifications[0]
    assert endpoint == "service_reconcile" and kw["severity"] == "warning"
    assert "aac-wazuh-agent (prod, 3 host(s))" in kw["body"]      # global inventory not double counted
    assert "aac-wazuh-agent (dev, 1 host(s))" in kw["body"]
    assert "config.vars.WAZUH_MANAGER" in kw["body"]
    assert kw["notification_id"] == "service_reconcile_abc123d"
    assert SECRET not in _json.dumps(eng.ctx.notifications, default=str)
    assert eng.ctx.emitted == []                                    # 1.5.x never queues jobs


def test_reconcile_dispatch_modes_act_like_notify_on_the_hotfix_line(caplog):
    eng = _Engine("deploy")
    with caplog.at_level(logging.INFO):
        res = _reconcile(eng)
    assert res.success and len(eng.ctx.notifications) == 1 and eng.ctx.emitted == []
    assert "needs plugin ≥ 1.7.0" in caplog.text


def test_reconcile_logs_key_names_never_values(caplog):
    eng = _Engine("notify")
    with caplog.at_level(logging.INFO):
        _reconcile(eng, _renders(secret_new="other-s3cret"))
    assert "config.secrets.KEY" in caplog.text
    assert SECRET not in caplog.text and "other-s3cret" not in caplog.text


def test_reconcile_detection_error_never_fails_the_stage():
    eng = _Engine("notify")
    stage = Stage("infra_plan")

    async def broken_git(engine, *args):
        raise RuntimeError("git exploded")

    stage._git = broken_git  # type: ignore[assignment]
    res = asyncio.run(stage._run_service_reconcile_action(eng, dict(CONTEXT), {"type": "service_reconcile"}, CHANGED))
    assert res.success and "error ignored" in res.message


def test_reconcile_no_change_is_a_quiet_noop():
    eng = _Engine("notify")
    same = _inv({"docker-a": {"services": [_svc("aac-x")]}})
    res = _reconcile(eng, {PROD: (same, same)})
    assert res.success and "no service entries changed" in res.message
    assert eng.ctx.notifications == []


def test_run_action_routes_service_reconcile_type():
    stage = Stage("infra_plan")
    seen = {}

    async def fake(engine, context, action, changed_files):
        seen["called"] = (action, changed_files)
        return rules.StageResult(True, "ok")

    stage._run_service_reconcile_action = fake  # type: ignore[assignment]
    res = asyncio.run(stage._run_action(None, {}, {"type": "service_reconcile"}, CHANGED, [], {}))
    assert res.success and seen["called"][1] == CHANGED


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-v"]))
