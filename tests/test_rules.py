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


# ---------------------------------------------------------- service reconcile (1.7)
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


class _Store:
    """In-memory stand-in for ReconcileStore (same surface the action uses)."""

    def __init__(self):
        self.items = []

    def add(self, **kw):
        rec = {"id": f"r{len(self.items) + 1}", **kw}
        self.items.append(rec)
        return rec

    def update(self, rid, **fields):
        for it in self.items:
            if it["id"] == rid:
                it.update(fields)
                return it
        return None

    def remove(self, rid):
        before = len(self.items)
        self.items = [i for i in self.items if i["id"] != rid]
        return len(self.items) != before

    def deferred(self):
        return [i for i in self.items if i.get("status") == "deferred"]


class DuplicateJob(Exception):
    """Shape of queue.DuplicateJob (matched by name + ``key`` in rules.py)."""

    def __init__(self, key, existing_id=None):
        super().__init__(key)
        self.key = key
        self.existing_id = existing_id


class _Engine:
    def __init__(self, mode="deploy", *, max_hosts=10, max_services=3, store=True, enqueue=None, exclude=None):
        self.config = SimpleNamespace(service_reconcile_mode=mode, service_reconcile_max_hosts=max_hosts,
                                      service_reconcile_max_services=max_services, auto_apply=False)
        if exclude is not None:          # None → attribute absent → built-in default applies
            self.config.service_reconcile_exclude_services = exclude
        self.ctx = _Ctx()
        self.reconcile_store = _Store() if store else None
        self.enqueued = []
        self._enqueue_impl = enqueue
        self.enqueue_pipeline = self._enqueue

    def _enqueue(self, payload, requested_by=None):
        if self._enqueue_impl is not None:
            return self._enqueue_impl(payload)
        self.enqueued.append(payload)
        return 100 + len(self.enqueued)


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


def _renders(manager_new="m.new", secret_new=SECRET, extra_dev_service=False, prod_hosts=("docker-a", "docker-b", "pve-1")):
    """old/new renders: prod inventory with hosts sharing the wazuh profile, dev with one host.
    The global inventory repeats the prod hosts and must NOT be counted again."""
    old_prod = _inv({h: {"services": [_svc("aac-wazuh-agent")]} for h in prod_hosts})
    new_prod = _inv({h: {"services": [_svc("aac-wazuh-agent", manager_new, secret_new)]} for h in prod_hosts})
    old_dev = _inv({"docker-dev": {"services": [_svc("aac-wazuh-agent")]}})
    dev_services = [_svc("aac-wazuh-agent", manager_new, secret_new)]
    if extra_dev_service:
        dev_services.append(_svc("aac-renovate"))
    new_dev = _inv({"docker-dev": {"services": dev_services}})
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


def _notification_text(engine):
    return _json.dumps(engine.ctx.notifications, default=str)


def test_reconcile_mode_off_detects_but_dispatches_nothing():
    eng = _Engine("off")
    res = _reconcile(eng)
    assert res.success and "mode 'off'" in res.message
    assert eng.enqueued == [] and eng.ctx.notifications == [] and eng.ctx.emitted == []


def test_reconcile_unknown_mode_is_treated_as_off():
    eng = _Engine("yolo")
    assert _reconcile(eng).success
    assert eng.enqueued == [] and eng.ctx.notifications == []


def test_reconcile_notify_mode_only_notifies_with_key_names():
    eng = _Engine("notify")
    res = _reconcile(eng)
    assert res.success and "notify only" in res.message
    assert eng.enqueued == []
    assert len(eng.ctx.notifications) == 1
    endpoint, kw = eng.ctx.notifications[0]
    assert endpoint == "service_reconcile" and kw["severity"] == "warning"
    assert "aac-wazuh-agent (prod, 3 host(s))" in kw["body"]
    assert "config.vars.WAZUH_MANAGER" in kw["body"]
    assert kw["notification_id"] == "service_reconcile_abc123d"
    assert SECRET not in _notification_text(eng)


def test_reconcile_deploy_queues_one_job_per_service_and_stage():
    eng = _Engine("deploy")
    res = _reconcile(eng)
    assert res.success and "2 queued, 0 deferred, 0 pending" in res.message
    by_branch = {p["service_branch"]: p for p in eng.enqueued}
    prod, dev = by_branch["main"], by_branch["dev"]
    assert prod["pipeline_type"] == "single_service" and prod["service_name"] == "aac-wazuh-agent"
    assert prod["target_hosts"] == ["docker-a", "docker-b", "pve-1"]      # global inventory not double counted
    assert prod["trigger"] == "controller-diff" and prod["source"] == "service_reconcile"
    assert prod["dry_run"] is False and prod["stage"] == "prod" and prod["site"] == "onprem"
    assert prod["changed_keys"] == ["config.vars.WAZUH_MANAGER"]
    assert prod["inventory_commit"] == "abc123def4567" and prod["source_job_id"] == 7
    assert dev["target_hosts"] == ["docker-dev"] and dev["stage"] == "dev"
    assert eng.reconcile_store.items == []                                # records removed after queueing
    endpoint, kw = eng.ctx.notifications[0]
    assert kw["severity"] == "info" and "queued job #101" in kw["body"]
    assert SECRET not in _notification_text(eng) and SECRET not in _json.dumps(eng.enqueued)


def test_reconcile_dry_run_mode_flags_the_jobs():
    eng = _Engine("dry_run")
    assert _reconcile(eng).success
    assert eng.enqueued and all(p["dry_run"] is True for p in eng.enqueued)


def test_reconcile_max_hosts_threshold_parks_the_big_job_only():
    eng = _Engine("deploy", max_hosts=2)
    res = _reconcile(eng)
    assert res.success and "1 queued, 0 deferred, 1 pending" in res.message
    assert [p["stage"] for p in eng.enqueued] == ["dev"]
    pending = eng.reconcile_store.items
    assert len(pending) == 1 and pending[0]["status"] == "pending"
    assert "max_hosts" in pending[0]["reason"] and pending[0]["payload"]["stage"] == "prod"
    assert "pending approval" in eng.ctx.notifications[0][1]["body"]
    assert eng.ctx.notifications[0][1]["severity"] == "warning"


def test_reconcile_max_services_threshold_parks_everything():
    eng = _Engine("deploy", max_services=2)
    res = _reconcile(eng, _renders(extra_dev_service=True))       # wazuh prod + wazuh dev + renovate dev
    assert res.success and "0 queued, 0 deferred, 3 pending" in res.message
    assert eng.enqueued == []
    assert len(eng.reconcile_store.items) == 3
    assert all("max_services" in i["reason"] for i in eng.reconcile_store.items)


def test_reconcile_duplicate_job_is_deferred_not_lost():
    def dup(payload):
        raise DuplicateJob(f"single_service:{payload['service_name']}", 55)

    eng = _Engine("deploy", enqueue=dup)
    res = _reconcile(eng)
    assert res.success and "0 queued, 2 deferred, 0 pending" in res.message
    deferred = eng.reconcile_store.deferred()
    assert len(deferred) == 2 and all(d["waiting_for_job"] == 55 for d in deferred)
    assert "deferred (job #55 active)" in eng.ctx.notifications[0][1]["body"]


def test_reconcile_dispatch_error_keeps_pending_and_never_fails_the_stage():
    def boom(payload):
        raise RuntimeError("db down")

    eng = _Engine("deploy", enqueue=boom)
    res = _reconcile(eng)
    assert res.success and "0 queued, 0 deferred, 2 pending" in res.message
    assert all(i["status"] == "pending" and "dispatch failed" in i["reason"] for i in eng.reconcile_store.items)


def test_reconcile_rollout_pipeline_never_dispatches():
    eng = _Engine("deploy")
    res = _reconcile(eng, pipeline_type="rollout")
    assert res.success and "rollout pipeline" in res.message
    assert eng.enqueued == [] and len(eng.ctx.notifications) == 1


def test_reconcile_detection_error_is_swallowed():
    eng = _Engine("deploy")
    stage = Stage("infra_plan")

    async def broken_git(engine, *args):
        raise RuntimeError("git exploded")

    stage._git = broken_git  # type: ignore[assignment]
    res = asyncio.run(stage._run_service_reconcile_action(eng, dict(CONTEXT), {"type": "service_reconcile"}, CHANGED))
    assert res.success and "error ignored" in res.message
    assert eng.enqueued == []


def test_reconcile_no_change_is_a_quiet_noop():
    eng = _Engine("deploy")
    same = _inv({"docker-a": {"services": [_svc("aac-x")]}})
    res = _reconcile(eng, {PROD: (same, same)})
    assert res.success and "no service entries changed" in res.message
    assert eng.enqueued == [] and eng.ctx.notifications == []


def test_reconcile_logs_key_names_never_values(caplog):
    eng = _Engine("deploy")
    with caplog.at_level(logging.INFO):
        _reconcile(eng, _renders(secret_new="other-s3cret"))
    assert "config.secrets.KEY" in caplog.text and "config.vars.WAZUH_MANAGER" in caplog.text
    assert SECRET not in caplog.text and "other-s3cret" not in caplog.text


def test_reconcile_without_store_still_dispatches_below_thresholds():
    eng = _Engine("deploy", store=False)
    res = _reconcile(eng)
    assert res.success and len(eng.enqueued) == 2


def test_reconcile_falls_back_to_the_bus_without_enqueue_hook():
    eng = _Engine("deploy")
    eng.enqueue_pipeline = None
    res = _reconcile(eng)
    assert res.success
    topics = [t for t, _ in eng.ctx.emitted]
    assert topics == ["iac:webhook_verified", "iac:webhook_verified"]
    assert eng.reconcile_store.items == []


def test_run_action_routes_service_reconcile_type():
    stage = Stage("infra_plan")
    seen = {}

    async def fake(engine, context, action, changed_files):
        seen["called"] = (action, changed_files)
        return rules.StageResult(True, "ok")

    stage._run_service_reconcile_action = fake  # type: ignore[assignment]
    res = asyncio.run(stage._run_action(None, {}, {"type": "service_reconcile"}, CHANGED, [], {}))
    assert res.success and seen["called"][1] == CHANGED


# ------------------------------------------------ exclude list (aac-hermes-agent by default)
HERMES = "aac-hermes-agent"


def _renders_with_hermes(hermes_name=HERMES):
    """prod: wazuh (3 hosts) + hermes (docker-gpu) change; dev: wazuh changes."""
    base = _renders()
    old_prod, new_prod = base[PROD]
    old_prod["all"]["hosts"]["docker-gpu"] = {"services": [_svc(hermes_name, "m.old")]}
    new_prod["all"]["hosts"]["docker-gpu"] = {"services": [_svc(hermes_name, "m.new")]}
    return base


def _reconcile_with_action(engine, action, renders=None):
    stage = _stage_with_renders(renders or _renders_with_hermes())
    return asyncio.run(stage._run_service_reconcile_action(engine, dict(CONTEXT), action, CHANGED))


def test_reconcile_excludes_hermes_by_default_and_reports_it_as_excluded():
    eng = _Engine("deploy")
    res = _reconcile(eng, _renders_with_hermes())
    assert res.success and "2 queued, 0 deferred, 0 pending approval, 1 excluded" in res.message
    assert sorted(p["service_name"] for p in eng.enqueued) == ["aac-wazuh-agent", "aac-wazuh-agent"]
    assert eng.reconcile_store.items == []                       # no pending/deferred record either
    endpoint, kw = eng.ctx.notifications[0]
    assert kw["title"].startswith("Service changes detected (3)")
    assert f"{HERMES} (prod, 1 host(s))" in kw["body"] and "→ excluded" in kw["body"]
    assert kw["severity"] == "warning"                           # operator must act by hand
    by_outcome = {(s["service"], s["outcome"]) for s in kw["payload"]["services"]}
    assert (HERMES, "excluded") in by_outcome
    assert kw["payload"]["excluded"] == [HERMES]
    assert SECRET not in _notification_text(eng)


def test_reconcile_exclusion_is_case_insensitive_and_tolerates_spaces():
    eng = _Engine("deploy", exclude=" AAC-Hermes-Agent ; aac-other,")
    res = _reconcile(eng, _renders_with_hermes())
    assert "1 excluded" in res.message
    assert all(p["service_name"] != HERMES for p in eng.enqueued) and len(eng.enqueued) == 2


def test_reconcile_cleared_exclude_setting_dispatches_hermes():
    eng = _Engine("deploy", exclude="")
    res = _reconcile(eng, _renders_with_hermes())
    assert res.success and "3 queued" in res.message and "excluded" not in res.message
    assert HERMES in {p["service_name"] for p in eng.enqueued}


def test_reconcile_rule_parameter_exclude_services_adds_to_the_setting():
    eng = _Engine("deploy", exclude=set())
    res = _reconcile_with_action(eng, {"type": "service_reconcile", "exclude_services": ["aac-wazuh-agent"]})
    assert "1 queued" in res.message and "2 excluded" in res.message
    assert [p["service_name"] for p in eng.enqueued] == [HERMES]
    eng2 = _Engine("deploy")
    res2 = _reconcile_with_action(eng2, {"type": "service_reconcile", "exclude_services": "aac-wazuh-agent"})
    assert "0 queued" not in res2.message and "3 excluded" in res2.message and eng2.enqueued == []


def test_reconcile_only_excluded_changes_notify_but_dispatch_nothing():
    eng = _Engine("deploy")
    only_hermes = _inv({"docker-gpu": {"services": [_svc(HERMES, "m.old")]}})
    only_hermes_new = _inv({"docker-gpu": {"services": [_svc(HERMES, "m.new")]}})
    res = _reconcile(eng, {PROD: (only_hermes, only_hermes_new)})
    assert res.success and "1 excluded service change(s) only" in res.message
    assert eng.enqueued == [] and eng.reconcile_store.items == [] and eng.ctx.emitted == []
    assert len(eng.ctx.notifications) == 1 and "→ excluded" in eng.ctx.notifications[0][1]["body"]


def test_reconcile_excluded_services_do_not_count_against_max_services():
    eng = _Engine("deploy", max_services=2)
    res = _reconcile(eng, _renders_with_hermes())                 # wazuh prod + wazuh dev + hermes prod
    assert "2 queued, 0 deferred, 0 pending approval, 1 excluded" in res.message
    assert eng.reconcile_store.items == []


def test_reconcile_excluded_in_notify_mode_and_mode_off():
    eng = _Engine("notify")
    res = _reconcile(eng, _renders_with_hermes())
    assert "notify only, 1 excluded" in res.message
    body = eng.ctx.notifications[0][1]["body"]
    assert f"{HERMES} (prod, 1 host(s))" in body and "→ excluded" in body
    assert "aac-wazuh-agent (prod, 3 host(s))" in body and "→ notify only" in body
    off = _Engine("off")
    res_off = _reconcile(off, _renders_with_hermes())
    assert "2 service change(s) detected, 1 excluded — mode 'off'" in res_off.message
    assert off.ctx.notifications == [] and off.enqueued == []


def test_reconcile_excluded_is_logged_without_values(caplog):
    eng = _Engine("deploy")
    with caplog.at_level(logging.INFO):
        _reconcile(eng, _renders_with_hermes())
    assert "EXCLUDED" in caplog.text and HERMES in caplog.text
    assert SECRET not in caplog.text and "m.new" not in caplog.text


def test_reconcile_excluded_helper_falls_back_to_default_without_config():
    assert Stage._reconcile_excluded(SimpleNamespace(), None) == {HERMES}
    assert Stage._reconcile_excluded(None, {"exclude_services": "aac-x"}) == {HERMES, "aac-x"}
    assert Stage._reconcile_excluded(_Engine(exclude=set()), {}) == set()


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-v"]))
