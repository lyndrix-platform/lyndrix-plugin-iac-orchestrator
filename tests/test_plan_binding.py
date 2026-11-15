"""OS-12: approving a pending infrastructure plan applies THAT plan or nothing.

The approval used to queue a plain ``infra_apply``; the job re-rendered from
whatever the iac-controller was by then and ran plan + apply in one go. A
controller change pushed between the review and the click was applied unseen.
"""
import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from iac_plugin.app.controller import api as api_mod
from iac_plugin.app.controller import plan_binding
from iac_plugin.app.controller.engine import DeploymentEngine, InfraApplyStage, InfraPlanStage
from iac_plugin.app.controller.job_payload import ENT_APPROVE, ENT_TRIGGER, validate_structure
from iac_plugin.app.controller.queue import SubmitResult

sys.path.insert(0, str(Path(__file__).parent))                    # the Terraform harness of test_job_mode
from test_job_mode import _applies, _run_tf, _tf_engine  # noqa: E402

SHA_A, SHA_B = "a" * 40, "b" * 40
ONE_ADD = {"summary": "Plan: 1 to add, 0 to change, 0 to destroy.", "hosts_to_create": ["docker-new"],
           "hosts_to_destroy": []}
ADD_AND_DESTROY = {"summary": "Plan: 1 to add, 0 to change, 1 to destroy.", "hosts_to_create": ["docker-new"],
                   "hosts_to_destroy": ["docker-hydra"]}


# --- fingerprints and the approval payload -------------------------------------------------------

def test_a_fingerprint_names_the_plan_and_changes_with_counts_or_hosts():
    fp = plan_binding.fingerprint(ONE_ADD)
    assert fp.startswith("Plan: 1 to add, 0 to change, 0 to destroy. #") and len(fp) <= 200
    assert fp == plan_binding.fingerprint({**ONE_ADD, "hosts_to_create": ["docker-new"], "fingerprint": "x"})
    assert fp != plan_binding.fingerprint(ADD_AND_DESTROY)
    # the same counts for ANOTHER host are another plan
    assert fp != plan_binding.fingerprint({**ONE_ADD, "hosts_to_create": ["docker-other"]})
    two = {**ONE_ADD, "hosts_to_create": ["b", "a"]}
    assert plan_binding.fingerprint(two) == plan_binding.fingerprint({**two, "hosts_to_create": ["a", "b"]})
    assert plan_binding.fingerprint({"summary": ""}) == "" and plan_binding.fingerprint(None) == ""


def test_differences_are_named_per_environment():
    reviewed = plan_binding.fingerprints({"onprem_prod": ONE_ADD, "onprem_dev": ONE_ADD})
    fresh = plan_binding.fingerprints({"onprem_prod": ADD_AND_DESTROY, "hetzner_prod": ONE_ADD})
    diffs = plan_binding.differences(reviewed, fresh)
    assert len(diffs) == 3
    assert any(d.startswith("hetzner_prod: no changes when reviewed") for d in diffs)
    assert any(d.startswith("onprem_dev: reviewed") and d.endswith("now no changes") for d in diffs)
    assert any("1 to destroy" in d for d in diffs)
    assert plan_binding.differences(reviewed, dict(reviewed)) == []


def test_the_approval_payload_carries_the_binding_and_passes_validation():
    pending = {"job_id": 41, "controller_sha": SHA_A, "envs": plan_binding.with_fingerprints({"onprem_prod": ONE_ADD})}
    payload = plan_binding.approval_payload(pending)
    assert payload["plan_controller_sha"] == SHA_A and payload["plan_job_id"] == 41 and payload["approve"] is True
    assert payload["plan_expected"] == {"onprem_prod": plan_binding.fingerprint(ONE_ADD)}
    stored = validate_structure(dict(payload))                    # what the queue stores, what the runner reads
    assert stored["plan_expected"] == payload["plan_expected"] and stored["plan_controller_sha"] == SHA_A
    assert plan_binding.is_approval(stored)
    assert not plan_binding.is_approval({"pipeline_type": "infra_apply", "approve": True, "trigger": "ui_infra_apply"})
    # A plan recorded without the commit (older version) cannot be approved.
    assert plan_binding.approval_payload({"job_id": 41, "envs": {"onprem_prod": ONE_ADD}}) is None
    assert plan_binding.approval_payload({"controller_sha": SHA_A, "envs": {}}) is None


# --- engine: the bound apply -------------------------------------------------------------------------

ENVS = [{"env_name": "onprem_prod", "host_count": 3, "representative_host": "docker-a"},
        {"env_name": "onprem_dev", "host_count": 1, "representative_host": "docker-dev"}]


def _engine(plans: dict, *, dry=False, tmp_path=None):
    """Engine whose environments plan to ``plans`` (env → plan record, None = no
    changes, an Exception = the plan fails)."""
    eng = DeploymentEngine.__new__(DeploymentEngine)
    eng.state = {}
    eng.db = MagicMock()
    eng._job_modes = {7: dry}
    eng.base_git_dir = tmp_path or MagicMock()
    eng._iter_terraform_environments = lambda: list(ENVS)
    eng._discard_saved_plans = MagicMock()
    eng.calls = []

    async def environment(env_ctx, mode, job_id, force_apply=False):
        env = env_ctx["env_name"]
        eng.calls.append((mode, env, force_apply))
        plan = plans.get(env)
        if isinstance(plan, Exception):
            raise plan
        if mode == "apply_saved":
            return True, {"created_hosts": list((plan or {}).get("hosts_to_create") or [])}
        if plan is None:
            return True, {"plan_summary": ""}
        return True, {"plan_summary": plan["summary"], "hosts_to_create": plan["hosts_to_create"],
                      "hosts_to_destroy": plan["hosts_to_destroy"]}

    eng.execute_terraform_environment = environment
    return eng


def _stored(eng):
    return [c.args[1] for c in eng.db.update_state.call_args_list if c.args[0] == "pending_infra_plan"]


def _reviewed(envs: dict, sha=SHA_A) -> dict:
    return {"controller_sha": sha, "expected": plan_binding.fingerprints(envs)}


def test_a_plan_records_the_controller_commit_and_fingerprints():
    eng = _engine({"onprem_prod": ONE_ADD})
    ok, data = asyncio.run(eng.execute_terraform_infra(mode="plan", job_id=7, controller_sha=SHA_A))
    pending = _stored(eng)[-1]
    assert ok and data["pending_envs"] == 1
    assert pending["controller_sha"] == SHA_A and pending["job_id"] == 7
    assert pending["envs"]["onprem_prod"]["fingerprint"] == plan_binding.fingerprint(ONE_ADD)
    assert plan_binding.approval_payload(pending)["plan_expected"] == plan_binding.fingerprints({"onprem_prod": ONE_ADD})


def test_the_reviewed_plan_is_applied_from_the_plan_files_that_were_compared():
    eng = _engine({"onprem_prod": ONE_ADD})
    ok, data = asyncio.run(eng.execute_terraform_infra(
        mode="apply", job_id=7, force_apply=True, reviewed=_reviewed({"onprem_prod": ONE_ADD}), controller_sha=SHA_A))
    assert ok and "refused" not in data and data["created_hosts"] == ["docker-new"]
    # every environment planned first (read-only), THEN only the saved plans with changes applied
    assert eng.calls == [("plan_save", "onprem_prod", False), ("plan_save", "onprem_dev", False),
                         ("apply_saved", "onprem_prod", True)]
    assert _stored(eng) == [{}]                                   # applied: nothing pending any more


@pytest.mark.parametrize("now,why", [
    ({"onprem_prod": ADD_AND_DESTROY}, "1 to destroy"),                                  # the audit's scenario
    ({"onprem_prod": {**ONE_ADD, "hosts_to_create": ["docker-other"]}}, "onprem_prod"),  # same counts, other host
    ({"onprem_prod": ONE_ADD, "onprem_dev": ONE_ADD}, "onprem_dev: no changes when reviewed"),
    ({}, "now no changes"),
])
def test_a_plan_that_differs_from_the_reviewed_one_is_not_applied(now, why):
    eng = _engine(now)
    ok, data = asyncio.run(eng.execute_terraform_infra(
        mode="apply", job_id=7, force_apply=True, reviewed=_reviewed({"onprem_prod": ONE_ADD}), controller_sha=SHA_A))
    assert ok is False and "NOTHING was applied" in data["refused"] and why in data["refused"]
    assert not any(mode == "apply_saved" for mode, _env, _force in eng.calls)
    eng._discard_saved_plans.assert_called()                      # the unapproved plan files are removed
    stored = _stored(eng)[-1]
    if now:                                                       # the fresh plan awaits a new review
        assert stored["controller_sha"] == SHA_A and stored["job_id"] == 7
        assert set(stored["envs"]) == set(now)
        assert stored["envs"]["onprem_prod"]["fingerprint"] == plan_binding.fingerprint(now["onprem_prod"])
    else:
        assert stored == {}


def test_a_plan_that_cannot_be_computed_means_nothing_is_applied_and_the_review_stands():
    eng = _engine({"onprem_prod": ONE_ADD, "onprem_dev": ValueError("backend unreachable")})
    ok, data = asyncio.run(eng.execute_terraform_infra(
        mode="apply", job_id=7, force_apply=True, reviewed=_reviewed({"onprem_prod": ONE_ADD}), controller_sha=SHA_A))
    assert ok is False and "could not be computed" in data["refused"] and "backend unreachable" in data["refused"]
    assert not any(mode == "apply_saved" for mode, _env, _force in eng.calls)
    assert _stored(eng) == []                                     # the reviewed plan stays pending


def test_an_unconfigured_environment_is_skipped_like_at_plan_time():
    eng = _engine({"onprem_prod": ONE_ADD, "onprem_dev": RuntimeError("Missing Terraform secret(s): x")})
    ok, data = asyncio.run(eng.execute_terraform_infra(
        mode="apply", job_id=7, force_apply=True, reviewed=_reviewed({"onprem_prod": ONE_ADD}), controller_sha=SHA_A))
    assert ok and data["skipped"] == 1 and ("apply_saved", "onprem_prod", True) in eng.calls


def test_a_dry_run_approval_verifies_and_applies_nothing():
    eng = _engine({"onprem_prod": ONE_ADD}, dry=True)
    ok, data = asyncio.run(eng.execute_terraform_infra(
        mode="apply", job_id=7, force_apply=True, reviewed=_reviewed({"onprem_prod": ONE_ADD}), controller_sha=SHA_A))
    assert ok and not any(mode == "apply_saved" for mode, _e, _f in eng.calls)
    assert _stored(eng) == []                                     # still pending


# --- the stage: controller commit --------------------------------------------------------------------

class _StageEngine:
    def __init__(self, head):
        self.head, self.calls = head, []

    async def pinned_controller_sha(self, context=None):
        return self.head

    async def execute_terraform_infra(self, mode, job_id, force_apply=False, *, reviewed=None, controller_sha=None):
        self.calls.append({"mode": mode, "force_apply": force_apply, "reviewed": reviewed, "controller_sha": controller_sha})
        return True, {"environments": 2, "created_hosts": [], "pending_envs": 1}


def _approval(sha=SHA_A, envs=None):
    return plan_binding.approval_payload({"job_id": 41, "controller_sha": sha,
                                          "envs": envs or {"onprem_prod": ONE_ADD}})


def test_a_moved_controller_refuses_the_approval_and_plans_again():
    """P4 reviews plan #41 made at commit A; R pushes B; P4 approves."""
    eng = _StageEngine(head=SHA_B)
    res = asyncio.run(InfraApplyStage().run(eng, {"job_id": 50, "payload": _approval(SHA_A)}))
    assert res.success is False and "NOTHING was applied" in res.message
    assert SHA_A[:12] in res.message and SHA_B[:12] in res.message and "#50" in res.message
    assert eng.calls == [{"mode": "plan", "force_apply": False, "reviewed": None, "controller_sha": SHA_B}]


def test_an_unchanged_controller_hands_the_reviewed_plan_to_the_bound_apply():
    eng = _StageEngine(head=SHA_A)
    payload = _approval(SHA_A)
    res = asyncio.run(InfraApplyStage().run(eng, {"job_id": 50, "payload": payload}))
    assert res.success
    assert eng.calls == [{"mode": "apply", "force_apply": True, "controller_sha": SHA_A,
                          "reviewed": {"controller_sha": SHA_A, "expected": payload["plan_expected"]}}]


@pytest.mark.parametrize("payload", [
    {"pipeline_type": "infra_apply", "approve": True, "trigger": "pending_plan_approval", "plan_job_id": 41},
    {"pipeline_type": "infra_apply", "approve": True, "plan_job_id": 41, "plan_controller_sha": SHA_A},
    {"pipeline_type": "infra_apply", "approve": True, "plan_expected": {"onprem_prod": "Plan: x #1"}},
])
def test_an_approval_without_the_full_binding_applies_nothing(payload):
    eng = _StageEngine(head=SHA_A)
    res = asyncio.run(InfraApplyStage().run(eng, {"job_id": 50, "payload": payload}))
    assert res.success is False and "NOTHING was applied" in res.message and eng.calls == []


def test_an_explicit_deploy_infra_is_not_an_approval_and_runs_as_before():
    eng = _StageEngine(head=SHA_B)
    payload = {"pipeline_type": "infra_apply", "approve": True, "manual": True, "trigger": "ui_infra_apply"}
    assert asyncio.run(InfraApplyStage().run(eng, {"job_id": 50, "payload": payload})).success
    assert eng.calls == [{"mode": "apply", "force_apply": True, "reviewed": None, "controller_sha": SHA_B}]


def test_a_refused_bound_apply_fails_the_stage_with_the_reason():
    eng = _StageEngine(head=SHA_A)

    async def refused(mode, job_id, force_apply=False, *, reviewed=None, controller_sha=None):
        return False, {"refused": "The plan is not the one that was reviewed — NOTHING was applied. onprem_prod: …"}

    eng.execute_terraform_infra = refused
    res = asyncio.run(InfraApplyStage().run(eng, {"job_id": 50, "payload": _approval(SHA_A)}))
    assert res.success is False and res.message.startswith("The plan is not the one that was reviewed")


def test_the_plan_stage_records_the_commit_pinned_in_the_prefix():
    eng = _StageEngine(head=SHA_A)
    asyncio.run(InfraPlanStage().run(eng, {"job_id": 41, "payload": {}}))
    assert eng.calls == [{"mode": "plan", "force_apply": False, "reviewed": None, "controller_sha": SHA_A}]

    real = DeploymentEngine.__new__(DeploymentEngine)

    async def head(slug):
        return SHA_B

    real.repo_head_sha = head
    assert asyncio.run(real.pinned_controller_sha({"iac_controller_sha": SHA_A})) == SHA_A   # pinned under the lock
    assert asyncio.run(real.pinned_controller_sha({})) == SHA_B                                  # else: HEAD now


# --- the containers: plan file in, the same plan file out ------------------------------------------------

def test_plan_save_only_plans_and_apply_saved_only_applies_the_file(monkeypatch):
    eng, shells = _tf_engine(monkeypatch, auto_apply=False)
    eng._set_job_mode(11, False)
    _run_tf(eng, mode="plan_save")
    assert "tofu plan -input=false -no-color -out=tfplan" in shells[-1] and not _applies(shells[-1])
    _run_tf(eng, mode="apply_saved", force_apply=True)
    assert shells[-1].rstrip().endswith("tofu apply -input=false -no-color -auto-approve tfplan")
    assert "tofu plan" not in shells[-1]                          # no second plan: the compared file is applied


def test_a_saved_plan_is_never_applied_without_approval_or_in_a_dry_run(monkeypatch):
    eng, shells = _tf_engine(monkeypatch, auto_apply=False)
    eng._set_job_mode(11, False)
    with pytest.raises(RuntimeError, match="saved plan"):
        _run_tf(eng, mode="apply_saved")                          # no approval, auto_apply off
    eng._set_job_mode(11, True)
    with pytest.raises(RuntimeError, match="saved plan"):
        _run_tf(eng, mode="apply_saved", force_apply=True)        # dry-run job
    assert shells == []


def test_apply_saved_reuses_the_run_dir_of_the_plan_and_needs_its_plan_file(tmp_path):
    eng = DeploymentEngine.__new__(DeploymentEngine)
    eng.base_git_dir = tmp_path
    calls = []

    async def docker(**kwargs):
        calls.append(kwargs)
        return True, {}

    eng.execute_terraform_docker = docker
    env = ENVS[0]
    with pytest.raises(RuntimeError, match="No saved plan"):
        asyncio.run(eng.execute_terraform_environment(env, "apply_saved", 7, force_apply=True))
    run_dir = tmp_path / "inventory_state/.terraform_runs/onprem_prod__apply__j7"
    run_dir.mkdir(parents=True)
    (run_dir / "tfplan").write_bytes(b"plan")
    asyncio.run(eng.execute_terraform_environment(env, "apply_saved", 7, force_apply=True))
    assert calls[0]["mode"] == "apply_saved" and calls[0]["force_apply"] is True
    assert calls[0]["run_rel_dir"] == "inventory_state/.terraform_runs/onprem_prod__apply__j7"
    assert calls[0]["stable_rel_dir"] == "inventory_state/.terraform_runs/onprem_prod__apply"
    assert calls[0]["state_key"] == "tf:onprem_prod"

    # … and what was not applied is removed again (its plan file must not linger).
    eng._discard_saved_plans([env], 7)
    assert not run_dir.exists()
    stable = tmp_path / "inventory_state/.terraform_runs/onprem_prod__apply"
    stable.mkdir()
    eng._discard_saved_plans([env], 0)                            # no job id: never the shared dir
    assert stable.exists()


# --- API ----------------------------------------------------------------------------------------------------

@pytest.fixture
def api(monkeypatch):
    state = {}
    db = SimpleNamespace(get_state=lambda key: {"data": state.get(key)} if state.get(key) else None,
                         update_state=lambda key, data, commit: state.__setitem__(key, data))
    monkeypatch.setattr(api_mod, "_ctx", MagicMock())
    monkeypatch.setattr(api_mod, "_engine", SimpleNamespace(db=db))
    submitted = []

    async def submit_job_ex(payload, *, entitlements=(), requested_by=None):
        submitted.append((validate_structure(dict(payload)), set(entitlements)))
        return SubmitResult(51)

    monkeypatch.setattr(api_mod, "_service", SimpleNamespace(state={}, submit_job_ex=submit_job_ex))
    return SimpleNamespace(state=state, submitted=submitted)


def test_the_approve_route_binds_the_approval_to_the_pending_plan(api):
    api.state["pending_infra_plan"] = {"job_id": 41, "controller_sha": SHA_A, "planned_at": "2026-11-15T10:00:00+00:00",
                                       "envs": plan_binding.with_fingerprints({"onprem_prod": ADD_AND_DESTROY})}
    shown = asyncio.run(api_mod.do_get_pending_plan())
    assert shown["pending"] and shown["approvable"] is True and shown["controller_sha"] == SHA_A
    asyncio.run(api_mod.do_apply_pending_plan(SimpleNamespace(username="p4")))
    payload, entitlements = api.submitted[0]
    assert entitlements == {ENT_TRIGGER, ENT_APPROVE}
    assert payload["plan_controller_sha"] == SHA_A and payload["plan_job_id"] == 41 and payload["approve"] is True
    assert payload["plan_expected"] == {"onprem_prod": plan_binding.fingerprint(ADD_AND_DESTROY)}


def test_a_pending_plan_without_the_commit_cannot_be_approved(api):
    api.state["pending_infra_plan"] = {"job_id": 40, "envs": {"onprem_prod": ONE_ADD}}       # stored by <= 1.6.1
    assert asyncio.run(api_mod.do_get_pending_plan())["approvable"] is False
    with pytest.raises(HTTPException) as exc:
        asyncio.run(api_mod.do_apply_pending_plan(SimpleNamespace(username="p4")))
    assert exc.value.status_code == 409 and "Run Check Env again" in exc.value.detail
    assert api.submitted == []
