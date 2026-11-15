"""OS-05 (option A): only the catalog's production branch of a service is
deployed to all its hosts; dev/test branches go to their stage; every other
branch is refused — for every trigger source, and again on the runner."""
import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import yaml
from fastapi import HTTPException

from iac_plugin.app.controller import api as api_mod
from iac_plugin.app.controller import branch_policy
from iac_plugin.app.controller.engine import BranchPolicyStage, SingleServiceStrategy
from iac_plugin.app.controller.job_payload import InvalidJobPayload

# The management block as the live iac-controller catalog has it (incl. its
# ``remove_git_…`` spelling of the dev/test keys).
CATALOG = {"service_catalog": {
    "remote_git_management": {"active": True, "remote_git_default_branch_prod": "main",
                              "remove_git_default_branch_test": "test", "remove_git_default_branch_dev": "dev"},
    "services": [{"name": "aac-traefik"}, {"name": "aac-legacy", "remote_git_default_branch_prod": "master"}],
}}
POLICY = branch_policy.from_catalog(CATALOG)


def _deploy(branch, service="aac-traefik", **extra):
    return {"pipeline_type": "single_service", "service_name": service, "service_branch": branch, **extra}


def test_the_policy_is_read_from_the_catalog_with_defaults():
    assert POLICY == {"prod": "main", "dev": "dev", "test": "test", "services": {"aac-legacy": "master"}}
    assert branch_policy.from_catalog({}) == {"prod": "main", "dev": "dev", "test": "test", "services": {}}
    renamed = branch_policy.from_catalog({"service_catalog": {"remote_git_management": {
        "remote_git_default_branch_prod": "production", "remote_git_default_branch_dev": "develop"}}})
    assert renamed["prod"] == "production" and renamed["dev"] == "develop" and renamed["test"] == "test"


@pytest.mark.parametrize("service,branch,stage", [
    ("aac-traefik", "main", "prod"),            # what the service CI template sends (PROD_BRANCH)
    ("aac-traefik", "dev", "dev"), ("aac-traefik", "feature-x-dev", "dev"), ("aac-traefik", "test", "test"),
    ("aac-traefik", "feature-x", None), ("aac-traefik", "master", None), ("aac-traefik", "hotfix", None),
    ("aac-legacy", "master", "prod"), ("aac-legacy", "main", None),      # the service's own production branch
    ("aac-unknown", "main", "prod"),                                     # not in the catalog: the default
])
def test_stage_of_a_branch(service, branch, stage):
    assert branch_policy.stage_of(POLICY, service, branch) == stage
    refused = branch_policy.violation(POLICY, _deploy(branch, service))
    assert (refused is None) is (stage is not None)
    if refused:
        assert f"branch '{branch}'" in refused and "production branch" in refused


def test_only_single_service_deploys_are_judged_and_no_policy_judges_nothing():
    assert branch_policy.violation(POLICY, {"pipeline_type": "rollout", "limit": "all"}) is None
    assert branch_policy.violation(None, _deploy("feature-x")) is None
    assert "only deploy the production branch" in branch_policy.violation(
        POLICY, _deploy("dev", require_prod_branch=True))
    assert branch_policy.violation(POLICY, _deploy("main", require_prod_branch=True)) is None


# --- trigger time: the one door --------------------------------------------------------------------------

def _publish(svc, policy=POLICY):
    svc.db.update_state(svc.BRANCH_POLICY_SNAPSHOT, {"items": policy}, "")


def test_every_trigger_source_is_refused_for_a_branch_that_may_not_be_deployed(iac_service, monkeypatch):
    svc = iac_service
    _publish(svc)                                                 # a control-plane pod: the runner's snapshot

    async def scenario():
        with pytest.raises(InvalidJobPayload, match="may not be deployed"):
            await svc.submit_job_ex(_deploy("feature-x"), entitlements=("trigger",))
        bus = await svc.enqueue_from_event(_deploy("feature-x"))                       # bus path: refused job
        ok = [await svc.submit_job(_deploy(b, service=f"aac-{i}"), entitlements=("trigger",))
              for i, b in enumerate(("main", "dev", "test", "x-dev"))]
        # API route → 400 with the reason
        monkeypatch.setattr(api_mod, "_ctx", svc.ctx)
        monkeypatch.setattr(api_mod, "_service", svc)
        with pytest.raises(HTTPException) as exc:
            await api_mod.do_trigger_service_deployment("aac-traefik", api_mod.DeployRequest(branch="feature-x"))
        return bus, ok, exc.value

    bus, ok, http = asyncio.run(scenario())
    assert bus is None and all(job_id and job_id > 0 for job_id in ok)
    assert http.status_code == 400 and "branch 'feature-x'" in http.detail and "'main'" in http.detail


def test_without_a_policy_the_trigger_is_queued_and_the_runner_decides(iac_service):
    svc = iac_service                                             # no checkout, no snapshot yet
    assert asyncio.run(svc.submit_job(_deploy("feature-x"), entitlements=("trigger",))) > 0


def test_the_runner_reads_the_policy_from_its_checkout(iac_service, monkeypatch, tmp_path):
    svc = iac_service
    _publish(svc, {**POLICY, "prod": "stale-snapshot"})
    catalog = tmp_path / branch_policy.CATALOG_RELATIVE
    catalog.parent.mkdir(parents=True)
    catalog.write_text(yaml.safe_dump(CATALOG))
    monkeypatch.setenv("PLUGIN_IAC_ORCHESTRATOR_INTERNAL_GIT_REPOS_DIR", str(tmp_path))
    assert svc.branch_policy()["prod"] == "main"                  # the checkout wins over the snapshot


def test_a_pipeline_hook_of_another_branch_is_answered_200_and_not_triggered(iac_service, monkeypatch):
    """GitLab disables a hook that answers 4xx — the refusal is in the body."""
    svc = iac_service
    _publish(svc)
    svc.ctx.get_secret = lambda key: "dummy-webhook-token-not-real" if key == "gitlab_webhook_token" else None
    monkeypatch.setattr(api_mod, "_ctx", svc.ctx)
    monkeypatch.setattr(api_mod, "_service", svc)
    monkeypatch.setattr(api_mod, "_engine", svc.engine)
    monkeypatch.setattr(api_mod, "_recent_events", {})

    def body(ref, pipeline_id):
        return {"object_kind": "pipeline", "project": {"path_with_namespace": "aac-application-definitions/aac-traefik"},
                "object_attributes": {"id": pipeline_id, "status": "success", "source": "push", "ref": ref},
                "commit": {"message": "feat: x"}}

    async def call(ref, pipeline_id):
        request = SimpleNamespace(json=lambda: _async(body(ref, pipeline_id)), client=SimpleNamespace(host="10.0.0.9"))
        return await api_mod.do_gitlab_webhook(request, "dummy-webhook-token-not-real")

    async def _async(value):
        return value

    refused = asyncio.run(call("feature-x", 1))
    assert refused["status"] == "accepted" and refused["triggered"] is False and "feature-x" in refused["reason"]
    assert asyncio.run(call("main", 2))["triggered"] is True


# --- the runner's own check ---------------------------------------------------------------------------------

def _engine(tmp_path, catalog=CATALOG):
    if catalog is not None:
        path = tmp_path / branch_policy.CATALOG_RELATIVE
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(catalog))
    return SimpleNamespace(base_git_dir=tmp_path, config=SimpleNamespace(services_dir=tmp_path / "services"))


def test_the_runner_refuses_before_anything_is_cloned(tmp_path):
    engine = _engine(tmp_path)
    res = asyncio.run(BranchPolicyStage(_deploy("feature-x")).run(engine, {}))
    assert res.success is False and "Nothing was cloned or deployed" in res.message
    assert asyncio.run(BranchPolicyStage(_deploy("main")).run(engine, {})).success
    assert asyncio.run(BranchPolicyStage(_deploy("dev")).run(engine, {})).message == "Branch allowed (dev)"
    forced = asyncio.run(BranchPolicyStage(_deploy("dev", require_prod_branch=True)).run(engine, {}))
    assert forced.success is False and "production branch 'main'" in forced.message
    # no catalog in the checkout: nothing to measure by — as before
    assert asyncio.run(BranchPolicyStage(_deploy("feature-x")).run(_engine(tmp_path / "empty", None), {})).success


def test_the_branch_check_is_the_first_stage_of_a_service_deploy(tmp_path):
    engine = _engine(tmp_path)
    stages = SingleServiceStrategy().build_stages(engine, _deploy("main"), {})
    assert isinstance(stages[0], BranchPolicyStage) and type(stages[1]).__name__ == "CloneServiceRepoStage"
