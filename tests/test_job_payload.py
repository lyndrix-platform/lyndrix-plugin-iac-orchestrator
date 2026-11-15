"""OS-09: job payloads are validated where they enter the queue and again when a
runner claims them; privileged flags only from entitled code paths; the bus is
untrusted input."""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlalchemy import select

from iac_plugin.app.controller import api as api_mod
from iac_plugin.app.controller import job_payload as jp
from iac_plugin.app.controller import maintenance_resolver as mres
from iac_plugin.app.controller import queue as queue_mod
from iac_plugin.app.controller.engine import TriggerHostRolloutStage
from iac_plugin.app.controller.queue import SubmitResult
from iac_plugin.app.model.models import IaCJob
from iac_plugin.stages import git as git_stage

BAD_NAMES = ["../victim", "/", "/etc", "-x", "--upload-pack=x", "a/b", "..", ".hidden", "a..b", "x;rm -rf", "a b",
             "{{lookup('pipe','id')}}", "x\ny", ""]


def _rows():
    with queue_mod.db_instance.SessionLocal() as s:
        return s.execute(select(IaCJob)).scalars().all()


# --- structure ------------------------------------------------------------------------

@pytest.mark.parametrize("bad", [b for b in BAD_NAMES if b])
def test_names_are_strict_tokens(bad):
    for payload in ({"pipeline_type": "single_service", "service_name": bad},
                    {"pipeline_type": "single_service", "service_name": "aac-x", "service_branch": bad},
                    {"pipeline_type": "host_provision", "host_name": bad},
                    {"pipeline_type": "rollout", "limit": bad},
                    {"pipeline_type": "rollout", "limit": "all", "target_services": ["aac-ok", bad]}):
        with pytest.raises(jp.InvalidJobPayload):
            jp.validate_structure(payload)


def test_unknown_types_and_keys():
    for ptype in ("exec", "infra_destroy", "", "single_service; x"):
        with pytest.raises(jp.InvalidJobPayload):
            jp.validate_structure({"pipeline_type": ptype, "service_name": "aac-x"})
    with pytest.raises(jp.InvalidJobPayload):
        jp.validate_structure("not a dict")
    with pytest.raises(jp.InvalidJobPayload):
        jp.validate_structure({"pipeline_type": "single_service"})         # no service
    clean = jp.validate_structure({"pipeline_type": "single_service", "service_name": "AAC-X", "dry_run": "true",
                                   "extra_vars": {"x": 1}, "playbook": "evil.yml", "ssh_key_secret": "iac_x",
                                   "remote_user": "root", "trigger": "ui"})
    assert clean == {"pipeline_type": "single_service", "service_name": "aac-x", "service_branch": "main",
                     "dry_run": True, "trigger": "ui"}
    assert jp.validate_structure(clean) == clean                            # idempotent (claim-time re-check)


def test_privileged_flags_only_where_they_mean_something():
    assert "approve" not in jp.validate_structure({"pipeline_type": "rollout", "limit": "all", "approve": True})
    assert "force" not in jp.validate_structure({"pipeline_type": "infra_plan", "force": True})
    assert jp.validate_structure({"pipeline_type": "infra_apply", "approve": True})["approve"] is True
    assert jp.validate_structure({"pipeline_type": "single_service", "service_name": "a", "force": 1})["force"] is True


def test_strip_unentitled():
    payload = {"pipeline_type": "infra_apply", "approve": True, "force": True, "trigger": "ui"}
    clean, dropped = jp.strip_unentitled(payload, ())
    assert clean == {"pipeline_type": "infra_apply", "trigger": "bus"} and sorted(dropped) == ["approve", "force", "trigger"]
    clean, dropped = jp.strip_unentitled(payload, (jp.ENT_TRIGGER, jp.ENT_APPROVE))
    assert clean["approve"] is True and clean["trigger"] == "ui" and dropped == ["force"]
    assert mres.policy_applies({"trigger": jp.UNTRUSTED_TRIGGER})           # an untrusted deploy follows the policy


# --- enqueue: the single entry point -----------------------------------------------------

@pytest.mark.parametrize("bad", ["../x", "/", "-x"])
def test_queue_never_stores_an_invalid_payload(sqlite_queue, bad):
    with pytest.raises(jp.InvalidJobPayload):
        sqlite_queue.enqueue({"pipeline_type": "single_service", "service_name": bad})
    with pytest.raises(jp.InvalidJobPayload):
        sqlite_queue.enqueue({"pipeline_type": "bootstrap_compliance", "host_name": bad})
    assert _rows() == []


def test_bus_trigger_is_untrusted(iac_service):
    svc = iac_service

    async def scenario():
        apply_id = await svc.enqueue_from_event({"pipeline_type": "infra_apply", "approve": True, "manual": True,
                                                 "trigger": "manual_infra_apply"})
        deploy_id = await svc.enqueue_from_event({"pipeline_type": "single_service", "service_name": "aac-x",
                                                  "force": True, "trigger": "ui"})
        rejected = [await svc.enqueue_from_event(p) for p in (
            {"pipeline_type": "single_service", "service_name": "../../security"},
            {"pipeline_type": "bootstrap_compliance", "host_name": "-e@/etc/passwd"},
            {"pipeline_type": "shell"}, None)]
        return apply_id, deploy_id, rejected

    apply_id, deploy_id, rejected = asyncio.run(scenario())
    assert rejected == [None, None, None, None]
    rows = {r.id: json.loads(r.payload) for r in _rows()}
    assert set(rows) == {apply_id, deploy_id}
    assert "approve" not in rows[apply_id] and rows[apply_id]["trigger"] == "bus"     # would only plan
    assert "force" not in rows[deploy_id] and rows[deploy_id]["trigger"] == "bus"     # follows the deploy policy


def test_entitled_code_path_keeps_its_flags(iac_service):
    job_id = asyncio.run(iac_service.submit_job(
        {"pipeline_type": "infra_apply", "approve": True, "trigger": "manual_infra_apply"},
        entitlements=(jp.ENT_TRIGGER, jp.ENT_APPROVE)))
    payload = json.loads(_rows()[0].payload)
    assert job_id and payload["approve"] is True and payload["trigger"] == "manual_infra_apply"


# --- claim time: the row is untrusted too ---------------------------------------------------

def test_poisoned_row_is_rejected_at_claim_and_never_executed(iac_service):
    svc = iac_service
    with queue_mod.db_instance.SessionLocal() as s:        # written past enqueue (another instance, SQL access)
        row = IaCJob(pipeline_type="single_service:x", status="QUEUED", logs="[]", pending_tasks="[]",
                     payload=json.dumps({"pipeline_type": "single_service", "service_name": "../../security",
                                         "approve": True}))
        s.add(row)
        s.commit()
        job_id = row.id
    ran = []

    async def run_pipeline(payload, job_id=None):
        ran.append(payload)

    svc.engine.run_pipeline = run_pipeline
    claimed_id, payload = svc.queue.claim_next("docker-auto-runner")
    asyncio.run(svc.run_claimed(payload, claimed_id))
    row = _rows()[0]
    assert ran == [] and claimed_id == job_id
    assert row.status == "FAILED" and row.current_step.startswith("Rejected: invalid job payload")
    assert any("nothing was executed" in line for line in svc.queue.read_log(job_id)["lines"])


def test_claimed_payload_reaches_the_engine_sanitised(iac_service):
    svc = iac_service
    with queue_mod.db_instance.SessionLocal() as s:
        row = IaCJob(pipeline_type="rollout", status="QUEUED", logs="[]", pending_tasks="[]",
                     payload=json.dumps({"pipeline_type": "rollout", "limit": "docker-dev", "approve": True,
                                         "force": True, "extra_vars": {"x": 1}}))
        s.add(row)
        s.commit()
    seen = []

    async def run_pipeline(payload, job_id=None):
        seen.append(payload)

    svc.engine.run_pipeline = run_pipeline
    job_id, payload = svc.queue.claim_next("docker-auto-runner")
    asyncio.run(svc.run_claimed(payload, job_id))
    assert seen == [{"pipeline_type": "rollout", "limit": "docker-dev"}]


# --- point of use ---------------------------------------------------------------------------

def test_clone_stage_never_leaves_the_services_dir(tmp_path, monkeypatch):
    services = tmp_path / "services"
    victim = tmp_path / "victim"
    services.mkdir()
    victim.mkdir()
    (victim / "important.txt").write_text("keep me")
    calls = []

    async def fake_exec(*cmd, **kw):
        calls.append(cmd)
        raise AssertionError("git must not be called")

    monkeypatch.setattr(git_stage.asyncio, "create_subprocess_exec", fake_exec)
    engine = SimpleNamespace(config=SimpleNamespace(services_dir=services, security_dir=tmp_path / "sec"),
                             base_git_dir=tmp_path / "git", ctx=SimpleNamespace(get_secret=lambda k: None))
    for name, branch in (("../victim", "main"), (str(victim), "main"), ("aac-x", "--upload-pack=x"), ("aac-x", "../m")):
        res = asyncio.run(git_stage.CloneServiceRepoStage(name, branch, {}).run(engine, {}))
        assert res.success is False and "Refusing" in res.message
    assert (victim / "important.txt").read_text() == "keep me" and calls == []


# --- entitled code paths ----------------------------------------------------------------------

def test_api_routes_submit_with_their_own_entitlements(monkeypatch):
    submitted = []

    async def submit_job_ex(payload, *, entitlements=(), requested_by=None):
        submitted.append((payload["pipeline_type"], set(entitlements)))
        return SubmitResult(7)

    ctx = MagicMock()
    monkeypatch.setattr(api_mod, "_ctx", ctx)
    monkeypatch.setattr(api_mod, "_service", SimpleNamespace(submit_job_ex=submit_job_ex))
    assert asyncio.run(api_mod.do_trigger_infra_plan())["job_id"] == 7
    asyncio.run(api_mod.do_trigger_infra_apply())
    asyncio.run(api_mod.do_trigger_service_deployment("aac-x", api_mod.DeployRequest(), trigger="gitlab-ci"))
    asyncio.run(api_mod.do_run_pipeline(api_mod.PipelineRequest(pipeline_type="rollout", limit="all")))
    assert submitted == [("infra_plan", {"trigger"}), ("infra_apply", {"trigger", "approve"}),
                         ("single_service", {"trigger"}), ("rollout", {"trigger"})]
    ctx.emit.assert_not_called()                     # triggers no longer travel over the bus


def test_invalid_payload_from_a_route_is_a_400(monkeypatch):
    from fastapi import HTTPException

    async def submit_job_ex(payload, *, entitlements=(), requested_by=None):
        raise jp.InvalidJobPayload("invalid limit")

    monkeypatch.setattr(api_mod, "_ctx", MagicMock())
    monkeypatch.setattr(api_mod, "_service", SimpleNamespace(submit_job_ex=submit_job_ex))
    with pytest.raises(HTTPException) as exc:
        asyncio.run(api_mod.do_trigger_infra_plan())
    assert exc.value.status_code == 400


def test_engine_hand_off_reports_what_really_happened():
    calls = []

    outcomes = [
        SubmitResult(31),
        SubmitResult(32, "follow_up", waiting_for=30),
        SubmitResult(33, "coalesced"),
        SubmitResult(None, "duplicate", duplicate_of=29, reason="a queued job for this target does something else"),
    ]

    async def submit(payload, *, entitlements=()):
        calls.append((payload, tuple(entitlements)))
        return outcomes[len(calls) - 1]

    engine = SimpleNamespace(submit_job_ex=submit, ctx=MagicMock())
    results = [asyncio.run(TriggerHostRolloutStage(name).run(engine, {}))
               for name in ("Docker-New", "docker-new", "docker-new", "docker-new")]
    assert calls[0] == ({"pipeline_type": "rollout", "limit": "docker-new", "manual": True,
                         "source": "host_provision_chain", "trigger": "engine"}, ("trigger",))
    queued, follow_up, coalesced, refused = (r.message for r in results)
    assert "queued for 'docker-new' as job #31" in queued
    assert "job #32" in follow_up and "job #30" in follow_up            # waits, is not lost
    assert "already queued as job #33" in coalesced
    assert "NOT queued" in refused and "job #29" in refused             # never reported as queued
    assert all(r.success for r in results)                              # the provision itself went through
    engine.ctx.emit.assert_not_called()
