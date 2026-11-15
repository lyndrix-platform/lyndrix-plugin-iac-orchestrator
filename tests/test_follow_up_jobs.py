"""OC-05: a trigger for a busy target is kept, not dropped — and never turns a job
into something its own trigger did not ask for.

Per target at most one job RUNS. A trigger that arrives meanwhile becomes one
QUEUED follow-up (claimed only when the running job is done); a further trigger
that would do exactly the same is merged into the job that has not started;
everything else (dry-run vs. real run, other branch/limit, approved runs) is
refused and reported — never merged, never upgraded.

Every test asserts what is NOT executed / NOT changed.
"""
import asyncio
import json
import logging
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlalchemy import select, update

from iac_plugin.app.controller import api as api_mod
from iac_plugin.app.controller import queue as queue_mod
from iac_plugin.app.controller.queue import DuplicateJob, JobWorker, SubmitResult, covers
from iac_plugin.app.model.models import IaCJob

CI = {"pipeline_type": "single_service", "service_name": "aac-x", "service_branch": "main", "trigger": "gitlab-ci"}
UI = {"pipeline_type": "single_service", "service_name": "aac-x", "service_branch": "main", "trigger": "ui",
      "manual": True}


def _rows():
    with queue_mod.db_instance.SessionLocal() as s:
        return s.execute(select(IaCJob).order_by(IaCJob.id)).scalars().all()


def _row(job_id):
    with queue_mod.db_instance.SessionLocal() as s:
        return s.get(IaCJob, job_id)


def _payload(job_id):
    return json.loads(_row(job_id).payload)


def _finish(job_id, status="SUCCESS"):
    with queue_mod.db_instance.SessionLocal() as s:
        s.execute(update(IaCJob).where(IaCJob.id == job_id).values(status=status, end_time=datetime.now()))
        s.commit()


# --- enqueue ---------------------------------------------------------------------------

def test_trigger_for_a_running_target_becomes_exactly_one_follow_up(sqlite_queue):
    q = sqlite_queue
    a = q.enqueue(CI, requested_by="gitlab-ci")
    assert q.claim_next("runner-1")[0] == a                      # commit A is being deployed

    b = q.enqueue_ex(CI, requested_by="gitlab-ci")               # commit B's CI triggers meanwhile
    assert b.job_id not in (a, -1) and b.waiting_for == a and not b.coalesced
    row = _row(b.job_id)
    assert row.status == "QUEUED" and f"waiting for job #{a}" in row.current_step
    assert _payload(b.job_id)["followup_of"] == a

    c = q.enqueue_ex(CI, requested_by="gitlab-ci")               # commit C: B has not started, it deploys HEAD
    assert c.job_id == b.job_id and c.coalesced and c.waiting_for is None
    assert [r.id for r in _rows()] == [a, b.job_id]              # never a third job
    assert _row(a).status == "RUNNING" and "aac-x" in _row(a).payload   # the running job is untouched


def test_claim_leaves_the_follow_up_until_the_first_job_is_done(sqlite_queue):
    q = sqlite_queue
    a = q.enqueue(CI)
    q.claim_next("runner-1")
    b = q.enqueue(CI)
    assert q.claim_next("runner-1") is None and q.claim_next("runner-2") is None   # not next to job A
    assert _row(b).status == "QUEUED" and _row(b).owner_instance is None

    other = q.enqueue({"pipeline_type": "single_service", "service_name": "aac-y", "trigger": "gitlab-ci"})
    assert q.claim_next("runner-1")[0] == other                  # an unrelated target is not held up
    assert q.claim_next("runner-1") is None

    _finish(a, "FAILED")                                         # done — whatever the result
    assert q.claim_next("runner-1")[0] == b
    assert q.claim_next("runner-1") is None


def test_a_queued_job_is_never_changed_by_a_trigger_that_does_something_else(sqlite_queue):
    q = sqlite_queue
    real = q.enqueue(CI)
    before = _row(real).payload
    for other in ({**CI, "dry_run": True}, {**CI, "service_branch": "dev"}):
        with pytest.raises(DuplicateJob) as exc:
            q.enqueue(other)
        assert exc.value.existing_id == real and "something else" in exc.value.reason
    assert _row(real).payload == before and len(_rows()) == 1    # not merged, not replaced, nothing added

    # the dangerous direction: a queued dry-run must never become a real run
    _finish(real)
    dry = q.enqueue({**CI, "dry_run": True})
    with pytest.raises(DuplicateJob):
        q.enqueue(CI)
    assert _payload(dry)["dry_run"] is True and len(_rows()) == 2
    assert q.claim_next("runner-1")[1]["dry_run"] is True


def test_a_follow_up_keeps_its_own_mode_and_the_running_job_keeps_its(sqlite_queue):
    q = sqlite_queue
    real = q.enqueue(CI)
    q.claim_next("runner-1")
    dry = q.enqueue_ex({**CI, "dry_run": True})
    assert dry.waiting_for == real and _payload(dry.job_id)["dry_run"] is True
    assert "dry_run" not in _payload(real)
    again = q.enqueue_ex({**CI, "dry_run": True})
    assert again.coalesced and again.job_id == dry.job_id
    with pytest.raises(DuplicateJob):                            # a real run is not folded into the queued dry-run
        q.enqueue(CI)
    assert _payload(dry.job_id)["dry_run"] is True and len(_rows()) == 2


def test_covers_is_strict_about_work_and_mode():
    assert covers(CI, CI) and covers(UI, UI)
    assert covers(UI, CI)                         # the queued job runs now anyway → the CI commit is in it
    assert not covers(CI, UI)                     # a policy-governed job may be parked; "run now" is not served
    assert covers({**UI, "force": True}, CI)
    for other in ({**CI, "dry_run": True}, {**CI, "service_branch": "dev"}, {**CI, "service_name": "aac-y"},
                  {**CI, "pipeline_type": "rollout"}):
        assert not covers(CI, other) and not covers(other, CI)
    plan = {"pipeline_type": "infra_apply", "trigger": "manual_infra_apply"}
    assert not covers(plan, {**plan, "approve": True}) and not covers({**plan, "approve": True}, plan)
    approved = {**plan, "approve": True, "plan_job_id": 7}
    assert covers(approved, dict(approved)) and not covers(approved, {**approved, "plan_job_id": 8})
    rollout = {"pipeline_type": "rollout", "limit": "all"}
    assert not covers(rollout, {**rollout, "target_services": ["aac-x"]})
    assert not covers(rollout, {**rollout, "target_services": []})


def test_policy_trigger_is_covered_by_a_queued_run_now_job_but_not_the_reverse(sqlite_queue):
    q = sqlite_queue
    ui = q.enqueue(UI, requested_by="alice")
    before = _row(ui).payload
    res = q.enqueue_ex(CI, requested_by="gitlab-ci")
    assert res.coalesced and res.job_id == ui
    assert _row(ui).payload == before and _row(ui).requested_by == "alice"       # nothing written into the job
    assert _row(ui).current_step.endswith("(+ newer trigger)")
    q.enqueue_ex(CI)
    assert _row(ui).current_step.count("(+ newer trigger)") == 1

    _finish(ui)
    ci = q.enqueue(CI)
    with pytest.raises(DuplicateJob):                 # "run now" is not served by a job the policy may park
        q.enqueue(UI)
    assert "force" not in _payload(ci) and _payload(ci)["trigger"] == "gitlab-ci"


def test_approved_run_is_never_parked_behind_another_job(sqlite_queue):
    q = sqlite_queue
    plan = q.enqueue({"pipeline_type": "infra_plan", "trigger": "manual_infra_plan"})
    q.claim_next("runner-1")                                     # a plan is publishing/planning right now
    with pytest.raises(DuplicateJob) as exc:
        q.enqueue({"pipeline_type": "infra_apply", "approve": True, "trigger": "pending_plan_approval",
                   "plan_job_id": 5})
    assert exc.value.existing_id == plan and "approved" in exc.value.reason
    assert [r.id for r in _rows()] == [plan]                     # no apply waits for "later"

    # a controller push during an (approved) apply still gets its plan — read-only, afterwards
    _finish(plan)
    apply_id = q.enqueue({"pipeline_type": "infra_apply", "approve": True, "trigger": "manual_infra_apply"})
    q.claim_next("runner-1")
    push = q.enqueue_ex({"pipeline_type": "infra_plan", "trigger": "manual_infra_plan"})
    assert push.waiting_for == apply_id
    follow_up = _payload(push.job_id)
    assert follow_up["pipeline_type"] == "infra_plan" and "approve" not in follow_up
    assert q.claim_next("runner-1") is None


def test_host_rollout_hand_off_survives_a_global_rollout_and_order_is_kept(sqlite_queue):
    q = sqlite_queue
    glob = q.enqueue({"pipeline_type": "rollout", "limit": "all", "manual": True})
    new_host = q.enqueue_ex({"pipeline_type": "rollout", "limit": "docker-new", "trigger": "engine"})
    assert new_host.waiting_for == glob and new_host.job_id != glob           # used to be dropped
    assert _payload(new_host.job_id)["limit"] == "docker-new"                 # its own limit, not widened

    assert q.claim_next("r")[0] == glob
    assert q.claim_next("r") is None                                          # host rollout not next to the global one
    _finish(glob)
    assert q.claim_next("r")[0] == new_host.job_id

    # a waiting global rollout is not overtaken by host rollouts that keep arriving
    glob2 = q.enqueue_ex({"pipeline_type": "rollout", "limit": "all"})
    assert glob2.waiting_for == new_host.job_id
    late = q.enqueue_ex({"pipeline_type": "rollout", "limit": "docker-late"})
    assert late.waiting_for == glob2.job_id
    assert q.claim_next("r") is None
    _finish(new_host.job_id)
    assert q.claim_next("r")[0] == glob2.job_id and q.claim_next("r") is None
    _finish(glob2.job_id)
    assert q.claim_next("r")[0] == late.job_id


def test_rows_without_a_key_still_block_their_target(sqlite_queue):
    """A RUNNING row written without ``dedupe_key`` (direct engine call, old plugin
    version): the claim derives the key from its tag."""
    q = sqlite_queue
    with queue_mod.db_instance.SessionLocal() as s:
        for tag in ("rollout", "single_service:aac-x", "infra_apply"):
            s.add(IaCJob(pipeline_type=tag, status="RUNNING", logs="[]", pending_tasks="[]"))
        s.commit()
    for payload in ({"pipeline_type": "rollout", "limit": "docker-dev"}, CI, {"pipeline_type": "infra_plan"}):
        assert q.enqueue_ex(payload).waiting_for is not None
    assert q.claim_next("r") is None


def test_parked_follow_up_supersedes_the_older_parked_job(sqlite_queue):
    """Trigger B arrives in the seconds job A is RUNNING before the policy stage
    parks it: B is a follow-up, gets claimed once A is parked and is parked too.
    One deploy per target and window — the newer trigger wins."""
    q = sqlite_queue
    due = datetime.now() + timedelta(days=1)
    a = q.enqueue(CI)
    q.claim_next("r")
    b = q.enqueue_ex({**CI, "service_branch": "release-2"}).job_id
    assert q.park(a, not_before=due, window_ci_ids=None, reason="window")
    assert q.claim_next("r")[0] == b
    assert q.park(b, not_before=due, window_ci_ids=None, reason="window")
    assert _row(b).status == "SCHEDULED" and _row(a).status == "ABORTED"
    assert f"Superseded by scheduled job #{b}" in _row(a).current_step and _row(a).not_before is None
    assert q.scheduled_count() == 1
    other = q.enqueue({"pipeline_type": "single_service", "service_name": "aac-y", "trigger": "gitlab-ci"})
    q.claim_next("r")
    assert q.park(other, not_before=due, window_ci_ids=None, reason="window")
    assert _row(b).status == "SCHEDULED" and q.scheduled_count() == 2           # other targets untouched


# --- worker ----------------------------------------------------------------------------

class _Ctx:
    instance_id = "runner-x"

    def __init__(self):
        self.log = logging.getLogger("test")

    def create_task(self, coro, name=None):
        return asyncio.get_running_loop().create_task(coro)


def test_follow_up_runs_after_its_blocker_exactly_once_and_never_next_to_it(sqlite_queue):
    q = sqlite_queue
    events: list[tuple] = []
    running: set[int] = set()
    overlap: list[tuple] = []

    async def scenario():
        gate = asyncio.Event()

        async def run_job(payload, job_id):
            if running:
                overlap.append((job_id, set(running)))
            running.add(job_id)
            events.append(("start", job_id))
            if job_id == a:
                await gate.wait()
            await asyncio.to_thread(_finish, job_id)
            running.discard(job_id)
            events.append(("end", job_id))

        a = q.enqueue(CI)
        worker = JobWorker(_Ctx(), q, run_job, max_parallel=3, poll_seconds=0.02, heartbeat_seconds=60)
        worker.start()
        for _ in range(200):
            await asyncio.sleep(0.01)
            if events:
                break
        b = q.enqueue_ex(CI)
        c = q.enqueue_ex(CI)
        await asyncio.sleep(0.3)                    # many polls with two free slots
        assert events == [("start", a)]             # the follow-up did NOT start next to job A
        gate.set()
        for _ in range(300):
            await asyncio.sleep(0.01)
            if ("end", b.job_id) in events:
                break
        await asyncio.sleep(0.15)                   # anything else that would (wrongly) start
        await worker.stop()
        return a, b, c

    a, b, c = asyncio.run(scenario())
    assert events == [("start", a), ("end", a), ("start", b.job_id), ("end", b.job_id)]
    assert not overlap
    assert c.coalesced and c.job_id == b.job_id and len(_rows()) == 2          # trigger C never ran on its own


# --- service / API: the caller learns what happened ---------------------------------------

def test_submit_reports_the_real_outcome(iac_service):
    svc = iac_service
    svc.engine._notify = MagicMock()

    async def scenario():
        first = await svc.submit_job_ex(dict(CI), entitlements=("trigger",))
        svc.queue.claim_next("docker-auto-runner")
        follow = await svc.submit_job_ex(dict(CI), entitlements=("trigger",))
        merged = await svc.submit_job_ex(dict(CI), entitlements=("trigger",))
        refused = await svc.submit_job_ex({**CI, "dry_run": True}, entitlements=("trigger",))
        return first, follow, merged, refused

    first, follow, merged, refused = asyncio.run(scenario())
    assert (first.outcome, first.queued) == ("queued", True)
    assert follow.outcome == "follow_up" and follow.waiting_for == first.job_id and follow.job_id != first.job_id
    assert merged.outcome == "coalesced" and merged.job_id == follow.job_id
    assert refused.outcome == "duplicate" and not refused.queued and refused.job_id is None
    assert refused.duplicate_of == follow.job_id and refused.reason
    assert len(_rows()) == 2
    titles = [c.args[0] for c in svc.engine._notify.call_args_list]
    assert titles == ["Pipeline Queued", "Pipeline Ignored"]     # the follow-up and the refusal are announced
    # the old int-or-None interface still answers truthfully
    assert asyncio.run(svc.submit_job({**CI, "dry_run": True}, entitlements=("trigger",))) is None


@pytest.mark.parametrize("result,status,in_message", [
    (SubmitResult(41), "accepted", "queued for aac-x"),
    (SubmitResult(42, "follow_up", waiting_for=41), "accepted", "starts when job #41"),
    (SubmitResult(42, "coalesced"), "accepted", "Already queued as job #42"),
    (SubmitResult(None, "duplicate", duplicate_of=42, reason="a queued job for this target does something else"),
     "duplicate", "Not queued"),
])
def test_trigger_endpoint_says_what_happened(monkeypatch, result, status, in_message):
    async def submit_job_ex(payload, *, entitlements=(), requested_by=None):
        return result

    ctx = MagicMock()
    monkeypatch.setattr(api_mod, "_ctx", ctx)
    monkeypatch.setattr(api_mod, "_service", SimpleNamespace(submit_job_ex=submit_job_ex))
    body = asyncio.run(api_mod.do_trigger_service_deployment("aac-x", api_mod.DeployRequest(), trigger="gitlab-ci"))
    assert body["status"] == status and in_message in body["message"]
    assert body["job_id"] == result.job_id and body["queued"] is result.queued and body["outcome"] == result.outcome
    assert body.get("waiting_for") == result.waiting_for and body.get("duplicate_of") == result.duplicate_of
    # "Pipeline … triggered" is announced only for a trigger a job will run
    assert ctx.notify.called is result.queued


def test_webhook_answers_200_and_tells_the_truth_about_a_refused_event(monkeypatch):
    """GitLab disables a hook that answers 4xx — the refusal is in the body."""
    async def submit_job_ex(payload, *, entitlements=(), requested_by=None):
        return SubmitResult(None, "duplicate", duplicate_of=9, reason="a queued job for this target does something else")

    async def no_token_check(_token):
        return None

    async def body():
        return {"object_kind": "pipeline", "project": {"path_with_namespace": "grp/aac-x", "name": "aac-x"},
                "object_attributes": {"id": 77, "status": "success", "source": "push", "ref": "main"}}

    monkeypatch.setattr(api_mod, "_ctx", MagicMock())
    monkeypatch.setattr(api_mod, "_service", SimpleNamespace(submit_job_ex=submit_job_ex))
    monkeypatch.setattr(api_mod, "_require_gitlab_token", no_token_check)
    monkeypatch.setattr(api_mod, "_recent_events", {})
    answer = asyncio.run(api_mod.do_gitlab_webhook(SimpleNamespace(json=body), "dummy-token-not-real"))
    assert answer["status"] == "accepted" and answer["triggered"] is False
    assert answer["outcome"] == "duplicate" and answer["duplicate_of"] == 9 and "Not queued" in answer["reason"]


# --- engine dispatch guard: the two-runner race ------------------------------------------

def _claim_by_force(svc, job_id, token):
    """What a second runner's claim does when it races the first one."""
    with queue_mod.db_instance.SessionLocal() as s:
        s.execute(update(IaCJob).where(IaCJob.id == job_id).values(
            status="RUNNING", owner_instance="docker-auto-runner", claim_token=token, claimed_at=datetime.now()))
        s.commit()
    svc.queue.claims[job_id] = token


def test_guard_hands_a_busy_claimed_job_back_to_the_queue_and_runs_nothing(iac_service):
    svc = iac_service
    svc.engine._notify = MagicMock()
    executed = []

    async def fake_execute(payload, job_id, pipeline_type):
        executed.append(job_id)

    svc.engine._execute_dispatched = fake_execute
    a = svc.queue.enqueue(CI)
    svc.queue.claim_next("runner-1")
    b = svc.queue.enqueue(CI)

    for attempt in range(svc.engine._MAX_GUARD_BOUNCES):
        _claim_by_force(svc, b, f"tok-{attempt}")
        asyncio.run(svc.engine.run_pipeline(dict(CI), job_id=b))
        row = _row(b)
        assert row.status == "QUEUED" and row.claim_token is None and row.owner_instance is None
        assert row.current_step.startswith("Queued — waiting")
    assert executed == []                                         # nothing of job B ran next to job A
    assert _row(a).status == "RUNNING"

    # a job the guard keeps rejecting is closed with the reason instead of bouncing for ever
    _claim_by_force(svc, b, "tok-last")
    asyncio.run(svc.engine.run_pipeline(dict(CI), job_id=b))
    assert _row(b).status == "ABORTED" and _row(b).current_step.startswith("Skipped:") and executed == []

    # … and once the target is free the claim takes a follow-up and it runs
    c = svc.queue.enqueue(CI)
    _finish(a)
    claimed = svc.queue.claim_next("docker-auto-runner")
    assert claimed[0] == c
    svc.queue.claims[c] = claimed.token
    asyncio.run(svc.engine.run_pipeline(claimed[1], job_id=c))
    assert executed == [c]
