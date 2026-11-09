"""Engine side of service reconcile (1.7): host-limited single_service jobs, the
``controller-diff`` trigger under the maintenance policy, and the deferred drain.
Needs the core on the path (see conftest)."""
from types import SimpleNamespace
from unittest.mock import MagicMock

from iac_plugin.app.controller import maintenance_resolver as mres
from iac_plugin.app.controller.engine import DeploymentEngine, SingleServiceStrategy
from iac_plugin.app.controller.queue import DuplicateJob
from iac_plugin.app.controller.reconcile_store import ReconcileStore
from iac_plugin.stages.ansible import AnsiblePlaybookStage


def _build(payload):
    eng = MagicMock()
    eng.config.services_dir = "/svc"
    eng.base_git_dir = MagicMock()
    stages = SingleServiceStrategy().build_stages(eng, payload, {})
    stage = stages[-1]
    assert isinstance(stage, AnsiblePlaybookStage)
    return stage


def test_target_hosts_limit_intersects_with_the_service_group():
    stage = _build({"service_name": "aac-wazuh-agent", "service_branch": "main",
                    "target_hosts": ["Docker-B", "docker-a", "docker-a", " "]})
    assert stage.limit == "docker-a:docker-b:&service_aac_wazuh_agent"
    assert stage.extra_vars["target_group"] == stage.limit        # playbooks use it as `hosts:`
    assert stage.extra_vars["target_service"] == "aac-wazuh-agent"


def test_target_hosts_limit_applies_to_dev_branch_too():
    stage = _build({"service_name": "aac-x", "service_branch": "dev", "target_hosts": ["docker-dev"]})
    assert stage.limit == "docker-dev:&service_aac_x"


def test_without_target_hosts_the_old_groups_are_used():
    assert _build({"service_name": "aac-x", "service_branch": "main"}).limit == "service_aac_x"
    assert _build({"service_name": "aac-x", "service_branch": "dev"}).limit == "stage_dev"
    assert _build({"service_name": "aac-x", "service_branch": "main", "target_hosts": []}).limit == "service_aac_x"


def test_controller_diff_trigger_follows_the_maintenance_policy():
    assert mres.policy_applies({"trigger": "controller-diff"}) is True
    assert mres.policy_applies({"trigger": "controller-diff", "force": True}) is False
    assert "controller-diff" in mres.POLICY_TRIGGERS


# --- deferred drain --------------------------------------------------------------

class _FakeDB:
    def __init__(self):
        self.states = {}

    def get_state(self, key):
        return {"data": self.states[key], "commit_hash": "latest"} if key in self.states else None

    def update_state(self, key, data, commit_hash):
        self.states[key] = data


def _engine(enqueue):
    eng = DeploymentEngine.__new__(DeploymentEngine)
    eng.reconcile_store = ReconcileStore(_FakeDB())
    eng.enqueue_pipeline = enqueue
    eng.queue = None
    eng.ctx = MagicMock()
    eng.config = SimpleNamespace(maintenance_linger_minutes=15)
    return eng


def _payload(svc="aac-x", stage="prod"):
    return {"pipeline_type": "single_service", "service_name": svc, "service_branch": "main",
            "target_hosts": ["h1"], "stage": stage, "site": "onprem", "trigger": "controller-diff"}


def test_drain_requeues_deferred_records_once_the_service_key_is_free():
    queued = []

    def enqueue(payload, requested_by=None):
        queued.append((payload, requested_by))
        return 77

    eng = _engine(enqueue)
    eng.reconcile_store.add(payload=_payload(), status="deferred", reason="waiting for job #55", waiting_for_job=55)
    eng.reconcile_store.add(payload=_payload("aac-y"), status="pending", reason="needs approval")   # untouched
    assert eng.drain_deferred_reconcile() == [77]
    assert queued[0][0]["service_name"] == "aac-x" and queued[0][1] == "controller-diff"
    left = eng.reconcile_store.list()
    assert [i["service"] for i in left] == ["aac-y"] and left[0]["status"] == "pending"


def test_drain_keeps_a_still_colliding_record_and_refreshes_waiting_for_job():
    def enqueue(payload, requested_by=None):
        raise DuplicateJob("single_service:aac-x", 56)

    eng = _engine(enqueue)
    rec = eng.reconcile_store.add(payload=_payload(), status="deferred", reason="waiting for job #55", waiting_for_job=55)
    assert eng.drain_deferred_reconcile() == []
    again = eng.reconcile_store.get(rec["id"])
    assert again["status"] == "deferred" and again["waiting_for_job"] == 56 and "#56" in again["reason"]


def test_drain_turns_hard_errors_into_pending_for_the_operator():
    def enqueue(payload, requested_by=None):
        raise RuntimeError("db down")

    eng = _engine(enqueue)
    rec = eng.reconcile_store.add(payload=_payload(), status="deferred", reason="waiting", waiting_for_job=1)
    assert eng.drain_deferred_reconcile() == []
    again = eng.reconcile_store.get(rec["id"])
    assert again["status"] == "pending" and "re-queue failed" in again["reason"]


def test_drain_drops_records_without_a_service_and_is_a_noop_without_hook():
    eng = _engine(lambda p, requested_by=None: 1)
    eng.reconcile_store.add(payload={}, status="deferred", reason="broken")
    assert eng.drain_deferred_reconcile() == [] and eng.reconcile_store.list() == []
    eng.enqueue_pipeline = None
    eng.reconcile_store.add(payload=_payload(), status="deferred", reason="w", waiting_for_job=1)
    assert eng.drain_deferred_reconcile() == [] and len(eng.reconcile_store.deferred()) == 1


def test_on_job_terminal_ends_override_and_drains():
    eng = _engine(lambda p, requested_by=None: 9)
    eng.reconcile_store.add(payload=_payload(), status="deferred", reason="w", waiting_for_job=3)
    eng.on_job_terminal(3)          # queue is None → end_override_for is a no-op, drain still runs
    assert eng.reconcile_store.list() == []
