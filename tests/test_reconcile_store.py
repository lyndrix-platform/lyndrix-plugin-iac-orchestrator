"""``app/controller/reconcile_store.py`` — pending/deferred service-reconcile records on top
of the job database's state blob. Loaded standalone with a dict-backed fake database."""
import contextlib
import importlib.util
import sys
from pathlib import Path

PLUGIN_ROOT = Path(__file__).resolve().parents[1]


def _load():
    spec = importlib.util.spec_from_file_location(
        "reconcile_store_under_test", PLUGIN_ROOT / "app" / "controller" / "reconcile_store.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["reconcile_store_under_test"] = mod
    spec.loader.exec_module(mod)
    return mod


rs = _load()


class FakeDB:
    def __init__(self):
        self.states = {}
        self.writes = 0

    def get_state(self, key):
        if key not in self.states:
            return None
        return {"data": self.states[key], "commit_hash": "latest"}

    def update_state(self, key, data, commit_hash):
        self.states[key] = data
        self.writes += 1


def _payload(svc="aac-x", stage="prod", hosts=("h1", "h2")):
    return {"pipeline_type": "single_service", "service_name": svc, "service_branch": "main",
            "target_hosts": list(hosts), "stage": stage, "site": "onprem",
            "changed_keys": ["config.vars.K"], "trigger": "controller-diff"}


def test_add_update_remove_roundtrip():
    db = FakeDB()
    store = rs.ReconcileStore(db)
    assert store.list() == [] and store.pending() == [] and store.deferred() == []
    rec = store.add(payload=_payload(), status=rs.STATUS_PENDING, reason="3 hosts > max_hosts (1)",
                    inventory_commit="abc1234", source_job_id=7)
    assert rec["id"] and rec["service"] == "aac-x" and rec["stage"] == "prod"
    assert rec["hosts"] == ["h1", "h2"] and rec["changed_keys"] == ["config.vars.K"]
    assert store.get(rec["id"])["reason"].startswith("3 hosts")
    assert [r["id"] for r in store.pending()] == [rec["id"]]

    upd = store.update(rec["id"], status=rs.STATUS_DEFERRED, waiting_for_job=42, reason="waiting for job #42")
    assert upd["status"] == "deferred" and upd["waiting_for_job"] == 42
    assert [r["id"] for r in store.deferred()] == [rec["id"]] and store.pending() == []

    assert store.update("nope", status="x") is None
    assert store.remove("nope") is False
    assert store.remove(rec["id"]) is True
    assert store.list() == []
    # state blob is a dict with an items list (survives JSON round trips in the real DB)
    assert isinstance(db.states[rs.STATE_KEY]["items"], list)


def test_lock_factory_is_used_and_failures_fall_back_to_unlocked():
    db = FakeDB()
    entered = []

    @contextlib.contextmanager
    def lock(name):
        entered.append(name)
        yield

    store = rs.ReconcileStore(db, lock_factory=lock)
    store.add(payload=_payload(), status=rs.STATUS_PENDING, reason="r")
    assert entered == [rs.LOCK_NAME]

    def broken(name):
        raise RuntimeError("no advisory locks here")

    store2 = rs.ReconcileStore(db, lock_factory=broken)
    store2.add(payload=_payload("aac-y"), status=rs.STATUS_PENDING, reason="r")
    assert {r["service"] for r in store2.list()} == {"aac-x", "aac-y"}


def test_without_db_everything_is_a_noop():
    store = rs.ReconcileStore(None)
    rec = store.add(payload=_payload(), status=rs.STATUS_PENDING, reason="r")
    assert rec["id"] and store.list() == [] and store.remove(rec["id"]) is False


def test_malformed_state_is_ignored():
    db = FakeDB()
    db.states[rs.STATE_KEY] = {"items": [1, "x", None, {"id": "ok", "status": "pending"}]}
    store = rs.ReconcileStore(db)
    assert [r["id"] for r in store.list()] == ["ok"]
    db.states[rs.STATE_KEY] = ["not", "a", "dict"]
    assert store.list() == []
    assert store.clear() == 0


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-v"]))
