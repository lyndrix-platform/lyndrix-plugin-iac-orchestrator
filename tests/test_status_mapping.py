"""CI status contract (plugin 1.6, finding 7 / decision D): ``accept=scheduled`` vs
the compatibility mode for parked deploys, QUEUED → running, and the ``ci:
maintenance`` commit loop guard."""

from datetime import datetime

from iac_plugin.app.controller import api as api_mod
from iac_plugin.app.controller.queue import from_queue_clock, to_queue_clock


def test_scheduled_with_accept_and_compat_mode():
    aware = datetime(2026, 10, 25, 2, 0).astimezone()  # any aware instant
    job = {"status": "SCHEDULED", "not_before": to_queue_clock(aware)}
    explicit = api_mod.map_service_status(job, accept="scheduled")
    assert explicit["status"] == "scheduled" and explicit["job_status"] == "SCHEDULED"
    assert explicit["not_before"] == from_queue_clock(
        to_queue_clock(aware)
    ).isoformat().replace("+00:00", "Z")
    # comma list, case-insensitive
    assert (
        api_mod.map_service_status(job, accept="failed, Scheduled")["status"]
        == "scheduled"
    )
    # old CI template: never red on a parked deploy
    compat = api_mod.map_service_status(job)
    assert (
        compat["status"] == "success"
        and compat["job_status"] == "SCHEDULED"
        and compat["not_before"]
    )
    assert api_mod.map_service_status(job, accept="")["status"] == "success"


def test_other_statuses_unchanged():
    assert api_mod.map_service_status({"status": "SUCCESS"}) == {
        "status": "success",
        "job_status": "SUCCESS",
    }
    assert api_mod.map_service_status({"status": "ABORTED"})["status"] == "failed"
    assert api_mod.map_service_status({"status": "RUNNING"})["status"] == "running"
    assert (
        api_mod.map_service_status({"status": "QUEUED"})["status"] == "running"
    )  # was "unknown" before 1.6
    assert api_mod.map_service_status({"status": "weird"})["status"] == "unknown"
    assert "not_before" not in api_mod.map_service_status(
        {"status": "SUCCESS", "not_before": None}
    )


def _pipeline_event(message: str) -> dict:
    return {
        "object_kind": "pipeline",
        "object_attributes": {"status": "success", "source": "push"},
        "commit": {"message": message},
    }


def test_maintenance_commit_does_not_trigger_deploy():
    ok, reason = api_mod._should_trigger_orchestrator(
        _pipeline_event("ci: maintenance default onprem/prod off")
    )
    assert ok is False and "maintenance" in reason
    ok, _ = api_mod._should_trigger_orchestrator(
        _pipeline_event("feat: add grafana dashboards")
    )
    assert ok is True
    ok, _ = api_mod._should_trigger_orchestrator(
        _pipeline_event("ci: automated state update")
    )
    assert ok is False
