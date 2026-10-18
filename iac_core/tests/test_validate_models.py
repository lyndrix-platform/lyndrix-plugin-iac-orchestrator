"""ServiceDefinition keeps unknown per-service keys (maintenance windows: the
controller's ``services[].maintenance`` / ``incidents`` must survive validation)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from validate.models import ServiceDefinition, validate_configuration  # noqa: E402


def test_service_definition_keeps_maintenance_and_incidents():
    svc = ServiceDefinition(
        name="aac-grafana",
        maintenance={"windows": [{"weekly": {"days": ["sun"]}}]},
        incidents={"enabled": False},
    )
    dumped = svc.model_dump()
    assert dumped["maintenance"] == {"windows": [{"weekly": {"days": ["sun"]}}]}
    assert dumped["incidents"] == {"enabled": False}
    assert dumped["deploy_type"] == "docker_compose"


def test_validate_configuration_passes_service_maintenance_through():
    cfg = validate_configuration(
        {
            "hosts": {
                "docker-a": {
                    "hostname": "a.int",
                    "ansible_host": "10.0.0.1",
                    "maintenance": {"windows": [], "inherit": True},
                    "services": [
                        {
                            "name": "aac-grafana",
                            "maintenance": {"deploy_policy": "window"},
                        }
                    ],
                },
            },
        }
    )
    host = cfg["hosts"]["docker-a"]
    assert host["maintenance"] == {"windows": [], "inherit": True}
    assert host["services"][0]["maintenance"] == {"deploy_policy": "window"}
