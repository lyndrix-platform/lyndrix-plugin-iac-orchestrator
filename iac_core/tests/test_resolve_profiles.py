"""Host service entries override same-named profile services (merge by name)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from generator import resolve_profiles  # noqa: E402


def _cfg(host_services):
    return {
        "profiles": {
            "docker_standard": {
                "baseline_roles": ["docker"],
                "services": [
                    {"name": "aac-cadvisor", "state": "present"},
                    {
                        "name": "aac-traefik",
                        "state": "present",
                        "config": {
                            "secrets": {"API_KEY": "x"},
                            "config": {"domain_name": "int.example"},
                        },
                    },
                ],
            },
        },
        "hosts": {
            "docker-test": {
                "hostname": "docker-test",
                "ansible_host": "10.0.0.1",
                "profiles": ["docker_standard"],
                "baseline_roles": ["common"],
                "services": host_services,
            },
        },
    }


def test_host_entry_is_merged_onto_profile_entry():
    cfg = _cfg([
        {"name": "aac-it-tools", "state": "present"},
        {"name": "aac-traefik", "config": {"config": {"acme_internal": {"enabled": True}}}},
    ])
    resolve_profiles(cfg)
    host = cfg["hosts"]["docker-test"]
    names = [s["name"] for s in host["services"]]
    assert names == ["aac-cadvisor", "aac-traefik", "aac-it-tools"]
    traefik = host["services"][1]
    assert traefik["state"] == "present"
    assert traefik["config"]["secrets"] == {"API_KEY": "x"}
    assert traefik["config"]["config"] == {
        "domain_name": "int.example",
        "acme_internal": {"enabled": True},
    }
    assert host["baseline_roles"] == ["docker", "common"]


def test_profile_is_not_mutated_for_other_hosts():
    cfg = _cfg([{"name": "aac-traefik", "config": {"config": {"domain_name": "other"}}}])
    resolve_profiles(cfg)
    profile_traefik = cfg["profiles"]["docker_standard"]["services"][1]
    assert profile_traefik["config"]["config"]["domain_name"] == "int.example"
    host_traefik = cfg["hosts"]["docker-test"]["services"][1]
    assert host_traefik["config"]["config"]["domain_name"] == "other"


def test_without_host_services_profile_services_are_kept():
    cfg = _cfg([])
    resolve_profiles(cfg)
    assert [s["name"] for s in cfg["hosts"]["docker-test"]["services"]] == ["aac-cadvisor", "aac-traefik"]
