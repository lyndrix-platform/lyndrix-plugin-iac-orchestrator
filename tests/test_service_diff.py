"""``stages/service_diff.py`` — pure per-host service-entry diff behind the
``service_reconcile`` rule action. Loaded standalone (no core, no package)."""
import importlib.util
import json
import sys
from pathlib import Path

PLUGIN_ROOT = Path(__file__).resolve().parents[1]


def _load():
    spec = importlib.util.spec_from_file_location("service_diff_under_test", PLUGIN_ROOT / "stages" / "service_diff.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["service_diff_under_test"] = mod
    spec.loader.exec_module(mod)
    return mod


sd = _load()

SECRET_OLD = "old-s3cr3t-value"
SECRET_NEW = "new-s3cr3t-value"


def _entry(name, manager="wazuh.old", secret=SECRET_OLD, extra=None):
    e = {"name": name, "config": {"vars": {"WAZUH_MANAGER": manager, "AGENT_GROUP": "docker"},
                                  "secrets": {"AGENT_KEY": secret}}}
    if extra:
        e.update(extra)
    return e


def _inventory(hosts: dict) -> dict:
    """Shape of iac_core ansible_gen: every host under all.hosts, groups as children with
    ``None`` host values."""
    children = {}
    for host, hv in hosts.items():
        for svc in (hv or {}).get("services") or []:
            children.setdefault(f"service_{svc['name'].replace('-', '_')}", {"hosts": {}})["hosts"][host] = None
    return {"all": {"hosts": hosts, "children": children}}


def _fleet(manager="wazuh.old", secret=SECRET_OLD, with_new_host=False):
    hosts = {
        "docker-a": {"ansible_host": "10.0.0.1", "services": [_entry("aac-wazuh-agent", manager, secret), _entry("aac-traefik")]},
        "docker-b": {"ansible_host": "10.0.0.2",
                     # host-level override: the manager is pinned on this host, so a profile
                     # change does not touch its rendered entry
                     "services": [_entry("aac-wazuh-agent", "wazuh.pinned", secret)]},
        "pve-node1": {"ansible_host": "10.0.0.9", "type": "hypervisor", "services": [_entry("aac-wazuh-agent", manager, secret)]},
        "no-services": {"ansible_host": "10.0.0.3", "services": None},
    }
    if with_new_host:
        hosts["docker-new"] = {"ansible_host": "10.0.0.4", "services": [_entry("aac-wazuh-agent", manager, secret)]}
    return _inventory(hosts)


def test_identical_inventories_yield_nothing():
    changed, removed = sd.diff_host_services(_fleet(), _fleet())
    assert changed == {} and removed == {}


def test_profile_var_change_hits_every_profile_host_including_hypervisor_but_not_overridden_host():
    changed, removed = sd.diff_host_services(_fleet("wazuh.old"), _fleet("wazuh.new"))
    assert set(changed) == {"aac-wazuh-agent"}
    ch = changed["aac-wazuh-agent"]
    assert sorted(ch.hosts) == ["docker-a", "pve-node1"]        # docker-b pinned → unchanged
    assert ch.changed_keys == ["config.vars.WAZUH_MANAGER"]
    assert removed == {}


def test_secret_change_counts_but_only_the_key_name_is_reported():
    changed, _ = sd.diff_host_services(_fleet(secret=SECRET_OLD), _fleet(secret=SECRET_NEW))
    ch = changed["aac-wazuh-agent"]
    assert ch.changed_keys == ["config.secrets.AGENT_KEY"]
    dumped = json.dumps(ch.as_dict()) + sd.summarize(changed, {})
    assert SECRET_OLD not in dumped and SECRET_NEW not in dumped


def test_added_service_on_existing_host_is_marked_added():
    new = _fleet()
    new["all"]["hosts"]["docker-b"]["services"].append(_entry("aac-renovate"))
    changed, removed = sd.diff_host_services(_fleet(), new)
    assert changed["aac-renovate"].hosts == ["docker-b"]
    assert changed["aac-renovate"].changed_keys == [sd.ADDED_MARKER]
    assert removed == {}


def test_removed_service_is_reported_only():
    new = _fleet()
    new["all"]["hosts"]["docker-a"]["services"] = [s for s in new["all"]["hosts"]["docker-a"]["services"]
                                                   if s["name"] != "aac-traefik"]
    changed, removed = sd.diff_host_services(_fleet(), new)
    assert changed == {}
    assert removed == {"aac-traefik": ["docker-a"]}
    assert "no cleanup" in sd.summarize(changed, removed)


def test_brand_new_host_is_skipped():
    """A host absent from the previous render is provisioned by host_provision."""
    changed, removed = sd.diff_host_services(_fleet(), _fleet(with_new_host=True))
    assert changed == {} and removed == {}


def test_services_null_and_malformed_hosts_are_tolerated():
    old = {"all": {"hosts": {"h": {"services": None}, "x": None, "y": "str"}}}
    new = {"all": {"hosts": {"h": {"services": [{"name": "aac-x"}]}, "x": None}}}
    changed, removed = sd.diff_host_services(old, new)
    assert changed["aac-x"].hosts == ["h"] and removed == {}
    assert sd.diff_host_services({}, {}) == ({}, {})
    assert sd.diff_host_services({"all": None}, {"all": None}) == ({}, {})


def test_order_noise_does_not_trigger():
    old = _fleet()
    new = _fleet()
    e = new["all"]["hosts"]["docker-a"]["services"][0]
    e["config"] = {"secrets": e["config"]["secrets"], "vars": dict(reversed(list(e["config"]["vars"].items())))}
    assert sd.diff_host_services(old, new) == ({}, {})


def test_changed_key_paths_descend_dicts_and_compare_lists_whole():
    old = {"a": {"b": 1, "c": [1, 2]}, "d": 1}
    new = {"a": {"b": 2, "c": [2, 1]}, "e": 1}
    assert sd.changed_key_paths(old, new) == ["a.b", "a.c", "d", "e"]
    assert sd.changed_key_paths(1, 1) == []
    assert sd.changed_key_paths(1, 2) == ["<entry>"]


def test_inventory_path_scope_and_branch_mapping():
    assert sd.inventory_path_scope("onprem/prod/ansible/inventory.yml") == ("onprem", "prod")
    assert sd.inventory_path_scope("hetzner/dev/ansible/inventory.yml") == ("hetzner", "dev")
    assert sd.inventory_path_scope("global/ansible/inventory.yml") is None       # would double count
    assert sd.inventory_path_scope("onprem/prod/terraform/terraform.tfvars.json") is None
    assert sd.inventory_path_scope("onprem/prod/ansible/group_vars/all.yml") is None
    assert sd.branch_for_stage("prod") == "main"
    assert sd.branch_for_stage("dev") == "dev"
    assert sd.branch_for_stage("test") == "test"
    assert sd.branch_for_stage("staging") == "main"


def test_changed_keys_are_capped():
    old = {"name": "aac-x", "config": {"vars": {f"K{i}": "a" for i in range(80)}}}
    new = {"name": "aac-x", "config": {"vars": {f"K{i}": "b" for i in range(80)}}}
    changed, _ = sd.diff_host_services({"all": {"hosts": {"h": {"services": [old]}}}},
                                       {"all": {"hosts": {"h": {"services": [new]}}}})
    assert len(changed["aac-x"].changed_keys) == sd.MAX_CHANGED_KEYS


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-v"]))
