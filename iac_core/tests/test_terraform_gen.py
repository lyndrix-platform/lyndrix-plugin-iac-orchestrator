"""
Unit tests for the modular Terraform generator (mapper + destroy-safety guard).

Pure logic, no infrastructure required. Run with:
    PYTHONPATH=iac_core/app python3 -m pytest iac_core/tests/test_terraform_gen.py
or standalone:
    python3 iac_core/tests/test_terraform_gen.py
"""
import sys
import json
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from gen.terraform import (  # noqa: E402
    build_terraform_state,
    write_terraform_state,
    TerraformSafetyError,
)
from gen.terraform.mapper import build_containers, build_providers  # noqa: E402
from gen.terraform.safety import evaluate_destroy_safety  # noqa: E402


def _config(hosts=None, hardware=None):
    return {"hosts": hosts or {}, "hardware_hosts": hardware or {}}


def _managed_host(**tf):
    base = {"node_name": "hydra", "vm_id": 2010, "ip": "10.1.10.250/24"}
    base.update(tf)
    return {
        "hostname": "netprov-prv",
        "ansible_host": "10.1.130.250",
        "baseline_roles": ["common", "docker"],
        "roles": ["docker", "kea_dhcp"],
        "services": [{"name": "aac-pdns-recursor"}, {"name": "stork"}],
        "terraform": {"is_managed": True, **base},
    }


def test_managed_host_becomes_full_container():
    state = build_terraform_state(_config(hosts={"netprov": _managed_host(vlan=2010)}))
    c = state["containers"]["netprov"]
    assert c["node_name"] == "hydra" and c["vm_id"] == 2010
    assert c["ip"] == "10.1.10.250/24" and c["vlan"] == 2010
    assert c["arch"] == "amd64" and c["nesting"] is True and c["disk_storage"] == "local-lvm"
    assert c["roles"] == ["common", "docker", "kea_dhcp"]
    assert c["services"] == ["aac-pdns-recursor", "stork"]
    assert "root_password" not in c and "ssh_key" not in c


def test_node_container_defaults_inherited_and_overrideable():
    """Containers inherit bridge/vlan from their hypervisor's container_defaults;
    explicit host-level values override the node default."""
    hw = {
        "hydra": {
            "terraform": {"is_used": True},
            "container_defaults": {"bridge": "vmbr0", "vlan": 2130},
        }
    }
    # Host without explicit vlan -> inherits from hydra
    host_no_vlan = _managed_host()  # node_name=hydra, no vlan in tf block
    state = build_terraform_state(_config(hosts={"h1": host_no_vlan}, hardware=hw))
    assert state["containers"]["h1"]["vlan"] == 2130
    assert state["containers"]["h1"]["bridge"] == "vmbr0"

    # Host with explicit vlan -> overrides hydra default
    host_override = _managed_host(vlan=2010)
    state2 = build_terraform_state(_config(hosts={"h2": host_override}, hardware=hw))
    assert state2["containers"]["h2"]["vlan"] == 2010


def test_unmanaged_and_missing_fields_are_excluded():
    hosts = {
        "unmanaged": {"hostname": "x", "terraform": {"is_managed": False}},
        "no_tf": {"hostname": "y"},
        "incomplete": {"hostname": "z", "terraform": {"is_managed": True, "vm_id": 5}},
        "good": _managed_host(),
    }
    containers = build_containers(_config(hosts=hosts))
    assert set(containers) == {"good"}


def test_providers_exclude_secrets():
    hw = {
        "hydra": {
            "ansible_host": "10.1.120.5",
            "terraform": {
                "is_used": True, "provider": "proxmox", "username": "root",
                "realm": "pam", "auth_type": "password", "ssh_agent": True,
                "password": "SECRET", "token": "SECRET", "ssh_key": "/tmp/k",
            },
        },
        "ignored": {"terraform": {"is_used": False}},
    }
    providers = build_providers(_config(hardware=hw))
    assert set(providers) == {"hydra"}
    p = providers["hydra"]
    assert p["provider"] == "proxmox" and p["endpoint"] == "10.1.120.5"
    assert "password" not in p and "token" not in p and "ssh_key" not in p


def test_guard_blocks_full_wipe():
    old = {"containers": {"a": {}, "b": {}}}
    try:
        evaluate_destroy_safety(old, {"containers": {}})
        assert False, "expected TerraformSafetyError"
    except TerraformSafetyError:
        pass


def test_guard_blocks_over_threshold():
    old = {"containers": {"a": {}, "b": {}, "c": {}, "d": {}}}
    try:
        evaluate_destroy_safety(old, {"containers": {"a": {}}}, max_destroy_ratio=0.5)
        assert False, "expected TerraformSafetyError"
    except TerraformSafetyError:
        pass


def test_guard_allows_growth_and_small_shrink():
    old = {"containers": {"a": {}, "b": {}}}
    evaluate_destroy_safety(old, {"containers": {"a": {}, "b": {}, "c": {}}})
    evaluate_destroy_safety(old, {"containers": {"a": {}}}, max_destroy_ratio=0.5)
    evaluate_destroy_safety(None, {"containers": {}})


def test_guard_allows_destroy_when_opted_in():
    old = {"containers": {"a": {}, "b": {}}}
    evaluate_destroy_safety(old, {"containers": {}}, allow_destroy=True)


def test_write_is_idempotent_and_atomic():
    with tempfile.TemporaryDirectory() as d:
        out = Path(d) / "onprem" / "dev"
        state = build_terraform_state(_config(hosts={"netprov": _managed_host()}))
        assert write_terraform_state(state, out) is True
        assert write_terraform_state(state, out) is False
        path = out / "terraform" / "terraform.tfvars.json"
        assert json.loads(path.read_text())["containers"]["netprov"]["vm_id"] == 2010
        assert not list((out / "terraform").glob(".tf_*"))


def test_write_blocks_destructive_second_run():
    with tempfile.TemporaryDirectory() as d:
        out = Path(d) / "onprem" / "dev"
        full = build_terraform_state(_config(hosts={
            "a": _managed_host(vm_id=1), "b": _managed_host(vm_id=2),
        }))
        write_terraform_state(full, out)
        empty = build_terraform_state(_config(hosts={}))
        try:
            write_terraform_state(empty, out)
            assert False, "expected TerraformSafetyError"
        except TerraformSafetyError:
            pass
        path = out / "terraform" / "terraform.tfvars.json"
        assert set(json.loads(path.read_text())["containers"]) == {"a", "b"}


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    for fn in fns:
        try:
            fn()
            print(f"PASS  {fn.__name__}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"FAIL  {fn.__name__}: {e}")
    print(f"\n{len(fns) - failed}/{len(fns)} passed")
    sys.exit(1 if failed else 0)
