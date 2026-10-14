"""Tests for the inventory publish path: Entity Registry ci_ids, credential
stripping and Server Manager registration (DeploymentEngine)."""
import asyncio
from unittest.mock import AsyncMock, MagicMock

from iac_plugin.app.controller.engine import DeploymentEngine


def _engine(inventory: dict, ctx=None) -> DeploymentEngine:
    eng = object.__new__(DeploymentEngine)  # skip __init__ (needs db/config/state)
    eng.ctx = ctx or MagicMock()
    eng._load_generated_inventory = lambda: inventory
    return eng


INV = {"all": {
    "hosts": {
        "docker-atlas": {
            "hostname": "docker-atlas.int.example", "ansible_host": "10.1.130.61",
            "ansible_groups": ["site_onprem", "stage_prod"], "baseline_roles": ["common", "docker"],
            "terraform": {"node_name": "hydra", "vm_id": 8003, "is_managed": True,
                          "password": "pw", "token": "tok", "ssh_key": "KEY", "api_token": "x",
                          "token_id": "terraform@pve!provider", "ssh_agent": True},
        },
        "hydra": {
            "ansible_host": "10.1.120.5", "type": "hypervisor",
            "ansible_groups": ["site_onprem", "physical_infrastructure"],
            "terraform": {"password": "pw", "username": "root"},
        },
    },
    "children": {"docker_hosts": {"hosts": {"docker-atlas": {}}}, "stage_prod": {"hosts": {"docker-atlas": {}}}},
}}


def test_public_terraform_strips_credentials():
    tf = DeploymentEngine._public_terraform({
        "password": 1, "token": 2, "ssh_key": 3, "api_token": 4, "my_secret": 5, "db_password": 6,
        "deploy_token": 7, "private_key": 8,
        "node_name": "hydra", "vm_id": 1, "token_id": "a!b", "ssh_agent": True,
    })
    assert tf == {"node_name": "hydra", "vm_id": 1, "token_id": "a!b", "ssh_agent": True}
    assert DeploymentEngine._public_terraform(None) == {}


def test_build_hosts_shape_and_classification():
    hosts = {h["name"]: h for h in _engine(INV)._build_inventory_hosts()}
    atlas, hydra = hosts["docker-atlas"], hosts["hydra"]
    assert atlas["entity_type"] == "host" and hydra["entity_type"] == "hypervisor"
    assert (atlas["site"], atlas["stage"]) == ("onprem", "prod") and hydra["stage"] is None
    assert "docker_hosts" in atlas["groups"] and "stage_prod" in atlas["groups"]
    assert "password" not in atlas["terraform"] and atlas["terraform"]["vm_id"] == 8003
    assert atlas["ci_id"] is None and atlas["ip"] == "10.1.130.61"


def test_attach_ci_ids_uses_registry_reply():
    ctx = MagicMock()
    ctx.request = AsyncMock(return_value={"ok": True, "results": [
        {"ok": True, "uuid": "u-atlas", "created": True, "conflict": False},
        {"ok": False, "error": "nope"},
    ]})
    eng = _engine(INV, ctx)
    hosts = eng._build_inventory_hosts()
    asyncio.run(eng._attach_ci_ids(hosts))
    by = {h["name"]: h for h in hosts}
    assert by["docker-atlas"]["ci_id"] == "u-atlas" and by["hydra"]["ci_id"] is None

    topic, payload = ctx.request.call_args.args[:2]
    assert topic == "entity_registry:identify"
    assert ctx.request.call_args.kwargs["reply_topic"] == "entity_registry:identified"
    items = {i["display_name"]: i for i in payload["items"]}
    assert items["docker-atlas"]["identifiers"] == {
        "iac.host": "docker-atlas", "pve.vm": "hydra/8003", "fqdn": "docker-atlas.int.example",
    }
    assert items["hydra"]["entity_type"] == "hypervisor" and "pve.vm" not in items["hydra"]["identifiers"]
    assert items["docker-atlas"]["source"] == "iac-orchestrator"
    assert "pw" not in str(payload) and "KEY" not in str(payload)  # no credentials on the bus


def test_attach_ci_ids_degrades_without_registry():
    ctx = MagicMock()
    ctx.request = AsyncMock(return_value=None)  # nobody answered (timeout)
    eng = _engine(INV, ctx)
    hosts = eng._build_inventory_hosts()
    asyncio.run(eng._attach_ci_ids(hosts))
    assert all(h["ci_id"] is None for h in hosts)

    old_core = MagicMock(spec=["emit", "log"])  # core API < 1.4: no ctx.request
    eng2 = _engine(INV, old_core)
    hosts2 = eng2._build_inventory_hosts()
    asyncio.run(eng2._attach_ci_ids(hosts2))
    assert all(h["ci_id"] is None for h in hosts2)


def test_publish_emits_inventory_and_registers_servers():
    ctx = MagicMock()
    ctx.request = AsyncMock(return_value={"ok": True, "results": [
        {"ok": True, "uuid": "u-atlas", "created": False, "conflict": False},
        {"ok": True, "uuid": "u-hydra", "created": False, "conflict": False},
    ]})
    eng = _engine(INV, ctx)
    asyncio.run(eng.emit_iac_inventory_updated())

    inv = next(c.args[1] for c in ctx.emit.call_args_list if c.args[0] == "iac:inventory_updated")
    assert {h["name"]: h["ci_id"] for h in inv["hosts"]} == {"docker-atlas": "u-atlas", "hydra": "u-hydra"}
    assert "pw" not in str(inv) and "KEY" not in str(inv)

    regs = {c.args[1]["external_id"]: c.args[1]
            for c in ctx.emit.call_args_list if c.args[0] == "server_manager:register_server"}
    assert regs["docker-atlas"]["server_type"] == "container"
    assert regs["docker-atlas"]["environment_id"] == "onprem:prod"
    assert regs["hydra"]["server_type"] == "physical"
    assert all(r["source"] == "iac-orchestrator" and r["writeback"] is False for r in regs.values())
    assert regs["docker-atlas"]["ci_id"] == "u-atlas"
    assert "stage_prod" not in regs["docker-atlas"]["tags"] and "docker_hosts" in regs["docker-atlas"]["tags"]


def test_publish_skips_when_inventory_missing():
    ctx = MagicMock()
    eng = _engine({}, ctx)
    asyncio.run(eng.emit_iac_inventory_updated())
    ctx.emit.assert_not_called()
