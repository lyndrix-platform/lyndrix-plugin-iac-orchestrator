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


# --- maintenance windows (1.6): service CIs, runs_on, ci_id in the monitoring payload,
#     windows_published once with replaces:true ---------------------------------------

import yaml  # noqa: E402
from pathlib import Path  # noqa: E402
from types import SimpleNamespace  # noqa: E402

from core.api import maintenance as mw  # noqa: E402

INV_SVC = {"all": {
    "hosts": {
        "docker-atlas": {
            "hostname": "docker-atlas.int.example", "ansible_host": "10.1.130.61",
            "ansible_groups": ["site_onprem", "stage_prod"],
            "terraform": {"node_name": "hydra", "vm_id": 8003, "password": "pw"},
            "maintenance": {"windows": [{"weekly": {"days": ["sun"], "start": "02:00", "end": "04:00", "tz": "UTC"}}]},
            "services": [{"name": "aac-grafana", "state": "present"}, {"name": "aac-loki"}],
        },
        "hydra": {"ansible_host": "10.1.120.5", "type": "hypervisor", "ansible_groups": ["site_onprem", "physical_infrastructure"]},
    },
    "children": {"stage_prod": {"hosts": {"docker-atlas": {}}}},
}}


def _registry_reply(*uuids):
    return {"ok": True, "results": [({"ok": True, "uuid": u, "conflict": False} if u else {"ok": False, "error": "unknown scheme"})
                                    for u in uuids]}


def test_services_identified_related_and_in_monitoring_payload():
    ctx = MagicMock()
    ctx.request = AsyncMock(return_value=_registry_reply("u-atlas", "u-hydra", "u-grafana", "u-loki"))
    eng = _engine(INV_SVC, ctx)
    eng._load_service_docker_names = lambda: {}
    asyncio.run(eng.emit_iac_inventory_updated())

    payload = ctx.request.call_args.args[1]
    items = payload["items"]
    assert [i["entity_type"] for i in items] == ["host", "hypervisor", "service", "service"]
    assert items[2]["identifiers"] == {"iac.service": "docker-atlas/aac-grafana"}
    assert items[2]["display_name"] == "aac-grafana @ docker-atlas" and items[2]["attributes"]["host"] == "docker-atlas"
    assert items[2]["source"] == "iac-orchestrator" and "pw" not in str(payload)

    assert eng._ci_map == {("host", "docker-atlas"): "u-atlas", ("host", "hydra"): "u-hydra",
                           ("service", "docker-atlas", "aac-grafana"): "u-grafana",
                           ("service", "docker-atlas", "aac-loki"): "u-loki"}

    inv = next(c.args[1] for c in ctx.emit.call_args_list if c.args[0] == "iac:inventory_updated")
    atlas = next(h for h in inv["hosts"] if h["name"] == "docker-atlas")
    assert atlas["ci_id"] == "u-atlas" and atlas["maintenance"]["windows"][0]["weekly"]["days"] == ["sun"]
    assert {(s["name"], s["ci_id"], s["host_ci_id"]) for s in inv["services"]} == {
        ("aac-grafana", "u-grafana", "u-atlas"), ("aac-loki", "u-loki", "u-atlas")}

    relates = [c.args[1] for c in ctx.emit.call_args_list if c.args[0] == "entity_registry:relate"]
    assert sorted((r["from_uuid"], r["to_uuid"], r["relation"]) for r in relates) == [
        ("u-grafana", "u-atlas", "runs_on"), ("u-loki", "u-atlas", "runs_on")]
    assert all(r["source"] == "iac-orchestrator" for r in relates)

    mon = eng._build_monitoring_inventory_payload(INV_SVC)
    assert {h["host_name"]: h["ci_id"] for h in mon["hosts"]} == {"docker-atlas": "u-atlas", "hydra": "u-hydra"}
    grafana = next(s for s in mon["services"] if s["service_slug"] == "aac-grafana")
    assert grafana["ci_id"] == "u-grafana" and grafana["host_ci_id"] == "u-atlas"


def test_old_registry_rejects_service_scheme_gracefully():
    ctx = MagicMock()
    ctx.request = AsyncMock(return_value=_registry_reply("u-atlas", "u-hydra", None, None))
    eng = _engine(INV_SVC, ctx)
    asyncio.run(eng.emit_iac_inventory_updated())
    inv = next(c.args[1] for c in ctx.emit.call_args_list if c.args[0] == "iac:inventory_updated")
    assert all(s["ci_id"] is None and s["host_ci_id"] == "u-atlas" for s in inv["services"])
    assert not [c for c in ctx.emit.call_args_list if c.args[0] == "entity_registry:relate"]
    assert eng._ci_map == {("host", "docker-atlas"): "u-atlas", ("host", "hydra"): "u-hydra"}


def test_identify_is_chunked():
    big = {"all": {"hosts": {f"h{i}": {"ansible_host": f"10.0.{i // 250}.{i % 250}"} for i in range(1200)}, "children": {}}}
    ctx = MagicMock()
    ctx.request = AsyncMock(side_effect=lambda topic, payload, **kw: {
        "ok": True, "results": [{"ok": True, "uuid": f"u-{i['display_name']}"} for i in payload["items"]]})
    eng = _engine(big, ctx)
    hosts = eng._build_inventory_hosts()
    asyncio.run(eng._attach_ci_ids(hosts, []))
    assert ctx.request.call_count == 3 and all(h["ci_id"] == f"u-{h['name']}" for h in hosts)


def _controller_tree(root: Path):
    sd = root / "environments" / "sites" / "onprem"
    std = sd / "stages" / "prod"
    std.mkdir(parents=True)
    (sd / "site_vars.yml").write_text(yaml.safe_dump({"site_vars": {"timezone": "UTC"}}))
    (std / "stage_vars.yml").write_text(yaml.safe_dump({"stage_vars": {"maintenance_default": {
        "enabled": True, "windows": [{"weekly": {"days": ["sun"], "start": "02:00", "end": "04:00"}}]}}}))
    (std / "hosts.yml").write_text(yaml.safe_dump({"hosts": {
        "docker-atlas": {"hostname": "a", "ansible_host": "10.0.0.1", "services": [{"name": "aac-grafana"}]},
        "docker-nocid": {"hostname": "b", "ansible_host": "10.0.0.2"},
    }}))


def test_publish_maintenance_emits_once_and_stores_snapshot(tmp_path):
    ctx = MagicMock()
    eng = _engine({}, ctx)
    eng.base_git_dir = tmp_path / "git"
    _controller_tree(eng.base_git_dir / "iac_controller")
    eng.config = SimpleNamespace(services_dir=tmp_path / "services")
    eng.db = MagicMock()
    eng._ci_map = {("host", "docker-atlas"): "u-atlas", ("service", "docker-atlas", "aac-grafana"): "u-grafana"}
    asyncio.run(eng.publish_maintenance())

    published = [c for c in ctx.emit.call_args_list if c.args[0] == mw.TOPIC_WINDOWS_PUBLISHED]
    assert len(published) == 1 and not published[0].kwargs.get("cluster")  # local emit
    payload = published[0].args[1]
    assert payload["source"] == "iac-orchestrator" and payload["replaces"] is True
    assert sorted(i["ci_id"] for i in payload["items"]) == ["u-atlas", "u-grafana"]  # docker-nocid not sent
    grafana = next(i for i in payload["items"] if i["ci_id"] == "u-grafana")
    assert grafana["kind"] == "service" and grafana["host_ci_id"] == "u-atlas"
    assert grafana["inherited_from"] == "stage_default" and grafana["deploy_policy"] == "window"
    assert grafana["windows"] == [{"type": "weekly", "days": ["sun"], "start": "02:00", "end": "04:00", "tz": "UTC"}]

    key, snapshot, _hash = eng.db.update_state.call_args.args
    assert key == "snapshot:maintenance_effective"
    assert {i["name"] for i in snapshot["items"]} == {"docker-atlas", "docker-nocid", "aac-grafana @ docker-atlas"}
    assert snapshot["defaults"][0]["enabled"] is True and snapshot["defaults"][0]["level"] == "stage"
    assert snapshot["published_at"] == payload["published_at"]


def test_publish_maintenance_without_checkout_is_silent(tmp_path):
    ctx = MagicMock()
    eng = _engine({}, ctx)
    eng.base_git_dir = tmp_path / "nowhere"
    eng.config = SimpleNamespace(services_dir=tmp_path / "services")
    assert asyncio.run(eng.publish_maintenance()) is False
    ctx.emit.assert_not_called()


def test_attach_ci_ids_records_registry_availability():
    ctx = MagicMock()
    ctx.request = AsyncMock(return_value={"ok": False, "error": "database not ready"})  # registry booting
    eng = _engine(INV, ctx)
    asyncio.run(eng._attach_ci_ids(eng._build_inventory_hosts(INV), []))
    assert eng._registry_ok is False

    ctx.request = AsyncMock(return_value=_registry_reply("u-atlas", "u-hydra"))
    asyncio.run(eng._attach_ci_ids(eng._build_inventory_hosts(INV), []))
    assert eng._registry_ok is True


def test_publish_maintenance_holds_back_the_event_while_registry_is_unavailable(tmp_path):
    """replaces:true + an empty set would wipe every consumer — keep the snapshot, skip the bus."""
    ctx = MagicMock()
    eng = _engine({}, ctx)
    eng.base_git_dir = tmp_path / "git"
    _controller_tree(eng.base_git_dir / "iac_controller")
    eng.config = SimpleNamespace(services_dir=tmp_path / "services")
    eng.db = MagicMock()
    eng._ci_map = {}
    eng._registry_ok = False
    assert asyncio.run(eng.publish_maintenance()) is False
    assert not [c for c in ctx.emit.call_args_list if c.args[0] == mw.TOPIC_WINDOWS_PUBLISHED]
    key, snapshot, _hash = eng.db.update_state.call_args.args
    assert key == "snapshot:maintenance_effective" and len(snapshot["items"]) == 3  # UI still sees them

    eng._registry_ok = True  # registry answered (even without ci_ids) → publish as before
    assert asyncio.run(eng.publish_maintenance()) is True
    assert len([c for c in ctx.emit.call_args_list if c.args[0] == mw.TOPIC_WINDOWS_PUBLISHED]) == 1
