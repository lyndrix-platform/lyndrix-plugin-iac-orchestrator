"""OS-01: nothing that leaves the plugin carries credentials or raw SSoT blocks.

One test per outbound payload builder. The fixtures plant an obviously fake
marker in every place the real SSoT keeps secrets (terraform block incl. nested
keys, host vars, service ``config``, URLs with userinfo, catalog entries); the
assertions walk the complete payload for it.
"""
import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import yaml

from core.api import maintenance as mw
from iac_plugin.app.controller import api as api_mod
from iac_plugin.app.controller import public_projection as pp
from iac_plugin.app.controller.engine import DeploymentEngine

SECRET = "dummy-secret-not-real"

TERRAFORM = {
    "node_name": "hydra", "vm_id": 8003, "is_managed": True, "cores": 4, "tags": ["docker"],
    "password": SECRET, "token": SECRET, "ssh_key": SECRET, "api_token": SECRET,
    "token_id": f"root@pam!{SECRET}", "username": SECRET, "realm": "pam", "auth_type": "password",
    "backend": {"secret_key": SECRET, "bucket": "tfstate"},
    "proxmox": {"password": SECRET},
    "mount": [{"password": SECRET}],
}

INVENTORY = {"all": {
    "hosts": {
        "docker-atlas": {
            "hostname": "docker-atlas.int.example", "ansible_host": "10.1.130.61",
            "ansible_groups": ["site_onprem", "stage_prod"],
            "baseline_roles": ["common", {"name": "x", "token": SECRET}],
            "profiles": ["docker_standard"],
            "ansible_password": SECRET, "git_auth_token": SECRET,
            "vault_vars": {"root_password": SECRET}, "terraform_vars": {"proxmox_password": SECRET},
            "terraform": dict(TERRAFORM),
            "maintenance": {"windows": [{"weekly": {"days": ["sun"], "start": "02:00", "end": "04:00"}}],
                            "notify": {"webhook_token": SECRET}},
            "incidents": {"enabled": True, "api_key": SECRET},
            "services": [{
                "name": "aac-grafana", "state": "present", "deploy_type": "docker_compose",
                "git_repo": f"https://deploy:{SECRET}@gitlab.example/aac/aac-grafana.git", "git_version": "main",
                "config": {"secrets": {"GF_ADMIN_PASSWORD": SECRET}, "vars": {"DB_PASSWORD": SECRET},
                           "dependencies": {"db": {"secrets": {"POSTGRES_PASSWORD": SECRET}}}},
            }],
        },
        "hydra": {
            "ansible_host": "10.1.120.5", "type": "hypervisor",
            "ansible_groups": ["site_onprem", "physical_infrastructure"],
            "terraform": {"password": SECRET, "token": SECRET, "ssh_key": SECRET, "username": "root"},
        },
    },
    "children": {"stage_prod": {"hosts": {"docker-atlas": {}}}, "site_onprem": {"hosts": {"docker-atlas": {}, "hydra": {}}}},
}}


def _engine(ctx=None) -> DeploymentEngine:
    eng = object.__new__(DeploymentEngine)
    eng.ctx = ctx or MagicMock()
    eng._load_generated_inventory = lambda: INVENTORY
    eng._load_service_docker_names = lambda: {}
    eng._ci_map = {}
    return eng


def _clean(payload) -> bool:
    return SECRET not in json.dumps(payload, default=str)


# --- helpers ---------------------------------------------------------------------

def test_public_terraform_is_an_allowlist_without_nested_values():
    tf = pp.public_terraform(TERRAFORM)
    assert tf == {"node_name": "hydra", "vm_id": 8003, "is_managed": True, "cores": 4, "tags": ["docker"]}
    assert pp.public_terraform(None) == {} and pp.public_terraform("x") == {}
    # an allowlisted key with a nested value is dropped, not passed through
    assert pp.public_terraform({"ip": {"password": SECRET}, "vm_id": 1}) == {"vm_id": 1}


def test_scrub_removes_credential_keys_at_any_depth():
    block = {"windows": [{"weekly": {"days": ["sun"]}}], "a": {"b": [{"client_secret": SECRET, "ok": 1}]},
             "PASSWORD": SECRET, "ssh_private_key": SECRET, "obj": object()}
    out = pp.scrub(block)
    assert out == {"windows": [{"weekly": {"days": ["sun"]}}], "a": {"b": [{"ok": 1}]}, "obj": None}


def test_public_url_strips_userinfo():
    assert pp.public_url(f"https://u:{SECRET}@gitlab.example/x.git") == "https://gitlab.example/x.git"
    assert pp.public_url("https://gitlab.example/x.git") == "https://gitlab.example/x.git"
    assert pp.public_url({"url": SECRET}) is None


# --- monitoring inventory sync (HTTP push to another plugin) ----------------------

def test_monitoring_payload_has_no_credentials_and_no_config_block():
    mon = _engine()._build_monitoring_inventory_payload(INVENTORY)
    assert _clean(mon)
    atlas = next(h for h in mon["hosts"] if h["host_name"] == "docker-atlas")
    assert atlas["terraform"] == {"node_name": "hydra", "vm_id": 8003, "is_managed": True, "cores": 4, "tags": ["docker"]}
    assert atlas["baseline_roles"] == ["common"] and atlas["profiles"] == ["docker_standard"]
    hydra = next(h for h in mon["hosts"] if h["host_name"] == "hydra")
    assert hydra["terraform"] == {}
    svc = mon["services"][0]
    assert svc["config"] == {} and svc["git_repo"] == "https://gitlab.example/aac/aac-grafana.git"
    # no raw host var travels along
    assert not {"ansible_password", "vault_vars", "git_auth_token", "terraform_vars"} & set(atlas)


def test_monitoring_sync_posts_exactly_the_sanitized_payload():
    eng = _engine()
    posted = []

    async def _post(payload):
        posted.append(payload)

    eng._post_monitoring_inventory = _post
    asyncio.run(eng.emit_monitoring_inventory_sync())
    assert len(posted) == 1 and _clean(posted[0])


# --- bus: iac:inventory_updated, registry identify/relate, server manager ---------

def test_inventory_host_and_service_builders_are_clean():
    eng = _engine()
    hosts = eng._build_inventory_hosts()
    assert _clean(hosts) and _clean(eng._build_inventory_services())
    atlas = next(h for h in hosts if h["name"] == "docker-atlas")
    assert atlas["maintenance"]["windows"][0]["weekly"]["days"] == ["sun"]   # structure kept
    assert "notify" in atlas["maintenance"] and atlas["maintenance"]["notify"] == {}
    assert atlas["incidents"] == {"enabled": True}


def test_every_event_of_the_inventory_publish_is_clean():
    ctx = MagicMock()
    ctx.request = AsyncMock(return_value={"ok": True, "results": [
        {"ok": True, "uuid": "u-atlas"}, {"ok": True, "uuid": "u-hydra"}, {"ok": True, "uuid": "u-grafana"}]})
    asyncio.run(_engine(ctx).emit_iac_inventory_updated())

    topics = [c.args[0] for c in ctx.emit.call_args_list]
    assert {"iac:inventory_updated", "server_manager:register_server", "entity_registry:relate"} <= set(topics)
    for call in ctx.emit.call_args_list:
        assert _clean(call.args[1]), call.args[0]
    assert _clean(ctx.request.call_args.args[1])       # entity_registry:identify items


# --- maintenance publisher (bus event + DB snapshot) ------------------------------

def _controller_tree(root: Path):
    glob = root / "environments" / "global"
    glob.mkdir(parents=True)
    (glob / "02_service_catalog.yml").write_text(yaml.safe_dump({"service_catalog": {
        "remote_git_management": {"active": True, "remote_git_token": SECRET},
        "services": [{"name": "aac-grafana", "deploy_type": "docker_compose", "state": "present",
                      "repository_name": "aac-grafana", "config": {"secrets": {"X": SECRET}},
                      "token": SECRET}],
    }}))
    (glob / "03_profiles.yml").write_text(yaml.safe_dump({"profiles": {}}))
    sd = root / "environments" / "sites" / "onprem"
    std = sd / "stages" / "prod"
    std.mkdir(parents=True)
    (sd / "site_vars.yml").write_text(yaml.safe_dump({"site_vars": {"timezone": "UTC", "vault_vars": {"pw": SECRET}}}))
    (sd / "hardware.yml").write_text(yaml.safe_dump({"hardware_hosts": {
        "hydra": {"ansible_host": "10.1.120.5", "terraform": {"is_used": True, "password": SECRET, "provider": "proxmox"}}}}))
    (std / "stage_vars.yml").write_text(yaml.safe_dump({"stage_vars": {"maintenance_default": {
        "enabled": True, "windows": [{"weekly": {"days": ["sun"], "start": "02:00", "end": "04:00"}}]}}}))
    (std / "hosts.yml").write_text(yaml.safe_dump({"hosts": {"docker-atlas": {
        "hostname": "a", "ansible_host": "10.0.0.1", "ansible_password": SECRET,
        "terraform": {"node_name": "hydra", "vm_id": 1, "password": SECRET, "provider": {"token": SECRET}},
        "services": [{"name": "aac-grafana", "config": {"secrets": {"X": SECRET}}}],
    }}}))


def test_maintenance_event_and_snapshot_are_clean(tmp_path):
    ctx = MagicMock()
    eng = _engine(ctx)
    eng.base_git_dir = tmp_path / "git"
    _controller_tree(eng.base_git_dir / "iac_controller")
    eng.config = SimpleNamespace(services_dir=tmp_path / "services")
    eng.db = MagicMock()
    eng._ci_map = {("host", "docker-atlas"): "u-atlas", ("service", "docker-atlas", "aac-grafana"): "u-grafana"}
    assert asyncio.run(eng.publish_maintenance()) is True

    published = [c.args[1] for c in ctx.emit.call_args_list if c.args[0] == mw.TOPIC_WINDOWS_PUBLISHED]
    assert len(published) == 1 and published[0]["items"] and _clean(published[0])
    assert _clean(eng.db.update_state.call_args.args[1])


# --- repo snapshots (stored in the shared DB, served to every reader) -------------

def test_repo_snapshots_are_clean(tmp_path, monkeypatch):
    git = tmp_path / "git"
    _controller_tree(git / "iac_controller")
    inv = git / "inventory_state" / "global" / "ansible"
    inv.mkdir(parents=True)
    (inv / "inventory.yml").write_text(yaml.safe_dump(INVENTORY))
    stored = {}
    engine = SimpleNamespace(
        config=SimpleNamespace(git_repos_dir=git),
        db=SimpleNamespace(update_state=lambda key, data, commit_hash="": stored.__setitem__(key, data)),
    )
    monkeypatch.setattr(api_mod, "_engine", engine)
    monkeypatch.setattr(api_mod, "_ctx", MagicMock())

    counts = api_mod.publish_repo_snapshots()
    assert set(counts) == {"catalog", "assignments", "terraform_hosts", "inventory_hosts", "branch_policy"}
    assert _clean(stored)
    catalog = stored["snapshot:catalog"]["items"]
    assert catalog == [{"name": "aac-grafana", "repository_name": "aac-grafana",
                        "deploy_type": "docker_compose", "state": "present"}]
    tf_hosts = {h["host"]: h for h in stored["snapshot:terraform_hosts"]["items"]}
    assert tf_hosts["docker-atlas"]["provider"] == "—"     # a nested provider block is never passed through
    assert tf_hosts["hydra"]["provider"] == "proxmox"
