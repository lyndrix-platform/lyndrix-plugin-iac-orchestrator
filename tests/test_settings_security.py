"""OS-02: settings are validated per key, authorised per tier, and checked again
where they are used (runner image, credential references, GitLab URL)."""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from iac_plugin.app import api as routes_mod
from iac_plugin.app.controller import api as api_mod
from iac_plugin.app.controller import config as config_mod
from iac_plugin.app.controller import engine as engine_mod
from iac_plugin.app.controller import gitlab_webhooks
from iac_plugin.app.controller.config import IaCConfig, RunnerImageNotAllowed
from iac_plugin.app.controller.engine import DeploymentEngine
from iac_plugin.app.controller.settings_schema import (
    OrchestratorSettings, SettingsPermissionError, SettingsValidationError,
    is_valid_credential_alias, reserved_vault_keys, resolve_credential,
)

GOOD_IMAGE = "registry.gitlab.int.fam-feser.de/iac-environment/iac-platform-assets/ansible-ci-image:1.2.3"


class FakeCtx:
    def __init__(self, **vault):
        self.vault = {"iac_token_registry": json.dumps(["gitlab-int"]), "gitlab-int": "dummy-token-not-real",
                      "iac_tf_proxmox_password": "dummy-proxmox-password-not-real",
                      "gitlab_webhook_token": "dummy-webhook-token-not-real", **vault}
        self.log = MagicMock()

    def get_secret(self, key):
        return self.vault.get(key)

    def set_secret(self, key, value):
        self.vault[key] = value
        return True


def _mgr(**vault):
    ctx = FakeCtx(**vault)
    return ctx, OrchestratorSettings(ctx, SimpleNamespace(state={}))


# --- per-key validation ------------------------------------------------------------

def test_token_reference_must_be_a_stored_credential_alias():
    ctx, mgr = _mgr()
    for ref in ("iac_tf_proxmox_password", "gitlab_webhook_token", "ansible_ssh_key", "unknown-alias"):
        with pytest.raises(SettingsValidationError):
            mgr.save_values({"gitlab_token_key": ref})
        with pytest.raises(SettingsValidationError):
            mgr.save_values({"repo_iac_controller_token_key": ref})
    assert "iac_gitlab_api_token_key" not in ctx.vault and "repo_iac_controller_config" not in ctx.vault
    assert mgr.save_values({"gitlab_token_key": "gitlab-int"})["saved"] == ["gitlab_token_key"]


@pytest.mark.parametrize("url", [
    "http://gitlab.int.fam-feser.de",              # not https
    "https://attacker.example",                    # host not allowed
    "https://user:pw@gitlab.int.fam-feser.de",     # credentials in the URL
    "ftp://gitlab.int.fam-feser.de", "not a url",
])
def test_gitlab_url_rules(url):
    ctx, mgr = _mgr()
    with pytest.raises(SettingsValidationError):
        mgr.save_values({"gitlab_url": url})
    assert "iac_gitlab_url" not in ctx.vault


def test_gitlab_host_allowlist_and_https_opt_out_are_deployment_settings(monkeypatch):
    ctx, mgr = _mgr()
    monkeypatch.setenv("PLUGIN_IAC_ORCHESTRATOR_ALLOWED_GIT_HOSTS", "git.other.example")
    assert mgr.save_values({"gitlab_url": "https://git.other.example"})["saved"] == ["gitlab_url"]
    with pytest.raises(SettingsValidationError):
        mgr.save_values({"gitlab_url": "http://git.other.example"})
    monkeypatch.setenv("PLUGIN_IAC_ORCHESTRATOR_ALLOW_INSECURE_URLS", "true")
    assert mgr.save_values({"gitlab_url": "http://git.other.example"})["saved"] == ["gitlab_url"]


@pytest.mark.parametrize("url", [
    "ext::sh -c id", "file:///etc", "--upload-pack=x", "https://attacker.example/x.git",
    "git@attacker.example:x/y.git", "https://tok:en@gitlab.int.fam-feser.de/x.git",
])
def test_repo_url_rejects_remote_helpers_foreign_hosts_and_embedded_credentials(url):
    ctx, mgr = _mgr()
    with pytest.raises(SettingsValidationError):
        mgr.save_values({"repo_iac_controller_url": url})
    assert "repo_iac_controller_config" not in ctx.vault
    ok = mgr.save_values({"repo_iac_controller_url": "https://gitlab.int.fam-feser.de/iac/iac-controller.git",
                          "repo_config_engine_url": "git@gitlab.int.fam-feser.de:iac/config-engine.git"})
    assert sorted(ok["saved"]) == ["repo_config_engine_url", "repo_iac_controller_url"]


@pytest.mark.parametrize("image", [
    "attacker/x:latest", "docker.io/attacker/x:latest", "-v/:/host", "registry.gitlab.int.fam-feser.de",
    "registry.gitlab.int.fam-feser.de.attacker.example/x:1", "ghcr.io/opentofu/opentofu-evil:1", "",
])
def test_runner_images_must_come_from_the_allowlist(image):
    ctx, mgr = _mgr()
    with pytest.raises(SettingsValidationError):
        mgr.save_values({"ansible_docker_image": image})
    assert "ansible_docker_image" not in ctx.vault


def test_runner_image_allowlist_is_configurable_by_the_deployment(monkeypatch):
    ctx, mgr = _mgr()
    assert mgr.save_values({"ansible_docker_image": GOOD_IMAGE,
                            "iac_terraform_docker_image": "ghcr.io/opentofu/opentofu:1.10.6"})["saved"]
    monkeypatch.setenv(config_mod.RUNNER_IMAGE_ALLOWLIST_ENV, "harbor.example/iac/")
    with pytest.raises(SettingsValidationError):
        mgr.save_values({"ansible_docker_image": GOOD_IMAGE.replace("1.2.3", "1.2.4")})
    assert mgr.save_values({"ansible_docker_image": "harbor.example/iac/ansible:2"})["saved"] == ["ansible_docker_image"]
    # the registry the token is sent to must be one of the allowlisted registries
    with pytest.raises(SettingsValidationError):
        mgr.save_values({"ansible_registry_url": "registry.attacker.example"})
    assert mgr.save_values({"ansible_registry_url": "harbor.example"})["saved"] == ["ansible_registry_url"]


def test_secret_slots_are_not_writable_through_save_values():
    ctx, mgr = _mgr(ansible_ssh_key="dummy-key-not-real")
    for key in ("ansible_ssh_key", "iac_tf_ssh_private_key", "iac_tf_ssh_key", "iac_tf_root_password",
                "iac_tf_backend_secret_key", "ansible_registry_token"):
        with pytest.raises(SettingsValidationError):
            mgr.save_values({key: "attacker-value-not-real"})
    assert ctx.vault["ansible_ssh_key"] == "dummy-key-not-real"
    # blank / sentinel = "keep", as before
    assert mgr.save_values({"ansible_ssh_key": "", "iac_tf_root_password": "********"})["saved"] == []
    # the slot route writes them
    mgr.set_secret_slot("ansible_ssh_key", "new-dummy-key-not-real")
    assert ctx.vault["ansible_ssh_key"] == "new-dummy-key-not-real"
    with pytest.raises(SettingsValidationError):
        mgr.set_secret_slot("ansible_docker_image", "x")       # not a secret slot
    with pytest.raises(SettingsValidationError):
        mgr.set_secret_slot("gitlab_webhook_token", "x")       # not a field at all


def test_one_invalid_value_saves_nothing():
    ctx, mgr = _mgr()
    with pytest.raises(SettingsValidationError):
        mgr.save_values({"group_id": "42", "ansible_docker_image": "attacker/x:latest"})
    assert "iac_gitlab_group_id" not in ctx.vault


def test_other_rules():
    ctx, mgr = _mgr()
    for bad in ({"group_id": "1; rm"}, {"test_deploy_allowed_hosts": "docker-dev, ../x"},
                {"sync_interval": 5}, {"maintenance_max_override_minutes": "abc"},
                {"ansible_registry_user": "a b"}, {"lyndrix_base_url": "http://10.1.10.99:8081"}):
        with pytest.raises(SettingsValidationError):
            mgr.save_values(bad)
    assert mgr.save_values({"test_deploy_allowed_hosts": "docker-dev, docker-test",
                            "lyndrix_base_url": "https://lyndrix.example"})["saved"]


# --- tiers ---------------------------------------------------------------------------

def test_only_changed_fields_need_their_tier():
    ctx, mgr = _mgr(ansible_docker_image=GOOD_IMAGE)
    operator = {"operator"}
    # a whole-form resubmit with untouched admin fields passes for an operator …
    res = mgr.save_values({"ansible_docker_image": GOOD_IMAGE, "gitlab_url": "https://gitlab.int.fam-feser.de",
                           "maintenance_linger_minutes": 20}, allowed_tiers=operator)
    assert res["saved"] == ["maintenance_linger_minutes"] and "ansible_docker_image" in res["unchanged"]
    # … a real change of an admin field does not, and nothing is written
    with pytest.raises(SettingsPermissionError) as exc:
        mgr.save_values({"ansible_docker_image": GOOD_IMAGE + "0", "maintenance_linger_minutes": 25},
                        allowed_tiers=operator)
    assert exc.value.tier == "settings_admin" and ctx.vault["iac_maintenance_linger_minutes"] == "20"
    with pytest.raises(SettingsPermissionError) as exc:
        mgr.save_values({"gitlab_token_key": "gitlab-int"}, allowed_tiers={"operator", "settings_admin"})
    assert exc.value.tier == "credentials_admin"
    assert mgr.save_values({"gitlab_token_key": "gitlab-int"},
                           allowed_tiers={"operator", "credentials_admin"})["saved"] == ["gitlab_token_key"]


def test_handler_maps_tiers_to_custom_permissions(monkeypatch):
    ctx, mgr = _mgr()
    monkeypatch.setattr(api_mod, "_ctx", ctx)
    monkeypatch.setattr(api_mod, "_service", SimpleNamespace(state={}))

    def identity(*perms):
        return SimpleNamespace(username="alice", allows=lambda p: p in perms)

    payload = api_mod.SettingsValuesRequest(values={"ansible_docker_image": GOOD_IMAGE})
    with pytest.raises(HTTPException) as exc:
        asyncio.run(api_mod.do_save_settings_values(payload, identity(routes_mod.PERM_WRITE)))
    assert exc.value.status_code == 403 and api_mod.PERM_SETTINGS_ADMIN in exc.value.detail
    res = asyncio.run(api_mod.do_save_settings_values(payload, identity(api_mod.PERM_SETTINGS_ADMIN)))
    assert res["saved"] == ["ansible_docker_image"]

    bad = api_mod.SettingsValuesRequest(values={"ansible_ssh_key": "attacker-key-not-real"})
    with pytest.raises(HTTPException) as exc:
        asyncio.run(api_mod.do_save_settings_values(
            bad, identity(api_mod.PERM_SETTINGS_ADMIN, api_mod.PERM_CREDENTIALS_ADMIN)))
    assert exc.value.status_code == 422 and "ansible_ssh_key" not in ctx.vault

    # legacy general form: same path
    general = api_mod.SettingsRequest(gitlab_url="https://attacker.example", gitlab_token_key="iac_tf_proxmox_password")
    with pytest.raises(HTTPException) as exc:
        asyncio.run(api_mod.do_save_settings(general, identity(api_mod.PERM_SETTINGS_ADMIN, api_mod.PERM_CREDENTIALS_ADMIN)))
    assert exc.value.status_code == 422 and "iac_gitlab_url" not in ctx.vault


def _route_permissions(router) -> dict:
    out = {}
    for route in router.routes:
        perms = set()
        for dep in route.dependant.dependencies:
            for cell in getattr(dep.call, "__closure__", None) or ():
                try:
                    value = cell.cell_contents
                except ValueError:
                    continue
                if isinstance(value, str):
                    perms.add(value)
        for method in route.methods:
            out[(method, route.path)] = perms
    return out


def test_routes_carry_the_custom_permissions():
    perms = _route_permissions(routes_mod.build_plugin_router(None))
    cred, sett = api_mod.PERM_CREDENTIALS_ADMIN, api_mod.PERM_SETTINGS_ADMIN
    assert cred in perms[("POST", "/settings/credentials")]
    assert cred in perms[("DELETE", "/settings/credentials/{alias}")]
    assert cred in perms[("POST", "/settings/secrets/{key}")]
    assert sett in perms[("POST", "/settings/webhooks/sync")]
    # the core hands plugin api:write to every INT_USER and lets the global
    # api:write satisfy it — the admin gates must not have that fallback
    for perm in (cred, sett, api_mod.PERM_INFRA_APPLY):
        assert perm.startswith("plugin:lyndrix.plugin.iac_orchestrator:")
        assert not perm.endswith((":api:read", ":api:write"))


def test_manifest_declares_the_permissions_and_maps_them_to_the_admin_role():
    from iac_plugin.entrypoint import manifest

    declared = {f"plugin:{manifest.id}:{p.id}" for p in manifest.custom_permissions}
    assert {api_mod.PERM_SETTINGS_ADMIN, api_mod.PERM_CREDENTIALS_ADMIN, api_mod.PERM_INFRA_APPLY} <= declared
    roles = {r.id: set(r.permissions) for r in manifest.roles}
    assert {api_mod.PERM_SETTINGS_ADMIN, api_mod.PERM_CREDENTIALS_ADMIN} <= roles["admin"]
    assert not {api_mod.PERM_SETTINGS_ADMIN, api_mod.PERM_CREDENTIALS_ADMIN} & (roles["operator"] | roles["viewer"])


# --- credential aliases --------------------------------------------------------------

def test_alias_cannot_shadow_a_setting_or_secret_slot():
    assert {"ansible_docker_image", "ansible_registry_token", "repo_iac_controller_config",
            "gitlab_webhook_token", "iac_auto_apply"} <= reserved_vault_keys()
    for alias in ("ansible_docker_image", "Ansible_Registry_Url", "repo_service_repos_config", "iac_x",
                  "gitlab_webhook_token", "a b", "../x", ""):
        assert not is_valid_credential_alias(alias)
    assert is_valid_credential_alias("gitlab-int")
    ctx = FakeCtx()
    assert resolve_credential(ctx, "gitlab-int") == "dummy-token-not-real"
    assert resolve_credential(ctx, "iac_tf_proxmox_password") is None
    assert resolve_credential(ctx, "gitlab_webhook_token") is None


# --- use-time checks (values stored before the fix / by another surface) --------------

def test_webhook_sync_never_sends_another_secret_as_api_token(monkeypatch):
    calls = []
    monkeypatch.setattr(gitlab_webhooks, "upsert_gitlab_group_webhooks", lambda *a, **k: calls.append(a) or {})
    base = dict(iac_gitlab_group_id="1", iac_lyndrix_base_url="https://lyndrix.example")
    poisoned = FakeCtx(iac_gitlab_url="https://gitlab.int.fam-feser.de",
                       iac_gitlab_api_token_key="iac_tf_proxmox_password", **base)
    with pytest.raises(gitlab_webhooks.WebhookConfigError):
        gitlab_webhooks.sync_group_webhooks_from_ctx(poisoned)
    plain_http = FakeCtx(iac_gitlab_url="http://attacker.example", iac_gitlab_api_token_key="gitlab-int", **base)
    with pytest.raises(gitlab_webhooks.WebhookConfigError):
        gitlab_webhooks.sync_group_webhooks_from_ctx(plain_http)
    assert calls == []
    gitlab_webhooks.sync_group_webhooks_from_ctx(
        FakeCtx(iac_gitlab_url="https://gitlab.int.fam-feser.de", iac_gitlab_api_token_key="gitlab-int", **base))
    assert len(calls) == 1 and calls[0][1] == "dummy-token-not-real"


def test_config_refuses_a_foreign_runner_image_at_use_time(monkeypatch):
    cfg = IaCConfig(FakeCtx(ansible_docker_image="attacker.example/pwn:latest",
                            iac_terraform_docker_image="ghcr.io/opentofu/opentofu:1.10.6"))
    with pytest.raises(RunnerImageNotAllowed):
        cfg.checked_runner_image("ansible")
    assert cfg.checked_runner_image("terraform") == "ghcr.io/opentofu/opentofu:1.10.6"
    # an image pinned by the deployment is trusted configuration (syntax only)
    monkeypatch.setenv("PLUGIN_IAC_ORCHESTRATOR_ANSIBLE_IMAGE", "harbor.example/iac/ansible:2")
    assert cfg.checked_runner_image("ansible") == "harbor.example/iac/ansible:2"
    monkeypatch.setenv("PLUGIN_IAC_ORCHESTRATOR_ANSIBLE_IMAGE", "--privileged")
    with pytest.raises(RunnerImageNotAllowed):
        cfg.checked_runner_image("ansible")


def test_engine_starts_no_container_from_a_foreign_image(monkeypatch, tmp_path):
    commands = []

    class _Proc:
        returncode = 0

        async def communicate(self, input=None):
            return b"", b""

    async def fake_exec(*cmd, **kwargs):
        commands.append(list(cmd))
        return _Proc()

    monkeypatch.setattr(engine_mod.shutil, "which", lambda _: "/usr/bin/docker")
    monkeypatch.setattr(engine_mod.asyncio, "create_subprocess_exec", fake_exec)
    monkeypatch.setenv("PLUGIN_IAC_ORCHESTRATOR_INTERNAL_SERVICES_DIR", str(tmp_path))

    eng = DeploymentEngine.__new__(DeploymentEngine)
    eng.state = {}
    eng.ctx = FakeCtx(ansible_ssh_key="dummy-key-not-real", ansible_docker_image="attacker.example/pwn:latest",
                      ansible_registry_url="registry.attacker.example", ansible_registry_user="u",
                      ansible_registry_token="dummy-registry-token-not-real")
    eng.config = IaCConfig(eng.ctx)
    eng._sync_runners = lambda *a, **k: None
    eng._job_inventory_snapshot_dir = lambda job_id: tmp_path / "none"
    watched = []

    async def fake_watch(*a, **k):
        watched.append(a)
        return True, {}

    eng._watch_detached_runner = fake_watch
    ok, _ = asyncio.run(eng.execute_ansible_docker(
        playbook_subpath="pb.yml", inventory_subpath="inv.yml", limit="h1", task_name="t", job_id=7))
    assert ok is False and watched == []
    assert not any(cmd[:2] == ["docker", "run"] for cmd in commands)
    assert not any(cmd[:2] == ["docker", "login"] for cmd in commands)   # token not sent to a foreign registry
    assert list((tmp_path / ".iac_keys").glob("*")) == []                # key file removed again
