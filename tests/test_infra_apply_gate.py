"""OS-03: the infra-apply permission cannot be side-stepped by plugin writers or
by holders of the GitLab webhook token."""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from iac_plugin.app import api as routes_mod
from iac_plugin.app.controller import api as api_mod
from iac_plugin.app.controller.config import IaCConfig
from iac_plugin.app.controller.settings_schema import OrchestratorSettings, SettingsPermissionError
from tests.test_settings_security import FakeCtx, _route_permissions

APPLY = api_mod.PERM_INFRA_APPLY


def _identity(*perms):
    return SimpleNamespace(username="bob", allows=lambda p: p in perms)


WRITER = _identity(routes_mod.PERM_READ, routes_mod.PERM_WRITE)


@pytest.fixture
def api(monkeypatch):
    ctx = FakeCtx()
    ctx.emit = MagicMock()
    ctx.notify = MagicMock()
    monkeypatch.setattr(api_mod, "_ctx", ctx)
    monkeypatch.setattr(api_mod, "_engine", SimpleNamespace(config=IaCConfig(ctx)))
    ctx.submitted = []

    async def submit_job(payload, *, entitlements=(), requested_by=None):
        ctx.submitted.append((dict(payload), set(entitlements)))
        return len(ctx.submitted)

    monkeypatch.setattr(api_mod, "_service", SimpleNamespace(state={}, submit_job=submit_job))
    return ctx


def _triggers(ctx):
    return [payload for payload, _ents in ctx.submitted]


def test_minting_the_webhook_token_needs_apply_and_credentials_permission():
    perms = _route_permissions(routes_mod.build_plugin_router(None))
    gate = perms[("POST", "/settings/webhook-token/generate")]
    assert {APPLY, api_mod.PERM_CREDENTIALS_ADMIN} <= gate
    # the two apply routes keep their gate
    assert APPLY in perms[("POST", "/infra/apply")] and APPLY in perms[("POST", "/infra/pending-plan/apply")]


def test_enabling_auto_apply_needs_the_apply_permission_disabling_does_not():
    ctx = FakeCtx()
    mgr = OrchestratorSettings(ctx, SimpleNamespace(state={}))
    admin_without_apply = {"operator", "settings_admin", "credentials_admin"}
    for key in ("auto_apply", "token_infra_apply_enabled"):
        with pytest.raises(SettingsPermissionError) as exc:
            mgr.save_values({key: True}, allowed_tiers=admin_without_apply)
        assert exc.value.tier == "infra_apply"
    assert "iac_auto_apply" not in ctx.vault and "iac_token_infra_apply_enabled" not in ctx.vault

    assert mgr.save_values({"auto_apply": True}, allowed_tiers=admin_without_apply | {"infra_apply"})["saved"] == ["auto_apply"]
    assert ctx.vault["iac_auto_apply"] == "True"
    # an operator can always close the gate — also when it reads as closed already
    assert mgr.save_values({"auto_apply": False}, allowed_tiers={"operator"})["saved"] == ["auto_apply"]
    assert ctx.vault["iac_auto_apply"] == "False"
    assert mgr.save_values({"auto_apply": False}, allowed_tiers={"operator"})["saved"] == ["auto_apply"]


def test_handler_reports_the_missing_apply_permission_for_auto_apply(api):
    payload = api_mod.SettingsRequest(auto_apply=True)
    with pytest.raises(HTTPException) as exc:
        asyncio.run(api_mod.do_save_settings(payload, _identity(routes_mod.PERM_WRITE, api_mod.PERM_SETTINGS_ADMIN)))
    assert exc.value.status_code == 403 and APPLY in exc.value.detail
    assert "iac_auto_apply" not in api.vault


def test_init_host_needs_apply_permission_unless_dry_run(api):
    request = api_mod.PipelineRequest(pipeline_type="init_host", host_name="docker-atlas")
    with pytest.raises(HTTPException) as exc:
        asyncio.run(api_mod.do_run_pipeline(request, WRITER))
    assert exc.value.status_code == 403 and APPLY in exc.value.detail
    assert _triggers(api) == []                                       # nothing was queued

    dry = api_mod.PipelineRequest(pipeline_type="init_host", host_name="docker-atlas", dry_run=True)
    asyncio.run(api_mod.do_run_pipeline(dry, WRITER))
    assert _triggers(api) == [{"pipeline_type": "init_host", "manual": True, "trigger": "ui_pipeline",
                               "host_name": "docker-atlas", "dry_run": True}]

    asyncio.run(api_mod.do_run_pipeline(request, _identity(routes_mod.PERM_WRITE, APPLY)))
    assert len(_triggers(api)) == 2 and "dry_run" not in _triggers(api)[1]
    # pipelines that never run Terraform for the target stay plain write
    asyncio.run(api_mod.do_run_pipeline(api_mod.PipelineRequest(pipeline_type="compliance", host_name="h1"), WRITER))
    assert len(_triggers(api)) == 3


def test_test_host_provision_needs_apply_permission(api):
    api.vault["iac_test_deploy_allowed_hosts"] = "docker-dev"
    with pytest.raises(HTTPException) as exc:
        asyncio.run(api_mod.do_trigger_test_host_deployment("docker-dev", api_mod.TestHostDeployRequest(), WRITER))
    assert exc.value.status_code == 403 and _triggers(api) == []


def test_token_apply_route_is_off_by_default(api):
    secret_reads = []
    real_get = api.get_secret
    api.get_secret = lambda key: secret_reads.append(key) or real_get(key)

    with pytest.raises(HTTPException) as exc:
        asyncio.run(api_mod.do_trigger_infra_apply_by_token("dummy-webhook-token-not-real"))
    assert exc.value.status_code == 404
    assert _triggers(api) == []                                       # no apply was queued
    assert "gitlab_webhook_token" not in secret_reads                 # disabled before the token is even read

    api.vault["iac_token_infra_apply_enabled"] = "true"
    with pytest.raises(HTTPException) as exc:
        asyncio.run(api_mod.do_trigger_infra_apply_by_token("wrong-token"))
    assert exc.value.status_code == 401 and _triggers(api) == []
    asyncio.run(api_mod.do_trigger_infra_apply_by_token("dummy-webhook-token-not-real"))
    assert _triggers(api)[0]["pipeline_type"] == "infra_apply" and _triggers(api)[0]["approve"] is True
    assert "approve" in api.submitted[0][1]            # entitled code path
