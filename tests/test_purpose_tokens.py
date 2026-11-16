"""OS-04 (option A, backward compatible): one token per purpose of the public
routes. An empty slot keeps the shared ``gitlab_webhook_token`` working for
that purpose exactly as before; a set slot accepts only its own token."""
import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from iac_plugin.app.controller import api as api_mod
from iac_plugin.app.controller import gitlab_webhooks
from iac_plugin.app.controller.queue import SubmitResult
from iac_plugin.app.controller.settings_schema import OrchestratorSettings, reserved_vault_keys

SHARED = "dummy-shared-token-not-real"
HOOK, SERVICE, CONTROLLER = "dummy-hook-token-not-real", "dummy-service-ci-token-not-real", "dummy-controller-ci-token-not-real"
PURPOSES = (api_mod.PURPOSE_HOOK, api_mod.PURPOSE_SERVICE_CI, api_mod.PURPOSE_CONTROLLER_CI)


class _Ctx:
    def __init__(self, **vault):
        self.vault = {"gitlab_webhook_token": SHARED, **vault}
        self.log = MagicMock()
        self.written = []

    def get_secret(self, key):
        return self.vault.get(key)

    def set_secret(self, key, value):
        self.written.append(key)
        self.vault[key] = value
        return True


def _request(address="10.1.10.50"):
    return SimpleNamespace(client=SimpleNamespace(host=address))


def _check(token, purpose, address="10.1.10.50"):
    return asyncio.run(api_mod._require_gitlab_token(token, purpose, _request(address)))


def _status(token, purpose, address="10.1.10.50"):
    try:
        return _check(token, purpose, address)
    except HTTPException as exc:
        return exc.status_code


@pytest.fixture
def ctx(monkeypatch):
    c = _Ctx()
    monkeypatch.setattr(api_mod, "_ctx", c)
    return c


def _reload():
    api_mod._token_cache.update(loaded=False, forced_at=0.0)


# --- nothing changes for a deployment that changes nothing ------------------------------------------

def test_with_empty_slots_the_shared_token_authorises_every_purpose_as_before(ctx):
    for purpose in PURPOSES:
        assert _check(SHARED, purpose) == "shared"
        assert _check(SHARED, purpose) == "shared"
    assert _check(SHARED, None) == "shared"
    # … with ONE deprecation warning per purpose, not one per request
    warnings = [c.args[0] for c in ctx.log.warning.call_args_list if c.args[0].startswith("DEPRECATED")]
    assert len(warnings) == 3 and "gitlab_service_ci_token" in "".join(warnings)
    assert asyncio.run(api_mod.token_modes()) == {p: "shared" for p in PURPOSES}
    assert ctx.written == []                                       # the code never writes a token


def test_a_set_slot_accepts_only_its_own_token_for_its_purpose(ctx):
    ctx.vault["gitlab_service_ci_token"] = SERVICE
    _reload()
    assert _check(SERVICE, api_mod.PURPOSE_SERVICE_CI) == "dedicated"
    assert _status(SHARED, api_mod.PURPOSE_SERVICE_CI) == 401      # the shared token is out — for THIS purpose
    assert _check(SHARED, api_mod.PURPOSE_HOOK) == "shared"        # the others are untouched
    assert _check(SHARED, api_mod.PURPOSE_CONTROLLER_CI) == "shared"
    # a purpose token is never accepted for another purpose
    assert _status(SERVICE, api_mod.PURPOSE_HOOK) == 401
    assert _status(SERVICE, api_mod.PURPOSE_CONTROLLER_CI) == 401
    assert asyncio.run(api_mod.token_modes()) == {"hook": "shared", "service_ci": "dedicated", "controller_ci": "shared"}


def test_all_three_slots_set(ctx):
    ctx.vault.update(gitlab_hook_token=HOOK, gitlab_service_ci_token=SERVICE, gitlab_controller_ci_token=CONTROLLER)
    _reload()
    tokens = {api_mod.PURPOSE_HOOK: HOOK, api_mod.PURPOSE_SERVICE_CI: SERVICE, api_mod.PURPOSE_CONTROLLER_CI: CONTROLLER}
    for purpose, token in tokens.items():
        assert _check(token, purpose, address=f"10.0.0.{len(purpose)}") == "dedicated"
        for other in (SHARED, *[t for p, t in tokens.items() if p != purpose]):
            assert _status(other, purpose, address=f"10.9.{len(purpose)}.{len(other)}") == 401
    assert ctx.vault["gitlab_webhook_token"] == SHARED and ctx.written == []   # never modified, moved or deleted
    assert not any(c.args[0].startswith("DEPRECATED") for c in ctx.log.warning.call_args_list)


def test_no_token_at_all_fails_closed(monkeypatch):
    monkeypatch.setattr(api_mod, "_ctx", _Ctx(gitlab_webhook_token=None))
    assert _status("anything", api_mod.PURPOSE_HOOK) == 500
    assert asyncio.run(api_mod.token_modes())["hook"] == "unconfigured"


def test_the_slots_are_secret_slots_of_the_credentials_tier(ctx):
    mgr = OrchestratorSettings(ctx, SimpleNamespace(state={}))
    schema = {f["key"]: f for f in mgr.get_schema()}
    for key in api_mod.PURPOSE_TOKEN_KEYS.values():
        assert schema[key]["secret_slot"] and schema[key]["tier"] == "credentials_admin"
        assert key in reserved_vault_keys()                        # never usable as a credential alias
    mgr.set_secret_slot("gitlab_hook_token", HOOK)
    assert ctx.vault["gitlab_hook_token"] == HOOK and ctx.vault["gitlab_webhook_token"] == SHARED
    assert HOOK not in str(mgr.get_values())                       # never echoed


# --- failure rate limit ---------------------------------------------------------------------------------

def test_rejected_calls_of_an_address_are_limited_but_a_valid_token_always_passes(ctx):
    runner = "10.1.10.77"                                          # the shared egress address of the CI runners
    codes = [_status("wrong-token-not-real", api_mod.PURPOSE_SERVICE_CI, runner) for _ in range(api_mod._FAIL_LIMIT + 5)]
    assert codes[:api_mod._FAIL_LIMIT] == [401] * api_mod._FAIL_LIMIT and set(codes[api_mod._FAIL_LIMIT:]) == {429}
    with pytest.raises(HTTPException) as exc:
        _check("wrong-token-not-real", api_mod.PURPOSE_SERVICE_CI, runner)
    assert 1 <= int(exc.value.headers["Retry-After"]) <= api_mod._FAIL_WINDOW_SECONDS + 1
    # no lock-out: the legitimate pipelines behind the same address go on
    assert _check(SHARED, api_mod.PURPOSE_SERVICE_CI, runner) == "shared"
    assert _status("wrong-token-not-real", api_mod.PURPOSE_HOOK, "10.1.10.78") == 401     # another address
    assert api_mod.webhook_auth_failures["count"] == api_mod._FAIL_LIMIT + 7


def test_the_limiter_forgets_old_failures_and_is_bounded(ctx, monkeypatch):
    clock = {"now": 1000.0}
    monkeypatch.setattr(api_mod.time, "monotonic", lambda: clock["now"])
    for _ in range(api_mod._FAIL_LIMIT):
        _status("wrong-token-not-real", api_mod.PURPOSE_HOOK, "10.2.0.1")
    assert _status("wrong-token-not-real", api_mod.PURPOSE_HOOK, "10.2.0.1") == 429
    clock["now"] += api_mod._FAIL_WINDOW_SECONDS + 1
    assert _status("wrong-token-not-real", api_mod.PURPOSE_HOOK, "10.2.0.1") == 401
    monkeypatch.setattr(api_mod, "_FAIL_TRACKED_ADDRESSES", 50)
    for n in range(500):
        _status("wrong-token-not-real", api_mod.PURPOSE_HOOK, f"203.0.113.{n}")
    assert len(api_mod._failures_by_address) <= 50


# --- hook purpose: what the hooks really send ---------------------------------------------------------------

def _hook_env(monkeypatch, ctx):
    submitted = []

    async def submit_job_ex(payload, *, entitlements=(), requested_by=None):
        submitted.append(dict(payload))
        return SubmitResult(7)

    ctx.emit, ctx.notify = MagicMock(), MagicMock()
    monkeypatch.setattr(api_mod, "_service", SimpleNamespace(submit_job_ex=submit_job_ex, branch_policy=lambda: None))
    monkeypatch.setattr(api_mod, "_recent_events", {})
    return submitted


def _pipeline_body(pipeline_id=1):
    return {"object_kind": "pipeline", "project": {"path_with_namespace": "grp/aac-x", "name": "aac-x"},
            "object_attributes": {"id": pipeline_id, "status": "success", "source": "push", "ref": "main"}}


def _hook(body, token):
    async def json():
        return body

    request = SimpleNamespace(json=json, client=SimpleNamespace(host="10.1.10.50"))
    return asyncio.run(api_mod.do_gitlab_webhook(request, token))


def test_the_group_hooks_send_merge_request_events_only():
    """What the rejection below is based on: the hooks this plugin registers."""
    source = __import__("inspect").getsource(gitlab_webhooks)
    for flag in ('"push_events": False', '"pipeline_events": False', '"job_events": False', '"tag_push_events": False'):
        assert flag in source
    assert '"merge_requests_events": True' in source


def test_with_the_hook_token_a_pipeline_body_never_starts_a_deployment(ctx, monkeypatch):
    ctx.vault["gitlab_hook_token"] = HOOK
    _reload()
    submitted = _hook_env(monkeypatch, ctx)
    answer = _hook(_pipeline_body(), HOOK)
    assert answer["status"] == "accepted" and answer["triggered"] is False and "hook token" in answer["reason"]
    assert submitted == [] and not ctx.emit.called                 # nothing queued, nothing announced
    merged = {"object_kind": "merge_request", "project": {"path_with_namespace": "grp/aac-x"},
              "object_attributes": {"iid": 3, "action": "merge", "state": "merged", "title": "t"}}
    assert _hook(merged, HOOK)["triggered"] is False and ctx.emit.called     # the notification still goes out


def test_on_the_shared_token_a_pipeline_hook_keeps_working_as_before(ctx, monkeypatch):
    submitted = _hook_env(monkeypatch, ctx)
    answer = _hook(_pipeline_body(), SHARED)
    assert answer["triggered"] is True and submitted[0]["service_branch"] == "main"


# --- service-CI purpose: the production branch, nothing else ------------------------------------------------

POLICY = {"prod": "main", "dev": "dev", "test": "test", "services": {"aac-legacy": "master"}}


def _deploy(monkeypatch, ctx, branch, *, prod_only, policy=POLICY, service="aac-traefik"):
    submitted = _hook_env(monkeypatch, ctx)
    monkeypatch.setattr(api_mod._service, "branch_policy", lambda: policy)
    asyncio.run(api_mod.do_trigger_service_deployment(service, api_mod.DeployRequest(branch=branch),
                                                      trigger="gitlab-ci", prod_only=prod_only))
    return submitted[0]


def test_the_dedicated_service_ci_token_deploys_the_production_branch_whatever_the_body_says(ctx, monkeypatch):
    assert _deploy(monkeypatch, ctx, "feature-x", prod_only=True)["service_branch"] == "main"
    assert _deploy(monkeypatch, ctx, "dev", prod_only=True, service="aac-legacy")["service_branch"] == "master"
    same = _deploy(monkeypatch, ctx, "main", prod_only=True)
    assert same["service_branch"] == "main" and "require_prod_branch" not in same      # the payload of today
    # no policy on this instance yet: the runner enforces it
    unknown = _deploy(monkeypatch, ctx, "main", prod_only=True, policy=None)
    assert unknown["service_branch"] == "main" and unknown["require_prod_branch"] is True


def test_on_the_shared_token_the_body_chooses_the_branch_as_before(ctx, monkeypatch):
    assert _deploy(monkeypatch, ctx, "dev", prod_only=False)["service_branch"] == "dev"


def test_the_service_route_passes_the_mode_of_the_token(ctx, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from iac_plugin.app.controller.webhook_router import build_webhook_router

    ctx.vault["gitlab_service_ci_token"] = SERVICE
    _reload()
    submitted = _hook_env(monkeypatch, ctx)
    monkeypatch.setattr(api_mod._service, "branch_policy", lambda: POLICY)
    app = FastAPI()
    app.include_router(build_webhook_router(None))
    client = TestClient(app)
    url = "/api/iac/deploy/service/aac-traefik/gitlab"
    assert client.post(url, json={"branch": "main"}, headers={"X-Gitlab-Token": SHARED}).status_code == 401
    assert client.post(url, json={"branch": "main"}, headers={"X-Gitlab-Token": HOOK}).status_code == 401
    ok = client.post(url, json={"branch": "dev"}, headers={"X-Gitlab-Token": SERVICE})
    assert ok.status_code == 200 and submitted[0]["service_branch"] == "main"
    # the controller-CI routes still run on the shared token (their slot is empty)
    assert client.post("/api/iac/infra/plan/gitlab", headers={"X-Gitlab-Token": SHARED}).status_code == 200
    assert client.post("/api/iac/infra/plan/gitlab", headers={"X-Gitlab-Token": SERVICE}).status_code == 401
    # … and the token apply route stays off by default
    monkeypatch.setattr(api_mod, "_engine", SimpleNamespace(config=SimpleNamespace(token_infra_apply_enabled=False)))
    assert client.post("/api/iac/infra/apply/gitlab", headers={"X-Gitlab-Token": SHARED}).status_code == 404


# --- hook registration ------------------------------------------------------------------------------------------

def test_the_hook_sync_registers_the_dedicated_hook_token_once_it_is_set(monkeypatch):
    used = []
    monkeypatch.setattr(gitlab_webhooks, "upsert_gitlab_group_webhooks",
                        lambda url, api_token, group, base, hook_secret: used.append(hook_secret) or {})
    vault = {"iac_gitlab_group_id": "7", "iac_lyndrix_base_url": "https://lyndrix.example.invalid",
             "iac_gitlab_api_token_key": "gitlab-int", "iac_token_registry": '["gitlab-int"]',
             "gitlab-int": "dummy-api-token-not-real"}
    ctx = _Ctx(**vault)
    gitlab_webhooks.sync_group_webhooks_from_ctx(ctx)
    ctx.vault["gitlab_hook_token"] = HOOK
    gitlab_webhooks.sync_group_webhooks_from_ctx(ctx)
    assert used == [SHARED, HOOK]
