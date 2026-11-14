"""OS-07: job output is masked before it is stored or streamed, --diff is off for
real runs, and log content needs its own permission."""
import asyncio
import json
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import yaml
from fastapi import HTTPException

from iac_plugin.app import api as routes_mod
from iac_plugin.app.controller import api as api_mod
from iac_plugin.app.controller import engine as engine_mod
from iac_plugin.app.controller.engine import DeploymentEngine
from iac_plugin.app.controller.redaction import MASK, SecretRedactor, secret_values_of
from iac_plugin.app.controller.utils import JobFileLogBridge, StageResult, current_job_id
from tests.test_reload_running_job import _Stage, _row, _start_job, _until, _wire
from tests.test_settings_security import FakeCtx, _route_permissions

TOKEN = "dummy-registry-token-not-real"
PEM = "-----BEGIN OPENSSH PRIVATE KEY-----\nZHVtbXkta2V5LWxpbmUtb25lLW5vdC1yZWFs\nZHVtbXkta2V5LWxpbmUtdHdvLW5vdC1yZWFs\n-----END OPENSSH PRIVATE KEY-----"


# --- the redactor ----------------------------------------------------------------------

def test_known_values_are_masked_longest_first():
    red = SecretRedactor([TOKEN, TOKEN + "-extended", "short", "", None, 42])
    assert red.redact(f"login with {TOKEN}-extended ok") == f"login with {MASK} ok"
    assert red.redact(f"POSTGRES_PASSWORD={TOKEN}") == f"POSTGRES_PASSWORD={MASK}"
    assert red.redact("a short word stays") == "a short word stays"       # < 8 chars is never registered
    assert len(red) == 2


def test_multi_line_secrets_are_masked_line_by_line_and_in_escaped_form():
    red = SecretRedactor([PEM])
    assert red.redact("+ZHVtbXkta2V5LWxpbmUtb25lLW5vdC1yZWFs") == f"+{MASK}"
    assert MASK in red.redact(PEM.replace("\n", "\\n"))
    assert red.redact("-----BEGIN OPENSSH PRIVATE KEY-----") == "-----BEGIN OPENSSH PRIVATE KEY-----"


@pytest.mark.parametrize("line,leak", [
    ("fatal: unable to access 'https://gitlab-ci-token:dummy-pw-not-real@gitlab.example/x.git/'", "dummy-pw-not-real"),
    ("> Authorization: Basic ZHVtbXk6bm90LXJlYWw=", "ZHVtbXk6bm90LXJlYWw="),
    ('"PRIVATE-TOKEN": "dummy-private-token-not-real"', "dummy-private-token-not-real"),
    ("token glpat-dummyDUMMYdummyDUMMY1234 rejected", "glpat-dummyDUMMYdummyDUMMY1234"),
])
def test_credential_shapes_are_masked_without_knowing_the_value(line, leak):
    out = SecretRedactor().redact(line)
    assert leak not in out and MASK in out


def test_secret_values_of_an_inventory():
    inv = {"all": {"hosts": {"h1": {
        "ansible_host": "10.0.0.1", "ansible_password": "dummy-ansible-pw-not-real",
        "vault_vars": {"root_password": "dummy-root-pw-not-real", "nested": {"x": "dummy-nested-not-real"}},
        "terraform": {"node_name": "hydra", "token": "dummy-pve-token-not-real"},
        "services": [{"name": "aac-grafana", "config": {"vars": {"TZ": "Europe/Berlin"},
                                                        "secrets": {"GF_ADMIN_PASSWORD": "dummy-grafana-pw-not-real"}}}],
    }}}}
    values = set(secret_values_of(inv))
    assert values == {"dummy-ansible-pw-not-real", "dummy-root-pw-not-real", "dummy-nested-not-real",
                      "dummy-pve-token-not-real", "dummy-grafana-pw-not-real"}


# --- every sink of a job's output ----------------------------------------------------------

def _engine(tmp_path, ctx=None):
    eng = DeploymentEngine.__new__(DeploymentEngine)
    eng.state = {}
    eng.ctx = ctx or MagicMock()
    eng.queue = None
    eng.base_git_dir = tmp_path / "git"
    eng.config = SimpleNamespace(get_log_path=lambda job_id: tmp_path / f"job_{job_id}.log")
    return eng


def test_log_line_file_and_database_sink_only_see_masked_text(tmp_path):
    eng = _engine(tmp_path)
    eng._redactor(5).add(TOKEN)
    stored = []
    eng._log_writers = {5: SimpleNamespace(append=stored.append), 6: SimpleNamespace(append=lambda line: None)}
    eng.queue = object()
    eng._log_line(5, f"[aac-x] +REGISTRY_TOKEN={TOKEN}")
    assert stored == [f"[aac-x] +REGISTRY_TOKEN={MASK}"]
    assert TOKEN not in (tmp_path / "job_5.log").read_text()
    eng._log_line(6, f"[aac-y] {TOKEN}")                 # another job never had this secret …
    assert MASK not in (tmp_path / "job_6.log").read_text()   # … so nothing is over-masked there


def test_engine_log_bridge_masks_before_file_and_sink(tmp_path):
    red = SecretRedactor([TOKEN])
    sunk = []
    bridge = JobFileLogBridge(tmp_path / "job_9.log", job_id=9, sink=sunk.append, redact=red.redact)
    logger = logging.getLogger("IaC:Engine:TestRedaction")
    logger.addHandler(bridge)
    logger.setLevel(logging.INFO)
    token = current_job_id.set(9)
    try:
        logger.info(f"[TF-Render] backend secret_key = {TOKEN}")
    finally:
        current_job_id.reset(token)
        logger.removeHandler(bridge)
    assert sunk == [f"[TestRedaction] [TF-Render] backend secret_key = {MASK}"]
    assert TOKEN not in (tmp_path / "job_9.log").read_text()


def test_redactor_is_primed_with_the_plugins_secret_slots_and_credentials(tmp_path):
    ctx = FakeCtx(ansible_ssh_key=PEM, ansible_registry_token=TOKEN, iac_tf_root_password="dummy-root-pw-not-real",
                  iac_tf_ssh_key="ssh-ed25519 AAAAdummy-public-key-not-secret root@x")
    eng = _engine(tmp_path, ctx)
    asyncio.run(eng._prime_redactor(3))
    red = eng._redactor(3)
    for secret in (TOKEN, "dummy-root-pw-not-real", "dummy-token-not-real", "dummy-webhook-token-not-real",
                   "ZHVtbXkta2V5LWxpbmUtb25lLW5vdC1yZWFs"):
        assert red.redact(f"x {secret} y") == f"x {MASK} y"
    assert "AAAAdummy-public-key-not-secret" in red.redact("ssh-ed25519 AAAAdummy-public-key-not-secret root@x")


def test_inventory_secrets_of_the_job_snapshot_are_registered(tmp_path):
    eng = _engine(tmp_path)
    inv = tmp_path / "git" / ".job_snapshots" / "job_4" / "inventory_state" / "global" / "ansible"
    inv.mkdir(parents=True)
    (inv / "inventory.yml").write_text(yaml.safe_dump({"all": {"hosts": {"h": {"services": [
        {"name": "aac-x", "config": {"secrets": {"DB_PASSWORD": "dummy-db-password-not-real"}}}]}}}}))
    assert eng._register_inventory_secrets(4) == 1
    assert eng._redact(4, "+DB_PASSWORD=dummy-db-password-not-real") == f"+DB_PASSWORD={MASK}"
    assert eng._register_inventory_secrets(99) == 0       # no inventory: nothing, no error


def test_failed_pipeline_never_leaks_through_log_or_notification(iac_service, monkeypatch, tmp_path):
    svc = iac_service
    svc.ctx.get_secret = lambda key: TOKEN if key == "ansible_registry_token" else None

    class _Boom:
        name = "Clone Service Repo: aac-x"

        async def run(self, engine, context):
            return StageResult(False, f"git: fatal: Authentication failed for token {TOKEN} " + "x" * 2000)

    async def scenario():
        _wire(svc, monkeypatch, tmp_path, [_Boom()])
        job_id = _start_job(svc, None)
        await _until(lambda: _row(job_id).status not in ("RUNNING", "QUEUED"))
        await _until(lambda: not svc.worker.running_ids)      # the job task flushed its log writer
        await svc.worker.stop()
        return job_id

    job_id = asyncio.run(scenario())
    assert _row(job_id).status == "FAILED"
    failed = [c for c in svc.ctx.notify.call_args_list if c.args[0] == "deployment_failed"]
    assert len(failed) == 1
    rendered = json.dumps([failed[0].args, failed[0].kwargs], default=str)
    assert TOKEN not in rendered and MASK in rendered
    assert len(failed[0].kwargs["body"]) <= 600                       # capped
    lines = svc.queue.read_log(job_id, tail=500)["lines"]
    assert lines and not any(TOKEN in line for line in lines)
    assert TOKEN not in (tmp_path / "logs" / f"job_{job_id}.log").read_text()


# --- --diff -----------------------------------------------------------------------------------

def _ansible_cmd(monkeypatch, tmp_path, *, check_mode, diff_mode=None):
    captured = {}

    class _Proc:
        returncode = 0

        async def communicate(self, input=None):
            return b"", b""

    async def fake_exec(*cmd, **kwargs):
        if cmd[:2] == ("docker", "run"):
            captured["cmd"] = list(cmd)
        return _Proc()

    monkeypatch.setattr(engine_mod.shutil, "which", lambda _: "/usr/bin/docker")
    monkeypatch.setattr(engine_mod.asyncio, "create_subprocess_exec", fake_exec)
    eng = DeploymentEngine.__new__(DeploymentEngine)
    eng.state = {}
    eng.ctx = MagicMock()
    eng.ctx.get_secret = lambda key: "dummy-key-not-real" if key == "ansible_ssh_key" else None
    eng.config = SimpleNamespace(services_dir=tmp_path, host_git_repos_dir="/g", host_services_dir="/s",
                                 ansible_docker_image="img")
    if diff_mode:
        eng.config.ansible_diff_mode = diff_mode
    eng._sync_runners = lambda *a, **k: None
    eng._job_inventory_snapshot_dir = lambda job_id: tmp_path / "none"

    async def fake_watch(*a, **k):
        return True, {}

    eng._watch_detached_runner = fake_watch
    asyncio.run(eng.execute_ansible_docker(playbook_subpath="pb.yml", inventory_subpath="inv.yml", limit="h1",
                                           task_name="t", job_id=0, check_mode=check_mode))
    return captured["cmd"]


def test_real_runs_get_no_diff_dry_runs_do(monkeypatch, tmp_path):
    assert "--diff" not in _ansible_cmd(monkeypatch, tmp_path, check_mode=False)
    assert "--diff" in _ansible_cmd(monkeypatch, tmp_path, check_mode=True)
    assert "--diff" not in _ansible_cmd(monkeypatch, tmp_path, check_mode=True, diff_mode="never")
    assert "--diff" in _ansible_cmd(monkeypatch, tmp_path, check_mode=False, diff_mode="always")


# --- log permission ---------------------------------------------------------------------------

def test_log_routes_need_the_log_permission():
    perms = _route_permissions(routes_mod.build_plugin_router(None))
    assert api_mod.PERM_LOGS in perms[("GET", "/jobs/{job_id}/logs")]
    assert not api_mod.PERM_LOGS.endswith((":api:read", ":api:write"))
    from iac_plugin.entrypoint import manifest

    roles = {r.id: set(r.permissions) for r in manifest.roles}
    assert api_mod.PERM_LOGS in roles["admin"] and api_mod.PERM_LOGS in roles["operator"]
    assert api_mod.PERM_LOGS not in roles["viewer"]
    assert api_mod.PERM_LOGS in {f"plugin:{manifest.id}:{p.id}" for p in manifest.custom_permissions}


def test_stream_ticket_scopes():
    plain, with_logs = api_mod.issue_stream_ticket("api:read"), api_mod.issue_stream_ticket("api:read", logs=True)
    assert api_mod._stream_access(None, plain) == (True, False)
    assert api_mod._stream_access(None, with_logs) == (True, True)
    assert api_mod._stream_access(None, plain[:-4] + "AAAA") == (False, False)       # tampered
    assert api_mod._stream_access(None, None) == (False, False)
    assert api_mod._validate_stream_ticket(plain) and api_mod._authorize_stream_request(None, plain)


def test_raw_log_download_needs_the_logs_scope():
    router = routes_mod.build_stream_router(None)
    raw = next(r.endpoint for r in router.routes if r.path.endswith("/jobs/{job_id}/logs/raw"))
    with pytest.raises(HTTPException) as exc:
        asyncio.run(raw(job_id=1, token=None, ticket=api_mod.issue_stream_ticket("api:read")))
    assert exc.value.status_code == 403 and api_mod.PERM_LOGS in exc.value.detail
    with pytest.raises(HTTPException) as exc:
        asyncio.run(raw(job_id=1, token=None, ticket="garbage"))
    assert exc.value.status_code == 401


def test_stream_and_runner_view_carry_log_lines_only_with_the_scope(monkeypatch):
    tasks = {"aac-x": {"status": "running_ansible", "logs": ["TASK [x]", "changed: [h]"], "job_id": 1}}
    monkeypatch.setattr(api_mod, "_ctx", MagicMock())
    monkeypatch.setattr(api_mod, "_engine", SimpleNamespace(db=SimpleNamespace(get_recent_jobs=lambda n: []),
                                                            state={"active_tasks": tasks}))
    monkeypatch.setattr(api_mod, "_service", SimpleNamespace(queue=None, runner_state=lambda: "online"))

    class _Req:
        async def is_disconnected(self):
            return False

    async def first_frame(ticket):
        resp = await api_mod.stream_jobs(_Req(), None, ticket)
        frame = await resp.body_iterator.__anext__()
        await resp.body_iterator.aclose()
        return json.loads(frame[len("data: "):])

    without = asyncio.run(first_frame(api_mod.issue_stream_ticket("api:read")))
    assert without["active_tasks"]["aac-x"] == {"status": "running_ansible", "logs": [], "job_id": 1}
    assert without["logs_included"] is False
    with_logs = asyncio.run(first_frame(api_mod.issue_stream_ticket("api:read", logs=True)))
    assert with_logs["active_tasks"]["aac-x"]["logs"] == ["TASK [x]", "changed: [h]"]

    view = asyncio.run(api_mod.do_job_runners(1, include_logs=False))
    assert view["runners"]["aac-x"]["logs"] == [] and view["logs_included"] is False
    assert asyncio.run(api_mod.do_job_runners(1))["runners"]["aac-x"]["logs"] == ["TASK [x]", "changed: [h]"]
    assert tasks["aac-x"]["logs"] == ["TASK [x]", "changed: [h]"]      # the live state was not modified
