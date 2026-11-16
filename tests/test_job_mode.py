"""OS-08 + OC-07: dry-run is a property of the JOB.

* Terraform honours it regardless of ``auto_apply`` / ``approve``.
* The executors re-read it from the persisted payload — a stage that does not
  pass a flag cannot run a dry-run job for real; an unreadable row fails closed.
* Nothing is resumed after a runner restart: an interrupted rollout is closed as
  FAILED "interrupted — re-trigger" and no pending service is deployed.

Every test asserts what is NOT executed.
"""
import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from iac_plugin.app.controller import engine as engine_mod
from iac_plugin.app.controller import queue as queue_mod
from iac_plugin.app.controller.engine import DeploymentEngine
from iac_plugin.app.model.models import IaCJob


class _Proc:
    returncode = 0

    async def communicate(self, input=None):
        return b"", b""

    async def wait(self):
        return 0


def _tf_engine(monkeypatch, *, auto_apply, db=None):
    """Engine wired for execute_terraform_docker; returns (engine, captured shell commands)."""
    shells: list[str] = []

    async def fake_exec(*cmd, **kwargs):
        if cmd[:2] == ("docker", "run"):
            shells.append(cmd[-1])
        return _Proc()

    class _Socket:
        async def spawn_runner(self, **req):
            shells.append(req["command"][-1])
            return {"status": "running"}

    monkeypatch.setattr(engine_mod.asyncio, "create_subprocess_exec", fake_exec)
    monkeypatch.setattr(engine_mod.shutil, "which", lambda _: "/usr/bin/docker")
    eng = DeploymentEngine.__new__(DeploymentEngine)
    eng.state = {}
    eng.ctx = MagicMock()
    eng.socket_client = _Socket()
    eng._terraform_run_semaphore = asyncio.Semaphore(1)
    eng._sync_runners = lambda *a, **k: None
    eng.config = SimpleNamespace(auto_apply=auto_apply, terraform_binary="tofu", terraform_docker_image="img",
                                 host_git_repos_dir="/h", host_terraform_providers_dir=None)
    if db is not None:
        eng.db = db

    async def fake_watch(*a, **k):
        return True, {}

    eng._watch_detached_runner = fake_watch
    return eng, shells


def _run_tf(eng, **kwargs):
    kwargs.setdefault("mode", "apply")
    return asyncio.run(eng.execute_terraform_docker(
        run_rel_dir="inventory_state/.terraform_runs/onprem_dev__docker-x", task_name="Terraform Provision: docker-x",
        job_id=kwargs.pop("job_id", 11), host_name="docker-x", **kwargs))


IMPORT = [{"addr": 'module.lxc_hydra.proxmox_virtual_environment_container.ct["docker-x"]',
           "ref": "hydra/101", "host": "docker-x"}]


def _applies(shell: str) -> bool:
    return "apply -input=false -no-color -auto-approve" in shell


def _mutates_state(shell: str) -> bool:
    return " import " in shell or "state rm" in shell


# --- Terraform -----------------------------------------------------------------------

def test_baseline_auto_apply_applies_a_real_job(monkeypatch):
    """Guards the assertions below: without dry-run this command line DOES apply."""
    eng, shells = _tf_engine(monkeypatch, auto_apply=True)
    eng._set_job_mode(11, False)
    _run_tf(eng, import_specs=IMPORT)
    assert _applies(shells[-1]) and _mutates_state(shells[-1])


@pytest.mark.parametrize("auto_apply,force_apply", [(True, False), (False, True), (True, True)])
def test_dry_run_job_never_applies_or_imports(monkeypatch, auto_apply, force_apply):
    eng, shells = _tf_engine(monkeypatch, auto_apply=auto_apply)
    eng._set_job_mode(11, True)
    ok, _ = _run_tf(eng, force_apply=force_apply, import_specs=IMPORT)
    assert ok and len(shells) == 1
    assert not _applies(shells[0]) and not _mutates_state(shells[0])
    assert "tofu plan -input=false -no-color -out=tfplan" in shells[0] and "tofu show -no-color tfplan" in shells[0]


def test_dry_run_adopt_plans_without_touching_state(monkeypatch):
    eng, shells = _tf_engine(monkeypatch, auto_apply=True)
    eng._set_job_mode(11, True)
    _run_tf(eng, mode="import", import_specs=IMPORT)
    assert not _mutates_state(shells[0]) and not _applies(shells[0]) and "tofu plan" in shells[0]


def test_mode_is_reread_from_the_persisted_payload(monkeypatch):
    """A path that never registered the job (re-attach, rule action, future code)
    still gets the job's mode from its row."""
    seen = []
    db = SimpleNamespace(get_job_payload=lambda job_id: seen.append(job_id) or {"dry_run": "true", "limit": "docker-x"})
    eng, shells = _tf_engine(monkeypatch, auto_apply=True, db=db)
    _run_tf(eng, job_id=42, force_apply=True)
    assert seen == [42] and not _applies(shells[0])


def test_unreadable_job_row_fails_closed(monkeypatch):
    def boom(job_id):
        raise RuntimeError("job store unavailable")

    eng, shells = _tf_engine(monkeypatch, auto_apply=True, db=SimpleNamespace(get_job_payload=boom))
    with pytest.raises(RuntimeError, match="refusing to execute"):
        _run_tf(eng, job_id=42, force_apply=True)
    assert shells == []                                   # nothing was started


def test_dry_run_infra_apply_keeps_the_pending_plan(monkeypatch):
    eng, _ = _tf_engine(monkeypatch, auto_apply=True)
    eng._set_job_mode(11, True)
    eng.db = MagicMock()
    eng._iter_terraform_environments = lambda: [{"env_name": "onprem_dev", "host_count": 1}]

    async def fake_env(env_ctx, mode, job_id, force_apply=False):
        return True, {}

    eng.execute_terraform_environment = fake_env
    ok, _ = asyncio.run(eng.execute_terraform_infra("apply", 11, force_apply=True))
    assert ok
    eng.db.update_state.assert_not_called()               # the reviewed plan was not applied → stays pending


# --- Ansible ---------------------------------------------------------------------------

def _ansible_cmd(monkeypatch, tmp_path, *, job_dry_run, check_mode):
    captured = {}

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
    eng.config = SimpleNamespace(auto_apply=True, services_dir=tmp_path, host_git_repos_dir="/g",
                                 host_services_dir="/s", ansible_docker_image="img")
    eng._sync_runners = lambda *a, **k: None
    eng._job_inventory_snapshot_dir = lambda job_id: tmp_path / "none"
    eng._set_job_mode(7, job_dry_run)

    async def fake_watch(*a, **k):
        return True, {}

    eng._watch_detached_runner = fake_watch
    asyncio.run(eng.execute_ansible_docker(playbook_subpath="pb.yml", inventory_subpath="inv.yml", limit="h1",
                                           task_name="t", job_id=7, check_mode=check_mode))
    return captured["cmd"]


def test_ansible_executor_forces_check_for_a_dry_run_job(monkeypatch, tmp_path):
    """The caller forgot the flag (check_mode=False) — the job's mode still wins."""
    assert "--check" in _ansible_cmd(monkeypatch, tmp_path, job_dry_run=True, check_mode=False)


def test_ansible_executor_unchanged_for_real_jobs(monkeypatch, tmp_path):
    assert "--check" not in _ansible_cmd(monkeypatch, tmp_path, job_dry_run=False, check_mode=False)
    assert "--check" in _ansible_cmd(monkeypatch, tmp_path, job_dry_run=False, check_mode=True)


# --- no resume after a runner restart ------------------------------------------------------

def _row(job_id):
    with queue_mod.db_instance.SessionLocal() as s:
        return s.get(IaCJob, job_id)


def test_interrupted_rollout_is_failed_not_resumed(sqlite_queue, monkeypatch):
    from iac_plugin.app.controller.service import IaCService

    real_sleep = asyncio.sleep

    async def fast_sleep(_seconds, *a, **k):
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", fast_sleep)
    ctx = MagicMock()
    ctx.subscribe = MagicMock(return_value=lambda fn: fn)
    ctx.get_secret = MagicMock(return_value=None)
    ctx.instance_id = "docker-auto-runner"
    ctx.roles = ["api", "runner"]
    ctx.create_task = lambda coro, name=None: asyncio.get_running_loop().create_task(coro)
    svc = IaCService(ctx)
    svc._runtime_paths_resolved = True
    assert not hasattr(svc.engine, "resume_bulk_rollout")

    # The interrupted job: a host-scoped DRY-RUN rollout with two services left.
    job_id = svc.queue.enqueue({"pipeline_type": "rollout", "limit": "docker-dev", "dry_run": True})
    assert svc.queue.claim_next("docker-auto-runner")[0] == job_id
    svc.db.update_pending_tasks(job_id, ["aac-traefik", "aac-cadvisor"])

    executed = []

    async def fake_ansible(**kwargs):
        executed.append(kwargs)
        return True, {}

    async def no_containers(*a, **k):
        return None

    async def not_finalized(job_id, *, reason=""):
        return False

    async def no_job_containers(*a, **k):
        return []

    svc.engine.execute_ansible_docker = fake_ansible
    svc.engine.reconcile_orphaned_runners = no_containers
    svc.engine.finalize_from_containers = not_finalized
    # Never the host's Docker: unmocked, the reconciliation listed (and removed)
    # the runner containers of whatever orchestrator runs on the same machine.
    svc.engine._list_job_containers = no_job_containers

    async def scenario():
        await svc.run_startup_reconciliation()
        await real_sleep(0.05)

    asyncio.run(scenario())
    row = _row(job_id)
    assert executed == []                                  # no playbook, neither --check nor real
    assert row.status == "FAILED" and "Interrupted by a runner restart" in row.current_step
    assert "2 service(s) were not deployed" in row.current_step
    assert any("re-trigger" in line for line in svc.queue.read_log(job_id)["lines"])


def test_reattached_container_cannot_complete_an_interrupted_rollout():
    """A re-attached playbook that exits 0 must not close the job as SUCCESS while
    other services of the same rollout were never started."""
    eng = DeploymentEngine.__new__(DeploymentEngine)
    eng.state = {}
    eng.db = MagicMock()
    eng.db.claims = {}
    eng.db.get_pending_tasks.return_value = ["aac-cadvisor"]
    eng.db.get_exec_state.return_value = {"stage": 4, "stages": 7, "stage_name": "Async Bulk Rollout", "last": False}
    eng._notify = MagicMock()
    eng._log_line = MagicMock()
    eng._log_writers = {}
    eng._cleanup_inventory_snapshot = MagicMock()

    async def watched(*a, **k):
        return True, {}

    eng._watch_detached_runner = watched
    container = {"id": "c1", "name": "aac-runner-aac-traefik", "task_name": "aac-traefik", "kind": "ansible",
                 "state": "running", "job_id": 5}
    assert asyncio.run(eng.recover_job(5, [container])) == "FAILED"
    statuses = [c.kwargs.get("status") or c.args[1] for c in eng.db.update_job.call_args_list]
    assert statuses == ["FAILED"]
    assert "not deployed" in eng.db.update_progress.call_args.args[2]


def test_interrupted_reason_treats_unreadable_rows_as_interrupted():
    eng = DeploymentEngine.__new__(DeploymentEngine)
    eng.db = MagicMock()
    eng.db.get_pending_tasks.return_value = []
    assert asyncio.run(eng.interrupted_reason(1)) is None
    eng.db.get_pending_tasks.side_effect = RuntimeError("db down")
    assert "re-trigger" in asyncio.run(eng.interrupted_reason(1))
