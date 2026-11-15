"""OS-06 (A): what a runner container may write.

Read-only mode (setting ``runner_readonly_mounts``, default on): Ansible gets
``git_repos`` and ``services`` read-only with every ``.git`` hidden, plus a
scratch dir per job; tofu gets only its run dir read-write and the provider
cache read-only. Off: the read-write mounts of <= 1.6.1, unchanged.
"""
import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from iac_plugin.app.controller import engine as engine_mod
from iac_plugin.app.controller.config import IaCConfig
from iac_plugin.app.controller.engine import DeploymentEngine
from iac_plugin.app.controller.settings_schema import OrchestratorSettings

sys.path.insert(0, str(Path(__file__).parent))
from test_job_mode import _run_tf, _tf_engine  # noqa: E402


def _trees(tmp_path):
    git_repos, services = tmp_path / "git_repos", tmp_path / "services"
    for repo in ("iac_controller", "inventory_state", "config_engine"):
        (git_repos / repo / ".git").mkdir(parents=True)
    (git_repos / ".job_snapshots").mkdir()
    for svc in ("aac-traefik", "aac-portainer"):
        (services / svc / ".git").mkdir(parents=True)
    (services / ".iac_keys").mkdir()
    (services / "aac-submodule").mkdir()
    (services / "aac-submodule" / ".git").write_text("gitdir: ../x")     # a gitlink file is not a directory
    eng = DeploymentEngine.__new__(DeploymentEngine)
    eng.base_git_dir = git_repos
    eng.config = SimpleNamespace(services_dir=services, host_terraform_providers_dir="/host/providers")
    return eng, git_repos


def _pairs(args, flag):
    return [args[i + 1] for i, a in enumerate(args) if a == flag]


# --- Ansible ---------------------------------------------------------------------------------------

def test_ansible_reads_the_trees_and_writes_only_its_scratch_dir(tmp_path):
    eng, git_repos = _trees(tmp_path)
    args = eng._ansible_mounts(42, "/host/git", "/host/svc", True)
    assert _pairs(args, "-v") == [
        "/host/git:/data/storage/git_repos:ro",
        "/host/svc:/data/storage/services:ro",
        "/host/git/.job_snapshots/job_42/scratch:/data/scratch:rw",
    ]
    assert (git_repos / ".job_snapshots/job_42/scratch").is_dir()             # exists before docker binds it
    assert _pairs(args, "-e") == ["IAC_SCRATCH_DIR=/data/scratch"]
    # every .git directory below the two trees is hidden behind an empty read-only tmpfs
    assert _pairs(args, "--tmpfs") == [
        f"/data/storage/git_repos/{repo}/.git:ro,size=64k,mode=0555"
        for repo in ("config_engine", "iac_controller", "inventory_state")
    ] + [f"/data/storage/services/{svc}/.git:ro,size=64k,mode=0555" for svc in ("aac-portainer", "aac-traefik")]
    # without a job there is no scratch dir (and nothing else becomes writable)
    assert not any(":rw" in v for v in _pairs(eng._ansible_mounts(0, "/host/git", "/host/svc", True), "-v"))


def test_ansible_mounts_with_the_setting_off_are_the_old_ones(tmp_path):
    eng, _ = _trees(tmp_path)
    assert eng._ansible_mounts(42, "/host/git", "/host/svc", False) == [
        "-v", "/host/git:/data/storage/git_repos", "-v", "/host/svc:/data/storage/services"]


@pytest.mark.parametrize("readonly", [True, False])
def test_the_ansible_container_is_started_with_those_mounts(monkeypatch, tmp_path, readonly):
    captured = {}

    class _Proc:
        returncode = 0

        async def communicate(self, input=None):
            return b"", b""

    async def fake_exec(*cmd, **kwargs):
        if len(cmd) > 2 and cmd[1] == "run":
            captured["cmd"] = list(cmd)
        return _Proc()

    monkeypatch.setattr(engine_mod.shutil, "which", lambda _: "/usr/bin/docker")
    monkeypatch.setattr(engine_mod.asyncio, "create_subprocess_exec", fake_exec)
    eng, _ = _trees(tmp_path)
    eng.state = {}
    eng.ctx = MagicMock()
    eng.ctx.get_secret = lambda key: "KEY" if key == "ansible_ssh_key" else None
    eng.config = SimpleNamespace(auto_apply=True, services_dir=tmp_path / "services", host_git_repos_dir="/host/git",
                                 host_services_dir="/host/svc", ansible_docker_image="img",
                                 runner_readonly_mounts=readonly)
    eng._sync_runners = lambda *a, **k: None
    eng._job_modes = {7: False}

    async def fake_watch(*a, **k):
        return True, {"successful_hosts": 1, "failed_hosts": 0}

    eng._watch_detached_runner = fake_watch
    ok, _ = asyncio.run(eng.execute_ansible_docker(playbook_subpath="pb.yml", inventory_subpath="inv.yml",
                                                   limit="h1", task_name="t", job_id=7))
    binds = _pairs(captured["cmd"], "-v")
    assert ok
    if readonly:
        assert "/host/git:/data/storage/git_repos:ro" in binds and "/host/svc:/data/storage/services:ro" in binds
        assert [b for b in binds if b.endswith(":rw")] == ["/host/git/.job_snapshots/job_7/scratch:/data/scratch:rw"]
        assert len(_pairs(captured["cmd"], "--tmpfs")) == 5
    else:
        assert binds == ["/host/git:/data/storage/git_repos", "/host/svc:/data/storage/services"]
        assert "--tmpfs" not in captured["cmd"]
    # the SSH key is still read from the (read-only) services tree and copied into the container
    assert any(arg.startswith("/data/storage/services/.iac_keys/") for arg in captured["cmd"])


# --- Terraform ----------------------------------------------------------------------------------------

RUN = "inventory_state/.terraform_runs/onprem_dev__docker-x"


def test_tofu_writes_only_its_run_dir_and_reads_the_provider_cache(tmp_path):
    eng, _ = _trees(tmp_path)
    assert _pairs(eng._terraform_mounts("/host/git", RUN + "__j11", True), "-v") == [
        f"/host/git/{RUN}__j11:/data/storage/git_repos/{RUN}__j11:rw",
        "/host/providers:/data/storage/terraform-providers:ro",
    ]
    assert _pairs(eng._terraform_mounts("/host/git", RUN, False), "-v") == [
        "/host/git:/data/storage/git_repos:rw", "/host/providers:/data/storage/terraform-providers:rw"]


@pytest.mark.parametrize("readonly", [True, False])
def test_the_tofu_container_and_its_init(monkeypatch, readonly):
    eng, shells = _tf_engine(monkeypatch, auto_apply=True)
    eng.config.runner_readonly_mounts = readonly
    eng.config.host_terraform_providers_dir = "/hp"
    eng._set_job_mode(11, False)
    commands = []
    real_spawn = eng._spawn_terraform_container

    async def spawn(name, cmd):
        commands.append(cmd)
        return await real_spawn(name, cmd)

    eng._spawn_terraform_container = spawn
    _run_tf(eng)
    binds, shell = _pairs(commands[0], "-v"), shells[-1]
    if readonly:
        assert binds == [f"/h/{RUN}:/data/storage/git_repos/{RUN}:rw", "/hp:/data/storage/terraform-providers:ro"]
        # a provider the read-only cache lacks is installed into the run dir instead of failing the run
        assert "tofu init -input=false -no-color || {" in shell and "unset TF_PLUGIN_CACHE_DIR; tofu init" in shell
    else:
        assert binds == ["/h:/data/storage/git_repos:rw", "/hp:/data/storage/terraform-providers:rw"]
        assert "tofu init -input=false -no-color; STEP=import" in shell and "unset TF_PLUGIN_CACHE_DIR" not in shell
    assert f"cd '/data/storage/git_repos/{RUN}'" in shell


# --- the setting ---------------------------------------------------------------------------------------------

def test_read_only_mounts_are_on_by_default_and_an_operator_can_switch_them_off(monkeypatch):
    vault = {}
    ctx = SimpleNamespace(get_secret=vault.get, set_secret=lambda k, v: vault.__setitem__(k, v) or True, log=MagicMock())
    config = IaCConfig(ctx)
    assert config.runner_readonly_mounts is True
    mgr = OrchestratorSettings(ctx, SimpleNamespace(state={}))
    assert mgr.save_values({"runner_readonly_mounts": False})["saved"] == ["runner_readonly_mounts"]
    assert vault["iac_runner_readonly_mounts"] == "false" and config.runner_readonly_mounts is False
    monkeypatch.setenv("PLUGIN_IAC_ORCHESTRATOR_RUNNER_READONLY_MOUNTS", "true")
    assert config.runner_readonly_mounts is True                              # env > Vault
