"""OS-06 (C): processes that execute repository content get an explicit,
minimal environment — not the core's with its secrets."""
import asyncio
import inspect

import pytest

from iac_plugin.app.controller import engine as engine_mod
from iac_plugin.app.controller import subprocess_env
from iac_plugin.app.controller.engine import DeploymentEngine

SECRETS = {
    "DB_PASSWORD": "dummy-db-password-not-real", "STORAGE_SECRET": "dummy-storage-secret-not-real",
    "LYNDRIX_MASTER_KEY": "dummy-master-key-not-real", "LYNDRIX_SYSTEM_API_KEY": "dummy-api-key-not-real",
    "LYNDRIX_ADMIN_PASSWORD": "dummy-admin-password-not-real", "VAULT_TOKEN": "dummy-vault-token-not-real",
    "VAULT_ADDR": "http://vault.invalid:8200", "DB_HOST": "db.invalid", "LYNDRIX_INSTANCE_ID": "runner-x",
    "PLUGIN_IAC_ORCHESTRATOR_ANSIBLE_IMAGE": "registry.invalid/x:1", "HTTPS_PROXY": "http://user:pw@proxy.invalid",
}


@pytest.fixture
def core_environment(monkeypatch):
    for name, value in SECRETS.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("LC_ALL", "C.UTF-8")
    monkeypatch.setenv("PLUGIN_IAC_ORCHESTRATOR_TF_MAX_DESTROY_RATIO", "0.25")


def test_minimal_env_carries_what_a_process_needs_and_nothing_of_the_core(core_environment):
    for env in (subprocess_env.minimal_env(), subprocess_env.generator_env(), subprocess_env.render_env()):
        assert "PATH" in env and env["LC_ALL"] == "C.UTF-8"
        assert not set(SECRETS) & set(env)
        assert not any(k.startswith(("LYNDRIX_", "VAULT_", "DB_")) for k in env)
    # the generator keeps its two own settings (destroy guard of the tfvars)
    assert subprocess_env.generator_env()["PLUGIN_IAC_ORCHESTRATOR_TF_MAX_DESTROY_RATIO"] == "0.25"
    assert "PLUGIN_IAC_ORCHESTRATOR_TF_MAX_DESTROY_RATIO" not in subprocess_env.render_env()
    # git reaches its remote (proxy, CA) — repository code never sees the proxy URL
    git = subprocess_env.git_env({"GIT_SSH_COMMAND": "ssh -i /k"})
    assert git["HTTPS_PROXY"] == SECRETS["HTTPS_PROXY"] and git["GIT_TERMINAL_PROMPT"] == "0"
    assert git["GIT_SSH_COMMAND"] == "ssh -i /k" and "DB_PASSWORD" not in git and "VAULT_TOKEN" not in git


def test_the_generator_is_started_without_the_cores_secrets(core_environment, monkeypatch, tmp_path):
    started = {}

    class _Proc:
        returncode = 0

        async def communicate(self):
            return b"", b""

    async def fake_exec(*cmd, **kwargs):
        started.update(cmd=cmd, env=kwargs.get("env"))
        return _Proc()

    monkeypatch.setattr(engine_mod.asyncio, "create_subprocess_exec", fake_exec)
    eng = DeploymentEngine.__new__(DeploymentEngine)
    eng.base_git_dir = tmp_path
    asyncio.run(eng._execute_native_generation())
    env = started["env"]
    assert env is not None and not set(SECRETS) & set(env)
    assert "iac_core/app" in env["PYTHONPATH"] and "PATH" in env
    assert env["PLUGIN_IAC_ORCHESTRATOR_TF_MAX_DESTROY_RATIO"] == "0.25"


def test_the_render_script_of_the_infra_engine_never_inherits_the_environment():
    """Both call sites (per host, per environment) pass the minimal environment."""
    source = inspect.getsource(DeploymentEngine)
    assert source.count("env=render_env()") == 2
    assert "os.environ.copy()" not in inspect.getsource(engine_mod)
