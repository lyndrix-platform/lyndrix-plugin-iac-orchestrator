"""OS-06 (D): how the plugin itself calls git.

* trust is per repository and per command — never ``safe.directory=*`` in the
  global git config;
* git does not run programs a checkout's own ``.git`` names (hooks, fsmonitor,
  credential helper);
* git children get the minimal git environment, not the core's.

Not covered here on purpose: a checkout a runner container could WRITE is not
made safe by options — that is what the read-only mounts are for.
"""
import asyncio
import subprocess
from types import SimpleNamespace

from iac_plugin.app.controller import state_publisher as sp
from iac_plugin.app.controller.subprocess_env import git_safety_args
from iac_plugin.stages import git as git_stage


def git(cwd, *args):
    return subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t", *args], cwd=cwd, check=True,
                          capture_output=True, text=True).stdout.strip()


def test_safety_args_trust_one_repository_and_name_no_programs(tmp_path):
    link = tmp_path / "link"
    (tmp_path / "real").mkdir()
    link.symlink_to(tmp_path / "real")
    args = git_safety_args(link)
    assert args[:4] == ["-c", f"safe.directory={link}", "-c", f"safe.directory={tmp_path / 'real'}"]
    assert "safe.directory=*" not in args
    for option in ("core.hooksPath=/dev/null", "core.fsmonitor=false", "credential.helper=", "protocol.ext.allow=never"):
        assert option in args


def test_a_planted_hook_or_fsmonitor_program_is_not_executed(tmp_path):
    """Real git: what a tampered checkout names in ``.git`` does not run."""
    repo = tmp_path / "inventory_state"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "main")
    (repo / "a.txt").write_text("1")
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "-m", "base")
    marker = tmp_path / "executed"
    for hook in ("pre-commit", "post-commit", "post-checkout", "reference-transaction"):
        path = repo / ".git" / "hooks" / hook
        path.write_text(f"#!/bin/sh\necho {hook} >> {marker}\n")
        path.chmod(0o755)
    git(repo, "config", "core.fsmonitor", f"sh -c 'echo fsmonitor >> {marker}'")

    async def scenario():
        (repo / "a.txt").write_text("2")
        assert (await sp._git(repo, "status", "--porcelain"))[0] == 0
        assert (await sp._git(repo, "add", "-A"))[0] == 0
        rc, out = await sp._git(repo, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "x")
        assert rc == 0, out
        assert (await sp._git(repo, "checkout", "-q", "-f", "main"))[0] == 0

    asyncio.run(scenario())
    assert not marker.exists()
    # the guard of this test: plain git DOES run them
    (repo / "a.txt").write_text("3")
    subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-am", "y"], cwd=repo,
                   capture_output=True)
    assert marker.exists()


def test_the_service_sync_writes_no_global_git_config_and_trusts_only_its_checkout(monkeypatch, tmp_path):
    calls = []

    class _Proc:
        returncode = 0

        async def communicate(self):
            return b"", b""

    async def fake_exec(*cmd, **kwargs):
        calls.append((cmd, kwargs.get("env") or {}))
        return _Proc()

    monkeypatch.setenv("DB_PASSWORD", "dummy-db-password-not-real")
    monkeypatch.setenv("LYNDRIX_MASTER_KEY", "dummy-master-key-not-real")
    monkeypatch.setattr(git_stage.asyncio, "create_subprocess_exec", fake_exec)
    services = tmp_path / "services"
    (services / "aac-x" / ".git").mkdir(parents=True)
    engine = SimpleNamespace(config=SimpleNamespace(services_dir=services, security_dir=tmp_path / "sec"),
                             base_git_dir=tmp_path / "git", ctx=SimpleNamespace(get_secret=lambda k: None))
    res = asyncio.run(git_stage.CloneServiceRepoStage("aac-x", "main", {}).run(engine, {}))
    assert res.success and len(calls) == 4                          # set-url, fetch, checkout, reset
    target = str(services / "aac-x")
    for cmd, env in calls:
        assert cmd[0] == "git" and "--global" not in cmd and "safe.directory=*" not in cmd
        assert f"safe.directory={target}" in cmd and "core.hooksPath=/dev/null" in cmd
        assert "DB_PASSWORD" not in env and "LYNDRIX_MASTER_KEY" not in env and env["GIT_TERMINAL_PROMPT"] == "0"
