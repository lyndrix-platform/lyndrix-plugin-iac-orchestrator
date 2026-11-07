"""inventory_state publisher (1.5.3) against real git repos (bare remote + clones).

``state_publisher`` is stdlib-only, so it is loaded straight from its file —
no lyndrix-core on the path needed.
"""
import asyncio
import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

PLUGIN_ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location(
    "iac_state_publisher", PLUGIN_ROOT / "app" / "controller" / "state_publisher.py")
sp = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = sp   # dataclasses resolve string annotations via sys.modules
_spec.loader.exec_module(sp)

CTRL_SHA = "0123456789abcdef0123456789abcdef01234567"


def git(cwd, *args):
    return subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t", *args],
                          cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


@pytest.fixture
def repos(tmp_path):
    remote = tmp_path / "remote.git"
    subprocess.run(["git", "init", "-q", "--bare", "-b", "main", str(remote)], check=True)
    seed = tmp_path / "seed"
    subprocess.run(["git", "clone", "-q", str(remote), str(seed)], check=True, capture_output=True)
    (seed / "onprem" / "prod").mkdir(parents=True)
    (seed / "onprem" / "prod" / "inventory.yml").write_text("all: {}\n")
    (seed / ".terraform_runs" / "onprem_prod__plan").mkdir(parents=True)
    (seed / ".terraform_runs" / "onprem_prod__plan" / "secrets.auto.tfvars").write_text("pw = \"x\"\n")
    git(seed, "checkout", "-q", "-b", "main")
    git(seed, "add", "-A")
    git(seed, "commit", "-q", "-m", "seed (old release committed run artifacts)")
    git(seed, "push", "-q", "origin", "main")
    work = tmp_path / "work"
    subprocess.run(["git", "clone", "-q", str(remote), str(work)], check=True, capture_output=True)
    return remote, seed, work


def publish(work, **kw):
    args = dict(controller_sha=CTRL_SHA, job_id=1340, pipeline="infra_plan",
                trigger="Webhook", token=None)
    args.update(kw)
    return asyncio.run(sp.publish_inventory_state(work, **args))


def remote_files(remote):
    return set(git(remote, "ls-tree", "-r", "--name-only", "main").splitlines())


def test_untracks_run_artifacts_and_commits_generated_state(repos):
    remote, _seed, work = repos
    (work / "onprem" / "prod" / "inventory.yml").write_text("all: {hosts: {a: {}}}\n")
    (work / ".terraform_runs" / "onprem_prod__plan" / "secrets.auto.tfvars").write_text("pw = \"new\"\n")

    res = publish(work)

    assert res.status == sp.PUSHED and res.ok
    assert res.untracked_artifacts is True
    assert res.files == ["onprem/prod/inventory.yml"]
    files = remote_files(remote)
    assert "onprem/prod/inventory.yml" in files
    assert not any(f.startswith(".terraform_runs/") for f in files)
    # working copy keeps the run dir; git ignores it from now on
    assert (work / ".terraform_runs" / "onprem_prod__plan" / "secrets.auto.tfvars").exists()
    log = git(remote, "log", "--format=%B", "-1", "main")
    assert "iac-controller@0123456789ab" in log and "job: #1340" in log
    assert log.startswith("ci: automated state update")
    assert git(remote, "rev-parse", "main") == res.commit_sha


def test_no_diff_means_no_commit(repos):
    remote, _seed, work = repos
    publish(work)                      # untrack commit
    before = git(remote, "rev-parse", "main")
    (work / ".terraform_runs" / "x__apply").mkdir(parents=True)
    (work / ".terraform_runs" / "x__apply" / "main.tf").write_text("# run artifact\n")

    res = publish(work)

    assert res.status == sp.NO_CHANGES and res.files == []
    assert git(remote, "rev-parse", "main") == before


def test_generated_deletions_are_committed(repos):
    remote, _seed, work = repos
    (work / "onprem" / "prod" / "inventory.yml").unlink()
    res = publish(work)
    assert res.status == sp.PUSHED
    assert "onprem/prod/inventory.yml" not in remote_files(remote)


def test_remote_moved_rebases_and_pushes(repos):
    remote, seed, work = repos
    (seed / "hetzner").mkdir()
    (seed / "hetzner" / "inventory.yml").write_text("all: {}\n")
    git(seed, "add", "hetzner")
    git(seed, "commit", "-q", "-m", "other writer")
    git(seed, "push", "-q", "origin", "main")
    (work / "onprem" / "prod" / "inventory.yml").write_text("changed: true\n")

    res = publish(work)

    assert res.status == sp.PUSHED
    assert {"hetzner/inventory.yml", "onprem/prod/inventory.yml"} <= remote_files(remote)
    assert git(remote, "rev-parse", "main") == res.commit_sha


def test_conflicting_writer_reports_push_failed(repos):
    remote, seed, work = repos
    (seed / "onprem" / "prod" / "inventory.yml").write_text("theirs: 1\n")
    git(seed, "commit", "-q", "-am", "other writer, same file")
    git(seed, "push", "-q", "origin", "main")
    theirs = git(remote, "rev-parse", "main")
    (work / "onprem" / "prod" / "inventory.yml").write_text("ours: 1\n")

    res = publish(work)

    assert res.status == sp.PUSH_FAILED and not res.ok
    assert git(remote, "rev-parse", "main") == theirs
    # rebase was aborted: working tree is not left mid-rebase
    assert not (work / ".git" / "rebase-merge").exists()
    assert not (work / ".git" / "rebase-apply").exists()


def test_no_remote_commits_locally(tmp_path):
    work = tmp_path / "w"
    subprocess.run(["git", "init", "-q", "-b", "main", str(work)], check=True)
    (work / "a.yml").write_text("a: 1\n")
    res = publish(work)
    assert res.status == sp.COMMITTED_LOCALLY and res.ok


def test_credential_env_never_puts_token_in_url():
    env = sp.credential_env("glpat-secret")
    assert env["GIT_CONFIG_KEY_0"] == "http.extraHeader"
    assert "glpat-secret" not in env["GIT_CONFIG_VALUE_0"]   # base64 only
