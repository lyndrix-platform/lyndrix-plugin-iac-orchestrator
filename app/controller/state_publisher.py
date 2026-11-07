"""Publish the generated ``inventory_state`` back to its git remote.

``inventory_state`` is a *generated artifact*: a pure function of the
iac-controller SSoT commit (plus the bundled generator). Every pipeline that
regenerates it and evaluates the rule engine must commit the result, otherwise
the remote goes stale and the rule engine diffs an old commit.

Why this does not go through the core ``git:commit_push`` bus handler:

* the core reports ``pushed`` even when the remote rejected the push
  (GitPython does not raise on a non-fast-forward reject);
* the commit message cannot carry the iac-controller SHA it was generated from.

What is versioned: the whole generated tree *including* the terraform run
directories (``.terraform_runs/<env>__<mode>`` with rendered
``secrets.auto.tfvars``). inventory_state lives on the on-prem GitLab only
(not reachable from outside) and is meant to keep them; SOPS encryption of
those files follows later. Only the terraform provider cache (``.terraform/``:
large binaries and symlinks into the shared provider mirror) is kept out via
``.git/info/exclude``. Never mirror this repo to GitHub or any public remote.

The credential handling mirrors the core (token as a per-process HTTP header,
never in ``.git/config`` or argv). SOPS-encrypted files are ordinary generated
files here — nothing in this module needs to read them.
"""
from __future__ import annotations

import asyncio
import base64
import logging
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

log = logging.getLogger("IaC:Engine")

#: Local ignore rules (``.git/info/exclude``): terraform provider cache only.
LOCAL_EXCLUDES = (".terraform/",)
#: Rules 1.5.3 wrote into ``.git/info/exclude`` that must go again: run
#: directories (incl. rendered secrets) are versioned in the on-prem GitLab.
OBSOLETE_EXCLUDES = (".terraform_runs/",)
_EXCLUDE_HEADER = "# lyndrix iac-orchestrator: terraform provider cache is never state"
_OBSOLETE_HEADERS = ("# lyndrix iac-orchestrator: run artifacts are never state",)

AUTHOR_NAME = "Lyndrix IaC"
AUTHOR_EMAIL = "iac@lyndrix.local"

# Final statuses (kept compatible with the old CommitPushStage contract).
PUSHED = "pushed"
NO_CHANGES = "no_changes"
COMMITTED_LOCALLY = "committed_locally"   # no remote configured
PUSH_FAILED = "push_failed"
COMMIT_FAILED = "commit_failed"
OK_STATUSES = frozenset({PUSHED, NO_CHANGES, COMMITTED_LOCALLY})


@dataclass
class PublishResult:
    status: str
    commit_sha: str = ""
    controller_sha: str = ""
    detail: str = ""
    files: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.status in OK_STATUSES


def credential_env(token: str | None) -> dict:
    """Process env that supplies ``token`` as an HTTP Basic header (core-compatible)."""
    env = os.environ.copy()
    env["GIT_TERMINAL_PROMPT"] = "0"
    if token:
        auth = base64.b64encode(f"oauth2:{token}".encode()).decode()
        env["GIT_CONFIG_COUNT"] = "1"
        env["GIT_CONFIG_KEY_0"] = "http.extraHeader"
        env["GIT_CONFIG_VALUE_0"] = f"Authorization: Basic {auth}"
    return env


def _ssh_env(private_key: str, key_path: str) -> dict:
    """Process env for an SSH remote (mirrors the core: pinned known_hosts, batch)."""
    env = os.environ.copy()
    known_hosts = os.environ.get(
        "GIT_KNOWN_HOSTS_FILE",
        os.path.join(os.environ.get("GIT_STORAGE_DIR", "/data/storage/git_repos"), ".ssh", "known_hosts"))
    os.makedirs(os.path.dirname(known_hosts), exist_ok=True)
    fd = os.open(key_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as fh:
        fh.write(private_key.replace("\\n", "\n").strip() + "\n")
    env["GIT_SSH_COMMAND"] = (f"ssh -i {key_path} -o StrictHostKeyChecking=accept-new "
                              f"-o UserKnownHostsFile={known_hosts} -o BatchMode=yes")
    return env


async def _git(repo: Path, *args: str, env: dict | None = None) -> tuple[int, str]:
    proc = await asyncio.create_subprocess_exec(
        "git", "-c", "safe.directory=*", *args,
        cwd=str(repo), env=env,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
    )
    out, _ = await proc.communicate()
    return proc.returncode, out.decode(errors="replace").strip()


async def head_sha(repo: Path) -> str:
    rc, out = await _git(repo, "rev-parse", "HEAD")
    return out if rc == 0 else ""


def build_message(controller_sha: str, job_id: int | None, pipeline: str, trigger: str) -> str:
    short = controller_sha[:12] if controller_sha else "unknown"
    subject = f"ci: automated state update — iac-controller@{short}"
    body = [
        f"iac-controller: {controller_sha or 'unknown'}",
        f"job: #{job_id}" if job_id else "job: -",
        f"pipeline: {pipeline or '-'}",
        f"trigger: {trigger or '-'}",
    ]
    # Subject keeps the historic "ci: automated state update" prefix: webhook
    # loop guards match on it.
    return subject + "\n\n" + "\n".join(body) + "\n"


def _ensure_local_excludes(repo: Path) -> str:
    """Keep the terraform provider cache out of ``git add`` without touching the
    repo's tracked .gitignore (local, idempotent) and drop the 1.5.3 rule that
    excluded ``.terraform_runs/``. Returns an error text, '' on success."""
    info = repo / ".git" / "info"
    try:
        info.mkdir(parents=True, exist_ok=True)
        exclude = info / "exclude"
        lines = exclude.read_text().splitlines() if exclude.exists() else []
        kept = [ln for ln in lines if ln not in OBSOLETE_EXCLUDES and ln not in _OBSOLETE_HEADERS]
        missing = [w for w in LOCAL_EXCLUDES if w not in kept]
        if missing:
            kept += [_EXCLUDE_HEADER, *missing]
        if kept != lines:
            exclude.write_text("\n".join(kept) + "\n")
    except OSError as exc:
        return f"could not write .git/info/exclude: {exc}"
    return ""


async def _current_branch(repo: Path) -> str:
    rc, out = await _git(repo, "symbolic-ref", "--short", "-q", "HEAD")
    return out if rc == 0 and out else "main"


async def _push(repo: Path, branch: str, env: dict, retries: int = 2) -> tuple[bool, str]:
    """Push HEAD; on a non-fast-forward reject rebase onto the remote and retry.

    A rebase conflict means another writer published a generation from a
    different controller commit — the rebase is aborted and the push reported
    as failed (the next pipeline regenerates from the newest controller HEAD).
    """
    last = ""
    for attempt in range(retries + 1):
        rc, out = await _git(repo, "push", "--porcelain", "origin", f"HEAD:refs/heads/{branch}", env=env)
        if rc == 0:
            return True, out
        last = out
        if attempt == retries:
            break
        rejected = "rejected" in out or "non-fast-forward" in out or "fetch first" in out
        if not rejected:
            break
        log.warning("inventory_state: push rejected (remote moved) — rebasing and retrying (%d/%d).",
                    attempt + 1, retries)
        rc, out = await _git(repo, "fetch", "origin", branch, env=env)
        if rc != 0:
            last = out
            break
        rc, out = await _git(repo, "-c", f"user.name={AUTHOR_NAME}", "-c", f"user.email={AUTHOR_EMAIL}",
                             "rebase", f"origin/{branch}")
        if rc != 0:
            await _git(repo, "rebase", "--abort")
            last = f"rebase onto origin/{branch} failed: {out}"
            break
    return False, last


async def publish_inventory_state(
    repo: Path,
    *,
    controller_sha: str,
    job_id: int | None,
    pipeline: str,
    trigger: str,
    token: str | None,
    push: bool = True,
) -> PublishResult:
    """Stage the generated tree (incl. ``.terraform_runs/``), commit only on a diff, push.

    The caller holds the engine's inventory_state lock (single writer per
    instance); cross-instance races are resolved by the rebase-and-retry push.
    """
    res = PublishResult(status=COMMIT_FAILED, controller_sha=controller_sha)
    if not (repo / ".git").exists():
        res.detail = f"{repo} is not a git checkout"
        return res

    lock = repo / ".git" / "index.lock"
    if lock.exists():
        log.warning("inventory_state: removing stale git index lock.")
        lock.unlink(missing_ok=True)

    err = _ensure_local_excludes(repo)
    if err:
        res.detail = err
        return res
    git_id = ("-c", f"user.name={AUTHOR_NAME}", "-c", f"user.email={AUTHOR_EMAIL}")

    # Stage the whole generated tree, run directories included (only the
    # provider cache is excluded via info/exclude).
    rc, out = await _git(repo, "add", "-A", "--", ".")
    if rc != 0:
        res.detail = f"git add failed: {out}"
        return res
    rc, out = await _git(repo, "diff", "--cached", "--name-only")
    res.files = [f for f in out.splitlines() if f] if rc == 0 else []

    if not res.files:
        res.status = NO_CHANGES
        log.info("inventory_state: generated state matches HEAD — nothing to commit.")
    else:
        msg = build_message(controller_sha, job_id, pipeline, trigger)
        rc, out = await _git(repo, *git_id, "commit", "-q", "-m", msg)
        if rc != 0:
            res.detail = f"git commit failed: {out}"
            return res
        log.info("inventory_state: committed %d generated file(s) from iac-controller@%s.",
                 len(res.files), (controller_sha or "unknown")[:12])

    res.commit_sha = await head_sha(repo)
    if not res.files:
        return res

    rc, remotes = await _git(repo, "remote")
    if not push or rc != 0 or "origin" not in remotes.split():
        res.status = COMMITTED_LOCALLY
        return res

    branch = await _current_branch(repo)
    _rc, url = await _git(repo, "remote", "get-url", "origin")
    ssh_remote = url.startswith(("git@", "ssh://"))
    key_path = ""
    try:
        if ssh_remote and token:
            fd, key_path = tempfile.mkstemp(prefix="lyndrix_state_push_", suffix=".pem")
            os.close(fd)
            env = _ssh_env(token, key_path)
        else:
            env = credential_env(token)
        ok, out = await _push(repo, branch, env)
    finally:
        if key_path:
            try:
                os.unlink(key_path)
            except OSError:
                log.warning("inventory_state: could not delete temporary SSH key file.")
    res.commit_sha = await head_sha(repo)   # a rebase rewrote it
    if ok:
        res.status = PUSHED
        log.info("inventory_state: pushed %s to origin/%s.", res.commit_sha[:12], branch)
    else:
        res.status = PUSH_FAILED
        res.detail = out[-500:]
    return res
