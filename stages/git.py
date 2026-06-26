import asyncio
import json
import os
import yaml
from pathlib import Path
from core.logger import get_logger

from .base import BaseStage
from ..utils import StageResult

log = get_logger("IaC:Engine:Git")

class SyncRepoStage(BaseStage):
    def __init__(self, role_slug: str):
        super().__init__(f"Sync Repo: {role_slug}")
        self.role_slug = role_slug

    async def run(self, engine, context: dict) -> StageResult:
        if await engine.execute_git_sync(self.role_slug):
            return StageResult(True, f"Synced {self.role_slug}")
        return StageResult(False, f"Failed to sync {self.role_slug}")

class CommitPushStage(BaseStage):
    def __init__(self, role_slug: str, message: str):
        super().__init__(f"Commit & Push: {role_slug}")
        self.role_slug = role_slug
        self.message = message

    async def run(self, engine, context: dict) -> StageResult:
        status = await engine.execute_git_commit_push(self.role_slug, self.message)
        context[f"{self.role_slug}_commit_status"] = status
        if status in ["pushed", "committed_locally", "no_changes"]:
            return StageResult(True, f"Git status for {self.role_slug}: {status}")
        return StageResult(False, f"Push failed for {self.role_slug} with status: {status}")

class CloneServiceRepoStage(BaseStage):
    def __init__(self, service_name: str, branch: str, payload: dict):
        super().__init__(f"Clone Service Repo: {service_name}")
        self.service_name = service_name
        self.branch = branch
        self.payload = payload

    async def run(self, engine, context: dict) -> StageResult:
        services_dir = engine.config.services_dir
        services_dir.mkdir(parents=True, exist_ok=True)
        target_dir = services_dir / self.service_name
        repo_url = None
        if not repo_url:
            catalog_file = engine.base_git_dir / "iac_controller" / "environments" / "global" / "02_service_catalog.yml"
            if catalog_file.exists():
                try:
                    with open(catalog_file, 'r') as f:
                        catalog_data = yaml.safe_load(f) or {}
                        catalog = catalog_data.get("service_catalog", {})
                        git_mgmt = catalog.get("remote_git_management", {})
                        services_list = catalog.get("services", [])
                        matched_svc = next((s for s in services_list if s.get("name") == self.service_name), None)
                        if matched_svc and git_mgmt.get("active"):
                            base_url = git_mgmt.get("remote_git_repository_toplevel", "").rstrip("/")
                            repo_name = matched_svc.get("repository_name", matched_svc.get("name"))
                            if base_url:
                                repo_url = f"{base_url}/{repo_name}.git"
                except Exception as e:
                    log.warning(f"Failed to parse source service_catalog: {e}")
        if not repo_url:
            repo_url = f"https://gitlab.int.fam-feser.de/aac-application-definitions/{self.service_name}.git"
        raw_svc_config = engine.ctx.get_secret("repo_service_repos_config")
        svc_token = None
        if raw_svc_config:
            try:
                svc_config = json.loads(raw_svc_config)
                token_key = svc_config.get("token_key")
                if token_key:
                    svc_token = engine.ctx.get_secret(token_key)
            except Exception: pass
        if repo_url.startswith("https://") and svc_token:
            repo_url = repo_url.replace("https://", f"https://gitlab-ci-token:{svc_token}@")
        auth_env = os.environ.copy()
        ssh_key = engine.ctx.get_secret("ansible_ssh_key")
        if ssh_key:
            key_path = engine.config.security_dir / "ansible_id_rsa"
            os.makedirs(os.path.dirname(key_path), exist_ok=True)
            with open(key_path, "w") as f:
                f.write(ssh_key.replace('\\n', '\n').strip() + '\n')
            os.chmod(key_path, 0o600)
            auth_env["GIT_SSH_COMMAND"] = f"ssh -i {key_path} -o StrictHostKeyChecking=no"
        async def _run_git(cmd, cwd=None):
            proc = await asyncio.create_subprocess_exec(
                *cmd, cwd=cwd, env=auth_env,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
            )
            out, _ = await proc.communicate()
            return proc.returncode, out.decode(errors="ignore").strip()

        try:
            # Git refuses to operate on a repo whose directory owner differs from the
            # current user — the NFS/bind-mounted service dirs are owned by a squashed
            # uid, so every fetch failed with "detected dubious ownership", which used to
            # fall through to the silent stale fallback below and deploy a month-old
            # checkout. Mark all repos safe up front (idempotent → single '*' entry),
            # mirroring what the core git-manager already does for the IaC repos.
            await _run_git(["git", "config", "--global", "--replace-all", "safe.directory", "*"])
            if (target_dir / ".git").exists():
                git_cmds = [
                    ["git", "fetch", "origin", self.branch],
                    ["git", "checkout", "-f", self.branch],
                    ["git", "reset", "--hard", f"origin/{self.branch}"],
                ]
                # Retry the whole sync once; a transient network/auth blip must not fall
                # through to deploying the existing (stale) checkout.
                last_err = ""
                for _attempt in range(2):
                    failed = None
                    for cmd in git_cmds:
                        rc, out = await _run_git(cmd, cwd=str(target_dir))
                        if rc != 0:
                            failed = f"{' '.join(cmd)} -> {out}"
                            break
                    if failed is None:
                        return StageResult(True, f"Updated {self.service_name}")
                    last_err = failed
                # Hard-fail: never silently deploy a stale service repo (wrong version/config).
                raise RuntimeError(f"Service repo sync failed after retry: {last_err}")
            else:
                if target_dir.exists():
                    import shutil
                    shutil.rmtree(target_dir)
                rc, out = await _run_git(["git", "clone", "-b", self.branch, repo_url, str(target_dir)])
                if rc != 0:
                    raise RuntimeError(f"Clone failed: {out}")
                return StageResult(True, f"Successfully cloned {self.service_name}")
        except Exception as e:
            # No silent stale fallback: surface the failure so the pipeline aborts loudly
            # instead of bringing a service up on stale config/version. Note whether a
            # local copy exists so the operator knows a manual/forced deploy is possible.
            stale = " — a local copy exists but was NOT used (refusing to deploy stale state)" if (target_dir / "service.yml").exists() else ""
            log.error(f"Git error for {self.service_name}: {e}{stale}")
            return StageResult(False, f"Service repo sync failed for {self.service_name}: {e}{stale}")

class SyncAllServicesStage(BaseStage):
    def __init__(self):
        super().__init__("Bulk Sync Service Repositories")

    async def run(self, engine, context: dict) -> StageResult:
        catalog_file = engine.base_git_dir / "iac_controller" / "environments" / "global" / "02_service_catalog.yml"
        if not catalog_file.exists():
            return StageResult(False, f"Source service_catalog.yml missing at {catalog_file}")
        try:
            with open(catalog_file, 'r') as f:
                catalog_data = yaml.safe_load(f) or {}
                catalog = catalog_data.get("service_catalog", {})
                services = catalog.get("services", [])
                git_mgmt = catalog.get("remote_git_management", {})
        except Exception as e:
            return StageResult(False, f"Raw catalog parse failed: {e}")
        if not services:
            log.warning("Parsed raw catalog, but no services found. Check YAML structure.")
            return StageResult(False, "No services found in raw catalog to sync.")
        default_branch = git_mgmt.get("remote_git_default_branch_prod", "main")
        log.info(f"Bulk syncing {len(services)} services (Branch: {default_branch})...")
        sem = asyncio.Semaphore(5) 
        async def bounded_sync(svc_name):
            async with sem:
                stage = CloneServiceRepoStage(svc_name, default_branch, {})
                result = await stage.run(engine, context)
                if not result.success: log.error(f"Sync failed for {svc_name}: {result.message}")
                return result
        tasks = [bounded_sync(svc.get("name")) for svc in services if svc.get("name")]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        failures = sum(1 for r in results if isinstance(r, Exception) or not getattr(r, 'success', False))
        msg = f"Bulk sync complete. {len(services) - failures} succeeded, {failures} failed."
        if failures > 0: return StageResult(False, f"CRITICAL: {failures} services failed to sync. Aborting rollout.")
        return StageResult(True, msg)