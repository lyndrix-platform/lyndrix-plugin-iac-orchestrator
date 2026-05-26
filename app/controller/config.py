import os
from pathlib import Path

class IaCConfig:
    def __init__(self, ctx):
        self.ctx = ctx

    def _get(self, env_var: str, vault_key: str = None, default: str = None) -> str:
        """Fetches a setting following the priority: Env Var > Vault/UI > Default."""
        val = os.getenv(env_var)
        if val is not None:
            return val
            
        if vault_key:
            val = self.ctx.get_secret(vault_key)
            if val is not None:
                return val
                
        return default

    @property
    def base_storage_dir(self) -> Path: return Path(self._get("PLUGIN_IAC_ORCHESTRATOR_INTERNAL_STORAGE_DIR", default="/data/storage"))

    @property
    def git_repos_dir(self) -> Path: return Path(self._get("PLUGIN_IAC_ORCHESTRATOR_INTERNAL_GIT_REPOS_DIR", default=str(self.base_storage_dir / "git_repos")))

    @property
    def services_dir(self) -> Path: return Path(self._get("PLUGIN_IAC_ORCHESTRATOR_INTERNAL_SERVICES_DIR", default=str(self.base_storage_dir / "services")))

    @property
    def logs_dir(self) -> Path: return Path(self._get("PLUGIN_IAC_ORCHESTRATOR_INTERNAL_LOGS_DIR", default=str(self.base_storage_dir / "logs")))

    @property
    def security_dir(self) -> Path: return Path(self._get("PLUGIN_IAC_ORCHESTRATOR_INTERNAL_SECURITY_DIR", default="/data/security"))

    # --- HOST PATHS FOR SIBLING DOCKER CONTAINERS ---
    @property
    def host_git_repos_dir(self) -> str: 
        return self._get("PLUGIN_IAC_ORCHESTRATOR_HOST_GIT_REPOS_DIR", default=str(self.git_repos_dir))

    @property
    def host_services_dir(self) -> str: 
        return self._get("PLUGIN_IAC_ORCHESTRATOR_HOST_SERVICES_DIR", default=str(self.services_dir))

    @property
    def host_security_dir(self) -> str: 
        return self._get("PLUGIN_IAC_ORCHESTRATOR_HOST_SECURITY_DIR", default=str(self.security_dir))

    @property
    def ansible_docker_image(self) -> str: return self._get("PLUGIN_IAC_ORCHESTRATOR_ANSIBLE_IMAGE", "ansible_docker_image", "registry.gitlab.int.fam-feser.de/aac-application-definitions/aac-template-engine:latest")

    @property
    def auto_apply(self) -> bool: return str(self._get("PLUGIN_IAC_ORCHESTRATOR_AUTO_APPLY", "iac_auto_apply", "false")).lower() == "true"

    @property
    def sync_interval_minutes(self) -> int:
        try: return int(self._get("PLUGIN_IAC_ORCHESTRATOR_SYNC_INTERVAL", "iac_sync_interval_minutes", "15"))
        except ValueError: return 15

    def get_log_path(self, job_id: int) -> Path:
        return self.logs_dir / f"job_{job_id}.log"