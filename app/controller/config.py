import os
from pathlib import Path

class IaCConfig:
    def __init__(self, ctx):
        self.ctx = ctx
        self._runtime_host_paths: dict[str, str] = {}

    def _get(self, env_var: str, vault_key: str | None = None, default: str = "") -> str:
        """Fetches a setting following the priority: Env Var > Vault/UI > Default.

        Every call site supplies a non-empty ``default``, so the result is always a
        ``str`` — the default is "" only as a safety net for the no-default case.
        """
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

    @property
    def terraform_providers_dir(self) -> Path:
        return Path(
            self._get(
                "PLUGIN_IAC_ORCHESTRATOR_INTERNAL_TERRAFORM_PROVIDERS_DIR",
                default=str(self.base_storage_dir / "terraform-providers"),
            )
        )

    # --- HOST PATHS FOR SIBLING DOCKER CONTAINERS ---
    @property
    def host_git_repos_dir(self) -> str: 
        return self._runtime_host_paths.get(
            "git_repos",
            self._get("PLUGIN_IAC_ORCHESTRATOR_HOST_GIT_REPOS_DIR", default=str(self.git_repos_dir)),
        )

    @property
    def host_services_dir(self) -> str: 
        return self._runtime_host_paths.get(
            "services",
            self._get("PLUGIN_IAC_ORCHESTRATOR_HOST_SERVICES_DIR", default=str(self.services_dir)),
        )

    @property
    def host_security_dir(self) -> str: 
        return self._runtime_host_paths.get(
            "security",
            self._get("PLUGIN_IAC_ORCHESTRATOR_HOST_SECURITY_DIR", default=str(self.security_dir)),
        )

    @property
    def host_terraform_providers_dir(self) -> str:
        return self._runtime_host_paths.get(
            "terraform_providers",
            self._get(
                "PLUGIN_IAC_ORCHESTRATOR_HOST_TERRAFORM_PROVIDERS_DIR",
                default=str(self.terraform_providers_dir),
            ),
        )

    @property
    def ansible_docker_image(self) -> str: return self._get("PLUGIN_IAC_ORCHESTRATOR_ANSIBLE_IMAGE", "ansible_docker_image", "registry.gitlab.int.fam-feser.de/aac-application-definitions/aac-template-engine:latest")

    @property
    def terraform_docker_image(self) -> str:
        return self._get(
            "PLUGIN_IAC_ORCHESTRATOR_TERRAFORM_IMAGE",
            "iac_terraform_docker_image",
            "ghcr.io/opentofu/opentofu:latest",
        )

    @property
    def terraform_binary(self) -> str:
        return self._get(
            "PLUGIN_IAC_ORCHESTRATOR_TERRAFORM_BINARY",
            "iac_terraform_binary",
            "tofu",
        )

    @property
    def auto_apply(self) -> bool: return str(self._get("PLUGIN_IAC_ORCHESTRATOR_AUTO_APPLY", "iac_auto_apply", "false")).lower() == "true"

    @property
    def sync_interval_minutes(self) -> int:
        try: return int(self._get("PLUGIN_IAC_ORCHESTRATOR_SYNC_INTERVAL", "iac_sync_interval_minutes", "15"))
        except ValueError: return 15

    @property
    def test_deploy_allowed_hosts(self) -> set[str]:
        raw = self._get(
            "PLUGIN_IAC_ORCHESTRATOR_TEST_DEPLOY_ALLOWED_HOSTS",
            "iac_test_deploy_allowed_hosts",
            "",
        ) or ""
        return {
            host.strip()
            for host in str(raw).split(",")
            if host and host.strip()
        }

    def get_log_path(self, job_id: int) -> Path:
        return self.logs_dir / f"job_{job_id}.log"

    def apply_runtime_mount_paths(self, mounts: dict[str, str]) -> None:
        """Apply socket-resolved host paths from the core socket manager."""
        if not isinstance(mounts, dict):
            return

        mapping = {
            "git_repos": mounts.get("/data/storage/git_repos"),
            "services": mounts.get("/data/storage/services"),
            "security": mounts.get("/data/security"),
            "terraform_providers": mounts.get("/data/storage/terraform-providers"),
        }
        self._runtime_host_paths.update({k: v for k, v in mapping.items() if v})
