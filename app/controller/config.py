import os
import re
from pathlib import Path

# --- Runner images (audit OS-02) ---------------------------------------------------
# The runner containers get the fleet SSH key and read-write repo mounts, so the image
# they are started from decides who owns the fleet. Images configured through the
# settings API/UI must come from this allowlist (registry or repository prefixes).
# The list is deployment-level on purpose (env only): whoever may change the image
# setting must not be able to widen the list through the same API.
RUNNER_IMAGE_ALLOWLIST_ENV = "PLUGIN_IAC_ORCHESTRATOR_RUNNER_IMAGE_ALLOWLIST"
DEFAULT_RUNNER_IMAGE_ALLOWLIST = (
    "registry.gitlab.int.fam-feser.de/",
    "ghcr.io/opentofu/opentofu",
)
ANSIBLE_IMAGE_ENV = "PLUGIN_IAC_ORCHESTRATOR_ANSIBLE_IMAGE"
TERRAFORM_IMAGE_ENV = "PLUGIN_IAC_ORCHESTRATOR_TERRAFORM_IMAGE"

# registry[:port]/repo[/repo...][:tag][@sha256:digest] — an explicit registry is
# required, and the reference can never start with '-' (docker would read an option).
_IMAGE_REF_RE = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9.-]*(:\d{1,5})?(/[a-z0-9][a-z0-9._-]*)+"
    r"(:[A-Za-z0-9_][A-Za-z0-9._-]{0,127})?(@sha256:[a-f0-9]{64})?$"
)


class RunnerImageNotAllowed(RuntimeError):
    """The configured runner image is malformed or outside the allowlist."""


def runner_image_allowlist() -> tuple[str, ...]:
    raw = os.getenv(RUNNER_IMAGE_ALLOWLIST_ENV)
    if raw is None:
        return DEFAULT_RUNNER_IMAGE_ALLOWLIST
    return tuple(p.strip() for p in raw.split(",") if p.strip())


def allowed_registry_hosts() -> set[str]:
    """Registry hosts (``host[:port]``) of the runner image allowlist."""
    return {p.split("/", 1)[0].lower() for p in runner_image_allowlist() if p}


def registry_host(url: str | None) -> str:
    """``host[:port]`` of a registry URL as given to ``docker login``."""
    text = str(url or "").strip().lower()
    for scheme in ("https://", "http://"):
        if text.startswith(scheme):
            text = text[len(scheme):]
    return text.split("/", 1)[0]


def runner_image_error(ref: str | None, *, check_allowlist: bool = True) -> str | None:
    """Why ``ref`` must not be used as a runner image (``None`` = fine)."""
    text = str(ref or "")
    if not _IMAGE_REF_RE.fullmatch(text):
        return (f"'{text}' is not a valid image reference "
                "(expected registry/repository[:tag][@sha256:digest])")
    if not check_allowlist:
        return None
    for prefix in runner_image_allowlist():
        if prefix.endswith("/"):
            if text.startswith(prefix):
                return None
        elif text == prefix or text.startswith((prefix + ":", prefix + "@", prefix + "/")):
            return None
    return (f"image '{text}' is not in the runner image allowlist "
            f"({', '.join(runner_image_allowlist()) or 'empty'}; {RUNNER_IMAGE_ALLOWLIST_ENV})")


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

    def checked_runner_image(self, kind: str) -> str:
        """The ``ansible``/``terraform`` runner image, verified right before use.

        Raises :class:`RunnerImageNotAllowed` instead of starting a container from
        an image outside the allowlist — whatever wrote the setting (API, legacy
        UI, a value stored before the allowlist existed). An image pinned through
        the environment is deployment configuration: only its syntax is checked.
        """
        if kind == "ansible":
            image, env_var = self.ansible_docker_image, ANSIBLE_IMAGE_ENV
        else:
            image, env_var = self.terraform_docker_image, TERRAFORM_IMAGE_ENV
        error = runner_image_error(image, check_allowlist=os.getenv(env_var) is None)
        if error:
            raise RunnerImageNotAllowed(f"Refusing to start the {kind} runner: {error}")
        return image

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
    def ansible_diff_mode(self) -> str:
        """When ansible-playbook gets ``--diff``: ``never`` | ``dry_run`` (default,
        only with --check) | ``always``. A diff prints the rendered content of
        every templated file — stack.env with its secrets included."""
        mode = str(self._get("PLUGIN_IAC_ORCHESTRATOR_ANSIBLE_DIFF", "iac_ansible_diff_mode", "dry_run")).strip().lower()
        return mode if mode in ("never", "dry_run", "always") else "dry_run"

    # --- orphan sweep (rD §3) — env > Vault (settings UI) > default ---
    @property
    def orphan_sweep_enabled(self) -> bool:
        return self._get_bool("PLUGIN_IAC_ORCHESTRATOR_ORPHAN_SWEEP_ENABLED", "iac_orphan_sweep_enabled", True)

    @property
    def orphan_sweep_grace_seconds(self) -> int:
        """No heartbeat and no log line for this long (and a dead owner) = orphaned."""
        return self._get_int("PLUGIN_IAC_ORCHESTRATOR_ORPHAN_SWEEP_GRACE_SECONDS",
                             "iac_orphan_sweep_grace_seconds", 90, minimum=30)

    @property
    def orphan_sweep_container_grace_seconds(self) -> int:
        """Grace for a job whose runner containers may still be running."""
        return self._get_int("PLUGIN_IAC_ORCHESTRATOR_ORPHAN_SWEEP_CONTAINER_GRACE_SECONDS",
                             "iac_orphan_sweep_container_grace_seconds", 1800, minimum=60)

    @property
    def runner_readonly_mounts(self) -> bool:
        """Runner containers get the repositories read-only (audit OS-06 A):
        Ansible reads ``git_repos``/``services`` and writes only its scratch
        dir; tofu writes only its run dir, the provider cache is read-only.
        On by default; off restores the read-write mounts of <= 1.6.1 (for a
        playbook that turns out to write into the tree)."""
        return self._get_bool("PLUGIN_IAC_ORCHESTRATOR_RUNNER_READONLY_MOUNTS", "iac_runner_readonly_mounts", True)

    #: Smallest limit that is honoured — anything below would stop ordinary jobs.
    JOB_MAX_RUNTIME_FLOOR_SECONDS = 600

    @property
    def job_max_runtime_seconds(self) -> int:
        """Longest time a job may stay RUNNING before the sweep stops and closes
        it (review C4); ``0`` = no limit. A value between 1 and the floor is
        raised to the floor instead of stopping every job after seconds."""
        value = self._get_int("PLUGIN_IAC_ORCHESTRATOR_JOB_MAX_RUNTIME_SECONDS",
                              "iac_job_max_runtime_seconds", 21600, minimum=0)
        return 0 if value <= 0 else max(self.JOB_MAX_RUNTIME_FLOOR_SECONDS, value)

    @property
    def tf_lock_timeout_seconds(self) -> int:
        """How long a Terraform run waits for the state lock of its environment
        before the job fails (another job is running tofu on that state)."""
        return self._get_int("PLUGIN_IAC_ORCHESTRATOR_TF_LOCK_TIMEOUT_SECONDS", "iac_tf_lock_timeout_seconds",
                             1800, minimum=0)

    @property
    def token_infra_apply_enabled(self) -> bool:
        """May the GitLab-token route trigger a fleet apply? Off unless explicitly enabled."""
        return self._get_bool("PLUGIN_IAC_ORCHESTRATOR_TOKEN_INFRA_APPLY", "iac_token_infra_apply_enabled", False)

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

    # --- MAINTENANCE WINDOWS (plugin 1.6) — env > Vault (settings UI) > default ---
    def _get_bool(self, env_var: str, vault_key: str, default: bool) -> bool:
        raw = str(self._get(env_var, vault_key, "true" if default else "false")).strip().lower()
        return raw in ("1", "true", "yes", "on")

    def _get_int(self, env_var: str, vault_key: str, default: int, *, minimum: int = 0) -> int:
        try:
            return max(minimum, int(str(self._get(env_var, vault_key, str(default))).strip()))
        except ValueError:
            return default

    @property
    def maintenance_deploy_marks_maintenance(self) -> bool:
        """Every executed deploy announces a maintenance override (decision B)."""
        return self._get_bool("PLUGIN_IAC_ORCHESTRATOR_MAINTENANCE_DEPLOY_MARKS_MAINTENANCE",
                              "iac_maintenance_deploy_marks_maintenance", True)

    @property
    def maintenance_max_override_minutes(self) -> int:
        """Upper bound of a deploy override (its end is unknown when it starts)."""
        return self._get_int("PLUGIN_IAC_ORCHESTRATOR_MAINTENANCE_MAX_OVERRIDE_MINUTES",
                             "iac_maintenance_max_override_minutes", 240, minimum=1)

    @property
    def maintenance_linger_minutes(self) -> int:
        """How long consumers keep the override after ``override_ended``."""
        return self._get_int("PLUGIN_IAC_ORCHESTRATOR_MAINTENANCE_LINGER_MINUTES",
                             "iac_maintenance_linger_minutes", 15, minimum=0)

    @property
    def maintenance_schedule_horizon_days(self) -> int:
        """How far ahead a ``deploy_policy: window`` deploy may be parked."""
        return self._get_int("PLUGIN_IAC_ORCHESTRATOR_MAINTENANCE_SCHEDULE_HORIZON_DAYS",
                             "iac_maintenance_schedule_horizon_days", 30, minimum=1)

    @property
    def maintenance_report_missing_window(self) -> bool:
        """Emit ``maintenance:window_missing`` when a window deploy runs without a window (decision H)."""
        return self._get_bool("PLUGIN_IAC_ORCHESTRATOR_MAINTENANCE_REPORT_MISSING_WINDOW",
                              "iac_maintenance_report_missing_window", True)

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
