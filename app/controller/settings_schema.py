"""Schema-driven, fully API-controllable settings surface for the IaC Orchestrator.

This is the single source of truth for every operator-tunable setting the plugin
exposes. The REST API renders its schema/values/save from here, so "all settings are
API-controllable" holds by construction — the same fields the NiceGUI offers.

Persistence intentionally reuses the EXISTING Vault keys the engine already reads
(``ctx.get_secret``/``set_secret``: ``iac_*``, ``ansible_*``, ``repo_<slug>_config`` …),
so no engine read-site changes and behaviour is identical to the NiceGUI settings page.

Security model (audit OS-02). These settings decide which image runs with the fleet
SSH key, where API tokens are sent and which secret a "credential" field points at —
so every value is validated per key and every field belongs to a *tier*:

* ``operator``          – harmless tuning (maintenance timings); plugin ``api:write``.
* ``settings_admin``    – URLs, runner images, registry, allowlists, sync switches.
* ``credentials_admin`` – which stored credential a setting uses.
* ``infra_apply``       – switches that unlock unattended infrastructure applies.

Secret material (SSH keys, passwords, tokens) is NOT writable through
``save_values`` at all: it has its own slot route (``set_secret_slot``). Only values
that actually change are validated and authorised, so a client that re-submits a whole
category (the React form does) is not blocked by fields it did not touch.

Secrets are never returned in plaintext: ``get_values`` masks them and reports a
``<key>__configured`` flag instead.
"""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field as _dc_field
from typing import Any
from urllib.parse import urlsplit

from .config import allowed_registry_hosts, registry_host, runner_image_error

# Matches the NiceGUI "leave blank to change" convention for secret inputs.
SECRET_SENTINEL = "********"

_DEFAULT_ANSIBLE_IMAGE = (
    "registry.gitlab.int.fam-feser.de/aac-application-definitions/"
    "aac-template-engine:latest"
)
_DEFAULT_TOFU_IMAGE = (
    "registry.gitlab.int.fam-feser.de/iac-environment/"
    "iac-platform-assets/opentofu-ci-image:latest"
)
_TOFU_FALLBACK_IMAGE = "ghcr.io/opentofu/opentofu:latest"
_DEFAULT_GITLAB_URL = "https://gitlab.int.fam-feser.de"

TIER_OPERATOR = "operator"
TIER_SETTINGS = "settings_admin"
TIER_CREDENTIALS = "credentials_admin"
TIER_INFRA_APPLY = "infra_apply"

#: Explicit opt-out of the https requirement for GitLab / callback / repo URLs.
ALLOW_INSECURE_URLS_ENV = "PLUGIN_IAC_ORCHESTRATOR_ALLOW_INSECURE_URLS"
#: Hosts API tokens may be sent to (GitLab API, git remotes). Deployment-level.
ALLOWED_GIT_HOSTS_ENV = "PLUGIN_IAC_ORCHESTRATOR_ALLOWED_GIT_HOSTS"

#: Largest secret a slot accepts (an RSA-8192 PEM is ~6.4 kB).
MAX_SECRET_BYTES = 64 * 1024

_HOST_TOKEN_RE = re.compile(r"^[A-Za-z0-9._-]+$")
_CREDENTIAL_ALIAS_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_REGISTRY_RE = re.compile(r"^(https://)?[A-Za-z0-9.-]+(:\d{1,5})?(/[A-Za-z0-9._/-]*)?$")
_SCP_GIT_RE = re.compile(r"^[A-Za-z0-9._-]+@([A-Za-z0-9.-]+):[A-Za-z0-9._~/-]+$")
_PLAIN_RE = re.compile(r"^[A-Za-z0-9._@+-]*$")


class SettingsValidationError(ValueError):
    """A submitted value violates the rule of its key (HTTP 422)."""

    def __init__(self, key: str, message: str):
        super().__init__(f"{key}: {message}")
        self.key = key


class SettingsPermissionError(PermissionError):
    """The caller may not change settings of this tier (HTTP 403)."""

    def __init__(self, tier: str, keys: list[str]):
        super().__init__(f"changing {', '.join(sorted(keys))} requires the '{tier}' permission")
        self.tier = tier
        self.keys = keys


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return default


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def insecure_urls_allowed() -> bool:
    return _as_bool(os.getenv(ALLOW_INSECURE_URLS_ENV), False)


@dataclass
class SettingField:
    """One operator-tunable setting.

    ``key`` is the stable API/JSON identifier. ``vault_key`` is where the value is
    actually stored (defaults to ``key``). Repo-role fields instead live inside the
    JSON blob ``repo_<repo_slug>_config`` under ``repo_subkey``.
    """

    key: str
    label: str
    kind: str = "str"  # str | bool | int | select | textarea | password
    category: str = "General"
    sensitive: bool = False
    default: Any = ""
    description: str = ""
    options: list[str] = _dc_field(default_factory=list)
    options_source: str | None = None  # "credentials" -> dynamic select options

    vault_key: str | None = None       # storage key (defaults to ``key``)
    state_key: str | None = None        # also mirror into service.state[...]
    bool_format: str = "lower"          # "lower" -> true/false, "python" -> True/False
    coalesce_default: str | None = None  # blank str -> write this instead of ""
    repo_slug: str | None = None         # repo-role JSON storage
    repo_subkey: str | None = None
    # OS env var that shadows the Vault value (env > vault in IaCConfig._get).
    # When set, a saved value silently never takes effect, so we must surface the
    # lock rather than report a false success.
    env_var: str | None = None
    # Who may change it (secure by default) and which rule its value must satisfy.
    tier: str = TIER_SETTINGS
    rule: str | None = None             # see OrchestratorSettings._validate
    minimum: int | None = None          # int fields
    maximum: int | None = None
    # Stored value that closes a safety gate. Writing it is always allowed for
    # the operator tier and never skipped as "unchanged": turning a gate off must
    # work even when the current value cannot be read (Vault hiccup, stale view).
    safe_value: str | None = None

    def storage_key(self) -> str:
        return self.vault_key or self.key

    def is_env_locked(self) -> bool:
        """True when an OS env var shadows this setting (Vault writes are inert)."""
        return bool(self.env_var and os.getenv(self.env_var) is not None)


_REPO_ROLES = [
    ("iac_controller", "IaC Controller (SSoT Source)"),
    ("infra_engine", "Infrastructure Engine (Terraform/Tofu)"),
    ("config_engine", "Configuration Engine (Ansible)"),
    ("inventory_state", "Inventory State (Generated Output)"),
    ("aac_factory", "AaC Factory (App Templates)"),
    ("service_repos", "Application Services (Default Auth)"),
]


def _build_fields() -> list[SettingField]:
    fields: list[SettingField] = [
        # ── Pipeline ──────────────────────────────────────────────────────────
        SettingField(
            "auto_apply", "Auto-Apply on webhook", kind="bool", category="Pipeline",
            default=False, vault_key="iac_auto_apply", state_key="auto_apply_enabled",
            bool_format="python", env_var="PLUGIN_IAC_ORCHESTRATOR_AUTO_APPLY",
            # Enabling it unlocks unattended applies → needs the apply permission
            # itself (audit OS-03); switching it off is open to every operator.
            safe_value="False", tier=TIER_INFRA_APPLY,
            description=(
                "Infrastructure only: let Terraform apply (infra_apply, rule mode: apply) and "
                "rule-engine Ansible actions run without a manual gate. Off = plan / --check for "
                "those. Service deploys, rollouts and compliance runs are never affected."
            ),
        ),
        SettingField(
            "token_infra_apply_enabled", "Token-triggered infra apply", kind="bool", category="Pipeline",
            default=False, bool_format="lower", vault_key="iac_token_infra_apply_enabled",
            env_var="PLUGIN_IAC_ORCHESTRATOR_TOKEN_INFRA_APPLY",
            safe_value="false", tier=TIER_INFRA_APPLY,
            description=(
                "Enable POST /api/iac/infra/apply/gitlab: a fleet-wide `tofu apply` for anyone holding the "
                "GitLab webhook token. Off by default — no CI pipeline uses it; approve plans in the dashboard."
            ),
        ),
        SettingField(
            "test_deploy_allowed_hosts", "Test-Deploy allowed hosts", category="Pipeline",
            vault_key="iac_test_deploy_allowed_hosts", rule="host_list",
            env_var="PLUGIN_IAC_ORCHESTRATOR_TEST_DEPLOY_ALLOWED_HOSTS",
            description="Comma-separated allowlist of hosts permitted as ad-hoc test-deploy targets.",
        ),
        # ── GitLab Webhooks ───────────────────────────────────────────────────
        SettingField(
            "gitlab_url", "GitLab Base URL", category="GitLab Webhooks",
            vault_key="iac_gitlab_url", default=_DEFAULT_GITLAB_URL, rule="gitlab_url",
        ),
        SettingField(
            "group_id", "GitLab Group ID", category="GitLab Webhooks",
            vault_key="iac_gitlab_group_id", rule="digits",
            description="Group whose project webhooks are reconciled by the auto-sync loop.",
        ),
        SettingField(
            "lyndrix_base_url", "Lyndrix Base URL", category="GitLab Webhooks",
            vault_key="iac_lyndrix_base_url", default="http://10.1.10.31:8081", rule="base_url",
            description="Public base URL GitLab calls back for webhooks.",
        ),
        SettingField(
            "gitlab_token_key", "GitLab API Credential", kind="select",
            category="GitLab Webhooks", vault_key="iac_gitlab_api_token_key",
            options_source="credentials", tier=TIER_CREDENTIALS, rule="credential_ref",
            description="Stored credential alias used for GitLab API calls (webhook upsert).",
        ),
        # One token per purpose (audit OS-04). Secret slots, written through
        # POST /settings/secrets/<key>. Empty = the shared webhook token keeps
        # authorising that purpose; set = ONLY this token is accepted for it.
        SettingField(
            "gitlab_hook_token", "Token: GitLab group hooks", kind="password", category="GitLab Webhooks",
            sensitive=True, vault_key="gitlab_hook_token", tier=TIER_CREDENTIALS,
            description="Secret of the GitLab group/project hooks (POST /api/iac/webhook/gitlab — merge request "
                        "notifications only; with this token a pipeline event never starts a deployment). "
                        "Empty: the shared webhook token is accepted.",
        ),
        SettingField(
            "gitlab_service_ci_token", "Token: service CI", kind="password", category="GitLab Webhooks",
            sensitive=True, vault_key="gitlab_service_ci_token", tier=TIER_CREDENTIALS,
            description="CI variable of the service pipelines (deploy/service/<svc>/gitlab and its status). With "
                        "this token only the catalog's production branch of the service is deployed. "
                        "Empty: the shared webhook token is accepted.",
        ),
        SettingField(
            "gitlab_controller_ci_token", "Token: controller CI", kind="password", category="GitLab Webhooks",
            sensitive=True, vault_key="gitlab_controller_ci_token", tier=TIER_CREDENTIALS,
            description="CI variable of the iac-controller pipeline (infra/plan/gitlab, webhook/sync, test-host). "
                        "Empty: the shared webhook token is accepted.",
        ),
        SettingField(
            "autosync_enabled", "Auto-sync webhooks", kind="bool",
            category="GitLab Webhooks", vault_key="iac_webhook_autosync_enabled",
            default=True, bool_format="lower",
        ),
        SettingField(
            "sync_interval", "Auto-sync interval (s)", kind="int",
            category="GitLab Webhooks", vault_key="iac_webhook_sync_interval_seconds",
            default=1800, minimum=300, maximum=86400,
            description="Seconds between webhook reconciliation runs (min 300).",
        ),
        # ── Ansible ───────────────────────────────────────────────────────────
        SettingField(
            "ansible_docker_image", "Ansible Runner Image", category="Ansible",
            vault_key="ansible_docker_image", default=_DEFAULT_ANSIBLE_IMAGE,
            env_var="PLUGIN_IAC_ORCHESTRATOR_ANSIBLE_IMAGE", rule="runner_image",
            description="Container image used to execute Ansible playbooks (must be in the runner image allowlist).",
        ),
        SettingField(
            "ansible_diff_mode", "Show file diffs (--diff)", kind="select", category="Ansible",
            options=["never", "dry_run", "always"], default="dry_run", rule="choice",
            vault_key="iac_ansible_diff_mode", env_var="PLUGIN_IAC_ORCHESTRATOR_ANSIBLE_DIFF",
            description="ansible-playbook --diff prints the rendered content of templated files (stack.env "
                        "with its secrets) into the job log. Default: only for dry-runs; known secret "
                        "values are masked in every job log either way.",
        ),
        SettingField(
            "ansible_ssh_key", "Ansible SSH Private Key", kind="textarea",
            category="Ansible", sensitive=True, vault_key="ansible_ssh_key",
            tier=TIER_CREDENTIALS,
            description="RSA private key the runner uses to reach managed hosts.",
        ),
        SettingField(
            "ansible_registry_url", "Private Registry URL", category="Ansible",
            vault_key="ansible_registry_url", rule="registry",
        ),
        SettingField(
            "ansible_registry_user", "Registry Username", category="Ansible",
            vault_key="ansible_registry_user", rule="plain",
        ),
        SettingField(
            "ansible_registry_token", "Registry Token/Password", kind="password",
            category="Ansible", sensitive=True, vault_key="ansible_registry_token",
            tier=TIER_CREDENTIALS,
        ),
        # ── Terraform ─────────────────────────────────────────────────────────
        SettingField(
            "iac_terraform_docker_image", "OpenTofu Runner Image", category="Terraform",
            vault_key="iac_terraform_docker_image", default=_DEFAULT_TOFU_IMAGE,
            coalesce_default=_TOFU_FALLBACK_IMAGE, rule="runner_image",
            env_var="PLUGIN_IAC_ORCHESTRATOR_TERRAFORM_IMAGE",
            description="Image for every tofu init/plan/apply. Blank falls back to the upstream OpenTofu image.",
        ),
        SettingField(
            "iac_tf_ssh_key", "Root SSH Public Key", kind="textarea", category="Terraform",
            sensitive=True, vault_key="iac_tf_ssh_key", tier=TIER_CREDENTIALS,
            description="Public key injected into newly provisioned hosts.",
        ),
        SettingField(
            "iac_tf_ssh_private_key", "Root SSH Private Key", kind="textarea",
            category="Terraform", sensitive=True, vault_key="iac_tf_ssh_private_key",
            tier=TIER_CREDENTIALS,
            description="Private counterpart used for the first bootstrap/compliance run.",
        ),
        SettingField(
            "iac_tf_root_password", "Root Password (new host)", kind="password",
            category="Terraform", sensitive=True, vault_key="iac_tf_root_password",
            tier=TIER_CREDENTIALS,
        ),
        SettingField(
            "iac_tf_backend_secret_key", "State Backend Secret Key (S3/MinIO)",
            kind="password", category="Terraform", sensitive=True,
            vault_key="iac_tf_backend_secret_key", tier=TIER_CREDENTIALS,
            description="Shared backend secret across all sites/stages.",
        ),
        # ── Maintenance windows (plugin 1.6, core API 1.13) ───────────────────
        # Read by IaCConfig.maintenance_* (env > Vault > default). The default
        # switch itself (maintenance_default.enabled) is NOT a setting here: it lives
        # in the iac-controller repo and is only displayed (decision C).
        SettingField(
            "maintenance_deploy_marks_maintenance", "Deploys mark maintenance", kind="bool",
            category="Maintenance", default=True, bool_format="lower", tier=TIER_OPERATOR,
            vault_key="iac_maintenance_deploy_marks_maintenance",
            env_var="PLUGIN_IAC_ORCHESTRATOR_MAINTENANCE_DEPLOY_MARKS_MAINTENANCE",
            description="Every executed service deploy announces a maintenance override for the service and "
                        "host CIs it touches (monitoring shows 'maintenance' instead of 'down').",
        ),
        SettingField(
            "maintenance_max_override_minutes", "Max override duration (min)", kind="int",
            category="Maintenance", default=240, minimum=1, maximum=10080, tier=TIER_OPERATOR,
            vault_key="iac_maintenance_max_override_minutes",
            env_var="PLUGIN_IAC_ORCHESTRATOR_MAINTENANCE_MAX_OVERRIDE_MINUTES",
            description="Upper bound of a deploy override; it ends earlier when the job finishes.",
        ),
        SettingField(
            "maintenance_linger_minutes", "Override linger (min)", kind="int",
            category="Maintenance", default=15, minimum=0, maximum=1440, tier=TIER_OPERATOR,
            vault_key="iac_maintenance_linger_minutes",
            env_var="PLUGIN_IAC_ORCHESTRATOR_MAINTENANCE_LINGER_MINUTES",
            description="Consumers keep the override this long after the deploy ended (containers restart, probes settle).",
        ),
        SettingField(
            "maintenance_schedule_horizon_days", "Schedule horizon (days)", kind="int",
            category="Maintenance", default=30, minimum=1, maximum=366, tier=TIER_OPERATOR,
            vault_key="iac_maintenance_schedule_horizon_days",
            env_var="PLUGIN_IAC_ORCHESTRATOR_MAINTENANCE_SCHEDULE_HORIZON_DAYS",
            description="A 'window' deploy is parked at most this far ahead; without a window in the horizon it "
                        "runs at once with a warning.",
        ),
        SettingField(
            "maintenance_report_missing_window", "Report missing window", kind="bool",
            category="Maintenance", default=True, bool_format="lower", tier=TIER_OPERATOR,
            vault_key="iac_maintenance_report_missing_window",
            env_var="PLUGIN_IAC_ORCHESTRATOR_MAINTENANCE_REPORT_MISSING_WINDOW",
            description="Emit maintenance:window_missing when a window deploy runs without a window (ITSM opens a task).",
        ),
        # ── Service reconcile (plugin 1.8) ────────────────────────────────────
        # Read by IaCConfig.service_reconcile_* (env > Vault > default). Second opt-in
        # besides the `service_reconcile` rule in config_engine/pipeline_rules.yml.
        SettingField(
            "service_reconcile_mode", "Controller-diff service deploys", kind="select",
            category="Service Reconcile", default="off",
            options=["off", "notify", "dry_run", "deploy"],
            vault_key="iac_service_reconcile_mode",
            env_var="PLUGIN_IAC_ORCHESTRATOR_SERVICE_RECONCILE_MODE",
            description=(
                "What happens when an iac-controller change alters a service's rendered entry "
                "(profile vars, secrets, …): off = log only, notify = bell/Discord, dry_run = one "
                "single_service --check job per service limited to the changed hosts, deploy = real deploys. "
                "Independent of auto_apply."
            ),
        ),
        SettingField(
            "service_reconcile_max_hosts", "Max hosts per auto job", kind="int",
            category="Service Reconcile", default=10,
            vault_key="iac_service_reconcile_max_hosts",
            env_var="PLUGIN_IAC_ORCHESTRATOR_SERVICE_RECONCILE_MAX_HOSTS",
            description="A service change touching more hosts than this is not deployed automatically; it waits "
                        "under /reconcile/pending for approval.",
        ),
        SettingField(
            "service_reconcile_max_services", "Max services per controller change", kind="int",
            category="Service Reconcile", default=3,
            vault_key="iac_service_reconcile_max_services",
            env_var="PLUGIN_IAC_ORCHESTRATOR_SERVICE_RECONCILE_MAX_SERVICES",
            description="More changed services than this in one controller commit (e.g. an edit to a shared "
                        "profile) → every one waits for approval.",
        ),
        SettingField(
            "service_reconcile_exclude_services", "Excluded services", category="Service Reconcile",
            default="aac-hermes-agent",
            vault_key="iac_service_reconcile_exclude_services",
            env_var="PLUGIN_IAC_ORCHESTRATOR_SERVICE_RECONCILE_EXCLUDE_SERVICES",
            description="Comma-separated service names the controller-diff reconcile never deploys — no job, "
                        "no approval, no deferred re-queue; they only appear as 'excluded' in the notification. "
                        "Default protects aac-hermes-agent (a single_service deploy tears the agent stack down). "
                        "Save an empty value to exclude nothing.",
        ),
    ]

    # ── Runner / orphan sweep (rD §3) ────────────────────────────────────────
    fields += [
        SettingField(
            "orphan_sweep_enabled", "Orphan sweep", kind="bool", category="Runner",
            default=True, bool_format="lower", vault_key="iac_orphan_sweep_enabled",
            env_var="PLUGIN_IAC_ORCHESTRATOR_ORPHAN_SWEEP_ENABLED",
            description="Close RUNNING jobs whose runner instance is gone as FAILED (runner_lost). "
                        "Runs on one control-plane instance; never re-runs a job.",
        ),
        SettingField(
            "orphan_sweep_grace_seconds", "Orphan grace (s)", kind="int", category="Runner",
            default=90, minimum=30, maximum=86400, vault_key="iac_orphan_sweep_grace_seconds",
            env_var="PLUGIN_IAC_ORCHESTRATOR_ORPHAN_SWEEP_GRACE_SECONDS",
            description="A job of a dead runner is failed after this long without heartbeat and log output "
                        "(at least twice the cluster lease TTL).",
        ),
        SettingField(
            "orphan_sweep_container_grace_seconds", "Orphan grace with containers (s)", kind="int",
            category="Runner", default=1800, minimum=60, maximum=604800,
            vault_key="iac_orphan_sweep_container_grace_seconds",
            env_var="PLUGIN_IAC_ORCHESTRATOR_ORPHAN_SWEEP_CONTAINER_GRACE_SECONDS",
            description="Grace for a job whose runner containers may still be running — time for the runner "
                        "to come back and re-attach.",
        ),
        SettingField(
            "runner_readonly_mounts", "Read-only repository mounts", kind="bool", category="Runner",
            default=True, bool_format="lower", vault_key="iac_runner_readonly_mounts",
            env_var="PLUGIN_IAC_ORCHESTRATOR_RUNNER_READONLY_MOUNTS",
            description="Runner containers cannot write the repositories: Ansible gets git_repos and services "
                        "read-only (with .git hidden) plus a scratch dir per job (/data/scratch); tofu gets only "
                        "its run dir read-write and the provider cache read-only. Switch off only if a playbook "
                        "must write into the tree — that restores the old read-write mounts.",
        ),
        SettingField(
            "job_max_runtime_seconds", "Maximum job runtime (s)", kind="int", category="Runner",
            default=21600, minimum=0, maximum=2592000, vault_key="iac_job_max_runtime_seconds",
            env_var="PLUGIN_IAC_ORCHESTRATOR_JOB_MAX_RUNTIME_SECONDS", rule="runtime_limit",
            description="A job that is still RUNNING after this long is stopped and closed as FAILED "
                        "(max_runtime): the runner is asked to stop its containers; if it does not answer the "
                        "job is closed anyway and its locks stay reserved for the container grace. "
                        "0 = no limit, otherwise at least 600. Default 6 h.",
        ),
    ]

    # ── Repository Roles (each persisted into repo_<slug>_config JSON) ─────────
    for slug, label in _REPO_ROLES:
        fields.append(SettingField(
            f"repo_{slug}_url", f"{label} — Git URL", category="Repository Roles",
            repo_slug=slug, repo_subkey="url", rule="git_url",
        ))
        fields.append(SettingField(
            f"repo_{slug}_token_key", f"{label} — Credential", kind="select",
            category="Repository Roles", options_source="credentials",
            repo_slug=slug, repo_subkey="token_key", tier=TIER_CREDENTIALS, rule="credential_ref",
        ))
    return fields


SETTINGS_FIELDS: list[SettingField] = _build_fields()
_FIELDS_BY_KEY = {f.key: f for f in SETTINGS_FIELDS}


# --- Vault keys of the plugin itself / credential aliases ---------------------------
# Credential aliases double as Vault keys in the plugin's own KV document. An alias
# must therefore never be the name of something else the plugin stores there, and a
# "credential reference" setting must never resolve to anything but an alias.
_FIXED_RESERVED_KEYS = frozenset({
    "gitlab_webhook_token", "gitlab_webhook_token_previous", "ansible_ssh_key",
    "iac_tf_ssh_private_key", "iac_token_registry", "system_api_key",
})


def reserved_vault_keys() -> frozenset[str]:
    """Every Vault key that is a setting or secret slot of the plugin (lower case)."""
    keys = set(_FIXED_RESERVED_KEYS)
    keys.update(f.storage_key().lower() for f in SETTINGS_FIELDS if not f.repo_slug)
    keys.update(f"repo_{slug}_config" for slug, _ in _REPO_ROLES)
    return frozenset(keys)


def is_valid_credential_alias(alias: Any) -> bool:
    """A usable credential alias: safe charset, not reserved, no ``iac_`` prefix
    (engine configuration). Case-insensitive so ``Iac_foo`` cannot slip through."""
    text = str(alias or "")
    lowered = text.lower()
    return bool(
        _CREDENTIAL_ALIAS_RE.fullmatch(text)
        and lowered not in reserved_vault_keys()
        and not lowered.startswith("iac_")
    )


def resolve_credential(ctx, alias: Any) -> str | None:
    """Secret behind a credential reference — ``None`` unless ``alias`` can only be
    a credential alias. Stops a reference that was pointed at another secret of
    the plugin (Proxmox password, SSH key, webhook token …) from being sent to a
    GitLab/git host, whoever stored that reference."""
    if not alias:
        return None
    if not is_valid_credential_alias(alias):
        try:
            ctx.log.error(f"SECURITY: credential reference '{alias}' is not a credential alias — ignored.")
        except Exception:
            pass
        return None
    return ctx.get_secret(str(alias))


def _http_url_error(value: str, *, allowed_hosts: set[str] | None) -> str | None:
    try:
        parts = urlsplit(value)
    except ValueError:
        return "is not a valid URL"
    if parts.scheme not in ("http", "https") or not parts.hostname:
        return "must be an http(s) URL"
    if parts.username or parts.password:
        return "must not contain credentials"
    if parts.query or parts.fragment:
        return "must not contain a query or fragment"
    if parts.scheme != "https" and not insecure_urls_allowed():
        return f"must use https (opt out with {ALLOW_INSECURE_URLS_ENV}=true)"
    if allowed_hosts is not None and parts.hostname.lower() not in allowed_hosts:
        return (f"host '{parts.hostname}' is not allowed "
                f"(allowed: {', '.join(sorted(allowed_hosts)) or 'none'}; {ALLOWED_GIT_HOSTS_ENV})")
    return None


def _url_host(value: Any) -> str | None:
    text = str(value or "").strip()
    scp = _SCP_GIT_RE.fullmatch(text)
    if scp:
        return scp.group(1).lower()
    try:
        return (urlsplit(text).hostname or "").lower() or None
    except ValueError:
        return None


def gitlab_url_error(value: str) -> str | None:
    """Use-time check of the GitLab base URL (the API token is sent there)."""
    env_hosts = os.getenv(ALLOWED_GIT_HOSTS_ENV)
    allowed = {h.strip().lower() for h in env_hosts.split(",") if h.strip()} if env_hosts is not None else None
    return _http_url_error(str(value or ""), allowed_hosts=allowed)


class OrchestratorSettings:
    """Reads/writes the orchestrator settings against the existing Vault keys."""

    def __init__(self, ctx, service):
        self._ctx = ctx
        self._service = service

    # --- credential registry (dynamic select options) ------------------------
    def credential_aliases(self) -> list[str]:
        raw = self._ctx.get_secret("iac_token_registry")
        try:
            return list(json.loads(raw)) if raw else []
        except Exception:
            return []

    def _load_repo(self, slug: str) -> dict:
        raw = self._ctx.get_secret(f"repo_{slug}_config")
        if raw:
            try:
                return json.loads(raw)
            except Exception:
                pass
        return {"url": "", "token_key": ""}

    def _read_raw(self, f: SettingField):
        if f.repo_slug:
            return self._load_repo(f.repo_slug).get(f.repo_subkey, "")
        raw = self._ctx.get_secret(f.storage_key())
        if raw is None and f.state_key and self._service is not None:
            # Vault (shared by every instance) is authoritative; the in-memory
            # mirror is only the fallback while Vault has no value.
            return self._service.state.get(f.state_key)
        return raw

    def _write_raw(self, f: SettingField, value: str) -> None:
        if f.repo_slug:
            cfg = self._load_repo(f.repo_slug)
            cfg[f.repo_subkey] = value
            self._ctx.set_secret(f"repo_{f.repo_slug}_config", json.dumps(cfg))
            return
        self._ctx.set_secret(f.storage_key(), value)

    # --- validation ------------------------------------------------------------
    def allowed_git_hosts(self) -> set[str]:
        """Hosts a GitLab/git URL may point at. Deployment setting when given;
        otherwise the built-in GitLab host plus every host already configured —
        a URL can then be edited, but pointing it at a *new* host (where the API
        token would be sent) needs the deployment allowlist."""
        raw = os.getenv(ALLOWED_GIT_HOSTS_ENV)
        if raw is not None:
            return {h.strip().lower() for h in raw.split(",") if h.strip()}
        hosts = {urlsplit(_DEFAULT_GITLAB_URL).hostname}
        current = [self._ctx.get_secret("iac_gitlab_url")]
        current += [self._load_repo(slug).get("url") for slug, _ in _REPO_ROLES]
        hosts.update(h for h in (_url_host(u) for u in current) if h)
        return {h for h in hosts if h}

    def _validate(self, f: SettingField, value: str) -> None:
        """Raise :class:`SettingsValidationError` unless ``value`` (already
        normalised to its stored string) satisfies the rule of ``f``."""
        if any(ord(ch) < 32 for ch in value):
            raise SettingsValidationError(f.key, "must not contain control characters")
        rule = f.rule
        if not rule or (value == "" and rule != "runner_image"):
            return
        error: str | None = None
        if rule == "gitlab_url":
            error = _http_url_error(value, allowed_hosts=self.allowed_git_hosts())
        elif rule == "base_url":
            error = _http_url_error(value, allowed_hosts=None)
        elif rule == "git_url":
            scp = _SCP_GIT_RE.fullmatch(value)
            if scp:
                host = scp.group(1).lower()
                if host not in self.allowed_git_hosts():
                    error = f"host '{host}' is not allowed ({ALLOWED_GIT_HOSTS_ENV})"
            elif value.startswith("ssh://"):
                host = _url_host(value)
                if not host or host not in self.allowed_git_hosts():
                    error = f"host '{host}' is not allowed ({ALLOWED_GIT_HOSTS_ENV})"
            else:
                # http(s) only — never git remote helpers (ext::, file://, fd::).
                error = _http_url_error(value, allowed_hosts=self.allowed_git_hosts())
        elif rule == "credential_ref":
            if value not in self.credential_aliases() or not is_valid_credential_alias(value):
                error = "must be one of the stored credential aliases (Settings → Credentials)"
        elif rule == "runner_image":
            error = runner_image_error(value)
        elif rule == "registry":
            if not _REGISTRY_RE.fullmatch(value):
                error = "must be a registry host (host[:port])"
            elif registry_host(value) not in allowed_registry_hosts():
                error = (f"registry '{registry_host(value)}' is not part of the runner image allowlist "
                         f"({', '.join(sorted(allowed_registry_hosts())) or 'empty'})")
        elif rule == "host_list":
            bad = [h for h in (p.strip() for p in value.split(",")) if h and not _HOST_TOKEN_RE.fullmatch(h)]
            if bad:
                error = f"invalid host token(s): {', '.join(bad)}"
        elif rule == "choice":
            if value not in f.options:
                error = f"must be one of {', '.join(f.options)}"
        elif rule == "digits":
            if not value.isdigit():
                error = "must be numeric"
        elif rule == "runtime_limit":
            # Seconds: 0 switches the limit off; a small positive value would
            # stop every ordinary job, so it is refused rather than clamped silently.
            if not value.isdigit() or 0 < int(value) < 600:
                error = "must be 0 (no limit) or at least 600 seconds"
        elif rule == "plain":
            if not _PLAIN_RE.fullmatch(value):
                error = "contains unsupported characters"
        if error:
            raise SettingsValidationError(f.key, error)

    def _normalise(self, f: SettingField, val: Any) -> str:
        """Submitted value → the string that would be stored."""
        if f.kind == "bool":
            b = _as_bool(val, bool(f.default))
            return str(b) if f.bool_format == "python" else ("true" if b else "false")
        if f.kind == "int":
            try:
                iv = int(float(str(val).strip()))
            except (TypeError, ValueError):
                raise SettingsValidationError(f.key, "must be a number")
            if f.minimum is not None and iv < f.minimum:
                raise SettingsValidationError(f.key, f"must be at least {f.minimum}")
            if f.maximum is not None and iv > f.maximum:
                raise SettingsValidationError(f.key, f"must be at most {f.maximum}")
            return str(iv)
        sval = ("" if val is None else str(val)).strip()
        if not sval and f.coalesce_default:
            sval = f.coalesce_default
        return sval

    def _effective(self, f: SettingField) -> str:
        """Currently effective value in stored-string form (defaults applied, as
        ``get_values`` reports them) — the baseline for "did this change?"."""
        raw = self._read_raw(f)
        if f.kind == "bool":
            b = _as_bool(raw, bool(f.default))
            return str(b) if f.bool_format == "python" else ("true" if b else "false")
        if f.kind == "int":
            return str(_safe_int(raw, int(f.default or 0)))
        return str(raw if raw not in (None, "") else (f.default or "")).strip()

    # --- public surface ------------------------------------------------------
    def get_schema(self) -> list[dict]:
        creds = [""] + self.credential_aliases()
        schema = []
        for f in SETTINGS_FIELDS:
            schema.append({
                "key": f.key,
                "label": f.label,
                "kind": f.kind,
                "category": f.category,
                "sensitive": f.sensitive,
                "default": f.default,
                "description": f.description,
                "options": creds if f.options_source == "credentials" else f.options,
                # Which permission tier a change needs; secret material is written
                # through its own route (POST /settings/secrets/<key>).
                "tier": f.tier,
                "secret_slot": f.sensitive,
            })
        return schema

    def get_values(self) -> dict:
        """Current values. Secrets are masked; ``<key>__configured`` / ``<key>__locked`` added."""
        vals: dict[str, Any] = {}
        for f in SETTINGS_FIELDS:
            raw = self._read_raw(f)
            # Surface env-shadowed settings so the UI can show them as locked
            # instead of letting an operator "save" a value that never applies.
            if f.is_env_locked():
                vals[f"{f.key}__locked"] = True
            if f.sensitive:
                vals[f.key] = ""
                vals[f"{f.key}__configured"] = bool(raw)
                continue
            if f.kind == "bool":
                vals[f.key] = _as_bool(raw, bool(f.default))
            elif f.kind == "int":
                vals[f.key] = _safe_int(raw, int(f.default or 0))
            else:
                vals[f.key] = raw if raw not in (None, "") else f.default
        return vals

    def save_values(self, updates: dict, *, allowed_tiers: set[str] | None = None) -> dict:
        """Validate and persist the provided keys — all or nothing.

        Unknown keys are ignored. A value equal to the effective one is skipped
        (not validated, not authorised, not written). Secret slots are refused
        unless blank. ``allowed_tiers`` is the set of tiers the caller may change
        (``None`` = trusted in-process caller).

        Raises :class:`SettingsValidationError` / :class:`SettingsPermissionError`
        before anything is written. Returns ``{"saved", "locked", "unchanged"}``;
        ``locked`` lists keys shadowed by an OS env var (a Vault write would be a
        silent no-op — env always wins in IaCConfig._get).
        """
        locked: list[str] = []
        unchanged: list[str] = []
        plan: list[tuple[SettingField, str, str]] = []
        for key, val in (updates or {}).items():
            f = _FIELDS_BY_KEY.get(key)
            if f is None:
                continue
            if f.sensitive:
                sval = ("" if val is None else str(val)).strip()
                if not sval or SECRET_SENTINEL in sval:
                    continue  # blank/sentinel -> keep existing secret
                raise SettingsValidationError(
                    f.key, "is a secret slot and cannot be written here — use "
                           f"POST /settings/secrets/{f.key} (credentials permission)")
            if f.is_env_locked():
                locked.append(f.key)
                continue
            stored = self._normalise(f, val)
            closes_gate = f.safe_value is not None and stored == f.safe_value
            if not closes_gate and stored == self._effective(f):
                unchanged.append(f.key)
                continue
            self._validate(f, stored)
            plan.append((f, stored, TIER_OPERATOR if closes_gate else f.tier))

        if allowed_tiers is not None:
            for tier in sorted({t for _, _, t in plan} - set(allowed_tiers) - {TIER_OPERATOR}):
                raise SettingsPermissionError(tier, [f.key for f, _, t in plan if t == tier])

        saved: list[str] = []
        for f, stored, _tier in plan:
            self._write_raw(f, stored)
            if f.state_key and self._service is not None and f.kind == "bool":
                self._service.state[f.state_key] = _as_bool(stored)
            saved.append(f.key)
        return {"saved": saved, "locked": locked, "unchanged": unchanged}

    def set_secret_slot(self, key: str, value: Any) -> None:
        """Store secret material (SSH key, password, token) in its slot."""
        f = _FIELDS_BY_KEY.get(str(key))
        if f is None or not f.sensitive:
            raise SettingsValidationError(str(key), "is not a secret slot")
        if f.is_env_locked():
            raise SettingsValidationError(f.key, "is pinned by the environment")
        sval = ("" if value is None else str(value)).strip()
        if not sval or SECRET_SENTINEL in sval:
            raise SettingsValidationError(f.key, "a value is required")
        if len(sval.encode("utf-8", "replace")) > MAX_SECRET_BYTES:
            raise SettingsValidationError(f.key, f"is larger than {MAX_SECRET_BYTES} bytes")
        if "\x00" in sval:
            raise SettingsValidationError(f.key, "must not contain NUL bytes")
        self._write_raw(f, sval)
