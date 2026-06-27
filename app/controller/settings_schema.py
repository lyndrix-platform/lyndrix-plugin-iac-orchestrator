"""Schema-driven, fully API-controllable settings surface for the IaC Orchestrator.

This is the single source of truth for every operator-tunable setting the plugin
exposes. The REST API renders its schema/values/save from here, so "all settings are
API-controllable" holds by construction — the same fields the NiceGUI offers.

Persistence intentionally reuses the EXISTING Vault keys the engine already reads
(``ctx.get_secret``/``set_secret``: ``iac_*``, ``ansible_*``, ``repo_<slug>_config`` …),
so no engine read-site changes and behaviour is identical to the NiceGUI settings page.

Secrets are never returned in plaintext: ``get_values`` masks them and reports a
``<key>__configured`` flag instead; ``save_values`` treats an empty or sentinel value
as "keep existing" (so a blank field never clobbers a stored secret).
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field as _dc_field
from typing import Any

# Matches the NiceGUI "leave blank to change" convention for secret inputs.
SECRET_SENTINEL = "********"

_DEFAULT_ANSIBLE_IMAGE = (
    "registry.gitlab.int.fam-feser.de/iac-environment/"
    "iac-platform-assets/ansible-ci-image:latest"
)
_DEFAULT_TOFU_IMAGE = (
    "registry.gitlab.int.fam-feser.de/iac-environment/"
    "iac-platform-assets/opentofu-ci-image:latest"
)
_TOFU_FALLBACK_IMAGE = "ghcr.io/opentofu/opentofu:latest"


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

    def storage_key(self) -> str:
        return self.vault_key or self.key

    def is_env_locked(self) -> bool:
        """True when an OS env var shadows this setting (Vault writes are inert)."""
        import os
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
            description="Execute immediately when a verified webhook arrives, without a manual plan gate.",
        ),
        SettingField(
            "test_deploy_allowed_hosts", "Test-Deploy allowed hosts", category="Pipeline",
            vault_key="iac_test_deploy_allowed_hosts",
            env_var="PLUGIN_IAC_ORCHESTRATOR_TEST_DEPLOY_ALLOWED_HOSTS",
            description="Comma-separated allowlist of hosts permitted as ad-hoc test-deploy targets.",
        ),
        # ── GitLab Webhooks ───────────────────────────────────────────────────
        SettingField(
            "gitlab_url", "GitLab Base URL", category="GitLab Webhooks",
            vault_key="iac_gitlab_url", default="https://gitlab.int.fam-feser.de",
        ),
        SettingField(
            "group_id", "GitLab Group ID", category="GitLab Webhooks",
            vault_key="iac_gitlab_group_id",
            description="Group whose project webhooks are reconciled by the auto-sync loop.",
        ),
        SettingField(
            "lyndrix_base_url", "Lyndrix Base URL", category="GitLab Webhooks",
            vault_key="iac_lyndrix_base_url", default="http://10.1.10.31:8081",
            description="Public base URL GitLab calls back for webhooks.",
        ),
        SettingField(
            "gitlab_token_key", "GitLab API Credential", kind="select",
            category="GitLab Webhooks", vault_key="iac_gitlab_api_token_key",
            options_source="credentials",
            description="Stored credential alias used for GitLab API calls (webhook upsert).",
        ),
        SettingField(
            "autosync_enabled", "Auto-sync webhooks", kind="bool",
            category="GitLab Webhooks", vault_key="iac_webhook_autosync_enabled",
            default=True, bool_format="lower",
        ),
        SettingField(
            "sync_interval", "Auto-sync interval (s)", kind="int",
            category="GitLab Webhooks", vault_key="iac_webhook_sync_interval_seconds",
            default=1800, description="Seconds between webhook reconciliation runs (min 300).",
        ),
        # ── Ansible ───────────────────────────────────────────────────────────
        SettingField(
            "ansible_docker_image", "Ansible Runner Image", category="Ansible",
            vault_key="ansible_docker_image", default=_DEFAULT_ANSIBLE_IMAGE,
            env_var="PLUGIN_IAC_ORCHESTRATOR_ANSIBLE_IMAGE",
            description="Container image used to execute Ansible playbooks.",
        ),
        SettingField(
            "ansible_ssh_key", "Ansible SSH Private Key", kind="textarea",
            category="Ansible", sensitive=True, vault_key="ansible_ssh_key",
            description="RSA private key the runner uses to reach managed hosts.",
        ),
        SettingField(
            "ansible_registry_url", "Private Registry URL", category="Ansible",
            vault_key="ansible_registry_url",
        ),
        SettingField(
            "ansible_registry_user", "Registry Username", category="Ansible",
            vault_key="ansible_registry_user",
        ),
        SettingField(
            "ansible_registry_token", "Registry Token/Password", kind="password",
            category="Ansible", sensitive=True, vault_key="ansible_registry_token",
        ),
        # ── Terraform ─────────────────────────────────────────────────────────
        SettingField(
            "iac_terraform_docker_image", "OpenTofu Runner Image", category="Terraform",
            vault_key="iac_terraform_docker_image", default=_DEFAULT_TOFU_IMAGE,
            coalesce_default=_TOFU_FALLBACK_IMAGE,
            env_var="PLUGIN_IAC_ORCHESTRATOR_TERRAFORM_IMAGE",
            description="Image for every tofu init/plan/apply. Blank falls back to the upstream OpenTofu image.",
        ),
        SettingField(
            "iac_tf_ssh_key", "Root SSH Public Key", kind="textarea", category="Terraform",
            sensitive=True, vault_key="iac_tf_ssh_key",
            description="Public key injected into newly provisioned hosts.",
        ),
        SettingField(
            "iac_tf_ssh_private_key", "Root SSH Private Key", kind="textarea",
            category="Terraform", sensitive=True, vault_key="iac_tf_ssh_private_key",
            description="Private counterpart used for the first bootstrap/compliance run.",
        ),
        SettingField(
            "iac_tf_root_password", "Root Password (new host)", kind="password",
            category="Terraform", sensitive=True, vault_key="iac_tf_root_password",
        ),
        SettingField(
            "iac_tf_backend_secret_key", "State Backend Secret Key (S3/MinIO)",
            kind="password", category="Terraform", sensitive=True,
            vault_key="iac_tf_backend_secret_key",
            description="Shared backend secret across all sites/stages.",
        ),
    ]

    # ── Repository Roles (each persisted into repo_<slug>_config JSON) ─────────
    for slug, label in _REPO_ROLES:
        fields.append(SettingField(
            f"repo_{slug}_url", f"{label} — Git URL", category="Repository Roles",
            repo_slug=slug, repo_subkey="url",
        ))
        fields.append(SettingField(
            f"repo_{slug}_token_key", f"{label} — Credential", kind="select",
            category="Repository Roles", options_source="credentials",
            repo_slug=slug, repo_subkey="token_key",
        ))
    return fields


SETTINGS_FIELDS: list[SettingField] = _build_fields()
_FIELDS_BY_KEY = {f.key: f for f in SETTINGS_FIELDS}


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
        if f.state_key and self._service is not None:
            return self._service.state.get(f.state_key)
        return self._ctx.get_secret(f.storage_key())

    def _write_raw(self, f: SettingField, value: str) -> None:
        if f.repo_slug:
            cfg = self._load_repo(f.repo_slug)
            cfg[f.repo_subkey] = value
            self._ctx.set_secret(f"repo_{f.repo_slug}_config", json.dumps(cfg))
            return
        self._ctx.set_secret(f.storage_key(), value)

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

    def save_values(self, updates: dict) -> dict:
        """Persist provided keys. Unknown keys are ignored; secrets blank = keep.

        Returns ``{"saved": [...], "locked": [...]}`` where ``locked`` lists keys
        whose value was NOT written because an OS env var shadows them (writing to
        Vault would be a silent no-op — env always wins in IaCConfig._get).
        """
        saved: list[str] = []
        locked: list[str] = []
        for key, val in (updates or {}).items():
            f = _FIELDS_BY_KEY.get(key)
            if f is None:
                continue
            if f.is_env_locked():
                locked.append(f.key)
                continue
            if f.sensitive:
                sval = ("" if val is None else str(val)).strip()
                if not sval or SECRET_SENTINEL in sval:
                    continue  # blank/sentinel -> keep existing secret
                self._write_raw(f, sval)
                saved.append(f.key)
                continue
            if f.kind == "bool":
                b = _as_bool(val, bool(f.default))
                stored = str(b) if f.bool_format == "python" else ("true" if b else "false")
                self._write_raw(f, stored)
                if f.state_key and self._service is not None:
                    self._service.state[f.state_key] = b
                saved.append(f.key)
            elif f.kind == "int":
                iv = _safe_int(val, int(f.default or 0))
                if f.key == "sync_interval":
                    iv = max(300, iv)
                self._write_raw(f, str(iv))
                saved.append(f.key)
            else:
                sval = ("" if val is None else str(val)).strip()
                if not sval and f.coalesce_default:
                    sval = f.coalesce_default
                self._write_raw(f, sval)
                saved.append(f.key)
        return {"saved": saved, "locked": locked}
