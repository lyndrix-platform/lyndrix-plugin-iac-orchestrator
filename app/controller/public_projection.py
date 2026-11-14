"""Public projections of SSoT-derived data (audit OS-01).

Everything the plugin hands to somebody else — the monitoring inventory sync,
bus events for other plugins, the repo snapshots every instance serves to
viewers — is built from the generated inventory and the iac-controller tree.
Those carry credentials in plain text: the merged ``terraform`` block of a
hypervisor holds the Proxmox password/API token/SSH key, a service's ``config``
block holds its ``secrets``, host vars hold ``ansible_password``/``vault_vars``.

Rule: outbound payloads are assembled from **allowlists** (this module), never
by passing a source block through. ``scrub`` is the second line of defence for
free-form blocks that must keep their structure (maintenance windows).
"""
from __future__ import annotations

from typing import Any
from urllib.parse import urlsplit, urlunsplit

#: Placement/sizing facts of a host's terraform block that are safe to publish.
#: Deliberately no auth fields (``username``, ``realm``, ``auth_type``,
#: ``token_id``, ``ssh_agent``) — consumers only need to identify the guest.
PUBLIC_TERRAFORM_KEYS = (
    "node_name", "vm_id", "is_managed", "is_used", "provider",
    "cores", "ram", "swap", "disk", "disk_storage",
    "bridge", "vlan", "ip", "gateway", "nameserver",
    "tags", "unprivileged", "nesting", "startup_order",
)

#: Fields of a service-catalog entry the UI needs (name, repo, routing hints).
PUBLIC_CATALOG_KEYS = (
    "name", "repository_name", "branch", "target_environment", "host",
    "deploy_type", "state", "description",
)

_SECRET_KEY_EXACT = frozenset({
    "password", "passwd", "pass", "pw", "token", "key", "secret", "secrets",
    "credentials", "credential", "authorization", "auth", "vault_vars", "token_id",
})
_SECRET_KEY_PARTS = ("password", "passwd", "secret", "token", "private", "credential", "apikey")
_SECRET_KEY_SUFFIXES = ("_key", "_pass", "_pw", "_auth")
_SCALARS = (str, int, float, bool)
_MAX_DEPTH = 8


def is_secret_key(key: Any) -> bool:
    """Does a mapping key look like it holds a credential?"""
    lk = str(key).strip().lower()
    return (
        lk in _SECRET_KEY_EXACT
        or any(part in lk for part in _SECRET_KEY_PARTS)
        or lk.endswith(_SECRET_KEY_SUFFIXES)
    )


def public_scalar(value: Any) -> Any:
    """``value`` when it is a plain scalar, else ``None`` (never a nested block)."""
    return value if value is None or isinstance(value, _SCALARS) else None


def public_names(value: Any) -> list[str]:
    """A list of plain names (roles, profiles, groups) — nested entries are dropped."""
    if not isinstance(value, (list, tuple)):
        return []
    return [str(v) for v in value if isinstance(v, (str, int)) and not isinstance(v, bool)]


def public_url(value: Any) -> str | None:
    """A URL without embedded credentials (``https://user:token@host`` → ``https://host``)."""
    if not isinstance(value, str) or not value:
        return None
    try:
        parts = urlsplit(value)
    except ValueError:
        return None
    if parts.scheme and "@" in parts.netloc:
        host = parts.netloc.rsplit("@", 1)[1]
        return urlunsplit((parts.scheme, host, parts.path, parts.query, parts.fragment))
    return value


def public_terraform(block: Any) -> dict:
    """Allowlisted view of a host's terraform block: known placement keys with
    scalar values (or flat lists of scalars) only — nothing nested survives, so
    a credential at any depth cannot pass."""
    out: dict = {}
    if not isinstance(block, dict):
        return out
    for key in PUBLIC_TERRAFORM_KEYS:
        if key not in block:
            continue
        value = block[key]
        if value is None or isinstance(value, _SCALARS):
            out[key] = value
        elif isinstance(value, (list, tuple)) and all(isinstance(v, _SCALARS) for v in value):
            out[key] = list(value)
    return out


def public_catalog_entry(entry: Any) -> dict | None:
    """Allowlisted view of one service-catalog entry (``None`` for a non-mapping)."""
    if not isinstance(entry, dict):
        return None
    return {k: public_scalar(entry[k]) for k in PUBLIC_CATALOG_KEYS if k in entry and public_scalar(entry[k]) is not None}


def scrub(value: Any, _depth: int = 0) -> Any:
    """Copy of a free-form block without credential-like keys at any depth.

    For blocks whose structure consumers evaluate (maintenance windows, incident
    rules) and that therefore cannot be reduced to a fixed key list.
    """
    if _depth > _MAX_DEPTH:
        return None
    if isinstance(value, dict):
        return {k: scrub(v, _depth + 1) for k, v in value.items() if not is_secret_key(k)}
    if isinstance(value, (list, tuple)):
        return [scrub(v, _depth + 1) for v in value]
    return public_scalar(value)


__all__ = [
    "PUBLIC_TERRAFORM_KEYS", "PUBLIC_CATALOG_KEYS", "is_secret_key", "public_scalar",
    "public_names", "public_url", "public_terraform", "public_catalog_entry", "scrub",
]
