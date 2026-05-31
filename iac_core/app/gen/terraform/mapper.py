"""
Pure mapping functions: validated SSoT config  ->  Terraform state dict.

No I/O, no globals, fully unit-testable. Two concerns:

  * ``build_providers`` -- every ``hardware_host`` with ``terraform.is_used``
    becomes a provider connection entry (Proxmox endpoint metadata, no secrets).
  * ``build_containers`` -- every ``host`` with ``terraform.is_managed`` becomes a
    fully-defaulted LXC container object matching the reference proxmox_lxc
    schema, carrying ``roles``/``services`` for the downstream Ansible bridge.

The functions never raise on a single bad host; they skip it and record a
warning so one malformed entry can't abort the whole generation.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List

from .schema import (
    CONTAINER_DEFAULTS,
    CONTAINER_PASSTHROUGH,
    CONTAINER_REQUIRED,
    KEY_CONTAINERS,
    KEY_PROVIDERS,
    PROVIDER_PUBLIC_FIELDS,
)

_log = logging.getLogger("IaC:Generator:TerraformGen")


def _truthy(value: Any) -> bool:
    """Tolerant boolean coercion (handles real bools and 'true'/'false' strings)."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes", "on")
    return bool(value)


def _collect_roles(host: Dict[str, Any]) -> List[str]:
    """Merge baseline_roles + roles + host_roles, de-duplicated, order preserved."""
    seen: Dict[str, None] = {}
    for key in ("baseline_roles", "roles", "host_roles"):
        for role in host.get(key) or []:
            if isinstance(role, str) and role and role not in seen:
                seen[role] = None
    return list(seen.keys())


def _collect_services(host: Dict[str, Any]) -> List[str]:
    """Extract service names for the Terraform -> Ansible metadata bridge."""
    names: List[str] = []
    for svc in host.get("services") or []:
        if isinstance(svc, dict) and svc.get("name"):
            names.append(svc["name"])
        elif isinstance(svc, str) and svc:
            names.append(svc)
    return names


def build_providers(config: Dict[str, Any], log: logging.Logger = _log) -> Dict[str, Any]:
    """Map hardware_hosts with terraform.is_used into provider connection entries."""
    providers: Dict[str, Any] = {}
    for name, details in (config.get("hardware_hosts") or {}).items():
        if not isinstance(details, dict):
            continue
        tf = details.get("terraform")
        if not isinstance(tf, dict) or not _truthy(tf.get("is_used")):
            continue

        entry: Dict[str, Any] = {}
        for field in PROVIDER_PUBLIC_FIELDS:
            if tf.get(field) is not None:
                entry[field] = tf[field]
        # Endpoint defaults to the node's management address when not explicit.
        # bpg/proxmox requires a full URL (https://HOST:8006).
        if "endpoint" not in entry:
            addr = tf.get("endpoint") or details.get("ansible_host") or details.get("ip")
            if addr:
                entry["endpoint"] = addr
        if "endpoint" in entry:
            ep = entry["endpoint"]
            if ep and not ep.startswith("http"):
                entry["endpoint"] = f"https://{ep}:8006"
        entry["node_name"] = name
        providers[name] = entry
    return providers


def build_container(name: str, host: Dict[str, Any], log: logging.Logger = _log) -> Dict[str, Any]:
    """
    Build one fully-defaulted container object, or return ``{}`` if the host is
    missing a required field (caller skips it).
    """
    tf = host.get("terraform")
    if not isinstance(tf, dict):
        return {}

    missing = [f for f in CONTAINER_REQUIRED if tf.get(f) in (None, "")]
    if missing:
        log.warning(
            f"Terraform: managed host '{name}' is missing required field(s) "
            f"{missing}; skipping to avoid an incomplete resource."
        )
        return {}

    container: Dict[str, Any] = dict(CONTAINER_DEFAULTS)
    container["hostname"] = host.get("hostname") or name

    for field in CONTAINER_PASSTHROUGH:
        if tf.get(field) is not None:
            container[field] = tf[field]

    # Description / tags can also live at host level.
    if not container.get("description"):
        container["description"] = host.get("description") or ""
    if not container.get("tags"):
        container["tags"] = list(host.get("tags") or [])

    container["roles"] = _collect_roles(host)
    container["services"] = _collect_services(host)
    return container


def build_containers(config: Dict[str, Any], log: logging.Logger = _log) -> Dict[str, Any]:
    """Map hosts with terraform.is_managed into the container map."""
    containers: Dict[str, Any] = {}
    for name, host in (config.get("hosts") or {}).items():
        if not isinstance(host, dict):
            continue
        tf = host.get("terraform")
        if not isinstance(tf, dict) or not _truthy(tf.get("is_managed")):
            continue
        obj = build_container(name, host, log)
        if obj:
            containers[name] = obj
    return containers


def build_terraform_state(config: Dict[str, Any], log: logging.Logger = _log) -> Dict[str, Any]:
    """Assemble the complete, secret-free Terraform state dict for one environment."""
    return {
        KEY_PROVIDERS: build_providers(config, log),
        KEY_CONTAINERS: build_containers(config, log),
    }
