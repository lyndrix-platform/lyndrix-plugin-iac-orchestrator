"""Pure inventory diff for the ``service_reconcile`` rule action (plugin 1.7 / 1.5.8).

Compares two renders of a generated stage inventory (``<site>/<stage>/ansible/inventory.yml``)
host by host and reports which *service entries* were added or mutated — that is what a
profile change in the iac-controller ends up as after ``iac_core`` resolved the profiles into
every host. Standalone importable: no core imports, no YAML parsing (callers hand in dicts),
so it is testable like ``rules.py``.

Secrets: the service entries carry ``config.secrets`` in plaintext. Nothing here ever returns
or formats a *value* — only key paths (``config.vars.WAZUH_MANAGER``) and host/service names.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field

#: Marker key path for a service entry that is new on a host (no previous entry to diff).
ADDED_MARKER = "+service"
#: Upper bound of key paths reported per service (log/notification size).
MAX_CHANGED_KEYS = 50

_BRANCH_FOR_STAGE = {"prod": "main", "dev": "dev", "test": "test"}

#: Services the ``service_reconcile`` action never dispatches unless the operator clears the
#: setting ``service_reconcile_exclude_services``. The single_service playbook tears the whole
#: compose stack down and recreates it (cd_rollout_single_service.yml: compose down + ``docker rm
#: -f`` by name prefix) — for the AI agent (aac-hermes-agent) that is a hard outage mid-session,
#: so a controller diff only *reports* it and the operator deploys deliberately.
DEFAULT_EXCLUDE_SERVICES = ("aac-hermes-agent",)


def normalize_service_names(values) -> set[str]:
    """Lower-cased, stripped service names from a comma/semicolon/newline separated string or
    an iterable (``None``/empty → empty set). The one normalisation every exclude-list check
    uses (setting, rule parameter, approval, deferred drain), so a stray space or capital letter
    in the settings dialog never un-protects a service."""
    if values is None:
        return set()
    if isinstance(values, str):
        parts = values.replace(";", ",").replace("\n", ",").split(",")
    else:
        parts = []
        for value in values:
            if isinstance(value, str):
                parts.extend(value.replace(";", ",").split(","))
            elif value is not None:
                parts.append(str(value))
    return {p.strip().lower() for p in parts if p and p.strip()}


def is_excluded(service, excluded: set[str]) -> bool:
    """``True`` when ``service`` is in the (normalised) exclude set — case-insensitive."""
    return str(service or "").strip().lower() in excluded


@dataclass
class ServiceChange:
    """One service whose rendered entry changed on at least one host (added or mutated)."""

    service: str
    hosts: list[str] = field(default_factory=list)
    changed_keys: list[str] = field(default_factory=list)

    def add(self, host: str, keys: list[str]) -> None:
        if host not in self.hosts:
            self.hosts.append(host)
        for k in keys:
            if k not in self.changed_keys and len(self.changed_keys) < MAX_CHANGED_KEYS:
                self.changed_keys.append(k)

    def as_dict(self) -> dict:
        return {"service": self.service, "hosts": sorted(self.hosts), "changed_keys": list(self.changed_keys)}


def inventory_path_scope(path: str) -> tuple[str, str] | None:
    """``onprem/prod/ansible/inventory.yml`` → ``("onprem", "prod")``; anything else → ``None``.

    Only the per-stage inventories are diffed. ``global/ansible/inventory.yml`` contains the
    same hosts again and would count every change twice.
    """
    parts = str(path or "").strip("/").split("/")
    if len(parts) != 4 or parts[2] != "ansible" or parts[3] != "inventory.yml":
        return None
    site, stage = parts[0], parts[1]
    if not site or not stage or site == "global":
        return None
    return site, stage


def branch_for_stage(stage: str) -> str:
    """Service repo branch a stage deploys from (mirrors ``SingleServiceStrategy``)."""
    return _BRANCH_FOR_STAGE.get(str(stage or "").lower(), "main")


def host_services(inventory: dict) -> dict[str, dict[str, dict]]:
    """``{host: {service_name: entry}}`` for every host with a ``services`` list.

    Walks ``all.hosts`` and every nested ``children.*.hosts`` (group memberships carry
    ``None`` as host value and are ignored). ``services: null`` → ``{}``.
    """
    result: dict[str, dict[str, dict]] = {}

    def _walk(node) -> None:
        if not isinstance(node, dict):
            return
        hosts = node.get("hosts")
        if isinstance(hosts, dict):
            for host, hv in hosts.items():
                if not isinstance(hv, dict) or "services" not in hv:
                    continue
                bucket = result.setdefault(str(host), {})
                for svc in (hv.get("services") or []):
                    if isinstance(svc, dict) and svc.get("name"):
                        bucket[str(svc["name"])] = svc
        children = node.get("children")
        if isinstance(children, dict):
            for child in children.values():
                _walk(child)

    if isinstance(inventory, dict):
        _walk(inventory)
        for top in inventory.values():
            _walk(top)
    return result


def _canonical(entry) -> str:
    return json.dumps(entry, sort_keys=True, default=str)


def changed_key_paths(old, new, prefix: str = "") -> list[str]:
    """Key paths whose value differs between ``old`` and ``new`` (dicts are descended,
    everything else is compared as a whole). Never includes values."""
    if isinstance(old, dict) and isinstance(new, dict):
        out: list[str] = []
        for key in sorted(set(old) | set(new), key=str):
            sub = f"{prefix}.{key}" if prefix else str(key)
            if key not in old or key not in new:
                out.append(sub)
            else:
                out.extend(changed_key_paths(old[key], new[key], sub))
        return out
    if _canonical(old) != _canonical(new):
        return [prefix or "<entry>"]
    return []


def diff_host_services(old_inventory: dict, new_inventory: dict) -> tuple[dict[str, ServiceChange], dict[str, list[str]]]:
    """Diff two inventory renders.

    Returns ``(changed, removed)``:

    * ``changed`` — ``{service: ServiceChange}`` for entries that are new on a host or whose
      canonical JSON differs (order noise does not count).
    * ``removed`` — ``{service: [hosts]}`` for entries that disappeared (reported only; orphan
      cleanup is not implemented).

    Hosts absent from the *old* inventory are skipped: a brand-new host is provisioned by the
    ``host_provision`` pipeline, not patched here (same rule as ``baseline_role``).
    """
    old_map = host_services(old_inventory)
    new_map = host_services(new_inventory)
    changed: dict[str, ServiceChange] = {}
    removed: dict[str, list[str]] = {}
    for host, new_entries in new_map.items():
        if host not in old_map:
            continue
        old_entries = old_map[host]
        for name, entry in new_entries.items():
            if name not in old_entries:
                changed.setdefault(name, ServiceChange(name)).add(host, [ADDED_MARKER])
                continue
            keys = changed_key_paths(old_entries[name], entry)
            if keys:
                changed.setdefault(name, ServiceChange(name)).add(host, keys)
        for name in old_entries:
            if name not in new_entries:
                removed.setdefault(name, []).append(host)
    for hosts in removed.values():
        hosts.sort()
    return changed, removed


def summarize(changed: dict[str, ServiceChange], removed: dict[str, list[str]]) -> str:
    """One-line-per-service summary for job logs: names, host counts and key paths only."""
    lines = []
    for name in sorted(changed):
        ch = changed[name]
        lines.append(f"{name}: {len(ch.hosts)} host(s), keys: {', '.join(ch.changed_keys) or '-'}")
    for name in sorted(removed):
        lines.append(f"{name}: removed on {len(removed[name])} host(s) (no cleanup)")
    return "; ".join(lines) if lines else "no service entry changed"
