"""Maintenance windows (plugin 1.6, core API 1.13): resolve the *effective* windows,
deploy policies and incident rules of every hypervisor, host and service from the
iac-controller tree plus the ``service.yml`` files the runner has cloned.

Pure functions — no database, no bus, no ``ctx``. The engine's publisher
(``DeploymentEngine.publish_maintenance``) and the ``MaintenancePolicyStage`` call them;
the tests run them against temporary directories. The vocabulary (window formats,
time evaluation, incident-rule cascade, topic names) is ``core.api.maintenance``.

Resolution chain (``00-ergebnis-p0.md`` §2.3 — the most specific level wins, lists
are *replaced*, never merged):

* service:    ``service.yml`` (+ ``stage_overrides[<stage>]``) → controller
              ``hosts[].services[].maintenance`` → host ``maintenance`` (unless
              ``inherit: false``) → stage ``maintenance_default`` (if ``enabled``) →
              site ``maintenance_default`` → none
* host:       own ``maintenance`` → stage default → site default → none
* hypervisor: own ``maintenance`` → site default → none (hardware sees no stage vars)
* incidents:  own block → ``incident_defaults[<kind>]`` of the stage → of the site → none
* deploy_policy: service → stage default (``prod`` → ``window``, else ``immediate``)

An invalid block is logged and *ignored* (the chain continues) — resolution never
aborts a deploy. Kinds are the Entity-Registry kinds and open (``is_valid_kind``).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional

import yaml

from core.api import maintenance as mw

log = logging.getLogger("IaC:Maintenance")

#: ``source`` of every maintenance event this plugin emits.
SOURCE = "iac-orchestrator"

#: Triggers whose deploys follow the service's ``deploy_policy`` (``window`` → park
#: until the next window). Everything else — UI catalog button, NiceGUI, host
#: rollouts, "run now" (``force``) — runs at once.
#: ``controller-diff`` (1.7): jobs the ``service_reconcile`` rule queues from an iac-controller
#: change are automatic deploys too — prod services with ``deploy_policy: window`` are parked.
POLICY_TRIGGERS = frozenset({"gitlab-ci", "webhook", "gitlab_pipeline_success", "controller-diff"})

#: Stages whose services default to ``deploy_policy: window``.
WINDOW_DEFAULT_STAGES = frozenset({"prod"})

#: Commit-message prefix reserved for the (future) SSoT-Manager default switch:
#: such a controller commit syncs but never triggers a deploy (loop guard).
MAINTENANCE_COMMIT_PREFIX = "ci: maintenance"

# ``("host", <name>)`` / ``("service", <host>, <service>)`` → Entity-Registry UUID.
CiMap = Mapping[tuple, str]


def policy_applies(payload: Mapping[str, Any] | None) -> bool:
    """Does the maintenance deploy policy govern this trigger payload?

    ``force`` (run now / promoted job) and non-webhook triggers bypass it.
    """
    p = payload or {}
    if p.get("force"):
        return False
    return str(p.get("trigger") or "") in POLICY_TRIGGERS


def is_maintenance_commit(message: str | None) -> bool:
    """True for the reserved ``ci: maintenance …`` controller commits (no deploy)."""
    return str(message or "").strip().lower().startswith(MAINTENANCE_COMMIT_PREFIX)


# --------------------------------------------------------------------------- #
# Controller tree
# --------------------------------------------------------------------------- #


def _read_yaml(path: Path) -> dict:
    """YAML file → dict (``{}`` when missing, non-mapping or unreadable — logged)."""
    if not path.is_file():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
    except Exception as exc:
        log.warning("MAINTENANCE: cannot read %s: %s", path, exc)
        return {}
    return data if isinstance(data, dict) else {}


def _merge(base: dict, nxt: Mapping) -> dict:
    """Deep merge with the iac_core loader semantics: dicts merge, lists are the
    union (first occurrence wins), scalars override."""
    for key, value in nxt.items():
        cur = base.get(key)
        if isinstance(cur, dict) and isinstance(value, Mapping):
            _merge(cur, value)
        elif isinstance(cur, list) and isinstance(value, list):
            for item in value:
                if item not in cur:
                    cur.append(item)
        else:
            base[key] = value
    return base


def site_dir(inventory_root: Path, site: str) -> Path:
    return Path(inventory_root) / "environments" / "sites" / site


def stage_dir(inventory_root: Path, site: str, stage: str) -> Path:
    return site_dir(inventory_root, site) / "stages" / stage


def discover(inventory_root: Path) -> list[tuple[str, str]]:
    """Every ``(site, stage)`` pair of the controller tree, sorted."""
    root = Path(inventory_root) / "environments" / "sites"
    if not root.is_dir():
        return []
    out: list[tuple[str, str]] = []
    for site in sorted(p.name for p in root.iterdir() if p.is_dir()):
        stages = root / site / "stages"
        if not stages.is_dir():
            continue
        for stage in sorted(p.name for p in stages.iterdir() if p.is_dir()):
            out.append((site, stage))
    return out


def load_tree(inventory_root: Path, site: str, stage: str) -> dict:
    """Raw merged site + stage configuration (values are literal, no Jinja).

    Reads ``site_vars.yml``, ``hardware.yml`` and ``stages/<stage>/{stage_vars,stage,
    hosts}.yml`` — the files that carry maintenance data — with the loader's merge
    semantics. Global files (``01..04_*.yml``) carry none and are skipped.
    """
    sd, std = site_dir(inventory_root, site), stage_dir(inventory_root, site, stage)
    tree: dict = {}
    for path in (
        sd / "site_vars.yml",
        sd / "hardware.yml",
        std / "stage_vars.yml",
        std / "stage.yml",
        std / "hosts.yml",
    ):
        _merge(tree, _read_yaml(path))
    tree.setdefault("site_vars", {})
    tree.setdefault("stage_vars", {})
    tree.setdefault("hosts", {})
    tree.setdefault("hardware_hosts", {})
    return tree


def find_service_hosts(
    inventory_root: Path, service_name: str, *, stage: str | None = None
) -> list[tuple[str, str, str]]:
    """``(site, stage, host)`` of every controller host that lists ``service_name``
    under ``services[]`` — optionally only in ``stage``. Sorted."""
    svc = str(service_name or "").strip().lower()
    out: list[tuple[str, str, str]] = []
    for site, stg in discover(inventory_root):
        if stage and stg != stage:
            continue
        hosts = load_tree(inventory_root, site, stg).get("hosts") or {}
        for host, data in hosts.items():
            if not isinstance(data, dict):
                continue
            for entry in data.get("services") or []:
                name = entry.get("name") if isinstance(entry, dict) else entry
                if str(name or "").strip().lower() == svc:
                    out.append((site, stg, str(host)))
                    break
    return sorted(out)


# --------------------------------------------------------------------------- #
# service.yml
# --------------------------------------------------------------------------- #


@dataclass
class ServiceMaint:
    """Maintenance-relevant part of one ``service.yml`` (keyed by the repo slug)."""

    slug: str
    name: Optional[str] = None  # ``service.name`` inside the file (compose/docker name)
    maintenance: Optional[dict] = None
    incidents: Optional[dict] = None
    stage_overrides: dict = field(
        default_factory=dict
    )  # stage → {maintenance?, incidents?}
    path: Optional[Path] = None

    def for_stage(self, stage: str | None) -> tuple[Optional[dict], Optional[dict]]:
        """``(maintenance, incidents)`` with ``stage_overrides[stage]`` applied — an
        override replaces the whole block for that stage."""
        ov = (
            self.stage_overrides.get(stage or "")
            if isinstance(self.stage_overrides, dict)
            else None
        )
        ov = ov if isinstance(ov, dict) else {}
        maint = ov["maintenance"] if "maintenance" in ov else self.maintenance
        inc = ov["incidents"] if "incidents" in ov else self.incidents
        return (
            maint if isinstance(maint, dict) else None,
            inc if isinstance(inc, dict) else None,
        )


def read_service_yml(path: Path, slug: str | None = None) -> ServiceMaint | None:
    """Parse one ``service.yml``; ``None`` when missing/unreadable (logged)."""
    path = Path(path)
    slug = slug or path.parent.name
    if not path.is_file():
        return None
    data = _read_yaml(path)
    if not data:
        return None
    svc_raw, ov_raw = data.get("service"), data.get("stage_overrides")
    svc: dict = svc_raw if isinstance(svc_raw, dict) else {}
    overrides: dict = ov_raw if isinstance(ov_raw, dict) else {}
    return ServiceMaint(
        slug=slug,
        name=str(svc.get("name")) if svc.get("name") else None,
        maintenance=(
            data.get("maintenance")
            if isinstance(data.get("maintenance"), dict)
            else None
        ),
        incidents=(
            data.get("incidents") if isinstance(data.get("incidents"), dict) else None
        ),
        stage_overrides={
            str(k): v for k, v in overrides.items() if isinstance(v, dict)
        },
        path=path,
    )


def scan_service_ymls(dirs: Iterable[Path]) -> dict[str, ServiceMaint]:
    """``slug → ServiceMaint`` for every ``<dir>/<slug>/service.yml`` (first dir wins).

    Only services the runner has cloned are present; the others inherit host/stage
    defaults (documented limit).
    """
    out: dict[str, ServiceMaint] = {}
    for root in dirs:
        root = Path(root)
        if not root.is_dir():
            continue
        for svc_yml in sorted(root.glob("*/service.yml")):
            slug = svc_yml.parent.name
            if slug in out:
                continue
            try:
                parsed = read_service_yml(svc_yml, slug)
            except Exception as exc:  # never let one broken repo stop the publish
                log.warning("MAINTENANCE: skipping %s: %s", svc_yml, exc)
                continue
            if parsed is not None:
                out[slug] = parsed
    return out


# --------------------------------------------------------------------------- #
# Resolution
# --------------------------------------------------------------------------- #


def _windows_of(block: Any, tz: str, where: str) -> Optional[list]:
    """Parsed schedules of a ``{windows: [...]}`` block, ``None`` when the block has
    no windows or is invalid (logged, the chain continues)."""
    if not isinstance(block, dict):
        return None
    raw = block.get("windows")
    if not raw:
        return None
    try:
        return mw.parse_windows(raw, default_tz=tz)
    except mw.MaintenanceError as exc:
        log.warning("MAINTENANCE: ignoring invalid windows at %s: %s", where, exc)
        return None


def _enabled(block: Any) -> bool:
    """``maintenance_default.enabled`` — missing means enabled."""
    if not isinstance(block, dict):
        return False
    val = block.get("enabled", True)
    if isinstance(val, str):
        return val.strip().lower() in ("1", "true", "yes", "on")
    return bool(val)


def _deploy_policy(*blocks: Any, stage: str | None, where: str) -> str:
    """First valid ``deploy_policy`` of the given maintenance blocks, else the stage default."""
    for block in blocks:
        if isinstance(block, dict) and block.get("deploy_policy") is not None:
            val = str(block["deploy_policy"]).strip().lower()
            if val in mw.DEPLOY_POLICIES:
                return val
            log.warning(
                "MAINTENANCE: invalid deploy_policy %r at %s — using the stage default",
                val,
                where,
            )
    return "window" if (stage or "") in WINDOW_DEFAULT_STAGES else "immediate"


def _incidents(own: Any, kind: str, *defaults: Any, where: str) -> Optional[dict]:
    try:
        rule = mw.resolve_incident_rule(
            own if isinstance(own, dict) else None,
            kind,
            *[d if isinstance(d, dict) else None for d in defaults],
        )
    except mw.MaintenanceError as exc:
        log.warning(
            "MAINTENANCE: ignoring invalid incidents block at %s: %s", where, exc
        )
        return None
    return mw.rule_to_dict(rule) if rule else None


def default_windows(tree: Mapping, tz: str, site: str, stage: str | None):
    """``(stage_default_schedules, site_default_schedules)`` — each ``None`` when the
    level defines nothing or is switched off (``enabled: false``)."""
    stage_vars = tree.get("stage_vars") or {}
    site_vars = tree.get("site_vars") or {}
    stage_def = stage_vars.get("maintenance_default") if stage else None
    site_def = site_vars.get("maintenance_default")
    stage_w = (
        _windows_of(stage_def, tz, f"{site}/{stage}/stage_vars.maintenance_default")
        if _enabled(stage_def)
        else None
    )
    site_w = (
        _windows_of(site_def, tz, f"{site}/site_vars.maintenance_default")
        if _enabled(site_def)
        else None
    )
    return stage_w, site_w


def resolve(
    site: str,
    stage: str,
    tree: Mapping,
    services: Mapping[str, ServiceMaint],
    ci: CiMap | None = None,
    *,
    default_tz: str | None = None,
    include_hypervisors: bool = True,
) -> list[dict]:
    """Effective maintenance items of one controller stage (plus the site's hypervisors).

    Item: ``{ci_id, kind, name, host_ci_id?, windows: [normal form], inherited_from,
    deploy_policy? (services), incidents?, site, stage, host?, service?, tz}``. Items
    without ``ci_id`` are returned too (the UI shows them); the publisher drops them
    before the bus (contract: an item without ``ci_id`` is never sent).
    """
    ci = ci or {}
    site_vars = tree.get("site_vars") or {}
    stage_vars = tree.get("stage_vars") or {}
    tz = default_tz or str(site_vars.get("timezone") or "UTC")
    stage_w, site_w = default_windows(tree, tz, site, stage)
    stage_inc = stage_vars.get("incident_defaults")
    site_inc = site_vars.get("incident_defaults")
    items: list[dict] = []

    def _item(kind: str, name: str, schedules, inherited_from, **extra) -> dict:
        return {
            "ci_id": None,
            "kind": kind,
            "name": name,
            "windows": mw.to_dicts(schedules or []),
            "inherited_from": inherited_from,
            "site": site,
            "stage": stage if kind != "hypervisor" else None,
            "tz": tz,
            **extra,
        }

    if include_hypervisors:
        for hw_name, hw in (tree.get("hardware_hosts") or {}).items():
            if not isinstance(hw, dict):
                continue
            nodes_raw = hw.get("nodes")
            nodes: dict = (
                nodes_raw
                if hw.get("is_cluster") and isinstance(nodes_raw, dict)
                else {hw_name: hw}
            )
            for node_name, node in nodes.items():
                if not isinstance(node, dict):
                    continue
                own = _windows_of(
                    node.get("maintenance"),
                    tz,
                    f"{site}/hardware.{node_name}.maintenance",
                )
                schedules, inherited = (
                    (own, None)
                    if own is not None
                    else (site_w, "site_default" if site_w else None)
                )
                item = _item(
                    "hypervisor",
                    str(node_name),
                    schedules,
                    inherited,
                    incidents=_incidents(
                        node.get("incidents"),
                        "hypervisor",
                        site_inc,
                        where=f"{site}/hardware.{node_name}.incidents",
                    ),
                )
                item["ci_id"] = ci.get(("host", str(node_name)))
                items.append(item)

    for host_name, host in (tree.get("hosts") or {}).items():
        if not isinstance(host, dict):
            continue
        host_name = str(host_name)
        maint_raw = host.get("maintenance")
        host_block: dict = maint_raw if isinstance(maint_raw, dict) else {}
        own = _windows_of(
            host_block, tz, f"{site}/{stage}/hosts.{host_name}.maintenance"
        )
        if own is not None:
            host_sched, host_inherited = own, None
        elif stage_w is not None:
            host_sched, host_inherited = stage_w, "stage_default"
        elif site_w is not None:
            host_sched, host_inherited = site_w, "site_default"
        else:
            host_sched, host_inherited = None, None
        host_ci = ci.get(("host", host_name))
        items.append(
            {
                **_item(
                    "host",
                    host_name,
                    host_sched,
                    host_inherited,
                    incidents=_incidents(
                        host.get("incidents"),
                        "host",
                        stage_inc,
                        site_inc,
                        where=f"{site}/{stage}/hosts.{host_name}.incidents",
                    ),
                ),
                "ci_id": host_ci,
            }
        )

        inherit_ok = host_block.get("inherit", True) is not False
        for entry in host.get("services") or []:
            if isinstance(entry, str):
                entry = {"name": entry}
            if not isinstance(entry, dict) or not entry.get("name"):
                continue
            svc = str(entry["name"]).strip()
            where = f"{site}/{stage}/hosts.{host_name}.services.{svc}"
            svc_yml = services.get(svc) or services.get(svc.lower())
            yml_maint, yml_inc = svc_yml.for_stage(stage) if svc_yml else (None, None)
            ctrl_maint = (
                entry.get("maintenance")
                if isinstance(entry.get("maintenance"), dict)
                else None
            )
            schedules, inherited = None, None
            for block, label in (
                (yml_maint, f"{svc}/service.yml"),
                (ctrl_maint, where),
            ):
                w = _windows_of(block, tz, label)
                if w is not None:
                    schedules, inherited = w, None
                    break
            if schedules is None:
                if own is not None and inherit_ok:
                    schedules, inherited = own, "host"
                elif stage_w is not None:
                    schedules, inherited = stage_w, "stage_default"
                elif site_w is not None:
                    schedules, inherited = site_w, "site_default"
            items.append(
                {
                    **_item(
                        "service",
                        f"{svc} @ {host_name}",
                        schedules,
                        inherited,
                        host=host_name,
                        service=svc,
                        deploy_policy=_deploy_policy(
                            yml_maint, ctrl_maint, stage=stage, where=where
                        ),
                        incidents=_incidents(
                            yml_inc if yml_inc is not None else entry.get("incidents"),
                            "service",
                            stage_inc,
                            site_inc,
                            where=where,
                        ),
                    ),
                    "ci_id": ci.get(("service", host_name, svc)),
                    "host_ci_id": host_ci,
                }
            )
    return items


def resolve_all(
    inventory_root: Path, services_dirs: Iterable[Path], ci: CiMap | None = None
) -> list[dict]:
    """Items of every site/stage of the controller tree (hypervisors once per site)."""
    services = scan_service_ymls(services_dirs)
    items: list[dict] = []
    seen_sites: set[str] = set()
    for site, stage in discover(inventory_root):
        tree = load_tree(inventory_root, site, stage)
        items.extend(
            resolve(
                site,
                stage,
                tree,
                services,
                ci,
                include_hypervisors=site not in seen_sites,
            )
        )
        seen_sites.add(site)
    return items


def default_switches(inventory_root: Path) -> list[dict]:
    """Read-only view of ``maintenance_default`` per site/stage for the UI card
    (decision C: the switch is edited in the controller repo, never from here).

    ``{site, stage, enabled, windows, source_file, level: "stage"|"site"|None, tz, errors}``
    """
    out: list[dict] = []
    for site, stage in discover(inventory_root):
        tree = load_tree(inventory_root, site, stage)
        site_vars = tree.get("site_vars") or {}
        tz = str(site_vars.get("timezone") or "UTC")
        stage_def = (tree.get("stage_vars") or {}).get("maintenance_default")
        site_def = site_vars.get("maintenance_default")
        if isinstance(stage_def, dict):
            block, level = stage_def, "stage"
            source = f"environments/sites/{site}/stages/{stage}/stage_vars.yml"
        elif isinstance(site_def, dict):
            block, level = site_def, "site"
            source = f"environments/sites/{site}/site_vars.yml"
        else:
            block, level, source = (
                None,
                None,
                f"environments/sites/{site}/stages/{stage}/stage_vars.yml",
            )
        errors = mw.validate_windows(
            block.get("windows") if block else None, default_tz=tz
        )
        windows: list[dict] = []
        if block and not errors:
            windows = mw.to_dicts(mw.parse_windows(block.get("windows"), default_tz=tz))
        out.append(
            {
                "site": site,
                "stage": stage,
                "level": level,
                "enabled": _enabled(block) and bool(block),
                "windows": windows,
                "source_file": source,
                "tz": tz,
                "errors": errors,
            }
        )
    return out


# --------------------------------------------------------------------------- #
# Deploy policy of one service
# --------------------------------------------------------------------------- #


@dataclass
class PolicyDecision:
    """What the ``MaintenancePolicyStage`` needs for one service deploy."""

    service: str
    deploy_policy: str = "immediate"
    windows: list = field(default_factory=list)  # normal form (for logs/UI)
    schedules: list = field(default_factory=list)  # parsed (for evaluation)
    host: Optional[str] = None  # the deciding (first sorted) target host
    site: Optional[str] = None
    stage: Optional[str] = None
    service_ci_id: Optional[str] = None
    host_ci_id: Optional[str] = None
    ci_ids: list = field(
        default_factory=list
    )  # every service + host CI the deploy touches
    inherited_from: Optional[str] = None
    notes: list = field(default_factory=list)  # hosts whose policy/windows differ

    @property
    def found(self) -> bool:
        return self.host is not None


def service_policy(
    items: Iterable[dict], service_name: str, hosts: Iterable[str]
) -> PolicyDecision:
    """Decide from resolved ``items``: the policy and windows of the **first target
    host (sorted)**; other hosts that differ are listed in ``notes`` (documented limit:
    one service on several hosts follows the first host's window)."""
    svc = str(service_name or "").strip().lower()
    wanted = sorted({str(h) for h in hosts})
    by_host: dict[str, dict] = {}
    for it in items:
        if (
            it.get("kind") == "service"
            and str(it.get("service") or "").lower() == svc
            and it.get("host") in wanted
        ):
            by_host.setdefault(it["host"], it)
    decision = PolicyDecision(service=svc)
    if not by_host:
        return decision
    ordered = [by_host[h] for h in wanted if h in by_host]
    primary = ordered[0]
    tz = primary.get("tz") or "UTC"
    try:
        schedules = mw.parse_windows(primary.get("windows") or [], default_tz=tz)
    except mw.MaintenanceError as exc:  # already normalised — defensive only
        log.warning(
            "MAINTENANCE: cannot re-parse windows of %s: %s", primary.get("name"), exc
        )
        schedules = []
    decision.deploy_policy = str(primary.get("deploy_policy") or "immediate")
    decision.windows = list(primary.get("windows") or [])
    decision.schedules = schedules
    decision.host, decision.site, decision.stage = (
        primary.get("host"),
        primary.get("site"),
        primary.get("stage"),
    )
    decision.service_ci_id, decision.host_ci_id = primary.get("ci_id"), primary.get(
        "host_ci_id"
    )
    decision.inherited_from = primary.get("inherited_from")
    ci_ids: list[str] = []
    for it in ordered:
        for key in ("ci_id", "host_ci_id"):
            val = it.get(key)
            if val and val not in ci_ids:
                ci_ids.append(val)
    decision.ci_ids = ci_ids
    for it in ordered[1:]:
        if it.get("deploy_policy") != primary.get("deploy_policy") or it.get(
            "windows"
        ) != primary.get("windows"):
            decision.notes.append(
                f"host {it.get('host')}: deploy_policy={it.get('deploy_policy')}, "
                f"{len(it.get('windows') or [])} window(s) — differs from {primary.get('host')} (which decides)"
            )
    return decision


# --------------------------------------------------------------------------- #
# Bus payloads / read-model helpers
# --------------------------------------------------------------------------- #


def windows_published_payload(
    items: Iterable[dict], *, now: datetime | None = None
) -> dict:
    """``maintenance:windows_published`` for our complete set — items without a
    ``ci_id`` or with an invalid kind are dropped (never sent)."""
    sent = [
        dict(it) for it in items if it.get("ci_id") and mw.is_valid_kind(it.get("kind"))
    ]
    return {
        "source": SOURCE,
        "published_at": mw.iso(now or mw.utcnow()),
        "replaces": True,
        "items": sent,
    }


def override_started_payload(
    override_id: str,
    ci_ids: Iterable[str],
    *,
    reason: str,
    now: datetime,
    max_override_minutes: int,
    linger_minutes: int,
) -> dict:
    return {
        "override_id": override_id,
        "ci_ids": list(ci_ids),
        "until": mw.iso(now + timedelta(minutes=max(1, int(max_override_minutes)))),
        "reason": reason,
        "source": SOURCE,
        "started_at": mw.iso(now),
        "linger_minutes": max(0, int(linger_minutes)),
    }


def override_ended_payload(
    override_id: str, *, now: datetime, linger_minutes: int
) -> dict:
    return {
        "override_id": override_id,
        "source": SOURCE,
        "ended_at": mw.iso(now),
        "linger_minutes": max(0, int(linger_minutes)),
    }


def window_missing_payload(
    decision: PolicyDecision, *, job_id: int | str, horizon_days: int, now: datetime
) -> dict:
    """``maintenance:window_missing`` (decision H) — requires ``decision.service_ci_id``."""
    assert decision.service_ci_id, "window_missing needs the service CI"
    return {
        "event_id": mw.make_window_missing_id(decision.service_ci_id, job_id),
        "ci_id": decision.service_ci_id,
        "kind": "service",
        "name": f"{decision.service} @ {decision.host}",
        "host_ci_id": decision.host_ci_id,
        "site": decision.site,
        "stage": decision.stage,
        "horizon_days": int(horizon_days),
        "job_id": job_id,
        "source": SOURCE,
        "detected_at": mw.iso(now),
        "reason": f"deploy_policy window, no window within {int(horizon_days)} days",
    }


def effective_state_of(
    item: Mapping,
    overrides: Iterable[mw.Override],
    now: datetime,
    *,
    horizon_days: int = 30,
) -> dict:
    """``{active, until, source, next_start, note}`` (ISO strings) of one item for the API."""
    tz = item.get("tz") or "UTC"
    try:
        schedules = mw.parse_windows(item.get("windows") or [], default_tz=tz)
    except mw.MaintenanceError:
        schedules = []
    ci_id = item.get("ci_id")
    own = [o for o in overrides if ci_id and ci_id in o.ci_ids]
    state = mw.effective_state(
        schedules, own, now, horizon=timedelta(days=max(1, int(horizon_days)))
    )
    return {
        "active": state.active,
        "until": mw.iso(state.until) if state.until else None,
        "source": state.source,
        "next_start": mw.iso(state.next_start) if state.next_start else None,
        "note": state.note,
    }


__all__ = [
    "SOURCE",
    "POLICY_TRIGGERS",
    "MAINTENANCE_COMMIT_PREFIX",
    "CiMap",
    "ServiceMaint",
    "PolicyDecision",
    "policy_applies",
    "is_maintenance_commit",
    "discover",
    "load_tree",
    "find_service_hosts",
    "read_service_yml",
    "scan_service_ymls",
    "default_windows",
    "resolve",
    "resolve_all",
    "default_switches",
    "service_policy",
    "windows_published_payload",
    "override_started_payload",
    "override_ended_payload",
    "window_missing_payload",
    "effective_state_of",
]
