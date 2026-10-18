"""Maintenance windows (plugin 1.6): resolution chain, defaults, deploy policy,
incident rules and the bus payload helpers of ``maintenance_resolver`` — pure
functions on a temporary iac-controller tree + service.yml files."""

import logging
from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml

from core.api import maintenance as mw
from iac_plugin.app.controller import maintenance_resolver as mres

SUN = {"weekly": {"days": ["sun"], "start": "02:00", "end": "04:00"}}
SAT = {"weekly": {"days": ["sat"], "start": "22:00", "end": "23:00", "tz": "UTC"}}
MON = {"weekly": {"days": ["mon"], "start": "01:00", "end": "02:00", "tz": "UTC"}}
TUE = {"weekly": {"days": ["tue"], "start": "03:00", "end": "04:00", "tz": "UTC"}}
WED = {"weekly": {"days": ["wed"], "start": "05:00", "end": "06:00", "tz": "UTC"}}


def _write(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data), encoding="utf-8")


def make_controller(
    root: Path,
    site: str,
    stage: str,
    *,
    site_vars=None,
    hardware=None,
    stage_vars=None,
    hosts=None
) -> Path:
    sd = root / "environments" / "sites" / site
    std = sd / "stages" / stage
    if site_vars is not None:
        _write(sd / "site_vars.yml", {"site_vars": site_vars})
    if hardware is not None:
        _write(sd / "hardware.yml", {"hardware_hosts": hardware})
    _write(std / "stage_vars.yml", {"stage_vars": stage_vars or {}})
    _write(std / "hosts.yml", {"hosts": hosts or {}})
    return root


def make_service(services_dir: Path, slug: str, data: dict) -> None:
    _write(services_dir / slug / "service.yml", data)


def _types(windows):
    return [(w["type"], tuple(w.get("days", []))) for w in windows]


@pytest.fixture
def tree(tmp_path):
    """One site with two stages (prod with a stage default, dev without) and a
    hypervisor; services exercising every level of the chain."""
    ctrl = tmp_path / "ctrl"
    make_controller(
        ctrl,
        "onprem",
        "prod",
        site_vars={
            "timezone": "Europe/Berlin",
            "maintenance_default": {"windows": [WED]},
            "incident_defaults": {
                "service": {
                    "enabled": True,
                    "offline_minutes": 10,
                    "severity": "high",
                    "auto_resolve": True,
                },
                "host": {
                    "enabled": True,
                    "offline_minutes": 5,
                    "severity": "critical",
                    "auto_resolve": True,
                },
                "hypervisor": {
                    "enabled": True,
                    "offline_minutes": 5,
                    "severity": "critical",
                    "auto_resolve": True,
                },
            },
        },
        hardware={
            "hydra": {"type": "proxmox", "maintenance": {"windows": [SAT]}},
            "cluster-a": {
                "is_cluster": True,
                "type": "proxmox",
                "nodes": {
                    "node1": {"type": "proxmox"},
                    "node2": {"type": "proxmox", "incidents": {"enabled": False}},
                },
            },
        },
        stage_vars={
            "maintenance_default": {"enabled": True, "windows": [TUE]},
            "incident_defaults": {
                "service": {
                    "enabled": True,
                    "offline_minutes": 3,
                    "severity": "medium",
                    "auto_resolve": False,
                },
            },
        },
        hosts={
            "host-own": {
                "hostname": "host-own.int",
                "ansible_host": "10.0.0.1",
                "maintenance": {"windows": [SUN, MON]},
                "incidents": {"offline_minutes": 1},
                "services": [
                    {"name": "svc-own"},  # service.yml decides
                    {
                        "name": "svc-ctrl",
                        "maintenance": {"windows": [SAT], "deploy_policy": "immediate"},
                    },
                    {"name": "svc-host"},  # inherits the host
                    {"name": "svc-bad"},  # invalid service.yml → host
                ],
            },
            "host-noinherit": {
                "hostname": "host-noinherit.int",
                "ansible_host": "10.0.0.2",
                "maintenance": {"windows": [SUN], "inherit": False},
                "services": [{"name": "svc-host"}],  # host blocked → stage default
            },
            "host-plain": {
                "hostname": "host-plain.int",
                "ansible_host": "10.0.0.3",
                "services": [
                    {"name": "svc-host", "incidents": {"enabled": False}},
                    "svc-own",
                ],
            },
        },
    )
    make_controller(
        ctrl,
        "onprem",
        "dev",
        stage_vars={},
        hosts={
            "dev-box": {
                "hostname": "dev-box.int",
                "ansible_host": "10.0.1.1",
                "services": [{"name": "svc-own"}, {"name": "svc-host"}],
            }
        },
    )
    make_controller(
        ctrl,
        "hetzner",
        "prod",
        site_vars={"timezone": "UTC"},
        stage_vars={"maintenance_default": {"enabled": False, "windows": [TUE]}},
        hosts={
            "hz-1": {
                "hostname": "hz-1.example",
                "ansible_host": "1.2.3.4",
                "services": [{"name": "svc-host"}],
            }
        },
    )
    services = tmp_path / "services"
    make_service(
        services,
        "svc-own",
        {
            "service": {"name": "own-compose"},
            "maintenance": {"windows": [SUN], "deploy_policy": "window"},
            "incidents": {"severity": "low"},
            "stage_overrides": {
                "dev": {"maintenance": {"windows": [MON], "deploy_policy": "immediate"}}
            },
        },
    )
    make_service(
        services,
        "svc-bad",
        {
            "service": {"name": "bad"},
            "maintenance": {"windows": [{"weekly": {"days": [], "start": "x"}}]},
        },
    )
    return ctrl, services


def _items(tree, site="onprem", stage="prod", ci=None):
    ctrl, services = tree
    svcs = mres.scan_service_ymls([services])
    return mres.resolve(site, stage, mres.load_tree(ctrl, site, stage), svcs, ci or {})


def _by_name(items):
    return {it["name"]: it for it in items}


def test_discover_and_find_service_hosts(tree):
    ctrl, _ = tree
    assert mres.discover(ctrl) == [
        ("hetzner", "prod"),
        ("onprem", "dev"),
        ("onprem", "prod"),
    ]
    assert mres.find_service_hosts(ctrl, "svc-host") == [
        ("hetzner", "prod", "hz-1"),
        ("onprem", "dev", "dev-box"),
        ("onprem", "prod", "host-noinherit"),
        ("onprem", "prod", "host-own"),
        ("onprem", "prod", "host-plain"),
    ]
    assert mres.find_service_hosts(ctrl, "SVC-OWN", stage="dev") == [
        ("onprem", "dev", "dev-box")
    ]
    assert mres.find_service_hosts(ctrl, "nope") == []


def test_service_chain_levels(tree, caplog):
    caplog.set_level(logging.WARNING, logger="IaC:Maintenance")
    by = _by_name(_items(tree))

    own = by[
        "svc-own @ host-own"
    ]  # service.yml wins, list REPLACES the host's two windows
    assert (
        _types(own["windows"]) == [("weekly", ("sun",))]
        and own["inherited_from"] is None
    )
    assert own["deploy_policy"] == "window"
    assert (
        own["windows"][0]["tz"] == "Europe/Berlin"
    )  # tz default from site_vars.timezone

    ctrl = by["svc-ctrl @ host-own"]  # controller services[].maintenance
    assert (
        _types(ctrl["windows"]) == [("weekly", ("sat",))]
        and ctrl["inherited_from"] is None
    )
    assert ctrl["deploy_policy"] == "immediate"

    host = by["svc-host @ host-own"]  # inherits the host's own windows (both)
    assert _types(host["windows"]) == [("weekly", ("sun",)), ("weekly", ("mon",))]
    assert (
        host["inherited_from"] == "host" and host["deploy_policy"] == "window"
    )  # prod default

    bad = by[
        "svc-bad @ host-own"
    ]  # invalid service.yml block → ignored, logged, chain continues
    assert bad["inherited_from"] == "host" and _types(bad["windows"]) == [
        ("weekly", ("sun",)),
        ("weekly", ("mon",)),
    ]
    assert any(
        "invalid windows" in r.message and "svc-bad" in r.message
        for r in caplog.records
    )

    noinh = by[
        "svc-host @ host-noinherit"
    ]  # inherit: false → skips the host → stage default
    assert noinh["inherited_from"] == "stage_default" and _types(noinh["windows"]) == [
        ("weekly", ("tue",))
    ]

    plain = by["svc-host @ host-plain"]  # host without windows → stage default
    assert plain["inherited_from"] == "stage_default"
    assert (
        by["svc-own @ host-plain"]["inherited_from"] is None
    )  # string entry in services[]


def test_host_and_hypervisor_chain(tree):
    by = _by_name(_items(tree))
    assert by["host-own"]["kind"] == "host" and by["host-own"]["inherited_from"] is None
    assert by["host-plain"]["inherited_from"] == "stage_default"
    assert by["hydra"]["kind"] == "hypervisor" and by["hydra"]["inherited_from"] is None
    assert _types(by["hydra"]["windows"]) == [("weekly", ("sat",))]
    # cluster nodes are hypervisors too; no own block → site default (hardware sees no stage)
    assert (
        by["node1"]["kind"] == "hypervisor"
        and by["node1"]["inherited_from"] == "site_default"
    )
    assert _types(by["node1"]["windows"]) == [("weekly", ("wed",))]
    assert by["hydra"]["stage"] is None and by["host-own"]["stage"] == "prod"


def test_site_default_and_disabled_stage_default(tree):
    # hetzner/prod: stage default present but enabled: false → nothing (no site default either)
    by = _by_name(_items(tree, "hetzner", "prod"))
    assert by["hz-1"]["windows"] == [] and by["hz-1"]["inherited_from"] is None
    assert (
        by["svc-host @ hz-1"]["windows"] == []
        and by["svc-host @ hz-1"]["inherited_from"] is None
    )
    # onprem/dev: no stage default → site default (WED); deploy_policy defaults to immediate on dev
    by = _by_name(_items(tree, "onprem", "dev"))
    assert by["svc-host @ dev-box"]["inherited_from"] == "site_default"
    assert _types(by["svc-host @ dev-box"]["windows"]) == [("weekly", ("wed",))]
    assert by["svc-host @ dev-box"]["deploy_policy"] == "immediate"
    # stage_overrides.dev replaces the service.yml block entirely
    own = by["svc-own @ dev-box"]
    assert (
        _types(own["windows"]) == [("weekly", ("mon",))]
        and own["deploy_policy"] == "immediate"
    )


def test_incident_rules_cascade(tree):
    by = _by_name(_items(tree))
    # service: own partial block (severity) filled from the STAGE default (most specific)
    assert by["svc-own @ host-own"]["incidents"] == {
        "enabled": True,
        "offline_minutes": 3,
        "severity": "low",
        "auto_resolve": False,
    }
    # service without own block → stage incident_defaults.service
    assert by["svc-host @ host-own"]["incidents"]["offline_minutes"] == 3
    # controller services[].incidents (enabled: false) wins over the defaults
    assert by["svc-host @ host-plain"]["incidents"]["enabled"] is False
    # host: own partial block over the SITE default (stage defines no host rule)
    assert by["host-own"]["incidents"] == {
        "enabled": True,
        "offline_minutes": 1,
        "severity": "critical",
        "auto_resolve": True,
    }
    # hypervisor: site default; node2 disables it
    assert by["hydra"]["incidents"]["severity"] == "critical"
    assert by["node2"]["incidents"]["enabled"] is False
    # no defaults anywhere → None
    assert (
        _by_name(_items(tree, "hetzner", "prod"))["svc-host @ hz-1"]["incidents"]
        is None
    )


def test_ci_ids_and_published_payload(tree):
    ci = {("host", "host-own"): "u-host", ("service", "host-own", "svc-own"): "u-svc"}
    items = _items(tree, ci=ci)
    by = _by_name(items)
    assert by["host-own"]["ci_id"] == "u-host"
    assert (
        by["svc-own @ host-own"]["ci_id"] == "u-svc"
        and by["svc-own @ host-own"]["host_ci_id"] == "u-host"
    )
    assert (
        by["svc-host @ host-own"]["ci_id"] is None
        and by["svc-host @ host-own"]["host_ci_id"] == "u-host"
    )

    payload = mres.windows_published_payload(
        items, now=datetime(2026, 10, 18, 12, 0, tzinfo=timezone.utc)
    )
    assert payload["source"] == "iac-orchestrator" and payload["replaces"] is True
    assert payload["published_at"] == "2026-10-18T12:00:00Z"
    assert sorted(i["ci_id"] for i in payload["items"]) == [
        "u-host",
        "u-svc",
    ]  # no ci_id → not sent


def test_resolve_all_lists_hypervisors_once(tree):
    ctrl, services = tree
    items = mres.resolve_all(ctrl, [services], {})
    assert [it["name"] for it in items if it["kind"] == "hypervisor"].count(
        "hydra"
    ) == 1
    assert {(it["site"], it["stage"]) for it in items if it["kind"] == "host"} == {
        ("hetzner", "prod"),
        ("onprem", "dev"),
        ("onprem", "prod"),
    }


def test_default_switches_read_only_view(tree):
    ctrl, _ = tree
    sw = {(d["site"], d["stage"]): d for d in mres.default_switches(ctrl)}
    on = sw[("onprem", "prod")]
    assert (
        on["enabled"] is True
        and on["level"] == "stage"
        and _types(on["windows"]) == [("weekly", ("tue",))]
    )
    assert on["source_file"] == "environments/sites/onprem/stages/prod/stage_vars.yml"
    off = sw[("hetzner", "prod")]
    assert off["enabled"] is False and off["level"] == "stage" and off["errors"] == []
    site_level = sw[("onprem", "dev")]
    assert site_level["level"] == "site" and site_level["enabled"] is True
    assert (
        site_level["source_file"] == "environments/sites/onprem/site_vars.yml"
        and site_level["tz"] == "Europe/Berlin"
    )


def test_service_policy_first_host_decides_and_notes_differences(tree):
    ci = {
        ("host", "host-own"): "u-h1",
        ("service", "host-own", "svc-host"): "u-s1",
        ("host", "host-plain"): "u-h3",
        ("service", "host-plain", "svc-host"): "u-s3",
    }
    items = _items(tree, ci=ci)
    d = mres.service_policy(
        items, "svc-host", ["host-plain", "host-own", "host-noinherit"]
    )
    assert d.found and d.host == "host-noinherit"  # sorted: noinherit < own < plain
    assert d.deploy_policy == "window" and d.inherited_from == "stage_default"
    assert len(d.schedules) == 1 and d.windows[0]["days"] == ["tue"]
    assert d.ci_ids == [
        "u-s1",
        "u-h1",
        "u-s3",
        "u-h3",
    ]  # noinherit has none; order kept, deduped
    assert (
        d.service_ci_id is None and d.host_ci_id is None
    )  # the deciding host has no CI
    assert any("host-own" in n for n in d.notes) and not any(
        "host-plain" in n for n in d.notes
    )

    assert not mres.service_policy(items, "svc-host", ["unknown"]).found
    assert not mres.service_policy(items, "nope", ["host-own"]).found


def test_policy_applies_and_commit_guard():
    assert mres.policy_applies({"trigger": "gitlab-ci"})
    assert mres.policy_applies({"trigger": "webhook"})
    assert not mres.policy_applies({"trigger": "gitlab-ci", "force": True})
    assert not mres.policy_applies({"trigger": "ui"})
    assert not mres.policy_applies({"manual": True})
    assert not mres.policy_applies(None)
    assert mres.is_maintenance_commit("ci: maintenance default onprem/prod off")
    assert mres.is_maintenance_commit("  CI: Maintenance window update")
    assert not mres.is_maintenance_commit("ci: automated state update")
    assert not mres.is_maintenance_commit(None)


def test_payload_helpers():
    now = datetime(2026, 10, 18, 10, 0, tzinfo=timezone.utc)
    started = mres.override_started_payload(
        "iac-job-7",
        ["a", "b"],
        reason="deploy",
        now=now,
        max_override_minutes=240,
        linger_minutes=15,
    )
    assert started == {
        "override_id": "iac-job-7",
        "ci_ids": ["a", "b"],
        "until": "2026-10-18T14:00:00Z",
        "reason": "deploy",
        "source": "iac-orchestrator",
        "started_at": "2026-10-18T10:00:00Z",
        "linger_minutes": 15,
    }
    ended = mres.override_ended_payload("iac-job-7", now=now, linger_minutes=15)
    assert ended == {
        "override_id": "iac-job-7",
        "source": "iac-orchestrator",
        "ended_at": "2026-10-18T10:00:00Z",
        "linger_minutes": 15,
    }
    d = mres.PolicyDecision(
        service="svc",
        host="h",
        site="onprem",
        stage="prod",
        service_ci_id="u-s",
        host_ci_id="u-h",
    )
    missing = mres.window_missing_payload(d, job_id=7, horizon_days=30, now=now)
    assert (
        missing["event_id"]
        == mw.make_window_missing_id("u-s", 7)
        == "window_missing:u-s:7"
    )
    assert (
        missing["kind"] == "service"
        and missing["name"] == "svc @ h"
        and missing["job_id"] == 7
    )
    assert missing["reason"] == "deploy_policy window, no window within 30 days"


def test_effective_state_of_item():
    now = datetime(2026, 10, 18, 2, 30, tzinfo=timezone.utc)  # a Sunday 02:30 UTC
    item = {
        "ci_id": "u",
        "windows": [
            {
                "type": "weekly",
                "days": ["sun"],
                "start": "02:00",
                "end": "04:00",
                "tz": "UTC",
            }
        ],
        "tz": "UTC",
    }
    st = mres.effective_state_of(item, [], now)
    assert (
        st["active"] is True
        and st["source"] == "window"
        and st["until"] == "2026-10-18T04:00:00Z"
    )
    override = mw.Override(
        override_id="o",
        ci_ids=("u",),
        until=now.replace(hour=8, minute=0),
        started_at=now.replace(hour=1, minute=0),
    )
    st2 = mres.effective_state_of(
        {"ci_id": "u", "windows": [], "tz": "UTC"}, [override], now
    )
    assert (
        st2["active"] is True
        and st2["source"] == "override"
        and st2["until"] == "2026-10-18T08:00:00Z"
    )
    assert (
        mres.effective_state_of({"ci_id": "other", "windows": []}, [override], now)[
            "active"
        ]
        is False
    )
