"""
Terraform provisioning panel.

Operational view of the *Provision* phase of the host lifecycle:

    Terraform (provision the host)  ->  Ansible (bring to spec)  ->  Services (deploy)

This panel scans the same site/host YAML the Assignments tab uses and surfaces a
``terraform:`` block per host (provider, resource, workspace, state). Hosts
without one are shown as "Ansible-only / unmanaged" so operators can see exactly
what still needs an IaC provisioning definition.

Actions wired here:

- **Check Env** — ``infra_plan``: a read-only ``tofu plan`` across every
  environment, comparing the live infrastructure against the desired plan.
- **Deploy Infra** — ``infra_apply``: an operator-approved ``tofu apply`` across
  every environment (guarded by a confirm dialog).
- **Provision** (per host) — ``host_provision`` with ``approve`` so the single
  host is brought into existence (Terraform) and bootstrapped (Ansible), also
  guarded by a confirm dialog.

All surfaces use the themed ``UIStyles`` cards (via ``components``) so light/dark
mode is handled centrally.
"""
from __future__ import annotations

import json
from pathlib import Path

import yaml
from nicegui import ui
from ui.theme import UIStyles

from . import components as c

# Provider is bpg/proxmox across all rendered environments (see terraform-templates).
_TF_PROVIDER = "bpg/proxmox"

# Map a host's most-recent terraform-type job status to a tile state.
_TF_JOB_PREFIXES = ("init_host", "host_provision", "adopt_host")


def _tfvars_host_index(config) -> dict:
    """One-pass index of every host that appears in a rendered
    ``inventory_state/<site>/<stage>/terraform/terraform.tfvars.json``.

    Returns ``{host_name: {"node": <node_name>, "env": "<site>_<stage>"}}`` so the
    provisioning tiles can show the real Terraform provider/resource without an
    rglob per host. Provider/resource aren't in the source host YAML — they only
    exist in the generated tfvars + module structure.
    """
    index: dict[str, dict] = {}
    inv_root = config.git_repos_dir / "inventory_state"
    if not inv_root.exists():
        return index
    for tfvars_path in inv_root.glob("*/*/terraform/terraform.tfvars.json"):
        try:
            with open(tfvars_path, "r", encoding="utf-8") as fh:
                data = json.load(fh) or {}
        except Exception:
            continue
        try:
            stage = tfvars_path.parents[1].name
            site = tfvars_path.parents[2].name
        except IndexError:
            continue
        for host_name, cfg in (data.get("containers") or {}).items():
            if not isinstance(cfg, dict):
                continue
            node = str(cfg.get("node_name") or "").strip()
            index[host_name] = {"node": node, "env": f"{site}_{stage}"}
    return index


def _host_state_from_jobs(jobs: list, host: str) -> str:
    """Derive a tile status from the most recent terraform-type job for ``host``.

    ``jobs`` is ``db.get_jobs_for_stats()`` (newest first), whose ``pipeline_type``
    is tagged ``<type>:<host>``. Returns one of: created / failed / provisioning /
    not provisioned.
    """
    for job in jobs:  # newest-first
        ptype = str(job.get("pipeline_type") or "")
        base, _, target = ptype.partition(":")
        if base not in _TF_JOB_PREFIXES or target.strip().lower() != host.lower():
            continue
        status = str(job.get("status") or "").upper()
        if status == "SUCCESS":
            return "created"
        if status in ("FAILED", "ERROR", "ABORTED"):
            return "failed"
        if status == "RUNNING":
            return "provisioning"
        return "unknown"
    return "not provisioned"


def _scan_terraform_hosts(ctx, service) -> list:
    """
    Parse site host YAML and return one entry per host with terraform metadata.
    """
    config = service.config
    base_dir = config.git_repos_dir / "iac_controller" / "environments"
    sites_dir = base_dir / "sites"
    results = []
    if not sites_dir.exists():
        return results

    # Derive the real Terraform metadata once: provider/resource come from the
    # generated tfvars (not the source host YAML), status from job history.
    tf_index = _tfvars_host_index(config)
    try:
        recent_jobs = service.db.get_jobs_for_stats(500)
    except Exception:
        recent_jobs = []

    for yaml_file in sites_dir.rglob("*.yml"):
        parts = yaml_file.parts
        try:
            site = parts[parts.index("sites") + 1]
            stage = parts[parts.index("stages") + 1] if "stages" in parts else "common"
            with open(yaml_file, "r") as f:
                data = yaml.safe_load(f) or {}
            all_hosts = {**(data.get("hosts") or {}), **(data.get("hardware_hosts") or {})}
            for host_name, host_data in all_hosts.items():
                if not isinstance(host_data, dict):
                    continue
                tf = host_data.get("terraform") if isinstance(host_data.get("terraform"), dict) else {}
                tf_meta = tf_index.get(host_name)
                # A host is "managed" by Terraform when it has a terraform: block OR
                # actually appears in the rendered tfvars containers.
                managed = bool(tf) or tf_meta is not None

                if tf_meta is not None:
                    node = tf_meta.get("node") or ""
                    provider = _TF_PROVIDER
                    resource = (
                        f'module.lxc_{node}.proxmox_virtual_environment_container.ct["{host_name}"]'
                        if node else 'proxmox_virtual_environment_container.ct'
                    )
                    state = _host_state_from_jobs(recent_jobs, host_name)
                else:
                    # Fall back to whatever the host YAML carried (usually nothing).
                    provider = tf.get("provider", "—")
                    resource = tf.get("resource", tf.get("type", "—"))
                    state = tf.get("state", "unknown")

                results.append({
                    "site": site,
                    "stage": stage,
                    "host": host_name,
                    "ansible_host": host_data.get("ansible_host") or host_data.get("address") or "—",
                    "managed": managed,
                    "provider": provider,
                    "resource": resource,
                    "workspace": tf.get("workspace", "default"),
                    "state": state,
                })
        except (ValueError, IndexError):
            continue
        except Exception as exc:
            ctx.log.error(f"UI: Terraform scan failed for {yaml_file}: {exc}")

    return sorted(results, key=lambda x: (not x["managed"], x["site"], x["stage"], x["host"]))


def render_terraform_panel(ctx, service):
    """Render the Provision/Terraform tab content. Returns the refresh callable."""

    state = service.state

    def _emit(payload: dict):
        ctx.emit("iac:webhook_verified", payload)

    @ui.refreshable
    def _panel():
        hosts = _scan_terraform_hosts(ctx, service)
        managed = [h for h in hosts if h["managed"]]
        unmanaged = [h for h in hosts if not h["managed"]]

        # Confirm dialog: whole-infra apply.
        with ui.dialog() as deploy_infra_dialog, ui.card().classes(
            f"{UIStyles.MODAL_CONTAINER} !bg-zinc-900 border border-violet-500/40"
        ):
            with ui.column().classes("gap-3 p-2"):
                with ui.row().classes("items-center gap-2"):
                    ui.icon("warning", size="22px").classes("text-violet-400")
                    ui.label("Deploy entire infrastructure?").classes(
                        "text-base font-bold text-slate-100"
                    )
                ui.label(
                    "This runs `tofu apply` across every Terraform environment and "
                    "will create, change or destroy real infrastructure to match the "
                    "desired plan. Run Check Env first to review the plan."
                ).classes("text-xs text-slate-400 max-w-md")
                with ui.row().classes("w-full justify-end gap-2 mt-1"):
                    ui.button("Cancel", on_click=deploy_infra_dialog.close).props(
                        "flat rounded size=sm color=zinc"
                    )
                    ui.button(
                        "Deploy Infra", icon="rocket_launch", color="violet",
                        on_click=lambda: [
                            _emit({
                                "pipeline_type": "infra_apply",
                                "approve": True,
                                "manual": True,
                                "trigger": "ui_infra_apply",
                            }),
                            deploy_infra_dialog.close(),
                            ui.notify("Infrastructure deploy queued.", type="positive"),
                        ],
                    ).props("unelevated rounded size=sm")

        # Header + actions
        with ui.row().classes("w-full justify-between items-end"):
            c.section_header(
                "Provisioning (Terraform)",
                "Bring bare hosts into existence before Ansible configures them.",
                icon="dns", color="violet",
            )
            with ui.row().classes("items-center gap-2"):
                ui.button(
                    "Check Env", icon="preview",
                    on_click=lambda: [
                        _emit({
                            "pipeline_type": "infra_plan",
                            "manual": True,
                            "trigger": "ui_infra_plan",
                        }),
                        ui.notify("Infrastructure plan (Check Env) queued.", type="info"),
                    ],
                ).props("outline rounded size=sm color=violet").tooltip(
                    "Run a read-only Terraform plan across all environments"
                ).bind_enabled_from(state, "is_running", backward=lambda x: not x)
                ui.button(
                    "Deploy Infra", icon="rocket_launch",
                    on_click=deploy_infra_dialog.open,
                ).props("unelevated rounded size=sm color=violet").tooltip(
                    "Apply Terraform across the entire infrastructure"
                ).bind_enabled_from(state, "is_running", backward=lambda x: not x)
                ui.button(icon="refresh", on_click=_panel.refresh).props(
                    "flat round color=zinc-500"
                )

        # KPI summary
        with ui.grid(columns="repeat(auto-fit, minmax(180px, 1fr))").classes("w-full gap-4"):
            c.kpi_card("Total Hosts", str(len(hosts)), icon="lan", color="indigo")
            c.kpi_card("Terraform-Managed", str(len(managed)), icon="cloud_done", color="violet",
                       sub="have a terraform block")
            c.kpi_card("Unmanaged", str(len(unmanaged)), icon="cloud_off", color="amber",
                       sub="Ansible-only / manual")

        if not hosts:
            with ui.column().classes("w-full items-center py-16 opacity-40"):
                ui.icon("dns", size="3em")
                ui.label("No hosts found. Ensure 'iac_controller/environments' is synced.").classes(
                    "text-sm font-medium"
                )
            return

        # Managed hosts
        if managed:
            with ui.row().classes("w-full items-center gap-3 mt-2"):
                ui.element("div").classes(
                    f"h-9 w-1 bg-gradient-to-b {c.accent_grad('violet')} shrink-0"
                )
                ui.label("Terraform-Managed Hosts").classes(UIStyles.TITLE_H3)
            with ui.grid(columns="repeat(auto-fill, minmax(320px, 1fr))").classes("w-full gap-4"):
                for h in managed:
                    _host_card(h, managed=True)

        # Unmanaged hosts
        if unmanaged:
            with ui.row().classes("w-full items-center gap-3 mt-4"):
                ui.element("div").classes(
                    f"h-9 w-1 bg-gradient-to-b {c.accent_grad('amber')} shrink-0"
                )
                ui.label("Awaiting Terraform Definition").classes(UIStyles.TITLE_H3)
            with ui.grid(columns="repeat(auto-fill, minmax(320px, 1fr))").classes("w-full gap-4"):
                for h in unmanaged:
                    _host_card(h, managed=False)

    def _host_card(h, *, managed: bool):
        color = "violet" if managed else "zinc"
        text_c = c.accent_text(color)
        with c.tile(color, inner="w-full p-4 gap-2", card_extra="flex flex-col"):
            with ui.row().classes("w-full justify-between items-start no-wrap"):
                with ui.column().classes("gap-0 min-w-0"):
                    ui.label(h["host"]).classes(
                        "text-md font-bold text-slate-800 dark:text-zinc-100 truncate"
                    ).tooltip(h["host"])
                    ui.label(f"{h['site']} / {h['stage']}").classes(UIStyles.LABEL_MINI)
                ui.icon("dns" if managed else "cloud_off", size="20px").classes(text_c)

            ui.separator().classes("opacity-20")

            for label, value, icon in (
                ("Address", h["ansible_host"], "lan"),
                ("Provider", h["provider"], "cloud"),
                ("Resource", h["resource"], "category"),
                ("Workspace", h["workspace"], "folder"),
            ):
                with ui.row().classes("w-full items-center gap-2 no-wrap"):
                    ui.icon(icon, size="13px").classes("text-slate-400 dark:text-zinc-600 shrink-0")
                    ui.label(label).classes("text-[11px] text-slate-400 dark:text-zinc-500 w-20 shrink-0")
                    ui.label(str(value)).classes(
                        "text-[11px] font-mono text-slate-600 dark:text-zinc-300 truncate"
                    )

            ui.separator().classes("mt-1 opacity-20")
            with ui.row().classes("w-full justify-between items-center gap-2"):
                if managed:
                    with ui.row().classes("items-center gap-1"):
                        c.status_badge("UNKNOWN" if h["state"] == "unknown" else h["state"].upper())
                        verify_btn = ui.button(
                            icon="sync",
                            on_click=lambda hn=h["host"]: (
                                _emit({"pipeline_type": "state_check", "host_name": hn, "manual": True}),
                                ui.notify(f"Checking live Terraform state for '{hn}'…", type="info"),
                            ),
                        ).props("flat round dense size=sm color=violet").tooltip(
                            "Verify live Terraform state (tofu state list) — result in History/notifications"
                        )
                        verify_btn.bind_enabled_from(state, "is_running", backward=lambda x: not x)
                else:
                    ui.label("No terraform block").classes(
                        "text-[10px] italic text-slate-400 dark:text-zinc-500"
                    )

                host_name = h["host"]
                with ui.dialog() as provision_dialog, ui.card().classes(
                    f"{UIStyles.MODAL_CONTAINER} !bg-zinc-900 border border-violet-500/40"
                ):
                    with ui.column().classes("gap-3 p-2"):
                        with ui.row().classes("items-center gap-2"):
                            ui.icon("warning", size="22px").classes("text-violet-400")
                            ui.label(f"Provision host '{host_name}'?").classes(
                                "text-base font-bold text-slate-100"
                            )
                        ui.label(
                            "This runs Terraform to bring the host into existence, then "
                            "the Ansible compliance bootstrap and host rollout. Real "
                            "infrastructure will be created or changed."
                        ).classes("text-xs text-slate-400 max-w-md")
                        with ui.row().classes("w-full justify-end gap-2 mt-1"):
                            ui.button("Cancel", on_click=provision_dialog.close).props(
                                "flat rounded size=sm color=zinc"
                            )
                            ui.button(
                                "Provision", icon="rocket_launch", color="violet",
                                on_click=lambda hn=host_name, d=provision_dialog: [
                                    _emit({
                                        "pipeline_type": "host_provision",
                                        "host_name": hn,
                                        "approve": True,
                                        "manual": True,
                                        "trigger": "ui_host_provision",
                                    }),
                                    d.close(),
                                    ui.notify(f"Host provisioning queued for '{hn}'.", type="positive"),
                                ],
                            ).props("unelevated rounded size=sm")

                btn = ui.button("Provision", icon="rocket_launch").props(
                    "unelevated rounded size=sm color=violet"
                )
                if managed:
                    btn.on_click(provision_dialog.open)
                    btn.tooltip(f"Provision {h['host']} (Terraform + Ansible bootstrap)")
                    btn.bind_enabled_from(state, "is_running", backward=lambda x: not x)
                else:
                    btn.classes("opacity-60").tooltip(
                        "Add a terraform: block to this host to enable provisioning"
                    ).set_enabled(False)

    _panel()
    return _panel.refresh
