"""
Terraform readiness panel.

Display-only (for now) view of the *Provision* phase. The Ansible pipeline is
live today; Terraform is the next step in the host lifecycle:

    Terraform (provision the host)  ->  Ansible (bring to spec)  ->  Services (deploy)

This panel scans the same site/host YAML the Assignments tab uses and surfaces a
``terraform:`` block per host (provider, resource, workspace, state). Hosts
without one are shown as "Ansible-only / unmanaged" so operators can see exactly
what still needs an IaC provisioning definition. The Provision actions are
intentionally disabled and clearly marked *coming soon* -- the wiring point is a
single ``ctx.emit("iac:webhook_verified", {"pipeline_type": "terraform_provision", ...})``
once the engine stage lands, so nothing here needs to change when it does.

All surfaces use the themed ``UIStyles`` cards (via ``components``) so light/dark
mode is handled centrally.
"""
from __future__ import annotations

import yaml
from nicegui import ui
from ui.theme import UIStyles

from . import components as c


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
                tf = host_data.get("terraform") or {}
                managed = bool(tf)
                results.append({
                    "site": site,
                    "stage": stage,
                    "host": host_name,
                    "ansible_host": host_data.get("ansible_host") or host_data.get("address") or "—",
                    "managed": managed,
                    "provider": tf.get("provider", "—") if isinstance(tf, dict) else "—",
                    "resource": tf.get("resource", tf.get("type", "—")) if isinstance(tf, dict) else "—",
                    "workspace": tf.get("workspace", "default") if isinstance(tf, dict) else "default",
                    "state": tf.get("state", "unknown") if isinstance(tf, dict) else "unknown",
                })
        except (ValueError, IndexError):
            continue
        except Exception as exc:
            ctx.log.error(f"UI: Terraform scan failed for {yaml_file}: {exc}")

    return sorted(results, key=lambda x: (not x["managed"], x["site"], x["stage"], x["host"]))


def render_terraform_panel(ctx, service):
    """Render the Provision/Terraform tab content. Returns the refresh callable."""

    @ui.refreshable
    def _panel():
        hosts = _scan_terraform_hosts(ctx, service)
        managed = [h for h in hosts if h["managed"]]
        unmanaged = [h for h in hosts if not h["managed"]]

        # Header + roadmap banner
        with ui.row().classes("w-full justify-between items-end"):
            c.section_header(
                "Provisioning (Terraform)",
                "Bring bare hosts into existence before Ansible configures them.",
                icon="dns",
            )
            ui.button(icon="refresh", on_click=_panel.refresh).props("flat round color=zinc-500")

        with ui.row().classes(UIStyles.WARNING_BANNER + " !bg-violet-500/10 !border-violet-500/30"):
            ui.icon("construction", size="18px").classes("text-violet-400 shrink-0 mt-0.5")
            with ui.column().classes("gap-0"):
                ui.label("Terraform execution is coming next.").classes(
                    "text-sm font-semibold text-violet-300"
                )
                ui.label(
                    "Add a `terraform:` block to a host in its site YAML "
                    "(provider, resource, workspace, state) to register it here. "
                    "Provision actions activate once the engine stage ships."
                ).classes("text-xs text-violet-300/70 leading-snug")

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
            ui.label("Terraform-Managed Hosts").classes(UIStyles.TITLE_H3 + " mt-2")
            with ui.grid(columns="repeat(auto-fill, minmax(320px, 1fr))").classes("w-full gap-4"):
                for h in managed:
                    _host_card(h, managed=True)

        # Unmanaged hosts
        if unmanaged:
            ui.label("Awaiting Terraform Definition").classes(UIStyles.TITLE_H3 + " mt-4")
            with ui.grid(columns="repeat(auto-fill, minmax(320px, 1fr))").classes("w-full gap-4"):
                for h in unmanaged:
                    _host_card(h, managed=False)

    def _host_card(h, *, managed: bool):
        color = "violet" if managed else "zinc"
        text_c = c.accent_text(color)
        with ui.card().classes(c.CARD):
            with ui.column().classes("w-full p-4 gap-2"):
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
                        c.status_badge("UNKNOWN" if h["state"] == "unknown" else h["state"].upper())
                    else:
                        ui.label("No terraform block").classes(
                            "text-[10px] italic text-slate-400 dark:text-zinc-500"
                        )
                    ui.button("Provision", icon="rocket_launch").props(
                        "unelevated rounded size=sm color=violet"
                    ).classes("opacity-60").tooltip("Coming soon — Terraform stage not yet enabled").set_enabled(False)

    _panel()
    return _panel.refresh
