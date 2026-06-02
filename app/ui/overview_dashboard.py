"""
Overview statistics dashboard for the IaC Orchestrator.

Renders a modern, at-a-glance summary at the top of the Overview tab:
  * KPI row (total deployments, success rate, avg duration, last deployment)
  * The host lifecycle pipeline (Provision -> Configure -> Deploy) with per-phase
    health, so Terraform shows up as soon as it produces jobs.
  * Status breakdown bars + a recent-deployments feed.

Read-only and refreshable, driven entirely by ``DeploymentStats``. All surfaces
use the themed ``UIStyles`` cards (via ``components``) so light/dark mode is
handled centrally.
"""
from __future__ import annotations

from nicegui import ui
from ui.theme import UIStyles

from ..controller import stats as stats_mod
from ..controller.pipeline_meta import get_phases
from . import components as c


def render_overview_dashboard(ctx, service):
    """Build a refreshable statistics dashboard. Returns the refresh callable."""
    engine = service.engine

    @ui.refreshable
    def _board():
        try:
            jobs = engine.db.get_jobs_for_stats(500)
        except Exception as exc:  # pragma: no cover - defensive
            ctx.log.error(f"UI: stats query failed: {exc}")
            jobs = []
        try:
            tasks = engine.db.get_job_tasks_for_stats(1500)
        except Exception as exc:
            ctx.log.error(f"UI: task stats query failed: {exc}")
            tasks = []
        s = stats_mod.compute(jobs, tasks)

        # ---- KPI row -------------------------------------------------------
        with ui.grid(columns="repeat(auto-fit, minmax(190px, 1fr))").classes("w-full gap-4"):
            c.kpi_card(
                "Total Deployments", str(s.total),
                icon="insights", color="indigo",
                sub=f"{s.finished} finished · {s.running} active",
            )
            rate_color = "emerald" if s.success_rate >= 80 else ("amber" if s.success_rate >= 50 else "rose")
            c.kpi_card(
                "Success Rate", f"{s.success_rate:.0f}%",
                icon="verified", color=rate_color,
                sub=f"{s.success} success · {s.failed} failed",
            )
            c.kpi_card(
                "Avg Duration", stats_mod.humanize_duration(s.avg_duration_s),
                icon="timer", color="sky",
                sub="successful runs",
            )
            last = (s.last_deployment_status or "—").title()
            last_color = "emerald" if s.last_deployment_status == "SUCCESS" else (
                "amber" if s.last_deployment_status == "RUNNING" else (
                    "rose" if s.last_deployment_status in ("FAILED", "ERROR") else "zinc"))
            last_sub = s.last_deployment_at.strftime("%Y-%m-%d %H:%M") if s.last_deployment_at else "No runs yet"
            c.kpi_card(
                "Last Deployment", last,
                icon="schedule", color=last_color, sub=last_sub,
            )

        # ---- Lifecycle pipeline -------------------------------------------
        with c.tile("indigo", inner="w-full p-4 gap-3", hover=False, card_extra="w-full"):
            c.section_header(
                "Host Lifecycle", "Provision → Configure → Deploy",
                icon="account_tree", color="indigo",
            )
            phase_lookup = {p.phase: p for p in s.by_phase}
            with ui.row().classes("w-full items-stretch gap-2 no-wrap overflow-x-auto"):
                phases = get_phases()
                for idx, pdef in enumerate(phases):
                    ps = phase_lookup.get(pdef.id)
                    _phase_step(pdef, ps)
                    if idx < len(phases) - 1:
                        ui.icon("chevron_right", size="22px").classes(
                            "text-slate-300 dark:text-zinc-700 self-center shrink-0"
                        )

        # ---- Breakdown + recent feed --------------------------------------
        with ui.grid(columns="repeat(auto-fit, minmax(320px, 1fr))").classes("w-full gap-4"):
            _status_breakdown(s)
            _recent_feed(s)

    def _phase_step(pdef, ps):
        text_c = c.accent_text(pdef.color)
        total = ps.total if ps else 0
        rate = ps.success_rate if ps else 0.0
        with c.tile(pdef.color, inner="w-full p-3 gap-2",
                    card_extra="flex-1 min-w-[150px]"):
            with ui.row().classes("w-full items-center justify-between no-wrap"):
                with ui.row().classes("items-center gap-2 no-wrap"):
                    ui.icon(pdef.icon, size="18px").classes(text_c)
                    ui.label(pdef.label).classes("text-sm font-bold text-slate-700 dark:text-zinc-200")
                ui.label(str(total)).classes(f"text-lg font-black font-mono {text_c}")
            ui.label(pdef.description).classes("text-[11px] text-slate-500 dark:text-zinc-400 leading-snug")
            if total:
                c.progress_bar(rate, pdef.color)
                with ui.row().classes("w-full justify-between text-[10px] text-slate-500 dark:text-zinc-400"):
                    ui.label(f"{ps.success} ok · {ps.failed} fail")
                    ui.label(f"{rate:.0f}%")
            else:
                with ui.row().classes("items-center gap-1 w-fit"):
                    ui.icon("hourglass_empty", size="11px").classes(text_c)
                    ui.label("No runs yet").classes(f"text-[10px] font-semibold {text_c}")

    def _status_breakdown(s):
        with c.tile("sky", inner="w-full p-4 gap-3", hover=False):
            c.section_header("Status Breakdown", icon="donut_large", color="sky")
            if not s.total:
                ui.label("No deployments recorded yet.").classes(UIStyles.TEXT_MUTED + " italic")
                return
            palette = {
                "SUCCESS": "emerald", "RUNNING": "amber",
                "FAILED": "rose", "ERROR": "rose", "ABORTED": "zinc",
            }
            for status, count in sorted(s.by_status.items(), key=lambda x: -x[1]):
                color = palette.get(status, "indigo")
                text_c = c.accent_text(color)
                pct = (count / s.total) * 100
                with ui.column().classes("w-full gap-1"):
                    with ui.row().classes("w-full justify-between items-center"):
                        with ui.row().classes("items-center gap-2"):
                            ui.element("div").classes(f"h-2 w-2 rounded-full {text_c.replace('text-', 'bg-')}")
                            ui.label(status.title()).classes("text-xs font-semibold text-slate-600 dark:text-zinc-300")
                        ui.label(f"{count}  ·  {pct:.0f}%").classes("text-[11px] text-slate-500 dark:text-zinc-400 font-mono")
                    c.progress_bar(pct, color)

    def _recent_feed(s):
        with c.tile("emerald", inner="w-full p-4 gap-2", hover=False):
            c.section_header("Recent Deployments", icon="history", color="emerald")
            if not s.recent:
                ui.label("Nothing here yet — trigger a deployment to populate the feed.").classes(
                    UIStyles.TEXT_MUTED + " italic"
                )
                return
            with ui.column().classes("w-full gap-1 max-h-[260px] overflow-y-auto pr-1"):
                for job in s.recent:
                    text_c = c.accent_text(job["color"])
                    with ui.row().classes(
                        "w-full items-center gap-3 py-1.5 px-2 rounded-lg no-wrap "
                        "hover:bg-slate-100 dark:hover:bg-zinc-800/60 transition-colors"
                    ):
                        ui.icon(job["icon"], size="16px").classes(f"{text_c} shrink-0")
                        with ui.column().classes("gap-0 flex-grow min-w-0"):
                            ui.label(f"#{job['id']}  {job['type_label']}").classes(
                                "text-xs font-semibold text-slate-700 dark:text-zinc-200 truncate"
                            )
                            ui.label(
                                f"{job['start_label']}  ·  {stats_mod.humanize_duration(job['duration_s'])}"
                            ).classes("text-[10px] text-slate-400 dark:text-zinc-500 truncate")
                        c.status_badge(job["status"])

    _board()
    return _board.refresh
