"""
Reusable, modern UI building blocks for the IaC Orchestrator dashboard.

Small, composable NiceGUI helpers that keep the look consistent (and the larger
view modules readable). All helpers respect light/dark via Tailwind ``dark:``.
"""
from __future__ import annotations

from typing import Optional

from nicegui import ui


# Tailwind colour stems we expect from pipeline_meta / stats.
_ACCENT = {
    "violet":  ("text-violet-400", "from-violet-400 via-purple-400 to-fuchsia-400", "bg-violet-500/15", "border-violet-500/30"),
    "sky":     ("text-sky-400", "from-sky-400 via-cyan-400 to-blue-400", "bg-sky-500/15", "border-sky-500/30"),
    "emerald": ("text-emerald-400", "from-emerald-400 via-teal-400 to-green-400", "bg-emerald-500/15", "border-emerald-500/30"),
    "amber":   ("text-amber-400", "from-amber-400 via-orange-400 to-yellow-400", "bg-amber-500/15", "border-amber-500/30"),
    "rose":    ("text-rose-400", "from-rose-400 via-red-400 to-pink-400", "bg-rose-500/15", "border-rose-500/30"),
    "indigo":  ("text-indigo-400", "from-indigo-400 via-violet-400 to-purple-400", "bg-indigo-500/15", "border-indigo-500/30"),
    "zinc":    ("text-zinc-400", "from-zinc-400 to-zinc-500", "bg-zinc-500/15", "border-zinc-500/30"),
}

_CARD = (
    "relative flex flex-col rounded-xl border border-slate-200 dark:border-zinc-800 "
    "bg-white dark:bg-zinc-900/60 overflow-hidden transition-all hover:border-slate-300 "
    "dark:hover:border-zinc-700 hover:shadow-lg"
)


def accent(color: str):
    return _ACCENT.get(color, _ACCENT["zinc"])


def kpi_card(label: str, value: str, *, icon: str, color: str = "indigo",
             sub: Optional[str] = None, trend: Optional[str] = None):
    """A compact KPI tile with a gradient top-bar, big value and optional sub-text."""
    text_c, grad, _, _ = accent(color)
    with ui.card().classes(_CARD + " p-0").style("min-width: 0"):
        ui.element("div").classes(f"h-1 w-full bg-gradient-to-r {grad}")
        with ui.column().classes("w-full p-4 gap-1"):
            with ui.row().classes("w-full items-center justify-between no-wrap"):
                ui.label(label).classes(
                    "text-[10px] font-bold uppercase tracking-widest "
                    "text-slate-400 dark:text-zinc-500 truncate"
                )
                ui.icon(icon, size="18px").classes(text_c)
            ui.label(value).classes(
                "text-3xl font-black text-slate-800 dark:text-zinc-100 leading-none"
            )
            if sub:
                ui.label(sub).classes("text-xs text-slate-500 dark:text-zinc-400 truncate")
            if trend:
                ui.label(trend).classes(f"text-[11px] font-semibold {text_c}")


def status_badge(status: str):
    """Render a coloured status badge consistent with the history tables."""
    s = (status or "").upper()
    if s == "SUCCESS":
        cls, ic = "bg-emerald-500/15 text-emerald-400 border-emerald-500/30", "check_circle"
    elif s == "RUNNING":
        cls, ic = "bg-amber-500/15 text-amber-400 border-amber-500/30", "autorenew"
    elif s in ("FAILED", "ERROR"):
        cls, ic = "bg-rose-500/15 text-rose-400 border-rose-500/30", "error"
    elif s == "ABORTED":
        cls, ic = "bg-zinc-500/15 text-zinc-400 border-zinc-500/30", "block"
    else:
        cls, ic = "bg-slate-500/15 text-slate-400 border-slate-500/30", "help"
    with ui.row().classes(
        f"items-center gap-1 px-2 py-0.5 rounded-full border {cls} "
        "text-[10px] font-bold uppercase tracking-wider no-wrap"
    ):
        ui.icon(ic, size="12px")
        ui.label(s or "—")


def progress_bar(percent: float, color: str = "indigo"):
    """A thin labelled progress bar (0..100)."""
    text_c, grad, _, _ = accent(color)
    pct = max(0.0, min(100.0, float(percent or 0)))
    with ui.column().classes("w-full gap-1"):
        with ui.element("div").classes(
            "w-full h-1.5 rounded-full bg-slate-200 dark:bg-zinc-800 overflow-hidden"
        ):
            ui.element("div").classes(
                f"h-full bg-gradient-to-r {grad} rounded-full transition-all"
            ).style(f"width: {pct}%")


def section_header(title: str, subtitle: str = "", icon: Optional[str] = None):
    with ui.row().classes("w-full items-center gap-3"):
        if icon:
            ui.icon(icon, size="20px").classes("text-slate-400 dark:text-zinc-500")
        with ui.column().classes("gap-0"):
            ui.label(title).classes(
                "text-base font-bold text-slate-800 dark:text-zinc-100"
            )
            if subtitle:
                ui.label(subtitle).classes("text-xs text-slate-500 dark:text-zinc-400")
