"""
Reusable, theme-aware UI building blocks for the IaC Orchestrator dashboard.

These intentionally build on ``ui.theme.UIStyles`` (the Lyndrix design system)
instead of hardcoding backgrounds, so light/dark mode is handled by the central
``lyndrix-card`` theming rather than ad-hoc Tailwind ``bg-white`` classes.
"""
from __future__ import annotations

from typing import Optional

from nicegui import ui
from ui.theme import UIStyles


# Themed card surface (respects light/dark via the `lyndrix-card` rule).
CARD = UIStyles.CARD_BASE + " !p-0"


# Accent stems keyed by the colours pipeline_meta / stats emit.
_ACCENT_TEXT = {
    "violet":  "text-violet-400",
    "sky":     "text-sky-400",
    "emerald": "text-emerald-400",
    "amber":   "text-amber-400",
    "rose":    "text-rose-400",
    "indigo":  "text-indigo-400",
    "cyan":    "text-cyan-400",
    "zinc":    "text-zinc-400",
}
_ACCENT_GRAD = {
    "violet":  "from-violet-400 via-purple-400 to-fuchsia-400",
    "sky":     "from-sky-400 via-cyan-400 to-blue-400",
    "emerald": "from-emerald-400 via-teal-400 to-green-400",
    "amber":   "from-amber-400 via-orange-400 to-yellow-400",
    "rose":    "from-rose-400 via-red-400 to-pink-400",
    "indigo":  "from-indigo-400 via-violet-400 to-purple-400",
    "cyan":    "from-cyan-400 to-sky-400",
    "zinc":    "from-zinc-400 to-zinc-500",
}


def accent_text(color: str) -> str:
    return _ACCENT_TEXT.get(color, _ACCENT_TEXT["zinc"])


def accent_grad(color: str) -> str:
    return _ACCENT_GRAD.get(color, _ACCENT_GRAD["zinc"])


def kpi_card(label: str, value: str, *, icon: str, color: str = "indigo",
             sub: Optional[str] = None):
    """A compact KPI tile: themed surface, accent icon, big value, optional sub."""
    text_c = accent_text(color)
    with ui.card().classes(CARD).style("min-width: 0"):
        with ui.column().classes("w-full p-4 gap-1"):
            with ui.row().classes("w-full items-center justify-between no-wrap"):
                ui.label(label).classes(UIStyles.LABEL_MINI + " truncate")
                ui.icon(icon, size="18px").classes(text_c)
            ui.label(value).classes(
                "text-3xl font-black leading-none text-slate-800 dark:text-zinc-100"
            )
            if sub:
                ui.label(sub).classes("text-xs text-slate-500 dark:text-zinc-400 truncate")


def status_badge(status: str):
    """Coloured status pill consistent with the history tables."""
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
    """A thin gradient progress bar (0..100) on a themed track."""
    grad = accent_grad(color)
    pct = max(0.0, min(100.0, float(percent or 0)))
    with ui.element("div").classes(
        "w-full h-1.5 rounded-full bg-slate-200 dark:bg-zinc-800 overflow-hidden"
    ):
        ui.element("div").classes(
            f"h-full bg-gradient-to-r {grad} rounded-full transition-all"
        ).style(f"width: {pct}%")


def section_header(title: str, subtitle: str = "", icon: Optional[str] = None):
    with ui.row().classes("w-full items-center gap-3"):
        if icon:
            ui.icon(icon, size="20px").classes(UIStyles.ICON_MUTED)
        with ui.column().classes("gap-0"):
            ui.label(title).classes(UIStyles.TITLE_H3)
            if subtitle:
                ui.label(subtitle).classes(UIStyles.TEXT_MUTED + " !text-xs")
