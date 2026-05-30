"""
Deployment statistics.

Pure, side-effect-free aggregation over job records returned by
``JobDatabase.get_jobs_for_stats``. Keeping this independent of the UI and the
DB session means it is trivially testable and reusable (UI cards, API, widget).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from .pipeline_meta import classify, get_phases, PHASE_OTHER

_SUCCESS = {"SUCCESS"}
_FAILURE = {"FAILED", "ERROR", "ABORTED"}
_RUNNING = {"RUNNING"}


@dataclass
class PhaseStat:
    phase: str
    label: str
    icon: str
    color: str
    total: int = 0
    success: int = 0
    failed: int = 0
    running: int = 0

    @property
    def success_rate(self) -> float:
        finished = self.success + self.failed
        return round((self.success / finished) * 100, 1) if finished else 0.0


@dataclass
class DeploymentStats:
    total: int = 0
    success: int = 0
    failed: int = 0
    running: int = 0
    success_rate: float = 0.0
    avg_duration_s: Optional[float] = None
    last_deployment_status: Optional[str] = None
    last_deployment_at: Optional[datetime] = None
    by_status: Dict[str, int] = field(default_factory=dict)
    by_phase: List[PhaseStat] = field(default_factory=list)
    recent: List[dict] = field(default_factory=list)   # newest-first, enriched

    @property
    def finished(self) -> int:
        return self.success + self.failed


def _duration_seconds(job: dict) -> Optional[float]:
    start, end = job.get("start_time"), job.get("end_time")
    if not start or not end:
        return None
    try:
        return max(0.0, (end - start).total_seconds())
    except Exception:
        return None


def _enrich(job: dict) -> dict:
    meta = classify(job.get("pipeline_type", ""))
    dur = _duration_seconds(job)
    start = job.get("start_time")
    return {
        "id": job.get("id"),
        "pipeline_type": job.get("pipeline_type"),
        "type_label": meta.label,
        "phase": meta.phase,
        "icon": meta.icon,
        "color": meta.color,
        "status": job.get("status"),
        "progress": job.get("progress", 0),
        "duration_s": dur,
        "start_time": start,
        "start_label": start.strftime("%Y-%m-%d %H:%M") if isinstance(start, datetime) else "—",
    }


def compute(jobs: List[dict]) -> DeploymentStats:
    """Aggregate a list of raw job dicts into a :class:`DeploymentStats`."""
    stats = DeploymentStats()
    if not jobs:
        # Still expose empty phase rows so the UI renders the lifecycle skeleton.
        stats.by_phase = [
            PhaseStat(p.id, p.label, p.icon, p.color) for p in get_phases()
        ]
        return stats

    phase_map: Dict[str, PhaseStat] = {
        p.id: PhaseStat(p.id, p.label, p.icon, p.color) for p in get_phases()
    }

    durations: List[float] = []
    for job in jobs:
        status = (job.get("status") or "").upper()
        stats.total += 1
        stats.by_status[status] = stats.by_status.get(status, 0) + 1

        meta = classify(job.get("pipeline_type", ""))
        ps = phase_map.get(meta.phase)
        if ps is None:
            ps = phase_map.setdefault(
                meta.phase, PhaseStat(meta.phase, meta.phase.title(), "category", "zinc")
            )
        ps.total += 1

        if status in _SUCCESS:
            stats.success += 1
            ps.success += 1
        elif status in _FAILURE:
            stats.failed += 1
            ps.failed += 1
        elif status in _RUNNING:
            stats.running += 1
            ps.running += 1

        dur = _duration_seconds(job)
        if dur is not None and status in _SUCCESS:
            durations.append(dur)

    stats.success_rate = (
        round((stats.success / stats.finished) * 100, 1) if stats.finished else 0.0
    )
    stats.avg_duration_s = round(sum(durations) / len(durations), 1) if durations else None

    # jobs are newest-first (ordered by id desc)
    newest = jobs[0]
    stats.last_deployment_status = newest.get("status")
    stats.last_deployment_at = newest.get("start_time")

    # Keep phase ordering consistent with the lifecycle, others appended last.
    ordered = [phase_map[p.id] for p in get_phases() if p.id in phase_map]
    extras = [v for k, v in phase_map.items()
              if k not in {p.id for p in get_phases()} and v.total]
    stats.by_phase = ordered + extras

    stats.recent = [_enrich(j) for j in jobs[:12]]
    return stats


def humanize_duration(seconds: Optional[float]) -> str:
    """Compact human duration, e.g. 42s, 3m 12s, 1h 4m."""
    if seconds is None:
        return "—"
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s"
    return f"{seconds // 3600}h {(seconds % 3600) // 60}m"
