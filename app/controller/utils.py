import contextvars
import logging
from pathlib import Path

# Identifies the pipeline job the current execution context belongs to. Set at
# the start of each ``run_pipeline`` so a per-job log handler can persist only
# its own job's records even when multiple pipelines run concurrently on the
# shared "IaC:Engine" logger.
current_job_id: contextvars.ContextVar[int | None] = contextvars.ContextVar(
    "iac_current_job_id", default=None
)


class StageResult:
    def __init__(self, success: bool, message: str = "", data: dict | None = None):
        self.success = success
        self.message = message
        self.data = data or {}

class JobFileLogBridge(logging.Handler):
    def __init__(self, log_path: Path, job_id: int | None = None):
        super().__init__()
        self.log_path = log_path
        self.job_id = job_id
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.setFormatter(logging.Formatter('%(message)s'))

    def emit(self, record):
        # Only persist records that belong to this bridge's job. When the active
        # context carries a different job_id, skip — this stops concurrent
        # pipelines from cross-contaminating each other's log files. Records with
        # no job context (None) fall through (startup/global lines).
        active = current_job_id.get()
        if self.job_id is not None and active is not None and active != self.job_id:
            return
        log_entry = self.format(record)
        component = record.name.split(':')[-1]
        try:
            with open(self.log_path, 'a', encoding='utf-8') as f:
                f.write(f"[{component}] {log_entry}\n")
        except Exception:
            pass