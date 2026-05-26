import logging
from pathlib import Path

class StageResult:
    def __init__(self, success: bool, message: str = "", data: dict = None):
        self.success = success
        self.message = message
        self.data = data or {}

class JobFileLogBridge(logging.Handler):
    def __init__(self, log_path: Path):
        super().__init__()
        self.log_path = log_path
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.setFormatter(logging.Formatter('%(message)s'))

    def emit(self, record):
        log_entry = self.format(record)
        component = record.name.split(':')[-1]
        try:
            with open(self.log_path, 'a', encoding='utf-8') as f:
                f.write(f"[{component}] {log_entry}\n")
        except Exception:
            pass