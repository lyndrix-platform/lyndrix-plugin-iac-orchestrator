"""Pending / deferred service-reconcile records (plugin 1.7).

The ``service_reconcile`` rule action turns controller-diff detections into ``single_service``
jobs. Two situations must not lose the detection, because the inventory diff that produced
it is consumed by the publish commit and the next pipeline sees ``no_changes``:

* **pending** — above the blast-radius thresholds (``service_reconcile_max_hosts`` /
  ``service_reconcile_max_services``) or the dispatch itself failed: an operator approves
  or dismisses it (``GET/POST /reconcile/pending…``).
* **deferred** — the queue refused the job (``DuplicateJob``: the same service is already
  queued/running): re-queued automatically when that job ends
  (:meth:`DeploymentEngine.drain_deferred_reconcile`).

Records live in the shared ``iac_orchestrator_state`` row ``pending_service_reconcile`` so every
cluster instance sees them. The store itself has no core imports: it receives the job
database (``get_state``/``update_state``) and an optional lock factory
(``db_instance.advisory_lock``) from the engine, so it is testable with a dict-backed fake.
"""
from __future__ import annotations

import contextlib
import logging
import uuid
from datetime import datetime, timezone
from typing import Callable, Iterator

log = logging.getLogger("IaC:Reconcile")

STATE_KEY = "pending_service_reconcile"
LOCK_NAME = "iac_service_reconcile"

STATUS_PENDING = "pending"
STATUS_DEFERRED = "deferred"


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


class ReconcileStore:
    """Small list store on top of ``JobDatabase.get_state`` / ``update_state``."""

    def __init__(self, db, lock_factory: Callable[[str], contextlib.AbstractContextManager] | None = None):
        self._db = db
        self._lock_factory = lock_factory

    # ------------------------------------------------------------------ internals
    @contextlib.contextmanager
    def _locked(self) -> Iterator[None]:
        """Serialize read-modify-write across instances when the core offers an advisory
        lock; without one (SQLite tests, old cores) run unlocked."""
        cm = None
        if callable(self._lock_factory):
            try:
                cm = self._lock_factory(LOCK_NAME)
            except Exception as exc:  # pragma: no cover - defensive
                log.debug("RECONCILE: advisory lock unavailable: %s", exc)
                cm = None
        if cm is None:
            yield
            return
        with cm:
            yield

    def _load(self) -> list[dict]:
        if self._db is None:
            return []
        try:
            record = self._db.get_state(STATE_KEY)
        except Exception as exc:
            log.error("RECONCILE: reading %s failed: %s", STATE_KEY, exc)
            return []
        data = (record or {}).get("data") or {}
        items = data.get("items") if isinstance(data, dict) else None
        return [dict(i) for i in (items or []) if isinstance(i, dict)]

    def _save(self, items: list[dict]) -> None:
        if self._db is None:
            return
        self._db.update_state(STATE_KEY, {"items": items, "updated_at": _utcnow()}, "latest")

    # ------------------------------------------------------------------- public
    def list(self) -> list[dict]:
        """Every record, oldest first."""
        return self._load()

    def get(self, record_id: str) -> dict | None:
        for item in self._load():
            if item.get("id") == record_id:
                return item
        return None

    def add(self, *, payload: dict, status: str, reason: str, waiting_for_job: int | None = None,
            controller_sha: str | None = None, inventory_commit: str | None = None,
            source_job_id: int | None = None) -> dict:
        """Append a record (written BEFORE any dispatch so a failure never loses the detection)."""
        record = {
            "id": uuid.uuid4().hex[:12],
            "status": status,
            "reason": reason,
            "service": payload.get("service_name"),
            "stage": payload.get("stage"),
            "site": payload.get("site"),
            "hosts": list(payload.get("target_hosts") or []),
            "changed_keys": list(payload.get("changed_keys") or []),
            "payload": dict(payload),
            "waiting_for_job": waiting_for_job,
            "controller_sha": controller_sha,
            "inventory_commit": inventory_commit,
            "source_job_id": source_job_id,
            "created_at": _utcnow(),
            "updated_at": _utcnow(),
        }
        with self._locked():
            items = self._load()
            items.append(record)
            self._save(items)
        return record

    def update(self, record_id: str, **fields) -> dict | None:
        with self._locked():
            items = self._load()
            for item in items:
                if item.get("id") == record_id:
                    item.update(fields)
                    item["updated_at"] = _utcnow()
                    self._save(items)
                    return item
        return None

    def remove(self, record_id: str) -> bool:
        with self._locked():
            items = self._load()
            kept = [i for i in items if i.get("id") != record_id]
            if len(kept) == len(items):
                return False
            self._save(kept)
            return True

    def pending(self) -> list[dict]:
        return [i for i in self._load() if i.get("status") == STATUS_PENDING]

    def deferred(self) -> list[dict]:
        return [i for i in self._load() if i.get("status") == STATUS_DEFERRED]

    def clear(self) -> int:
        with self._locked():
            items = self._load()
            self._save([])
            return len(items)


def is_duplicate_job(exc: BaseException) -> bool:
    """``DuplicateJob`` detection without importing the queue module (rules.py is loaded
    standalone in tests; the class is matched by name + ``key`` attribute)."""
    return type(exc).__name__ == "DuplicateJob" and hasattr(exc, "key")
