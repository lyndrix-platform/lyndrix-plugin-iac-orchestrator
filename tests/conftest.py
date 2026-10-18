"""Test bootstrap for the orchestrator plugin.

* ``core.services`` must be imported before ``core.api`` (circular import in
  lyndrix-core — the runtime loads services first via main.py).
* The plugin uses package-relative imports (``from ...stages``), so it is only
  importable as a *package*, the way core loads it. We register the plugin
  root under the alias ``iac_plugin`` so tests can do
  ``from iac_plugin.app.controller.engine import DeploymentEngine``.
* Vendored dependencies (deepdiff, …) live in ``vendor/``.

Run inside the dev container:  PYTHONPATH=/app pytest -q tests
"""
# TODO(agent): fix the core.api ↔ ui.layout cycle in lyndrix-core and drop the first import.
import sys
import types
from pathlib import Path

import core.services  # noqa: F401

_ROOT = Path(__file__).resolve().parents[1]
_VENDOR = _ROOT / "vendor"
if _VENDOR.is_dir() and str(_VENDOR) not in sys.path:
    sys.path.insert(0, str(_VENDOR))

if "iac_plugin" not in sys.modules:
    _pkg = types.ModuleType("iac_plugin")
    _pkg.__path__ = [str(_ROOT)]  # namespace-style package rooted at the plugin dir
    sys.modules["iac_plugin"] = _pkg


import pytest  # noqa: E402


@pytest.fixture
def sqlite_queue(monkeypatch):
    """A :class:`JobQueue` on an in-memory SQLite engine patched into the core's
    ``db_instance`` (shared by every module that imports it from ``core.api``)."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.pool import StaticPool

    from iac_plugin.app.controller import queue as queue_mod
    from iac_plugin.app.model.models import Base

    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    monkeypatch.setattr(queue_mod.db_instance, "is_connected", True, raising=False)
    monkeypatch.setattr(queue_mod.db_instance, "SessionLocal", sessionmaker(bind=engine), raising=False)
    monkeypatch.setattr(queue_mod.db_instance, "engine", engine, raising=False)
    monkeypatch.setattr(queue_mod.db_instance, "advisory_lock", None, raising=False)
    jq = queue_mod.JobQueue()
    jq.ensure_schema()
    return jq
