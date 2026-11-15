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
def sqlite_queue(monkeypatch, tmp_path):
    """A :class:`JobQueue` on a SQLite FILE patched into the core's ``db_instance``
    (shared by every module that imports it from ``core.api``).

    A file, not ``sqlite://`` + StaticPool: the worker runs its queries in threads
    (``asyncio.to_thread``) and an in-memory database shares ONE connection between
    all of them — a session closing in one thread rolled back the uncommitted
    statement of another. With a file every session has its own connection, like
    in production.
    """
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    from iac_plugin.app.controller import queue as queue_mod
    from iac_plugin.app.model.models import Base

    engine = create_engine(f"sqlite:///{tmp_path / 'jobs.db'}",
                           connect_args={"check_same_thread": False, "timeout": 30})
    Base.metadata.create_all(engine)
    monkeypatch.setattr(queue_mod.db_instance, "is_connected", True, raising=False)
    monkeypatch.setattr(queue_mod.db_instance, "SessionLocal", sessionmaker(bind=engine), raising=False)
    monkeypatch.setattr(queue_mod.db_instance, "engine", engine, raising=False)
    monkeypatch.setattr(queue_mod.db_instance, "advisory_lock", None, raising=False)
    jq = queue_mod.JobQueue()
    jq.ensure_schema()
    return jq


@pytest.fixture
def iac_service(sqlite_queue):
    """An :class:`IaCService` on the SQLite queue with a mock context whose
    ``create_task`` schedules on the running loop. ``svc.ctx`` is the mock."""
    import asyncio
    from unittest.mock import MagicMock

    from iac_plugin.app.controller.service import IaCService

    ctx = MagicMock()
    ctx.subscribe = MagicMock(return_value=lambda fn: fn)
    ctx.get_secret = MagicMock(return_value=None)
    ctx.instance_id = "docker-auto-runner"
    ctx.roles = ["api", "runner"]
    ctx.create_task = lambda coro, name=None: asyncio.get_running_loop().create_task(coro)
    svc = IaCService(ctx)
    svc._runtime_paths_resolved = True
    return svc


@pytest.fixture(autouse=True)
def _reset_api_caches():
    """The API module keeps short-lived process caches (webhook token, public
    health, shared SSE snapshot) — never let one test's values reach the next."""
    from iac_plugin.app.controller import api as api_mod

    def _reset():
        api_mod._token_cache.update(value=None, at=0.0, forced_at=0.0, loaded=False)
        api_mod._health_cache.update(at=0.0, value=None)
        api_mod._snapshot_cache.update(at=0.0, value=None, task=None)
        api_mod.webhook_auth_failures.update(count=0, last_at=None)

    _reset()
    yield
    _reset()


class LoopGuard:
    """Fails a test when blocking work runs on an event-loop thread (review C3).

    Every database session of the plugin (``read_committed_session`` — the
    JobDatabase and the JobQueue both go through it) and every
    ``yaml.safe_load`` is checked; ``guard.ctx(ctx)`` adds the Vault accessor
    of a (mock) plugin context and ``guard.watch(obj, "method", …)`` any other
    synchronous call. The test's OWN inspection of the database belongs outside
    the loop or inside ``with guard.paused():``.
    """

    def __init__(self):
        self.violations: list[str] = []
        self._paused = 0

    def check(self, what: str) -> None:
        import asyncio
        import traceback

        if self._paused:
            return
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return                                   # a worker thread: fine
        frames = [f for f in traceback.extract_stack()[:-2] if "/asyncio/" not in f.filename][-5:]
        trail = " <- ".join(f"{Path(f.filename).name}:{f.lineno} {f.name}" for f in reversed(frames))
        self.violations.append(f"{what} on the event loop: {trail}")

    def wrap(self, fn, what: str):
        def guarded(*args, **kwargs):
            self.check(what)
            return fn(*args, **kwargs)

        return guarded

    def watch(self, obj, *names: str, label: str | None = None):
        for name in names:
            setattr(obj, name, self.wrap(getattr(obj, name), f"{label or type(obj).__name__}.{name}"))
        return obj

    def ctx(self, ctx):
        return self.watch(ctx, "get_secret", label="ctx")

    def paused(self):
        guard = self

        class _Paused:
            def __enter__(self):
                guard._paused += 1

            def __exit__(self, *exc):
                guard._paused -= 1
                return False

        return _Paused()


@pytest.fixture
def loop_guard(monkeypatch):
    """Opt-in: the test fails when a database session, a YAML parse or a guarded
    call happens on the event loop. ``loop_guard.violations`` can be inspected
    (and cleared) by a test that provokes one on purpose."""
    import yaml

    from iac_plugin.app.model import database as database_mod

    guard = LoopGuard()
    monkeypatch.setattr(database_mod, "read_committed_session",
                        guard.wrap(database_mod.read_committed_session, "database session"))
    monkeypatch.setattr(yaml, "safe_load", guard.wrap(yaml.safe_load, "yaml.safe_load"))
    yield guard
    assert not guard.violations, "blocking calls on the event loop:\n" + "\n".join(guard.violations)
