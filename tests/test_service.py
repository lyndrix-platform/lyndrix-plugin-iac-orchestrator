"""
Smoke tests for IaCService composition.

These tests verify that IaCService can be constructed with a minimal stub
context (matching the ModuleContext interface) without raising errors.
No database or plugin runtime is required.
"""
from unittest.mock import MagicMock


def _make_stub_ctx():
    """Return a lightweight stub that satisfies the ModuleContext interface."""
    log = MagicMock()
    log.info = MagicMock()
    log.warning = MagicMock()
    log.error = MagicMock()
    log.debug = MagicMock()

    ctx = MagicMock()
    ctx.log = log
    ctx.state = {}
    ctx.subscribe = MagicMock(return_value=lambda fn: fn)
    ctx.emit = MagicMock()
    ctx.create_task = MagicMock()
    ctx.get_secret = MagicMock(return_value=None)
    ctx.set_secret = MagicMock()
    return ctx


def test_iac_service_construction():
    """IaCService must compose without raising given a valid stub context."""
    from iac_plugin.app.controller.service import IaCService

    ctx = _make_stub_ctx()
    service = IaCService(ctx)

    assert service is not None
    assert service.config is not None
    assert service.db is not None
    assert service.engine is not None
    assert isinstance(service.state, dict)


def test_iac_service_has_required_methods():
    """IaCService must expose the expected public interface."""
    from iac_plugin.app.controller.service import IaCService

    ctx = _make_stub_ctx()
    service = IaCService(ctx)

    required = [
        "bootstrap_db",
        "register_api_routes",
        "run_pipeline",
        "run_startup_reconciliation",
        "emit_monitoring_inventory_sync",
    ]
    for method_name in required:
        assert callable(getattr(service, method_name, None)), (
            f"IaCService is missing method: {method_name}"
        )


def test_iac_service_initial_state():
    """The shared state dict must be pre-populated with the expected keys."""
    from iac_plugin.app.controller.service import IaCService

    ctx = _make_stub_ctx()
    service = IaCService(ctx)

    assert "auto_apply_enabled" in service.state
    assert "last_deployment" in service.state
    assert "is_running" in service.state
    assert "active_tasks" in service.state


def test_runtime_mount_resolution_is_retried_after_a_failure(monkeypatch):
    """OC-21: a socket timeout at boot used to pin the fallback paths until restart."""
    import asyncio

    from iac_plugin.app.controller import service as service_mod
    from iac_plugin.app.controller.service import IaCService

    svc = IaCService(_make_stub_ctx())
    clock = {"t": 100.0}
    monkeypatch.setattr(service_mod.time, "monotonic", lambda: clock["t"])
    calls = []

    async def resolve(targets):
        calls.append(len(targets))
        if len(calls) == 1:
            raise TimeoutError("socket docker:resolve_mounts timed out")
        return {"/data/storage/git_repos": "/host/git"}

    svc.socket_client.resolve_runtime_mounts = resolve
    asyncio.run(svc.ensure_runtime_paths())                 # fails
    asyncio.run(svc.ensure_runtime_paths())                 # within the pause: no second socket round trip
    assert len(calls) == 1 and not getattr(svc, "_runtime_paths_resolved", False)
    clock["t"] += IaCService.RUNTIME_PATH_RETRY_SECONDS + 1
    asyncio.run(svc.ensure_runtime_paths())                 # retried and resolved
    asyncio.run(svc.ensure_runtime_paths())
    assert len(calls) == 2 and svc._runtime_paths_resolved is True
    assert svc.config.host_git_repos_dir == "/host/git"
