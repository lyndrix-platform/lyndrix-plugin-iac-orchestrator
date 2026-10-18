"""Boot publish chain: retried with back-off while the Entity Registry is still booting."""

import asyncio
from unittest.mock import AsyncMock

from iac_plugin.app.controller.service import IaCService


class _FakeEngine:
    def __init__(self, registry_ok_sequence):
        self._seq = list(registry_ok_sequence)
        self._registry_ok = None
        self.calls = 0

    async def emit_iac_inventory_updated(self):
        self.calls += 1
        self._registry_ok = self._seq.pop(0) if self._seq else self._registry_ok

    async def emit_monitoring_inventory_sync(self):
        pass

    async def publish_maintenance(self):
        return self._registry_ok is not False


def _service(engine) -> IaCService:
    svc = object.__new__(IaCService)  # skip __init__ (needs ctx/db/config)
    svc.engine = engine
    return svc


def test_chain_runs_once_when_registry_answers():
    eng = _FakeEngine([True])
    sleep = AsyncMock()
    asyncio.run(_service(eng).publish_inventory_chain(retry_delays=(1, 2), sleep=sleep))
    assert eng.calls == 1
    sleep.assert_not_called()


def test_chain_retries_until_registry_is_ready():
    eng = _FakeEngine([False, False, True])
    sleep = AsyncMock()
    asyncio.run(
        _service(eng).publish_inventory_chain(retry_delays=(15, 30, 60), sleep=sleep)
    )
    assert eng.calls == 3
    assert [c.args[0] for c in sleep.call_args_list] == [15, 30]


def test_chain_gives_up_after_the_delays():
    eng = _FakeEngine([False])
    sleep = AsyncMock()
    asyncio.run(_service(eng).publish_inventory_chain(retry_delays=(1, 2), sleep=sleep))
    assert eng.calls == 3  # first attempt + one per delay
    assert sleep.call_count == 2


def test_chain_without_registry_at_all_runs_once():
    """Old core without ctx.request never sets the flag (None) → no retry loop."""
    eng = _FakeEngine([None])
    sleep = AsyncMock()
    asyncio.run(_service(eng).publish_inventory_chain(retry_delays=(1,), sleep=sleep))
    assert eng.calls == 1 and sleep.call_count == 0


def test_default_delays_are_bounded():
    assert sum(IaCService.PUBLISH_RETRY_DELAYS) <= 600
