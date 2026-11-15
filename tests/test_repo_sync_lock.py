"""OC-14: a repository sync never runs inside another pipeline's
sync · generate · publish section (it resets the working trees)."""
import asyncio
from types import SimpleNamespace

from iac_plugin.app.controller.engine import DeploymentEngine, StatePrepareStage
from iac_plugin.app.controller.utils import StageResult


def _engine():
    eng = DeploymentEngine.__new__(DeploymentEngine)
    eng._state_publish_lock = asyncio.Lock()
    eng.events = []

    async def execute_git_sync(repo):
        eng.events.append(f"sync:{repo}")
        await asyncio.sleep(0)
        return True

    eng.execute_git_sync = execute_git_sync
    return eng


def test_repo_sync_waits_for_a_pipeline_that_is_preparing_state():
    async def scenario():
        eng = _engine()
        generated = asyncio.Event()
        publish = asyncio.Event()

        class Generate:
            name = "Native Artifact Generation"

            async def run(self, engine, context):
                engine.events.append("generate")
                generated.set()
                await publish.wait()          # the generated tree is on disk, not yet committed
                return StageResult(True, "ok")

        class Publish:
            name = "Commit & Push: inventory_state"

            async def run(self, engine, context):
                engine.events.append("publish")
                return StageResult(True, "ok")

        prefix = asyncio.create_task(StatePrepareStage([Generate(), Publish()]).run(eng, {}))
        await generated.wait()
        sync = asyncio.create_task(eng.sync_core_repos())
        await asyncio.sleep(0.1)
        assert eng.events == ["generate"]     # the sync did NOT reset the trees under the pipeline
        assert not sync.done()
        publish.set()
        assert (await prefix).success and await sync is True
        return eng.events

    events = asyncio.run(scenario())
    assert events[:2] == ["generate", "publish"]
    assert events[2:] == ["sync:iac_controller", "sync:inventory_state", "sync:config_engine", "sync:aac_factory"]


def test_pipeline_prefix_waits_for_a_running_repo_sync():
    async def scenario():
        eng = _engine()
        gate = asyncio.Event()
        calls = eng.events

        async def slow_sync(repo):
            calls.append(f"sync:{repo}")
            if repo == "inventory_state":
                await gate.wait()
            return True

        eng.execute_git_sync = slow_sync

        class Generate:
            name = "Native Artifact Generation"

            async def run(self, engine, context):
                calls.append("generate")
                return StageResult(True, "ok")

        sync = asyncio.create_task(eng.sync_core_repos())
        await asyncio.sleep(0.05)
        prefix = asyncio.create_task(StatePrepareStage([Generate()]).run(eng, {}))
        await asyncio.sleep(0.1)
        assert "generate" not in calls        # no generation while the trees are being reset
        gate.set()
        await asyncio.gather(sync, prefix)
        return calls

    calls = asyncio.run(scenario())
    assert calls.index("generate") > calls.index("sync:aac_factory")


def test_sync_reports_a_failed_repository():
    async def scenario():
        eng = _engine()

        async def failing(repo):
            return repo != "config_engine"

        eng.execute_git_sync = failing
        return await eng.sync_core_repos(), eng._state_publish_lock.locked()

    ok, still_locked = asyncio.run(scenario())
    assert ok is False and still_locked is False
