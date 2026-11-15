"""Review C3 / audit OC-08 (B2, B3, B4, B6, B7): the runner's event loop never
waits for the database, Vault or a large YAML file.

The class of the 2026-11-11 incident: one synchronous call against a dead
connection froze the loop — the core's cluster heartbeat with it — and the
runner was reported gone. The ``loop_guard`` fixture (conftest) fails a test
when a database session, a YAML parse or a guarded Vault read happens on a
loop thread.
"""
import asyncio
import threading
import time
from unittest.mock import MagicMock

import pytest

from iac_plugin.app.controller import engine as engine_mod
from iac_plugin.app.controller import offloop
from iac_plugin.app.controller import queue as queue_mod
from iac_plugin.app.controller.engine import DeploymentEngine, JobParked
from iac_plugin.app.controller.offloop import CoalescedWriter, off_loop
from iac_plugin.app.controller.utils import StageResult
from iac_plugin.app.model.models import IaCJob
from iac_plugin.stages import ansible as ansible_stage

ME = "docker-auto-runner"


def _row(job_id):
    with queue_mod.db_instance.SessionLocal() as s:
        return s.get(IaCJob, job_id)


async def _max_loop_lag(coro):
    """Run ``coro`` next to a 10 ms ticker; return (result, worst tick delay)."""
    lags, stop = [], asyncio.Event()

    async def ticker():
        while not stop.is_set():
            t = time.monotonic()
            await asyncio.sleep(0.01)
            lags.append(time.monotonic() - t - 0.01)

    tick = asyncio.create_task(ticker())
    try:
        result = await coro
    finally:
        stop.set()
        await tick
    return result, max(lags or [0.0])


class _Stage:
    def __init__(self, name="work", result=True, exc=None, body=None):
        self.name, self.result, self.exc, self.body = name, result, exc, body

    async def run(self, engine, context):
        if self.body is not None:
            await self.body(engine, context)
        if self.exc is not None:
            raise self.exc
        return StageResult(self.result, "ok" if self.result else "boom")


class _Strategy:
    sync_infra_engine = False
    commit_state = False
    organizes_tasks = False

    def __init__(self, stages):
        self.stages = stages

    def db_tag(self, payload):
        return "connectivity"

    def build_stages(self, engine, payload, context):
        return self.stages


def _wire(svc, monkeypatch, tmp_path, stages):
    """Real engine, queue and database; the shared prefix (git sync, generation)
    is a no-op."""
    async def ok(*a, **k):
        return True

    async def nothing(*a, **k):
        return None

    eng = svc.engine
    eng.execute_git_sync = ok
    eng._generate_inventory_and_snapshot = nothing
    eng.emit_iac_inventory_updated = nothing
    eng.emit_monitoring_inventory_sync = nothing
    eng.publish_maintenance = nothing
    eng._publish_snapshots = nothing
    monkeypatch.setenv("PLUGIN_IAC_ORCHESTRATOR_INTERNAL_LOGS_DIR", str(tmp_path / "logs"))
    monkeypatch.setenv("PLUGIN_IAC_ORCHESTRATOR_INTERNAL_GIT_REPOS_DIR", str(tmp_path / "git"))
    (tmp_path / "logs").mkdir(exist_ok=True)
    monkeypatch.setattr(engine_mod, "get_pipeline_strategy", lambda ptype: _Strategy(stages))
    return eng


def _claim(svc, payload=None):
    job_id = svc.queue.enqueue(payload or {"pipeline_type": "connectivity"})
    claim = svc.queue.claim_next(ME, svc.incarnation)
    svc.db.claims[job_id] = claim.token
    return job_id


# --- the helper itself ------------------------------------------------------------------------

def test_the_guard_reports_a_database_call_on_the_loop(iac_service, loop_guard):
    svc = iac_service
    job_id = _claim(svc)

    async def scenario():
        svc.db.update_progress(job_id, 10, "on the loop")          # the defect
        await off_loop(svc.db.update_progress, job_id, 20, "off the loop")

    asyncio.run(scenario())
    assert len(loop_guard.violations) == 1 and "database session" in loop_guard.violations[0]
    loop_guard.violations.clear()


def test_off_loop_uses_the_plugins_own_pool_and_keeps_the_context():
    """Not the loop's default executor: the core's heartbeat lives there, and a
    few hanging plugin calls must not starve it."""
    import contextvars

    marker = contextvars.ContextVar("marker", default=None)

    async def scenario():
        marker.set("job-7")
        return await off_loop(lambda: (threading.current_thread().name, marker.get()))

    name, seen = asyncio.run(scenario())
    assert name.startswith("iac-offloop") and seen == "job-7"


# --- coalescing ---------------------------------------------------------------------------------

def test_progress_writes_are_coalesced_the_newest_wins_and_the_last_is_written():
    """A thousand TASK lines against a database that answers slowly: one write
    in flight, one waiting — and what was pushed last is what is stored."""
    release, writes = threading.Event(), []

    def write(job_id, values):
        release.wait(5)
        writes.append(dict(values))

    eng = DeploymentEngine.__new__(DeploymentEngine)
    eng.__dict__["_progress_writer"] = CoalescedWriter(write, merge=eng._merge_progress, name="progress")

    async def scenario():
        async def produce():
            eng.push_progress(5, progress=1, current_step="Ansible: first")
            await asyncio.sleep(0.05)                      # the first write is in flight now
            for n in range(2, 1000):
                eng.push_progress(5, progress=n, current_step=f"Ansible: task {n}")
            eng.push_progress(5, progress=None, current_step="Ansible: last")   # keeps progress 999

        _, lag = await _max_loop_lag(produce())
        release.set()
        assert await eng.flush_progress(5, timeout=5)
        return lag

    lag = asyncio.run(scenario())
    assert lag < 0.2
    assert len(writes) == 2
    assert writes[0]["current_step"] == "Ansible: first"
    assert writes[-1] == {"progress": 999, "current_step": "Ansible: last", "exec_state": None}


def test_a_hanging_database_costs_one_thread_per_job_not_one_per_update():
    release, started = threading.Event(), []

    def write(job_id, values):
        started.append(job_id)
        release.wait(5)

    writer = CoalescedWriter(write)

    async def scenario():
        for _ in range(50):
            for job_id in (1, 2, 3):
                writer.push(job_id, {"n": _})
            await asyncio.sleep(0)
        await asyncio.sleep(0.1)
        in_flight = list(started)
        release.set()
        for job_id in (1, 2, 3):
            assert await writer.flush(job_id, timeout=5)
        return in_flight

    assert sorted(asyncio.run(scenario())) == [1, 2, 3]


def test_runner_snapshots_are_coalesced_too():
    """They used to be one task and one thread per second per job."""
    release, written = threading.Event(), []
    eng = DeploymentEngine.__new__(DeploymentEngine)
    eng.state = {"active_tasks": {"t": {"status": "running_ansible", "logs": [], "job_id": 9}}}
    eng._runners_last_push = {}
    eng.ctx = MagicMock()
    eng.queue = MagicMock()

    def set_runners(job_id, snapshot):
        release.wait(5)
        written.append(snapshot["t"]["logs"])

    eng.queue.set_runners = set_runners

    async def scenario():
        for n in range(30):
            eng.state["active_tasks"]["t"]["logs"] = [f"line {n}"]
            eng._sync_runners(9, force=True)
            await asyncio.sleep(0)
        release.set()
        assert await eng._runner_snapshots.flush(9, timeout=5)

    asyncio.run(scenario())
    assert len(written) <= 2 and written[-1] == ["line 29"]
    eng.ctx.create_task.assert_not_called()


# --- ordering: the terminal write is the last write ----------------------------------------------

def test_a_late_progress_write_never_overtakes_the_terminal_write(iac_service, loop_guard):
    """The progress write in flight hangs while the job ends: the terminal write
    waits for it (in its thread), stores what was still waiting, closes the row —
    and whatever is pushed afterwards is dropped."""
    svc = iac_service
    eng = svc.engine
    job_id = _claim(svc)
    order, release = [], threading.Event()
    real_progress, real_close = svc.db.update_progress, svc.db.update_job

    def slow_progress(jid, progress=None, current_step=None, **kw):
        if current_step == "Ansible: slow":
            release.wait(5)
        order.append(("progress", current_step))
        return real_progress(jid, progress, current_step, **kw)

    def close(job_id, status):
        order.append(("terminal", status))
        return real_close(job_id, status)

    svc.db.update_progress, svc.db.update_job = slow_progress, close

    async def scenario():
        eng.push_progress(job_id, 60, "Ansible: slow")
        await asyncio.sleep(0.05)                                   # in flight, hanging
        eng.push_progress(job_id, 70, "Ansible: waiting")
        closing = asyncio.ensure_future(off_loop(eng.close_job_row, job_id, "SUCCESS", end_override=False))
        await asyncio.sleep(0.05)
        assert not closing.done()                                   # … behind the write in flight
        release.set()
        assert await closing is True
        eng.push_progress(job_id, 5, "Ansible: too late")           # a stray watcher
        await eng.flush_progress(job_id, timeout=2)

    asyncio.run(scenario())
    assert order == [("progress", "Ansible: slow"), ("progress", "Ansible: waiting"), ("terminal", "SUCCESS")]
    row = _row(job_id)
    assert row.status == "SUCCESS" and row.progress == 100 and row.current_step == "Ansible: waiting"


def test_an_abort_reason_is_still_written_after_the_job_task_closed_the_row(iac_service):
    """The job task writes ABORTED in its ``finally``; ``abort_job`` then records
    why — that step is the one write that follows a terminal write on purpose."""
    svc = iac_service
    job_id = _claim(svc)
    svc.engine.close_job_row(job_id, "ABORTED", end_override=False)
    svc.db.claims.pop(job_id, None)                                 # the worker lets go of the claim
    assert svc.engine.close_job_row(job_id, "ABORTED", step="Aborted by User", end_override=False) is False
    row = _row(job_id)
    assert row.status == "ABORTED" and row.current_step == "Aborted by User"


# --- whole pipelines under the guard ---------------------------------------------------------------

def test_a_pipeline_never_touches_the_database_or_vault_on_the_loop(iac_service, loop_guard, monkeypatch, tmp_path):
    svc = iac_service
    loop_guard.ctx(svc.ctx)

    async def body(engine, context):
        for n in range(20):
            engine.push_progress(context["job_id"], 50 + n, f"Ansible: task {n}")
            await asyncio.sleep(0)

    eng = _wire(svc, monkeypatch, tmp_path, [_Stage("one", body=body), _Stage("two")])
    job_id = _claim(svc)
    asyncio.run(eng.run_pipeline({"pipeline_type": "connectivity"}, job_id=job_id))
    row = _row(job_id)
    assert row.status == "SUCCESS" and row.progress == 100 and row.current_step == "Completed Successfully"
    assert '"last": true' in row.exec_state                         # the stage marks arrived, in order


def test_a_failing_and_a_parked_pipeline_stay_off_the_loop(iac_service, loop_guard, monkeypatch, tmp_path):
    from datetime import datetime, timedelta, timezone

    svc = iac_service
    loop_guard.ctx(svc.ctx)
    eng = _wire(svc, monkeypatch, tmp_path, [_Stage("broken", exc=RuntimeError("boom"))])
    failed = _claim(svc)
    asyncio.run(eng.run_pipeline({"pipeline_type": "connectivity"}, job_id=failed))
    row = _row(failed)
    assert row.status == "FAILED" and row.current_step == "Failed"

    not_before = datetime.now(timezone.utc) + timedelta(days=1)
    eng = _wire(svc, monkeypatch, tmp_path, [_Stage("park", exc=JobParked(not_before, "window opens later"))])
    parked = _claim(svc)
    asyncio.run(eng.run_pipeline({"pipeline_type": "connectivity"}, job_id=parked))
    row = _row(parked)
    assert row.status == "RUNNING"                                   # the stage parks the row; no terminal write
    assert row.current_step.startswith("Scheduled for ")
    # Nothing is sealed for a parked job: its id runs again when the window opens.
    eng.push_progress(parked, 0, "Pending Start")
    assert asyncio.run(eng.flush_progress(parked)) is True


def test_the_terminal_retry_sleeps_in_a_thread_not_on_the_loop(iac_service, loop_guard, monkeypatch, tmp_path):
    """Review C5 retries the terminal write for up to 7.5 s after a lost
    connection. That used to happen where ``update_job`` was called: on the loop."""
    svc = iac_service
    eng = _wire(svc, monkeypatch, tmp_path, [_Stage()])
    job_id = _claim(svc)
    real = svc.db.update_job

    def slow_close(job_id, status):
        time.sleep(0.5)                                             # the backoff of retry_terminal_write
        return real(job_id, status)

    svc.db.update_job = slow_close

    async def scenario():
        return await _max_loop_lag(eng.run_pipeline({"pipeline_type": "connectivity"}, job_id=job_id))

    _, lag = asyncio.run(scenario())
    assert lag < 0.2
    assert _row(job_id).status == "SUCCESS"


def test_watching_a_container_does_not_write_per_task_line_on_the_loop(iac_service, loop_guard, monkeypatch):
    svc = iac_service
    eng = svc.engine
    job_id = _claim(svc)
    lines = [f"TASK [role : step {n}] ***" for n in range(200)] + [
        "PLAY RECAP *********", "host : ok=3 changed=1 unreachable=0 failed=0 skipped=0 rescued=0 ignored=0"]

    class _Stream:
        def __init__(self):
            self._lines = [(line + "\n").encode() for line in lines]

        async def readline(self):
            await asyncio.sleep(0)
            return self._lines.pop(0) if self._lines else b""

    class _Proc:
        def __init__(self, out=b""):
            self.stdout, self._out = _Stream(), out

        async def communicate(self):
            return self._out, b""

    async def fake_exec(*cmd, **kw):
        return _Proc(b"0" if cmd[:2] == ("docker", "wait") else b"")

    monkeypatch.setattr(engine_mod.asyncio, "create_subprocess_exec", fake_exec)
    eng._log_line = MagicMock()
    writes = []
    real = svc.db.update_progress
    svc.db.update_progress = lambda *a, **k: (writes.append(a), real(*a, **k))[1]

    async def scenario():
        ok, _stats = await eng._watch_detached_runner("aac-runner-x", "x", job_id, kind="ansible")
        await eng.flush_progress(job_id)
        return ok

    assert asyncio.run(scenario()) is True
    assert 1 <= len(writes) < 200                                    # coalesced, not one UPDATE per line
    assert _row(job_id).current_step == "Ansible: role : step 199"


# --- stages -----------------------------------------------------------------------------------------

def test_bulk_rollout_bookkeeping_is_off_the_loop_and_in_order(loop_guard, tmp_path):
    """The pending list is written by concurrently finishing deploys: the row
    must end with the list of the LAST one (recovery decides from it whether a
    rollout was interrupted)."""
    catalog = tmp_path / "iac_controller" / "environments" / "global"
    catalog.mkdir(parents=True)
    names = [f"aac-svc-{n}" for n in range(8)]
    (catalog / "02_service_catalog.yml").write_text(
        "service_catalog:\n  services:\n" + "".join(f"    - name: {n}\n" for n in names))
    stored = []

    class _Db:
        def update_pending_tasks(self, job_id, pending):
            loop_guard.check("update_pending_tasks")
            time.sleep(0.01 * len(pending))                         # longer lists are slower: tempt a reorder
            stored.append(list(pending))

        def update_progress(self, *a, **k):
            loop_guard.check("update_progress")

    async def deploy(**kwargs):
        await asyncio.sleep(0)
        return True, {"successful_hosts": 1, "failed_hosts": 0}

    engine = MagicMock()
    engine.base_git_dir = tmp_path
    engine.db = _Db()
    engine.execute_ansible_docker = deploy
    engine.push_progress = MagicMock()
    stage = ansible_stage.AsyncBulkRolloutStage("global/ansible/inventory.yml", limit="all")
    res = asyncio.run(stage.run(engine, {"job_id": 3}))
    assert res.success
    assert stored[0] == names and stored[-1] == []
    assert [len(p) for p in stored] == sorted((len(p) for p in stored), reverse=True)
    assert engine.push_progress.call_args.kwargs["current_step"] == "Bulk Deploying (8/8)"


def test_terraform_settings_and_secrets_are_read_off_the_loop(loop_guard, monkeypatch, tmp_path):
    """``execute_terraform_docker`` reads binary, auto-apply and image (Vault)."""
    eng = DeploymentEngine.__new__(DeploymentEngine)
    eng.state = {}
    eng.queue = None
    eng.ctx = loop_guard.ctx(MagicMock())
    eng.ctx.get_secret.return_value = None

    class _Config:
        host_git_repos_dir = str(tmp_path)
        host_terraform_providers_dir = None

        @property
        def terraform_binary(self):
            loop_guard.check("config.terraform_binary")
            return "tofu"

        @property
        def auto_apply(self):
            loop_guard.check("config.auto_apply")
            return False

        @property
        def tf_lock_timeout_seconds(self):
            loop_guard.check("config.tf_lock_timeout_seconds")
            return 5

        def checked_runner_image(self, kind):
            loop_guard.check("config.checked_runner_image")
            return "ghcr.io/opentofu/opentofu:1.8"

    eng.config = _Config()
    eng._terraform_run_semaphore = asyncio.Semaphore(1)
    eng._job_modes = {4: False}
    spawned = []

    async def spawn(name, cmd):
        spawned.append(cmd)

    async def watched(*a, **k):
        return True, {}

    eng._spawn_terraform_container = spawn
    eng._watch_detached_runner = watched
    ok, _ = asyncio.run(eng.execute_terraform_docker(run_rel_dir="x", task_name="tf", job_id=4, mode="plan"))
    assert ok and "ghcr.io/opentofu/opentofu:1.8" in spawned[0]


# --- boot ---------------------------------------------------------------------------------------------

def test_the_database_bootstrap_runs_off_the_loop_and_the_runner_boot_waits_for_it(iac_service, loop_guard):
    """``create_all`` + the bootstrap advisory lock (up to ~55 s when several
    instances start together) used to run inside ``setup()``."""
    svc = iac_service
    loop_guard.ctx(svc.ctx)
    events, release = [], threading.Event()
    real_bootstrap = svc.bootstrap_db

    def slow_bootstrap(base=None):
        loop_guard.check("bootstrap_db")
        release.wait(5)
        real_bootstrap(base)
        events.append("bootstrapped")

    async def reconcile():
        events.append("reconcile")

    svc.bootstrap_db = slow_bootstrap
    svc.run_startup_reconciliation = reconcile
    svc.start_runner = lambda max_parallel=None: events.append("worker")

    async def scenario():
        svc.schedule_bootstrap()
        boot = asyncio.ensure_future(svc.run_runner_boot())
        _, lag = await _max_loop_lag(asyncio.sleep(0.2))
        assert events == [] and lag < 0.1                           # nothing listed or claimed yet; loop is free
        release.set()
        await boot

    asyncio.run(scenario())
    assert events == ["bootstrapped", "reconcile", "worker"]


def test_a_trigger_during_the_bootstrap_waits_for_the_tables(iac_service):
    svc = iac_service
    release = threading.Event()
    real_bootstrap = svc.bootstrap_db
    svc.bootstrap_db = lambda base=None: (release.wait(5), real_bootstrap(base))[1]

    async def scenario():
        svc.schedule_bootstrap()
        submit = asyncio.ensure_future(svc.submit_job({"pipeline_type": "connectivity"}))
        await asyncio.sleep(0.1)
        assert not submit.done()
        release.set()
        return await submit

    assert asyncio.run(scenario()) > 0


@pytest.fixture(autouse=True)
def _fresh_pool():
    yield
    offloop.shutdown_pool()                                          # a later caller gets a fresh pool
