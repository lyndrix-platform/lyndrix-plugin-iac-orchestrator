"""OC-01 + OC-02: Terraform runs are isolated per job, serialised per state by a
database lock, and their containers are labelled so abort and restart recovery
find them."""
import asyncio
import json
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlalchemy import update

from iac_plugin.app.controller import engine as engine_mod
from iac_plugin.app.controller import queue as queue_mod
from iac_plugin.app.controller.engine import DeploymentEngine
from iac_plugin.app.model.models import IaCJob

RUN_DIR = "inventory_state/.terraform_runs/onprem_prod__docker-x"


class _Proc:
    returncode = 0

    def __init__(self, out=b""):
        self._out = out

    async def communicate(self, input=None):
        return self._out, b""

    async def wait(self):
        return 0


def _engine(monkeypatch, queue, tmp_path, *, ps_output=b"", watch_seconds=0.15, lock_timeout=30):
    """Engine with recorded docker fakes. ``events`` gets (kind, detail) tuples."""
    events: list[tuple] = []
    running: dict[str, int] = {}

    async def fake_exec(*cmd, **kwargs):
        if cmd[:2] == ("docker", "run"):
            name = cmd[cmd.index("--name") + 1]
            job = int(next(c.split("=")[1] for c in cmd if c.startswith("iac_job_id=")))
            running[name] = job
            events.append(("run", name, list(cmd)))
        elif cmd[:3] == ("docker", "rm", "-f"):
            events.append(("rm", list(cmd[3:]), dict(running)))
            for target in cmd[3:]:
                running.pop(target, None)
        elif cmd[:2] == ("docker", "stop"):
            events.append(("stop", list(cmd[2:])))
        elif cmd[:2] == ("docker", "ps"):
            return _Proc(ps_output)
        return _Proc()

    monkeypatch.setattr(engine_mod.asyncio, "create_subprocess_exec", fake_exec)
    monkeypatch.setattr(engine_mod.shutil, "which", lambda _: "/usr/bin/docker")
    eng = DeploymentEngine.__new__(DeploymentEngine)
    eng.state = {}
    eng.ctx = MagicMock()
    eng.ctx.instance_id = "docker-auto-runner"
    eng.db = MagicMock()
    eng.queue = queue
    eng.base_git_dir = tmp_path
    eng._terraform_run_semaphore = asyncio.Semaphore(1)
    eng._TF_LOCK_POLL_SECONDS = 0.02
    eng._sync_runners = lambda *a, **k: None
    eng.config = SimpleNamespace(auto_apply=False, terraform_binary="tofu", terraform_docker_image="img",
                                 host_git_repos_dir="/h", host_terraform_providers_dir=None,
                                 tf_lock_timeout_seconds=lock_timeout)

    async def fake_watch(c_name, task_name, job_id, **kwargs):
        events.append(("watch-start", c_name))
        await asyncio.sleep(watch_seconds)
        ok = running.get(c_name) == job_id          # False when somebody removed our container
        running.pop(c_name, None)
        events.append(("watch-end", c_name, ok))
        return ok, {"exit_code": 0 if ok else None}

    eng._watch_detached_runner = fake_watch
    return eng, events


def _running_jobs(queue, n):
    ids = [queue.enqueue({"pipeline_type": "state_check", "host_name": f"h{i}"}) for i in range(n)]
    for _ in ids:
        queue.claim_next("docker-auto-runner", "core:this")
    return ids


def _tf(eng, job_id, **kwargs):
    kwargs.setdefault("mode", "apply")
    kwargs.setdefault("state_key", "tf:onprem_prod")
    return eng.execute_terraform_docker(run_rel_dir=RUN_DIR, task_name="Terraform Provision: docker-x",
                                        job_id=job_id, host_name="docker-x", **kwargs)


# --- OC-01 ------------------------------------------------------------------------------

def test_two_jobs_for_one_host_never_kill_each_other(sqlite_queue, monkeypatch, tmp_path):
    """p05: host_provision (approved apply) is applying, a state check for the same
    host is claimed one moment later."""
    eng, events = _engine(monkeypatch, sqlite_queue, tmp_path)
    apply_job, check_job = _running_jobs(sqlite_queue, 2)

    async def scenario():
        a = asyncio.create_task(_tf(eng, apply_job, force_apply=True))
        await asyncio.sleep(0.03)
        b = asyncio.create_task(_tf(eng, check_job, mode="state_list"))
        return await asyncio.gather(a, b)

    (ok_a, _), (ok_b, _) = asyncio.run(scenario())
    assert ok_a and ok_b
    runs = [e for e in events if e[0] == "run"]
    names = [e[1] for e in runs]
    assert names == [f"iac-tf-j{apply_job}-Terraform-Provision--docker-x", f"iac-tf-j{check_job}-Terraform-Provision--docker-x"]
    # nothing ever removed a container that was running for ANOTHER job
    for _kind, targets, alive in (e for e in events if e[0] == "rm"):
        assert not (set(targets) & set(alive)), f"rm -f of a running container: {targets}"
    # serialised by the state lock: the second run starts after the first ended
    order = [(e[0], e[1]) for e in events if e[0] in ("run", "watch-end")]
    assert order == [("run", names[0]), ("watch-end", names[0]), ("run", names[1]), ("watch-end", names[1])]
    assert sqlite_queue.lock_holder("tf:onprem_prod") is None        # released


def test_terraform_container_is_labelled(sqlite_queue, monkeypatch, tmp_path):
    eng, events = _engine(monkeypatch, sqlite_queue, tmp_path, watch_seconds=0)
    (job_id,) = _running_jobs(sqlite_queue, 1)
    asyncio.run(_tf(eng, job_id, mode="plan"))
    cmd = next(e[2] for e in events if e[0] == "run")
    labels = {cmd[i + 1] for i, c in enumerate(cmd) if c == "--label"}
    assert {f"iac_job_id={job_id}", "iac_task_name=Terraform Provision: docker-x", "iac_kind=terraform",
            "iac_tf_mode=plan"} <= labels
    assert "--rm" not in cmd and cmd[cmd.index("--name") + 1].startswith(f"iac-tf-j{job_id}-")


def test_lock_semantics(sqlite_queue):
    q = sqlite_queue
    a, b = _running_jobs(q, 2)
    assert q.acquire_lock("tf:onprem_prod", a) == (True, a)
    assert q.acquire_lock("tf:onprem_prod", a) == (True, a)          # re-entrant for the holder
    assert q.acquire_lock("tf:onprem_prod", b) == (False, a)
    assert q.acquire_lock("tf:onprem_dev", b) == (True, b)           # another state is independent
    assert q.release_lock("tf:onprem_prod", b) is False              # only the holder releases
    assert q.acquire_lock("tf:onprem_prod", b) == (False, a)
    # the holder's job is closed without releasing (crash, abort): its container
    # may still run — the lock is only free after the takeover grace (review C2)
    with queue_mod.db_instance.SessionLocal() as s:
        s.execute(update(IaCJob).where(IaCJob.id == a).values(status="FAILED"))
        s.commit()
    assert q.acquire_lock("tf:onprem_prod", b) == (False, a)
    with queue_mod.db_instance.SessionLocal() as s:
        s.execute(update(IaCJob).where(IaCJob.id == a).values(
            heartbeat_utc=datetime.utcnow() - timedelta(seconds=q.lock_takeover_grace + 1)))
        s.commit()
    assert q.acquire_lock("tf:onprem_prod", b) == (True, b)
    assert q.release_lock("tf:onprem_prod", b) is True and q.lock_holder("tf:onprem_prod") is None


def test_busy_state_fails_the_second_job_instead_of_running_it(sqlite_queue, monkeypatch, tmp_path):
    """The holder is RUNNING and never releases (its runner hangs, its tofu may
    still be applying): the second job must not start a tofu on that state."""
    eng, events = _engine(monkeypatch, sqlite_queue, tmp_path, lock_timeout=0)
    holder, waiter = _running_jobs(sqlite_queue, 2)
    assert sqlite_queue.acquire_lock("tf:onprem_prod", holder)[0]
    with pytest.raises(RuntimeError, match=f"in use by job #{holder}"):
        asyncio.run(_tf(eng, waiter, force_apply=True))
    assert [e for e in events if e[0] in ("run", "rm")] == []         # nothing started, nothing removed
    assert sqlite_queue.lock_holder("tf:onprem_prod") == holder


def test_cancelled_run_keeps_the_lock_until_its_job_is_closed(sqlite_queue, monkeypatch, tmp_path):
    eng, events = _engine(monkeypatch, sqlite_queue, tmp_path, watch_seconds=5)
    first, second = _running_jobs(sqlite_queue, 2)

    async def scenario():
        task = asyncio.create_task(_tf(eng, first, force_apply=True))
        for _ in range(100):
            await asyncio.sleep(0.01)
            if any(e[0] == "watch-start" for e in events):
                break
        task.cancel()                                   # abort / plugin reload: the container may live on
        await asyncio.gather(task, return_exceptions=True)
        held = sqlite_queue.lock_holder("tf:onprem_prod")
        with queue_mod.db_instance.SessionLocal() as s:  # … until the job is closed
            s.execute(update(IaCJob).where(IaCJob.id == first).values(status="ABORTED"))
            s.commit()
        # … and then until the runner has stopped its containers (review C2).
        closed = sqlite_queue.acquire_lock("tf:onprem_prod", second)
        assert sqlite_queue.release_job_locks(first) == 1
        eng2, _ = _engine(monkeypatch, sqlite_queue, tmp_path, watch_seconds=0)
        ok, _stats = await _tf(eng2, second, mode="plan")
        return held, closed, ok

    held, closed, ok = asyncio.run(scenario())
    assert held == first and closed == (False, first) and ok is True


# --- per-job run dir -----------------------------------------------------------------------

def test_provision_renders_into_a_per_job_dir_and_promotes_it(sqlite_queue, monkeypatch, tmp_path):
    eng, events = _engine(monkeypatch, sqlite_queue, tmp_path, watch_seconds=0)
    job_id, other_running, other_done = _running_jobs(sqlite_queue, 3)
    with queue_mod.db_instance.SessionLocal() as s:
        s.execute(update(IaCJob).where(IaCJob.id == other_done).values(status="SUCCESS"))
        s.commit()
    tf = tmp_path / "inventory_state" / "onprem" / "prod" / "terraform"
    tf.mkdir(parents=True)
    (tf / "terraform.tfvars.json").write_text(json.dumps({"containers": {"docker-x": {"node_name": "hydra", "vm_id": 101}}}))
    (tmp_path / "infra_engine" / "scripts" / "python").mkdir(parents=True)
    (tmp_path / "infra_engine" / "scripts" / "python" / "render_environment.py").write_text("")
    (tmp_path / "infra_engine" / "terraform-templates").mkdir()
    eng._build_terraform_secrets_payload = lambda ctx: ({}, [])
    runs = tmp_path / "inventory_state" / ".terraform_runs"
    stable, foreign, leftover = (runs / "onprem_prod__docker-x", runs / f"onprem_prod__docker-x__j{other_running}",
                                 runs / f"onprem_prod__docker-x__j{other_done}")
    for d, marker in ((stable, "old"), (foreign, "in-use"), (leftover, "stale")):
        d.mkdir(parents=True)
        (d / "marker").write_text(marker)

    real_watch = eng._watch_detached_runner

    async def watch(c_name, task_name, jid, **kwargs):
        (runs / f"onprem_prod__docker-x__j{jid}" / "tfplan").write_text("plan")   # what tofu leaves behind
        assert (stable / "marker").read_text() == "old"                            # untouched while we run
        return await real_watch(c_name, task_name, jid, **kwargs)

    eng._watch_detached_runner = watch
    ok, _ = asyncio.run(eng.execute_terraform_provision("docker-x", job_id, mode="plan"))
    assert ok
    shell = next(e[2] for e in events if e[0] == "run")[-1]
    assert f"cd '/data/storage/git_repos/{RUN_DIR}__j{job_id}'" in shell
    assert (stable / "tfplan").read_text() == "plan" and not (stable / "marker").exists()   # promoted
    assert not (runs / f"onprem_prod__docker-x__j{job_id}").exists()
    assert (foreign / "marker").read_text() == "in-use"                                     # a running job's dir
    assert not leftover.exists()                                                            # finished job's leftover


# --- OC-02: abort and restart see Terraform containers -------------------------------------

PS = (b"aaa111\tiac-tf-j7-Terraform-Apply--onprem_prod\trunning\t7\tTerraform Apply: onprem_prod\tterraform\n"
      b"bbb222\taac-runner-aac-traefik-docker-dev\trunning\t7\taac-traefik\tansible\n"
      b"ccc333\tiac-tf-j7-old\texited\t7\tTerraform Plan: onprem_dev\t\n")


def test_abort_stops_terraform_gracefully_then_removes_everything(sqlite_queue, monkeypatch, tmp_path):
    eng, events = _engine(monkeypatch, sqlite_queue, tmp_path, ps_output=PS)
    eng.state["active_tasks"] = {"Terraform Apply: onprem_prod": {"status": "running_terraform", "job_id": 7}}
    assert asyncio.run(eng.stop_job_containers(7)) == 3
    ops = [e for e in events if e[0] in ("stop", "rm")]
    assert ops[0] == ("stop", ["-t", "30", "aaa111"])                 # only the running tofu container
    assert ops[1][0] == "rm" and ops[1][1] == ["aaa111", "bbb222", "ccc333"]
    assert eng.state["active_tasks"]["Terraform Apply: onprem_prod"]["status"] == "aborted"


def test_restart_recovery_reattaches_terraform_containers(sqlite_queue, monkeypatch, tmp_path):
    eng, _events = _engine(monkeypatch, sqlite_queue, tmp_path, ps_output=PS)
    containers = asyncio.run(eng._list_job_containers())
    found = {(c["name"], c["task_name"], c["job_id"], c["kind"], c["state"]) for c in containers}
    assert ("iac-tf-j7-Terraform-Apply--onprem_prod", "Terraform Apply: onprem_prod", 7, "terraform", "running") in found
    assert ("aac-runner-aac-traefik-docker-dev", "aac-traefik", 7, "ansible", "running") in found
    assert ("iac-tf-j7-old", "Terraform Plan: onprem_dev", 7, "terraform", "exited") in found    # kind from the name
    eng.register_recovered(7, containers)
    assert eng.state["active_tasks"]["Terraform Apply: onprem_prod"]["status"] == "running_terraform"
    assert eng.attended_job_ids() == {7}                    # heartbeated like a running job task
