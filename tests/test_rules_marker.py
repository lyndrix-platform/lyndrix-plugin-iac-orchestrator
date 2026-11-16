"""OC-06: rule actions are not lost when a pipeline fails between publishing the
generated state and its rule stage.

The rule stage evaluates everything between the marker (last completed rule
stage) and the state the pipeline pinned — against real git repositories, with
the real stage class and the real marker store. The Ansible stage is replaced
by a recorder, so every test can assert what was executed, in which MODE, and
what was NOT executed.
"""
import asyncio
import subprocess
import textwrap
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from iac_plugin.app.controller.engine import DeploymentEngine
from iac_plugin.app.model.database import JobDatabase
from iac_plugin.stages import git as git_stage
from iac_plugin.stages import rules as rules_mod
from iac_plugin.stages.rules import DynamicRuleExecutionStage as Rules

INVENTORY = "global/ansible/inventory.yml"
TFVARS = "onprem/prod/terraform/terraform.tfvars.json"
BYPASS = "No state changes generated. Bypassing execution."


def git(repo, *args):
    return subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t", *args], cwd=repo, check=True,
                          capture_output=True, text=True).stdout.strip()


def _inventory(roles_by_host: dict) -> str:
    hosts = "".join(f"    {host}: {{baseline_roles: [{', '.join(roles)}]}}\n" for host, roles in roles_by_host.items())
    return "all:\n  hosts:\n" + hosts


class Repo:
    """inventory_state + config_engine below one ``base_git_dir``."""

    def __init__(self, root):
        self.root = root
        self.inv = root / "inventory_state"
        (self.inv / "global/ansible").mkdir(parents=True)
        git(self.inv, "init", "-q", "-b", "main")
        (root / "config_engine").mkdir()
        (root / "config_engine/pipeline_rules.yml").write_text(textwrap.dedent("""
            rules:
              - name: baseline
                paths: ["global/ansible/inventory.yml"]
                actions: [{type: baseline_role}]
        """))
        self.roles = {"docker-a": ["chrony"], "docker-b": ["chrony"]}
        self.base = self.commit("base")

    def commit(self, message: str, **gained) -> str:
        """Publish a new state; ``gained`` = host (with ``_`` for ``-``) → new role."""
        for host, role in gained.items():
            self.roles[host.replace("_", "-")].append(role)
        (self.inv / INVENTORY).write_text(_inventory(self.roles))
        git(self.inv, "add", "-A")
        git(self.inv, "commit", "-q", "--allow-empty", "-m", f"ci: automated state update — {message}")
        return git(self.inv, "rev-parse", "HEAD")

    def other_file(self, message: str) -> str:
        path = self.inv / TFVARS
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('{"containers": {}}')
        git(self.inv, "add", "-A")
        git(self.inv, "commit", "-q", "-m", f"ci: automated state update — {message}")
        return git(self.inv, "rev-parse", "HEAD")


class Recorder:
    """Stands in for AnsiblePlaybookStage: records every action the rule engine fires."""

    def __init__(self):
        self.calls: list[dict] = []
        self.fail = False
        self.gate: asyncio.Event | None = None

    def stage(self, **kwargs):
        recorder = self

        class _Stage:
            async def run(self, engine, context):
                recorder.calls.append({"limit": kwargs.get("limit"), "role": (kwargs.get("extra_vars") or {}).get("role_to_run"),
                                       "check_mode": kwargs.get("check_mode"), "job_id": context.get("job_id")})
                if recorder.gate is not None:
                    await recorder.gate.wait()
                if recorder.fail:
                    return rules_mod.StageResult(False, "baseline role failed on the host")
                return rules_mod.StageResult(True, "ok")

        return _Stage()

    @property
    def real(self):
        return [(c["limit"], c["role"]) for c in self.calls if not c["check_mode"]]

    @property
    def checked(self):
        return [(c["limit"], c["role"]) for c in self.calls if c["check_mode"]]


@pytest.fixture
def env(sqlite_queue, tmp_path, monkeypatch):
    repo = Repo(tmp_path / "git")
    rec = Recorder()
    monkeypatch.setattr(rules_mod, "AnsiblePlaybookStage", lambda **kw: rec.stage(**kw))
    db = JobDatabase()
    db.set_rules_marker(repo.base)                           # everything up to "base" is evaluated
    engine = SimpleNamespace(base_git_dir=repo.root, db=db, config=SimpleNamespace(auto_apply=True),
                             _notify=MagicMock())
    return SimpleNamespace(repo=repo, rec=rec, db=db, engine=engine, queue=sqlite_queue)


def _ctx(head: str, *, base: str | None = None, published: bool = True, job_id: int = 1, dry_run: bool = False) -> dict:
    """Context as PublishInventoryStateStage leaves it for a pipeline."""
    ctx = {"job_id": job_id, "dry_run": dry_run, "inventory_state_head_sha": head,
           "inventory_state_base_sha": base or head,
           "inventory_state_commit_status": "pushed" if published else "no_changes"}
    if published:
        ctx["inventory_state_commit_sha"] = head
    return ctx


def _run(env, ctx, pipeline="infra_plan"):
    return asyncio.run(Rules(pipeline).run(env.engine, ctx))


# --- the finding -----------------------------------------------------------------------

def test_next_pipeline_evaluates_what_the_failed_pipeline_published(env):
    """p06: X publishes C1 (docker-a gains a role) and fails before its rule stage.
    Y publishes C2 (an unrelated file) — its rules now see C1 + C2."""
    c1 = env.repo.commit("C1 by pipeline X (X failed in its plan stage)", docker_a="wazuh_agent")
    assert env.db.rules_marker()["sha"] == env.repo.base       # X never reached its rule stage
    c2 = env.repo.other_file("C2 by pipeline Y")

    res = _run(env, _ctx(c2, base=c1))
    assert res.success
    assert env.rec.real == [("docker-a", "wazuh_agent")]        # C1's action ran — once, on that host only
    assert env.db.rules_marker() == {"sha": c2, "failures": 0, "last_error": "", "job_id": 1}

    # … and exactly once: the next pipelines find nothing pending
    assert _run(env, _ctx(c2, published=False, job_id=2)).message == BYPASS
    assert len(env.rec.calls) == 1


def test_pipeline_without_own_changes_catches_up(env):
    """It used to answer "No state changes generated" and bypass."""
    c1 = env.repo.commit("C1 by a pipeline that failed afterwards", docker_b="docker")
    res = _run(env, _ctx(c1, published=False), pipeline="rollout")
    assert res.success and env.rec.real == [("docker-b", "docker")]
    assert env.db.rules_marker()["sha"] == c1


def test_only_its_own_commit_when_nothing_is_pending(env):
    c1 = env.repo.commit("C1", docker_a="wazuh_agent")
    assert _run(env, _ctx(c1, base=env.repo.base)).success
    c2 = env.repo.commit("C2", docker_b="docker")
    assert _run(env, _ctx(c2, base=c1, job_id=2)).success
    assert env.rec.real == [("docker-a", "wazuh_agent"), ("docker-b", "docker")]   # docker-a NOT a second time


# --- catching up never changes the mode -------------------------------------------------

def test_caught_up_actions_stay_checks_while_auto_apply_is_off(env):
    env.engine.config.auto_apply = False
    c1 = env.repo.commit("C1 (failed pipeline)", docker_a="wazuh_agent")
    c2 = env.repo.commit("C2", docker_b="docker")
    assert _run(env, _ctx(c2, base=c1)).success
    assert env.rec.real == []                                   # nothing was applied for real
    assert sorted(env.rec.checked) == [("docker-a", "wazuh_agent"), ("docker-b", "docker")]
    assert env.db.rules_marker()["sha"] == c2                   # consumed, as a plan-gated run always was


def test_dry_run_evaluates_in_check_mode_and_leaves_everything_pending(env):
    c1 = env.repo.commit("C1 (failed pipeline)", docker_a="wazuh_agent")
    res = _run(env, _ctx(c1, published=False, dry_run=True), pipeline="rollout")
    assert res.success and env.rec.real == [] and env.rec.checked == [("docker-a", "wazuh_agent")]
    assert env.db.rules_marker()["sha"] == env.repo.base        # a dry-run changes nothing

    env.rec.fail = True                                         # … and a failing dry-run is not counted
    assert not _run(env, _ctx(c1, published=False, dry_run=True, job_id=2), pipeline="rollout").success
    assert env.db.rules_marker()["failures"] == 0

    env.rec.fail = False
    assert _run(env, _ctx(c1, published=False, job_id=3)).success
    assert env.rec.real == [("docker-a", "wazuh_agent")]        # the real run still does the work


# --- failures ----------------------------------------------------------------------------

def test_failed_evaluation_stays_pending_and_is_given_up_after_three_attempts(env):
    c1 = env.repo.commit("C1", docker_a="wazuh_agent")
    env.rec.fail = True
    for attempt in (1, 2, 3):
        res = _run(env, _ctx(c1, published=attempt == 1, job_id=attempt, base=env.repo.base if attempt == 1 else c1))
        assert not res.success
        marker = env.db.rules_marker()
        assert marker["sha"] == env.repo.base and marker["failures"] == attempt
        assert "baseline role failed" in marker["last_error"]
    assert len(env.rec.calls) == 3
    env.engine._notify.assert_not_called()

    # 4th pipeline brings its own change: the old backlog is skipped — announced — and
    # only the pipeline's own commit is evaluated (the behaviour before the marker).
    env.rec.fail = False
    c2 = env.repo.commit("C2", docker_b="docker")
    res = _run(env, _ctx(c2, base=c1, job_id=4))
    assert res.success
    assert env.rec.real[3:] == [("docker-b", "docker")]         # docker-a's role is NOT retried a 4th time
    title, text, severity = env.engine._notify.call_args.args[:3]
    assert title == "Rule actions skipped" and severity == "negative"
    assert INVENTORY in text and "NOT retried" in text and "baseline role failed" in text
    assert env.db.rules_marker() == {"sha": c2, "failures": 0, "last_error": "", "job_id": 4}


def test_given_up_backlog_with_nothing_new_is_a_bypass(env):
    c1 = env.repo.commit("C1", docker_a="wazuh_agent")
    env.db.set_rules_marker(env.repo.base, failures=3, last_error="boom")
    res = _run(env, _ctx(c1, published=False))
    assert res.success and res.message == BYPASS and env.rec.calls == []
    assert env.db.rules_marker()["sha"] == c1 and env.engine._notify.called


def test_unreadable_marker_fails_the_stage_and_runs_nothing(env):
    c1 = env.repo.commit("C1", docker_a="wazuh_agent")

    def broken():
        raise RuntimeError("(2013, 'Lost connection to server during query')")

    env.engine.db = SimpleNamespace(rules_marker=broken, set_rules_marker=MagicMock())
    res = _run(env, _ctx(c1))
    assert not res.success and "NOT evaluated" in res.message
    assert env.rec.calls == [] and not env.engine.db.set_rules_marker.called   # never guessed, marker untouched


def test_marker_outside_the_history_falls_back_to_the_own_commit(env):
    """inventory_state was replaced / rewritten: no diff against an unknown commit."""
    env.db.set_rules_marker("0" * 40)
    c1 = env.repo.commit("C1", docker_a="wazuh_agent")
    c2 = env.repo.commit("C2", docker_b="docker")
    assert _run(env, _ctx(c2, base=c1)).success
    assert env.rec.real == [("docker-b", "docker")]             # own commit only, as before the marker
    assert env.db.rules_marker()["sha"] == c2                   # re-anchored


def test_without_a_marker_store_the_stage_behaves_as_before(env):
    env.engine.db = None
    c1 = env.repo.commit("C1", docker_a="wazuh_agent")
    c2 = env.repo.commit("C2", docker_b="docker")
    assert _run(env, {"job_id": 1, "inventory_state_commit_status": "no_changes"}).message == BYPASS
    assert _run(env, {"job_id": 1, "inventory_state_commit_status": "pushed", "inventory_state_commit_sha": c2}).success
    assert env.rec.real == [("docker-b", "docker")] and c1


def test_old_side_of_content_diffs_is_the_marker():
    stage = Rules("infra_plan")
    seen = []

    async def fake_git(engine, *args):
        seen.append(args)
        return (0, "")

    stage._git = fake_git
    stage._ref, stage._base = "ccc333", "aaa111"
    asyncio.run(stage._get_changed_files(None))
    assert seen[-1] == ("diff", "--name-only", "--no-renames", "aaa111", "ccc333")
    asyncio.run(stage._changed_terraform_hosts(None, TFVARS))
    asyncio.run(stage._changed_baseline_roles(None, INVENTORY))
    assert ("show", f"aaa111:{TFVARS}") in seen and ("show", f"aaa111:{INVENTORY}") in seen
    assert not [a for a in seen if "~1" in a[-1]]


# --- one rule engine at a time -------------------------------------------------------------

def test_two_pipelines_never_evaluate_the_same_changes_twice(env):
    """Both see the same pending state. The second waits for the rule engine and
    then finds nothing left — the action runs once, not twice."""
    c1 = env.repo.commit("C1", docker_a="wazuh_agent")
    eng = DeploymentEngine.__new__(DeploymentEngine)
    eng.queue, eng.db, eng.base_git_dir = env.queue, env.db, env.repo.root
    eng.ctx = SimpleNamespace(instance_id="docker-auto-runner")
    eng.config = SimpleNamespace(auto_apply=True)
    eng._notify = MagicMock()
    eng._TF_LOCK_POLL_SECONDS = 0.02
    jobs = []
    for _ in range(2):                                           # two RUNNING jobs (the lock belongs to a job)
        env.queue.enqueue({"pipeline_type": "connectivity"})
        jobs.append(env.queue.claim_next("docker-auto-runner")[0])

    async def scenario():
        env.rec.gate = asyncio.Event()
        first = asyncio.create_task(Rules("infra_plan").run(eng, _ctx(c1, published=False, job_id=jobs[0])))
        for _ in range(200):
            await asyncio.sleep(0.01)
            if env.rec.calls:
                break
        second = asyncio.create_task(Rules("rollout").run(eng, _ctx(c1, published=False, job_id=jobs[1])))
        await asyncio.sleep(0.2)
        assert not second.done() and len(env.rec.calls) == 1     # waiting, NOT evaluating next to the first
        env.rec.gate.set()
        return await asyncio.gather(first, second)

    first, second = asyncio.run(scenario())
    assert first.success and second.success and second.message == BYPASS
    assert env.rec.real == [("docker-a", "wazuh_agent")]
    assert env.queue.lock_holder("rules") is None                # released


# --- the publish stage pins the state and initialises the marker ---------------------------

def test_publish_stage_pins_the_state_and_initialises_the_marker_once(sqlite_queue, tmp_path):
    repo = Repo(tmp_path / "git")
    db = JobDatabase()

    async def no_sha(_slug):
        return "0123456789abcdef0123456789abcdef01234567"

    engine = SimpleNamespace(base_git_dir=repo.root, db=db, repo_head_sha=no_sha, repo_token=lambda _slug: None)
    assert db.rules_marker() is None

    (repo.inv / INVENTORY).write_text(_inventory({"docker-a": ["chrony", "wazuh_agent"]}))   # "generated"
    ctx = {"job_id": 7, "payload": {"pipeline_type": "infra_plan"}}
    res = asyncio.run(git_stage.PublishInventoryStateStage().run(engine, ctx))
    head = git(repo.inv, "rev-parse", "HEAD")
    assert res.success and head != repo.base
    assert ctx["inventory_state_head_sha"] == ctx["inventory_state_commit_sha"] == head
    assert ctx["inventory_state_base_sha"] == repo.base
    # first run after the upgrade: the commit it just published is already "pending",
    # so a failure before the rule stage no longer loses it
    assert db.rules_marker()["sha"] == repo.base

    ctx2 = {"job_id": 8, "payload": {"pipeline_type": "rollout"}}
    assert asyncio.run(git_stage.PublishInventoryStateStage().run(engine, ctx2)).success
    assert ctx2["inventory_state_commit_status"] == "no_changes" and "inventory_state_commit_sha" not in ctx2
    assert ctx2["inventory_state_head_sha"] == head              # pinned also without an own commit
    assert db.rules_marker()["sha"] == repo.base                 # initialised once, never moved by publishing


def test_publish_stage_without_a_checkout_fails_cleanly(sqlite_queue, tmp_path):
    """No inventory_state checkout yet: a stage result, not a crash while pinning the state."""
    async def no_sha(_slug):
        return ""

    (tmp_path / "git").mkdir()
    engine = SimpleNamespace(base_git_dir=tmp_path / "git", db=JobDatabase(), repo_head_sha=no_sha,
                             repo_token=lambda _slug: None)
    ctx = {"job_id": 1, "payload": {"pipeline_type": "infra_plan"}}
    res = asyncio.run(git_stage.PublishInventoryStateStage().run(engine, ctx))
    assert not res.success and "not a git checkout" in res.message
    assert "inventory_state_head_sha" not in ctx and JobDatabase().rules_marker() is None


# --- review C6: the marker only moves forward (compare-and-set) --------------------------------------
#
# The rules lock keeps a second rule stage out — as long as the holder job is
# RUNNING in the database. A job that was closed from outside (orphan sweep,
# maximum runtime) goes on until its runner notices, and its lock is taken
# over after the grace. These tests run two rule stages side by side the way
# that looks from the database: without the lock.

def test_set_rules_marker_is_a_compare_and_set(env):
    db, base = env.db, env.repo.base
    c1 = env.repo.commit("one", docker_a="node_exporter")
    assert db.set_rules_marker(c1, job_id=1, expected="f" * 40) is False      # not what the marker is
    assert db.rules_marker()["sha"] == base
    assert db.set_rules_marker(c1, job_id=1, expected=None) is False          # "absent" — but there is one
    assert db.set_rules_marker(c1, job_id=1, expected=base) is True
    assert db.set_rules_marker(base, job_id=2, expected=base) is False        # the loser of the race
    marker = db.rules_marker()
    assert marker["sha"] == c1 and marker["job_id"] == 1
    assert db.set_rules_marker(base) is True                                  # unconditional, as before
    assert db.rules_marker()["sha"] == base


def _overlap(env, first_ctx, second_ctx):
    """Rule stage A starts and hangs in its action; B runs to its end; A finishes."""
    async def scenario():
        gate = env.rec.gate = asyncio.Event()
        first = asyncio.ensure_future(Rules("infra_plan").run(env.engine, first_ctx))
        while not env.rec.calls:
            await asyncio.sleep(0.01)
        env.rec.gate = None                                    # only A waits
        second = await Rules("infra_plan").run(env.engine, second_ctx)
        gate.set()
        return await first, second

    return asyncio.run(scenario())


def test_a_stage_with_an_outdated_view_does_not_move_the_marker_back(env):
    """A (stale lock, still running) evaluates base..c1, B meanwhile base..c2 and
    moves the marker to c2. A's unconditional write used to set it back to c1:
    the next pipeline evaluated c1..c2 again — the same rule actions twice."""
    c1 = env.repo.commit("one", docker_a="node_exporter")
    c2 = env.repo.commit("two", docker_b="fail2ban")
    res_a, res_b = _overlap(env, _ctx(c1, base=env.repo.base, job_id=1), _ctx(c2, base=c1, job_id=2))
    assert res_a.success and res_b.success
    marker = env.db.rules_marker()
    assert marker["sha"] == c2 and marker["job_id"] == 2       # not moved back by job 1

    calls = len(env.rec.calls)
    assert _run(env, _ctx(c2, published=False, job_id=3)).message == BYPASS
    assert len(env.rec.calls) == calls                          # nothing is evaluated a second time


def test_a_marker_that_is_still_behind_is_moved_forward_from_where_it_is(env):
    """The other stage was the OLDER one: its marker (c1) is behind what this
    stage evaluated (c2) — the marker goes forward to c2, never stays at c1."""
    c1 = env.repo.commit("one", docker_a="node_exporter")
    c2 = env.repo.commit("two", docker_b="fail2ban")
    res_a, res_b = _overlap(env, _ctx(c2, base=c1, job_id=1), _ctx(c1, base=env.repo.base, job_id=2))
    assert res_a.success and res_b.success
    marker = env.db.rules_marker()
    assert marker["sha"] == c2 and marker["job_id"] == 1


def test_a_failure_is_not_recorded_on_a_marker_that_moved_on(env):
    c1 = env.repo.commit("one", docker_a="node_exporter")
    c2 = env.repo.commit("two", docker_b="fail2ban")

    async def scenario():
        gate = env.rec.gate = asyncio.Event()
        first = asyncio.ensure_future(Rules("infra_plan").run(env.engine, _ctx(c1, base=env.repo.base, job_id=1)))
        while not env.rec.calls:
            await asyncio.sleep(0.01)
        env.rec.gate = None
        await Rules("infra_plan").run(env.engine, _ctx(c2, base=c1, job_id=2))
        env.rec.fail = True                                    # A's action fails in the end
        gate.set()
        return await first

    assert asyncio.run(scenario()).success is False
    marker = env.db.rules_marker()
    assert marker["sha"] == c2 and marker["failures"] == 0     # B's success stands


def test_giving_up_a_backlog_is_refused_when_the_marker_moved(env, monkeypatch):
    """The give-up skips changes for good and announces it: only the stage whose
    view of the marker is current may do that."""
    stale = env.repo.commit("stale", docker_a="node_exporter")
    own = env.repo.commit("own", docker_b="fail2ban")
    env.db.set_rules_marker(env.repo.base, failures=Rules.MAX_BACKLOG_ATTEMPTS, last_error="boom")
    real_set = env.db.set_rules_marker

    def set_after_someone_else(sha, **kwargs):
        real_set(own, job_id=99)                               # another job got there first
        return real_set(sha, **kwargs)

    monkeypatch.setattr(env.db, "set_rules_marker", set_after_someone_else)
    res = _run(env, _ctx(own, base=stale, job_id=1))
    assert res.success is False and "moved by another job" in res.message
    assert env.rec.calls == [] and not env.engine._notify.called
    assert env.db.rules_marker()["sha"] == own and env.db.rules_marker()["job_id"] == 99
