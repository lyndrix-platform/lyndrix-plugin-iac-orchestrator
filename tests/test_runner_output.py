"""OS-14: a runner's result is its exit code (+ Ansible recap), never a phrase in
the output. The two tolerated non-zero cases are tied to tool, step and shape."""
import asyncio
from unittest.mock import MagicMock

import pytest

from iac_plugin.app.controller import engine as engine_mod
from iac_plugin.app.controller.engine import DeploymentEngine
from iac_plugin.app.controller.runner_output import RunnerOutputParser

RECAP_OK = ["PLAY RECAP *********************************************************************",
            "docker-dev                 : ok=12   changed=3    unreachable=0    failed=0    skipped=2    rescued=0    ignored=0"]
RECAP_FAILED = [RECAP_OK[0],
                "docker-dev                 : ok=4    changed=0    unreachable=0    failed=1    skipped=0    rescued=0    ignored=0",
                "docker-test                : ok=4    changed=0    unreachable=1    failed=0    skipped=0    rescued=0    ignored=0",
                "docker-prod                : ok=9    changed=1    unreachable=0    failed=0    skipped=0    rescued=0    ignored=0"]
NO_HOSTS = ["[WARNING]: Could not match supplied host pattern, ignoring: service_aac_x",
            "ERROR! Specified inventory, host pattern and/or --limit leaves us with no hosts to target."]
TF_ALREADY_RUNNING = [
    "module.lxc_hydra.proxmox_virtual_environment_container.ct[\"docker-x\"]: Modifying... [id=101]",
    "╷",
    "│ Error: error starting container: received an HTTP 500 response - Reason: CT 101 already running",
    "│ ",
    "│   with module.lxc_hydra.proxmox_virtual_environment_container.ct[\"docker-x\"],",
    "│   on modules/lxc/main.tf line 12, in resource \"proxmox_virtual_environment_container\" \"ct\":",
    "╵",
    "[TF] step <apply> failed with exit code 1",
]


def _verdict(kind, lines, exit_code, **kw):
    parser = RunnerOutputParser(kind, **kw)
    for line in lines:
        parser.feed(line)
    return parser.verdict(exit_code)[0], parser


# --- Ansible ---------------------------------------------------------------------------

def test_host_output_cannot_turn_a_failed_playbook_into_success():
    """The audit's scenario: a managed host prints the Proxmox phrase in a task."""
    lines = ["TASK [compliance : check] ****", "fatal: [docker-dev]: FAILED! => {\"msg\": \"CT 1 is already running\"}",
             "ok: [docker-dev] => {\"msg\": \"no hosts to target\"}", *RECAP_FAILED]
    ok, parser = _verdict("ansible", lines, 2)
    assert ok is False and parser.failed_hosts == 2 and parser.successful_hosts == 1


def test_no_hosts_phrase_after_tasks_ran_is_not_a_skip():
    lines = ["PLAY [all] ****", "TASK [x] ****", "ok: [h] => {\"msg\": \"" + NO_HOSTS[1] + "\"}", NO_HOSTS[1]]
    assert _verdict("ansible", lines, 1)[0] is False


def test_warning_alone_is_not_a_skip():
    """Before: 'Could not match supplied host pattern' + non-zero exit = success."""
    assert _verdict("ansible", [NO_HOSTS[0]], 2)[0] is False
    assert _verdict("ansible", [NO_HOSTS[0], "ERROR! the playbook: x.yml could not be found"], 1)[0] is False


@pytest.mark.parametrize("error_line", [NO_HOSTS[1], "[ERROR]: " + NO_HOSTS[1][7:]])
def test_empty_limit_is_still_a_clean_skip(error_line):
    ok, parser = _verdict("ansible", [NO_HOSTS[0], error_line], 1)
    assert ok is True and parser.successful_hosts == 0 and parser.failed_hosts == 0


def test_recap_cross_checks_exit_code_zero():
    assert _verdict("ansible", ["TASK [x] ***", *RECAP_OK], 0)[0] is True
    assert _verdict("ansible", ["TASK [x] ***", *RECAP_FAILED], 0)[0] is False
    assert _verdict("ansible", ["TASK [x] ***", *RECAP_OK], 2)[0] is False      # exit code wins
    assert _verdict("ansible", RECAP_OK, None)[0] is False                      # exit code lost


def test_recap_shaped_task_output_is_not_counted():
    lines = ["TASK [debug] ***", "ok: [h] => docker-dev : ok=1 changed=0 unreachable=0 failed=7", *RECAP_OK]
    ok, parser = _verdict("ansible", lines, 0)
    assert ok is True and parser.failed_hosts == 0 and parser.successful_hosts == 1


# --- Terraform ---------------------------------------------------------------------------

def test_already_running_is_tolerated_only_for_a_real_apply_of_that_host():
    assert _verdict("terraform", TF_ALREADY_RUNNING, 1, tolerate_already_running=True, tf_host="docker-x")[0] is True
    assert _verdict("terraform", TF_ALREADY_RUNNING, 1, tolerate_already_running=True)[0] is True       # env-wide apply
    assert _verdict("terraform", TF_ALREADY_RUNNING, 1)[0] is False                                    # plan / gated
    assert _verdict("terraform", TF_ALREADY_RUNNING, 1, tolerate_already_running=True, tf_host="other")[0] is False
    assert _verdict("ansible", TF_ALREADY_RUNNING, 1, tolerate_already_running=True)[0] is False       # never for Ansible


def test_already_running_does_not_mask_other_errors_or_steps():
    other = ["╷", "│ Error: creating container: storage 'local-fast' does not exist", "╵"]
    lines = TF_ALREADY_RUNNING[:-1] + other + TF_ALREADY_RUNNING[-1:]
    assert _verdict("terraform", lines, 1, tolerate_already_running=True)[0] is False
    init_failed = TF_ALREADY_RUNNING[:-1] + ["[TF] step <init> failed with exit code 1"]
    assert _verdict("terraform", init_failed, 1, tolerate_already_running=True)[0] is False
    # the phrase outside an Error block (e.g. echoed by a provisioner) proves nothing
    assert _verdict("terraform", ["CT 101 already running", "[TF] step <apply> failed with exit code 1"], 1,
                    tolerate_already_running=True)[0] is False


def test_terraform_stats_are_kept():
    lines = ['  # module.lxc_hydra.proxmox_virtual_environment_container.ct["docker-a"] will be created',
             '  # module.lxc_hydra.proxmox_virtual_environment_container.ct["docker-b"] will be destroyed',
             "Plan: 1 to add, 0 to change, 1 to destroy.",
             'module.lxc_hydra.proxmox_virtual_environment_container.ct["docker-a"]: Creation complete after 9s [id=7]']
    ok, parser = _verdict("terraform", lines, 0)
    stats = parser.stats(0)
    assert ok and stats["hosts_to_create"] == ["docker-a"] and stats["hosts_to_destroy"] == ["docker-b"]
    assert stats["created_hosts"] == ["docker-a"] and stats["plan_summary"].startswith("Plan: 1 to add")


# --- watcher integration --------------------------------------------------------------------

class _Stream:
    def __init__(self, lines):
        self._lines = [(line + "\n").encode() for line in lines]

    async def readline(self):
        return self._lines.pop(0) if self._lines else b""


def _watch(monkeypatch, lines, exit_code, **kwargs):
    removed = []

    class _Proc:
        def __init__(self, out=b""):
            self.stdout = _Stream(lines)
            self._out = out

        async def communicate(self):
            return self._out, b""

    async def fake_exec(*cmd, **kw):
        if cmd[:2] == ("docker", "wait"):
            return _Proc(str(exit_code).encode())
        if cmd[:2] == ("docker", "rm"):
            removed.append(cmd[-1])
        return _Proc()

    monkeypatch.setattr(engine_mod.asyncio, "create_subprocess_exec", fake_exec)
    eng = DeploymentEngine.__new__(DeploymentEngine)
    eng.state = {}
    eng.db = MagicMock()
    eng._log_line = MagicMock()
    ok, stats = asyncio.run(eng._watch_detached_runner("aac-runner-x", "x", 5, **kwargs))
    logged = [c.args[1] for c in eng._log_line.call_args_list]
    return ok, stats, logged, removed


def test_watcher_fails_an_ansible_run_that_prints_the_proxmox_phrase(monkeypatch):
    ok, stats, logged, removed = _watch(
        monkeypatch, ["TASK [x] ***", "fatal: [h]: FAILED! => CT 1 is already running", *RECAP_FAILED], 2)
    assert ok is False and stats["exit_code"] == 2 and removed == ["aac-runner-x"]
    assert any("exited with code 2" in line for line in logged)
    assert not any("State drift" in line for line in logged)


def test_watcher_kind_follows_the_container_name_on_recovery_paths(monkeypatch):
    """A re-attached Ansible container is never judged by the Terraform rule."""
    ok, _stats, _logged, _ = _watch(monkeypatch, TF_ALREADY_RUNNING, 1, tolerate_already_running=True)
    assert ok is False
