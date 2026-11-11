"""Adopt/import guard (1.5.2).

The generated shell runs inside the runner prelude (``exec 2>&1; set -euo
pipefail`` + EXIT trap), so the tests execute it exactly that way — the 1.4.3
guard passed its tests without ``pipefail`` but aborted silently in production
("step <import> failed with exit code 1", nothing else in the log).

Cases: tracked at the target address (node ok → nothing; node drift → state rm
+ re-import), tracked under another node's module (state rm + re-import),
tracked nowhere (import, non-fatal), and a site name handed as ``host_name``.
"""
import os
import shutil
import subprocess
import tempfile

import pytest

from iac_plugin.app.controller.engine import (
    AdoptHostStrategy,
    DeploymentEngine,
    TerraformAdoptScopeStage,
    TerraformAdoptStage,
)

ADDR = 'module.lxc_cerberus.proxmox_virtual_environment_container.ct["onp-netp-iot"]'
OLD = 'module.lxc_hydra.proxmox_virtual_environment_container.ct["onp-netp-iot"]'
OTHER = 'module.lxc_hydra.proxmox_virtual_environment_container.ct["docker-hydra"]'
SPEC = {"addr": ADDR, "ref": "cerberus/2031", "host": "onp-netp-iot"}

# Same prelude as execute_terraform_docker (minus cd/init).
PRELUDE = (
    "exec 2>&1; set -euo pipefail; STEP=import; "
    "trap 'rc=$?; if [ \"$rc\" -ne 0 ]; then "
    "echo \"[TF] step <$STEP> failed with exit code $rc\"; fi; exit \"$rc\"' EXIT; "
)

# A shell with `set -o pipefail` (the runner image's busybox sh has it; Debian's dash does not).
_SH = shutil.which("busybox") and ["busybox", "sh", "-c"] or (shutil.which("bash") and ["bash", "--posix", "-c"])
pytestmark = pytest.mark.skipif(not _SH, reason="needs busybox or bash for `set -o pipefail`")


def _run(cmd: str, state_list: list[str], node: str = "cerberus", import_fails: bool = False):
    """Run the generated shell with a fake ``tofu``: ``state list`` writes one
    line per call like Go does (so an early-quitting reader SIGPIPEs it),
    ``state show`` reports ``node``, everything else is echoed."""
    work = tempfile.mkdtemp()
    fixture = os.path.join(work, "state.txt")
    with open(fixture, "w", encoding="utf-8") as fh:
        fh.write("".join(f"{line}\n" for line in state_list))
    fake = (
        'tofu() { '
        'if [ "$1" = state ] && [ "$2" = list ]; then '
        f'while IFS= read -r l; do echo "$l"; done < "{fixture}"; '
        'elif [ "$1" = state ] && [ "$2" = show ]; then '
        f'echo "resource x {{"; echo "    node_name      = \\"{node}\\""; echo "}}"; '
        'elif [ "$1" = import ] && [ "${FAIL_IMPORT:-0}" = 1 ]; then echo "TOFU $*"; echo "Error: boom"; return 1; '
        'else echo "TOFU $*"; fi; }; '
    )
    env = dict(os.environ, FAIL_IMPORT="1" if import_fails else "0")
    out = subprocess.run(_SH + [PRELUDE + fake + cmd + "echo REACHED-PLAN"],
                         capture_output=True, text=True, check=False, cwd=work, env=env)
    leftovers = sorted(set(os.listdir(work)) - {"state.txt"})
    shutil.rmtree(work, ignore_errors=True)
    return out.returncode, out.stdout + out.stderr, leftovers


def test_tracked_ct_on_the_right_node_is_left_alone():
    # The docker-cerberus case of job #1335: target address present among others.
    cmd = DeploymentEngine._build_import_cmd("tofu", [SPEC])
    rc, out, left = _run(cmd, [OTHER, ADDR, OLD.replace("onp-netp-iot", "zz")])
    assert rc == 0, out
    assert "already in state (node_name=cerberus)" in out and "REACHED-PLAN" in out
    assert "TOFU import" not in out and "state rm" not in out
    assert left == []


def test_tracked_ct_with_stale_node_name_is_reimported():
    # The onp-netp-prv case: right module, but state still says node_name=hydra.
    cmd = DeploymentEngine._build_import_cmd("tofu", [SPEC])
    rc, out, _ = _run(cmd, [ADDR], node="hydra")
    assert rc == 0, out
    assert "tracked with node_name=hydra but the CT lives on cerberus" in out
    assert f"TOFU state rm -no-color {ADDR}" in out
    assert f"TOFU import -input=false -no-color {ADDR} cerberus/2031" in out


def test_ct_under_another_node_module_is_dropped_and_reimported():
    cmd = DeploymentEngine._build_import_cmd("tofu", [SPEC])
    rc, out, _ = _run(cmd, [OLD, OTHER])
    assert rc == 0, out
    assert f"{OLD} is tracked under another node module" in out
    assert f"TOFU state rm -no-color {OLD}" in out
    assert f"TOFU import -input=false -no-color {ADDR} cerberus/2031" in out
    assert "state mv" not in out


def test_failed_reimport_after_state_rm_is_fatal_and_visible():
    cmd = DeploymentEngine._build_import_cmd("tofu", [SPEC])
    rc, out, _ = _run(cmd, [OLD], import_fails=True)
    assert rc == 1
    assert "Error: boom" in out and "re-import of" in out
    assert "[TF] step <import> failed with exit code 1" in out and "REACHED-PLAN" not in out


def test_untracked_ct_is_imported():
    # The onp-netp-iot case (adopt job #1321): tracked nowhere, other CTs exist.
    cmd = DeploymentEngine._build_import_cmd("tofu", [SPEC])
    rc, out, _ = _run(cmd, [OTHER])
    assert rc == 0, out
    assert f"{ADDR} not in state - importing cerberus/2031" in out
    assert f"TOFU import -input=false -no-color {ADDR} cerberus/2031" in out
    assert "state rm" not in out and "REACHED-PLAN" in out


def test_untracked_ct_import_failure_is_not_fatal():
    cmd = DeploymentEngine._build_import_cmd("tofu", [SPEC])
    rc, out, _ = _run(cmd, [], import_fails=True)
    assert rc == 0, out
    assert "Error: boom" in out and "import skipped" in out and "REACHED-PLAN" in out


def test_spec_without_host_falls_back_to_plain_import():
    cmd = DeploymentEngine._build_import_cmd("tofu", [{"addr": ADDR, "ref": "cerberus/2031"}])
    rc, out, _ = _run(cmd, [OTHER])
    assert rc == 0, out
    assert "import -input=false" in out and "state rm" not in out


class _Engine:
    def __init__(self, hosts, sites):
        self._hosts, self._sites = hosts, sites

    def is_known_host(self, name):
        return name in self._hosts

    def is_known_site(self, name):
        return name in self._sites


def test_site_name_as_host_name_adopts_the_scope():
    strategy = AdoptHostStrategy()
    engine = _Engine(hosts={"onp-netp-iot"}, sites={"onprem"})
    stages = strategy.build_stages(engine, {"host_name": "onprem"}, {})
    assert len(stages) == 1 and isinstance(stages[0], TerraformAdoptScopeStage)
    assert stages[0].limit == "onprem"
    ctx = {}
    stages = strategy.build_stages(engine, {"host_name": "onp-netp-iot"}, ctx)
    assert isinstance(stages[0], TerraformAdoptStage) and ctx["host_name"] == "onp-netp-iot"
    stages = strategy.build_stages(engine, {"limit": "all"}, {})
    assert isinstance(stages[0], TerraformAdoptScopeStage) and stages[0].limit == "all"
