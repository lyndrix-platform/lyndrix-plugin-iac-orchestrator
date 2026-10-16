"""Adopt/import self-heal (1.4.3): a CT tracked under another node's module is
moved with ``state mv`` instead of being imported twice; a site name handed as
``host_name`` runs the scoped adopt."""
import subprocess
import tempfile

from iac_plugin.app.controller.engine import (
    AdoptHostStrategy,
    DeploymentEngine,
    TerraformAdoptScopeStage,
    TerraformAdoptStage,
)

ADDR = 'module.lxc_cerberus.proxmox_virtual_environment_container.ct["onp-netp-iot"]'
OLD = 'module.lxc_hydra.proxmox_virtual_environment_container.ct["onp-netp-iot"]'
SPEC = {"addr": ADDR, "ref": "cerberus/2031", "host": "onp-netp-iot"}


def _run(cmd: str, state_list: str) -> str:
    """Execute the generated POSIX shell with a fake ``tofu`` that answers
    ``state list`` from a fixture file and echoes every other invocation."""
    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as fh:
        fh.write(state_list + "\n")
        fixture = fh.name
    fake = (
        'tofu() { if [ "$1" = state ] && [ "$2" = list ]; then cat "' + fixture + '"; '
        'else echo "TOFU $*"; fi; }; '
    )
    out = subprocess.run(["sh", "-c", fake + cmd], capture_output=True, text=True, check=False)
    return out.stdout + out.stderr


def test_ct_under_another_node_module_is_moved_not_imported():
    cmd = DeploymentEngine._build_import_cmd("tofu", [SPEC])
    out = _run(cmd, f"{OLD}\nmodule.lxc_hydra.proxmox_virtual_environment_container.ct[\"docker-hydra\"]")
    assert f"moving {OLD} -> {ADDR}" in out
    assert f"TOFU state mv -no-color {OLD} {ADDR}" in out
    assert "TOFU import" not in out


def test_untracked_ct_is_imported():
    cmd = DeploymentEngine._build_import_cmd("tofu", [SPEC])
    out = _run(cmd, 'module.lxc_hydra.proxmox_virtual_environment_container.ct["docker-hydra"]')
    assert f"importing {ADDR}" in out
    assert f"TOFU import -input=false -no-color {ADDR} cerberus/2031" in out
    assert "state mv" not in out


def test_tracked_ct_is_left_alone():
    cmd = DeploymentEngine._build_import_cmd("tofu", [SPEC])
    out = _run(cmd, f"{ADDR}\n{OLD}")
    assert "already in state" in out and "state mv" not in out and "TOFU import" not in out


def test_spec_without_host_falls_back_to_plain_import():
    cmd = DeploymentEngine._build_import_cmd("tofu", [{"addr": ADDR, "ref": "cerberus/2031"}])
    assert "state mv" not in cmd and "import -input=false" in cmd


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
