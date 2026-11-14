"""OC-15: "Global Adopt" / "Site Adopt" keep their scope through the queue."""
import json
from types import SimpleNamespace
from unittest.mock import MagicMock

from iac_plugin.app.controller.engine import (
    DeploymentEngine, TerraformAdoptScopeStage, TerraformAdoptStage, get_pipeline_strategy,
)
from iac_plugin.app.controller.job_payload import validate_structure
from iac_plugin.app.controller.queue import dedupe_key_for, normalize_payload


def _engine(tmp_path):
    tf = tmp_path / "inventory_state" / "onprem" / "prod" / "terraform"
    tf.mkdir(parents=True)
    (tf / "terraform.tfvars.json").write_text(json.dumps({"containers": {"docker-a": {"node_name": "hydra", "vm_id": 101}}}))
    ctx = MagicMock()
    ctx.subscribe = MagicMock(return_value=lambda fn: fn)
    return DeploymentEngine(ctx, {}, MagicMock(), SimpleNamespace(git_repos_dir=tmp_path))


def _stage(engine, ui_payload):
    payload = validate_structure(ui_payload)             # what the queue stores
    return payload, get_pipeline_strategy("adopt_host").build_stages(engine, payload, {})[0]


def test_global_and_site_adopt_run_the_scope_stage(tmp_path):
    engine = _engine(tmp_path)
    payload, stage = _stage(engine, {"pipeline_type": "adopt_host", "limit": "all", "manual": True})
    assert "host_name" not in payload and isinstance(stage, TerraformAdoptScopeStage) and stage.limit == "all"
    assert dedupe_key_for(payload) == "adopt_host:scope:all"
    payload, stage = _stage(engine, {"pipeline_type": "adopt_host", "limit": "onprem", "manual": True})
    assert isinstance(stage, TerraformAdoptScopeStage) and stage.limit == "onprem"
    assert dedupe_key_for(payload) == "adopt_host:scope:onprem"


def test_host_adopt_and_rows_queued_before_the_fix(tmp_path):
    engine = _engine(tmp_path)
    payload, stage = _stage(engine, {"pipeline_type": "adopt_host", "host_name": "Docker-A"})
    assert isinstance(stage, TerraformAdoptStage) and stage.host_name == "docker-a"
    assert dedupe_key_for(payload) == "adopt_host:docker-a"
    old_row = {"pipeline_type": "adopt_host", "limit": "all", "host_name": "all"}      # 1.4–1.6.1 normalisation
    stage = get_pipeline_strategy("adopt_host").build_stages(engine, old_row, {})[0]
    assert isinstance(stage, TerraformAdoptScopeStage) and stage.limit == "all"


def test_other_host_scoped_types_still_take_the_limit_as_host():
    assert normalize_payload({"pipeline_type": "compliance", "limit": "Docker-A"})["host_name"] == "docker-a"
    assert normalize_payload({"pipeline_type": "init_host", "limit": "docker-a"})["host_name"] == "docker-a"
