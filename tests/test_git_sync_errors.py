"""OC-16: the reason of a failed repo sync reaches the job log."""
import asyncio
import json
import logging
from unittest.mock import MagicMock

from iac_plugin.app.controller.engine import DeploymentEngine


def _sync(reply, caplog):
    eng = DeploymentEngine.__new__(DeploymentEngine)
    eng.pending_syncs = {}
    eng.ctx = MagicMock()
    eng.ctx.get_secret = lambda key: json.dumps({"url": "https://gitlab.int.fam-feser.de/iac/x.git", "token_key": ""}) \
        if key == "repo_iac_controller_config" else None

    def emit(topic, payload):
        if topic == "git:sync" and reply is not None:
            asyncio.get_running_loop().create_task(eng._on_git_status({**reply, "request_id": payload["request_id"]}))

    eng.ctx.emit = emit
    with caplog.at_level(logging.ERROR, logger="IaC:Engine"):
        return asyncio.run(eng.execute_git_sync("iac_controller"))


def test_core_error_text_is_logged(caplog):
    ok = _sync({"repo_id": "iac_controller", "status": "error",
                "error": "fatal: Authentication failed for 'https://gitlab.int.fam-feser.de/iac/x.git/'"}, caplog)
    assert ok is False
    assert "Git sync of 'iac_controller' failed: fatal: Authentication failed" in caplog.text


def test_success_logs_nothing(caplog):
    assert _sync({"repo_id": "iac_controller", "status": "synced"}, caplog) is True
    assert caplog.text == ""
