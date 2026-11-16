"""The suite can never reach the host's Docker daemon (staging finding O-2).

On a runner host the engine's start-up reconciliation lists the containers
labelled ``iac_job_id`` and removes those whose job is not RUNNING in the test
database — a test run killed real IaC jobs that way. Two layers prevent it:
the autouse guard in ``tests/conftest.py`` and the stub ``docker`` that the
root ``conftest.py`` puts first on ``PATH``.
"""
from __future__ import annotations

import asyncio
import os
import shutil
import subprocess

import pytest

from .conftest import RealDockerCalled


def test_async_exec_of_docker_raises():
    async def scenario():
        await asyncio.create_subprocess_exec("docker", "ps")

    with pytest.raises(RealDockerCalled):
        asyncio.run(scenario())


def test_async_shell_of_docker_raises():
    async def scenario():
        await asyncio.create_subprocess_shell("docker ps -q")

    with pytest.raises(RealDockerCalled):
        asyncio.run(scenario())


@pytest.mark.parametrize("call", [subprocess.run, subprocess.call, subprocess.check_call, subprocess.check_output])
def test_sync_calls_of_docker_raise(call):
    # Looked up at call time: the guard replaces the module attributes per test.
    guarded = getattr(subprocess, call.__name__)
    with pytest.raises(RealDockerCalled):
        guarded(["docker", "ps"])
    with pytest.raises(RealDockerCalled):
        guarded(["/usr/bin/docker", "rm", "-f", "x"])


def test_other_commands_are_not_affected():
    assert subprocess.run(["true"]).returncode == 0


def test_path_resolves_docker_to_the_stub():
    """Second layer: whatever bypasses the Python guard executes a stub."""
    found = shutil.which("docker")
    assert found is not None
    assert os.path.basename(os.path.dirname(found)).startswith("iac-tests-no-docker-")
    with open(found) as fh:
        assert "must not call docker" in fh.read()


def test_engine_container_listing_cannot_reach_docker(iac_service):
    """The call that did the damage: an unmocked listing raises instead of
    running ``docker ps`` (the engine reports it as "state unknown")."""
    engine = iac_service.engine

    async def scenario():
        return await engine._list_job_containers()

    with pytest.raises(Exception) as err:
        asyncio.run(scenario())
    assert "docker" in str(err.value).lower() or isinstance(err.value, RealDockerCalled)
