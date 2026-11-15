"""Session-wide safety net for BOTH test trees (``tests/`` and ``iac_core/tests``).

The suite must never reach the Docker daemon of the machine it runs on: on a
runner host the engine's start-up reconciliation lists the containers labelled
``iac_job_id`` and removes those whose job is not RUNNING in the (test)
database. Staging 2026-11-15 lost IaC jobs that way.

Two independent layers:

* ``tests/conftest.py::_no_real_docker`` makes the Python entry points raise.
* This file puts a stub ``docker`` first on ``PATH`` before any test module is
  imported. Whatever slips past the first layer (a new call site, a child
  process) executes the stub, which does nothing but record the call — and the
  session then fails.
"""
from __future__ import annotations

import os
import stat
import tempfile
from pathlib import Path

_STUB_DIR = Path(tempfile.mkdtemp(prefix="iac-tests-no-docker-"))
_CALLS = _STUB_DIR / "calls.log"
_stub = _STUB_DIR / "docker"
_stub.write_text(
    "#!/bin/sh\n"
    f"echo \"$*\" >> '{_CALLS}'\n"
    "echo 'iac-orchestrator tests must not call docker' >&2\n"
    "exit 97\n"
)
_stub.chmod(_stub.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
os.environ["PATH"] = f"{_STUB_DIR}{os.pathsep}{os.environ.get('PATH', '')}"


def pytest_sessionfinish(session, exitstatus):
    """A recorded call means a test reached for Docker: fail the run loudly."""
    calls = _CALLS.read_text().splitlines() if _CALLS.exists() else []
    for path in (_CALLS, _stub):
        try:
            path.unlink()
        except OSError:
            pass
    try:
        _STUB_DIR.rmdir()
    except OSError:
        pass
    if calls:
        print("\nERROR: the test session invoked `docker` (stubbed) %d time(s):" % len(calls))
        for line in calls[:20]:
            print("  docker " + line)
        session.exitstatus = 3
