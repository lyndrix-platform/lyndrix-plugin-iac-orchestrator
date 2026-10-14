"""Test bootstrap for the orchestrator plugin.

* ``core.services`` must be imported before ``core.api`` (circular import in
  lyndrix-core — the runtime loads services first via main.py).
* The plugin uses package-relative imports (``from ...stages``), so it is only
  importable as a *package*, the way core loads it. We register the plugin
  root under the alias ``iac_plugin`` so tests can do
  ``from iac_plugin.app.controller.engine import DeploymentEngine``.
* Vendored dependencies (deepdiff, …) live in ``vendor/``.

Run inside the dev container:  PYTHONPATH=/app pytest -q tests
"""
# TODO(agent): fix the core.api ↔ ui.layout cycle in lyndrix-core and drop the first import.
import sys
import types
from pathlib import Path

import core.services  # noqa: F401

_ROOT = Path(__file__).resolve().parents[1]
_VENDOR = _ROOT / "vendor"
if _VENDOR.is_dir() and str(_VENDOR) not in sys.path:
    sys.path.insert(0, str(_VENDOR))

if "iac_plugin" not in sys.modules:
    _pkg = types.ModuleType("iac_plugin")
    _pkg.__path__ = [str(_ROOT)]  # namespace-style package rooted at the plugin dir
    sys.modules["iac_plugin"] = _pkg
