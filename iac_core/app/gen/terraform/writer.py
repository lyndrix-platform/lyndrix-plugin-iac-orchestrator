"""
Writer: ties the mapper + safety guard together and persists the state.

``write_terraform_state`` is the single entry point the generator calls per
environment. It:

  1. reads the previously generated state (for the destroy comparison),
  2. runs the destroy-safety guard,
  3. atomically writes ``<output_dir>/terraform/terraform.tfvars.json``.

Safety knobs are read from the environment so they can be driven by the plugin's
standard ``PLUGIN_IAC_ORCHESTRATOR_*`` settings without threading config through
the whole generator:

  * ``PLUGIN_IAC_ORCHESTRATOR_TF_ALLOW_DESTROY``      (default: false)
  * ``PLUGIN_IAC_ORCHESTRATOR_TF_MAX_DESTROY_RATIO``  (default: 0.5)
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

from .safety import (
    DEFAULT_MAX_DESTROY_RATIO,
    TerraformSafetyError,
    atomic_write_json,
    evaluate_destroy_safety,
    read_existing_state,
)

_log = logging.getLogger("IaC:Generator:TerraformGen")

TFVARS_FILENAME = "terraform.tfvars.json"


def _env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in ("true", "1", "yes", "on")


def _env_ratio(name: str, default: float = DEFAULT_MAX_DESTROY_RATIO) -> float:
    val = os.getenv(name)
    if val is None:
        return default
    try:
        return max(0.0, min(1.0, float(val)))
    except ValueError:
        return default


def tfvars_path(output_dir: Path) -> Path:
    return Path(output_dir) / "terraform" / TFVARS_FILENAME


def write_terraform_state(
    new_state: Dict[str, Any],
    output_dir: Path,
    *,
    context: str = "",
    allow_destroy: Optional[bool] = None,
    max_destroy_ratio: Optional[float] = None,
    log: logging.Logger = _log,
) -> bool:
    """
    Guard + persist a single environment's Terraform state.

    Raises ``TerraformSafetyError`` (propagated to the caller) when the write
    would destroy live infrastructure and destruction is not explicitly allowed.
    Returns True if the file was written, False if it was already up to date.
    """
    if allow_destroy is None:
        allow_destroy = _env_bool("PLUGIN_IAC_ORCHESTRATOR_TF_ALLOW_DESTROY", False)
    if max_destroy_ratio is None:
        max_destroy_ratio = _env_ratio(
            "PLUGIN_IAC_ORCHESTRATOR_TF_MAX_DESTROY_RATIO", DEFAULT_MAX_DESTROY_RATIO
        )

    path = tfvars_path(output_dir)
    old_state = read_existing_state(path)

    evaluate_destroy_safety(
        old_state,
        new_state,
        max_destroy_ratio=max_destroy_ratio,
        allow_destroy=allow_destroy,
        context=context,
        log=log,
    )

    return atomic_write_json(path, new_state, log=log)


__all__ = [
    "write_terraform_state",
    "tfvars_path",
    "TerraformSafetyError",
    "TFVARS_FILENAME",
]
