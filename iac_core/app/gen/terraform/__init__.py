"""
Modular Terraform state generator for the IaC Orchestrator.

Mirrors the layout of the reference ``infra-stack`` repo (proxmox_lxc module +
per-environment tfvars) and adds a destroy-safety guard so a racy or partial
generation can never emit a state that tears down live infrastructure.

Public surface:

    build_terraform_state(config)            -> dict   (pure, no I/O)
    write_terraform_state(state, output_dir) -> bool   (guarded, atomic write)
    generate_terraform_state(config, dir)    -> bool   (build + write, one shot)
    TerraformSafetyError                              (raised on unsafe destroy)
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict

from .mapper import (
    build_container,
    build_containers,
    build_providers,
    build_terraform_state,
)
from .safety import TerraformSafetyError, evaluate_destroy_safety
from .writer import tfvars_path, write_terraform_state

_log = logging.getLogger("IaC:Generator:TerraformGen")


def generate_terraform_state(
    config: Dict[str, Any], output_dir: Path, *, context: str = "", log: logging.Logger = _log
) -> bool:
    """Build and persist an environment's Terraform state in one call."""
    state = build_terraform_state(config, log)
    return write_terraform_state(state, Path(output_dir), context=context, log=log)


__all__ = [
    "build_terraform_state",
    "build_containers",
    "build_container",
    "build_providers",
    "write_terraform_state",
    "generate_terraform_state",
    "evaluate_destroy_safety",
    "tfvars_path",
    "TerraformSafetyError",
]
