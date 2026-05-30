"""
Backwards-compatible shim.

The Terraform generator now lives in the modular ``gen.terraform`` package
(schema / mapper / safety / writer). This module re-exports the stable surface
so existing imports (``from gen.terraform_gen import generate_terraform_state``)
keep working.
"""
from __future__ import annotations

from gen.terraform import (  # noqa: F401
    TerraformSafetyError,
    build_terraform_state,
    generate_terraform_state,
    write_terraform_state,
)

__all__ = [
    "generate_terraform_state",
    "build_terraform_state",
    "write_terraform_state",
    "TerraformSafetyError",
]
