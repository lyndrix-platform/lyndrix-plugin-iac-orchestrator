"""
Destroy-safety guard for generated Terraform state.

The orchestrator regenerates ``terraform.tfvars.json`` from the SSoT and commits
it to ``inventory_state``; a downstream ``terraform apply`` then reconciles real
infrastructure against it. The danger the user explicitly called out:

    A racy or partial generation run (e.g. reading a half-synced iac_controller,
    a parser hiccup, or an accidentally-emptied stage) could emit an EMPTY or
    drastically-shrunken container map. ``terraform apply`` would then happily
    plan to DESTROY every existing container.

This module is the defence-in-depth that makes that impossible by default:

  * ``evaluate_destroy_safety`` compares the previously-generated state with the
    new one and refuses (raises ``TerraformSafetyError``) when the change would
    remove every managed container, or remove more than ``max_destroy_ratio`` of
    them, unless destruction is explicitly opted into.
  * ``atomic_write_json`` writes via a temp file + ``os.replace`` so a crash mid
    write can never leave a truncated (and therefore destructive) state file.

Safety is ON by default. An operator who genuinely wants to tear hosts down sets
``PLUGIN_IAC_ORCHESTRATOR_TF_ALLOW_DESTROY=true`` (or passes ``allow_destroy``).
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional

from .schema import KEY_CONTAINERS

_log = logging.getLogger("IaC:Generator:TerraformGen")

DEFAULT_MAX_DESTROY_RATIO = 0.5


class TerraformSafetyError(RuntimeError):
    """Raised when writing the new Terraform state would destroy live infra."""


def _containers(state: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(state, dict):
        return {}
    value = state.get(KEY_CONTAINERS)
    return value if isinstance(value, dict) else {}


def read_existing_state(path: Path) -> Optional[Dict[str, Any]]:
    """Load the previously generated state, or None if absent/corrupt."""
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def evaluate_destroy_safety(
    old_state: Optional[Dict[str, Any]],
    new_state: Dict[str, Any],
    *,
    max_destroy_ratio: float = DEFAULT_MAX_DESTROY_RATIO,
    allow_destroy: bool = False,
    context: str = "",
    log: logging.Logger = _log,
) -> None:
    """
    Raise ``TerraformSafetyError`` if applying ``new_state`` would destructively
    shrink the managed container set beyond the allowed threshold.

    A *growing* or *unchanged* set is always safe. The guard only ever fires on
    removals, so it can never block legitimate provisioning.
    """
    old = _containers(old_state)
    new = _containers(new_state)
    where = f" [{context}]" if context else ""

    # Nothing existed before -> any new state is non-destructive.
    if not old:
        return

    removed = set(old) - set(new)
    if not removed:
        return

    if allow_destroy:
        log.warning(
            f"Terraform safety{where}: destroy explicitly allowed; "
            f"{len(removed)} container(s) will be removed: {sorted(removed)}"
        )
        return

    # Full wipe: new set is empty but old was not -> almost always a bug/race.
    if not new:
        raise TerraformSafetyError(
            f"Refusing to write Terraform state{where}: the new container map is "
            f"EMPTY while {len(old)} container(s) currently exist "
            f"({sorted(old)}). This would destroy all managed infrastructure. "
            f"Set PLUGIN_IAC_ORCHESTRATOR_TF_ALLOW_DESTROY=true to override."
        )

    ratio = len(removed) / len(old)
    if ratio > max_destroy_ratio:
        raise TerraformSafetyError(
            f"Refusing to write Terraform state{where}: it would remove "
            f"{len(removed)}/{len(old)} container(s) "
            f"({ratio:.0%} > {max_destroy_ratio:.0%} limit): {sorted(removed)}. "
            f"Set PLUGIN_IAC_ORCHESTRATOR_TF_ALLOW_DESTROY=true to override or "
            f"raise PLUGIN_IAC_ORCHESTRATOR_TF_MAX_DESTROY_RATIO."
        )

    # Within the allowed shrink window -> permitted but logged loudly.
    log.warning(
        f"Terraform safety{where}: removing {len(removed)}/{len(old)} container(s) "
        f"({ratio:.0%}, within {max_destroy_ratio:.0%} limit): {sorted(removed)}"
    )


def atomic_write_json(path: Path, data: Dict[str, Any], log: logging.Logger = _log) -> bool:
    """
    Write ``data`` as pretty JSON only if it differs from the current file.
    The write is atomic (temp file in the same dir + ``os.replace``), so readers
    and a crashing process never observe a half-written, destructive state.

    Returns True if the file was (re)written, False if it was already current.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                if json.load(f) == data:
                    log.debug(f"No changes detected for {path}. Skipping write.")
                    return False
        except (json.JSONDecodeError, OSError):
            pass  # corrupt/unreadable -> overwrite

    fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), prefix=".tf_", suffix=".json.tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, sort_keys=True)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_name, path)
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise

    log.info(f"Updated Terraform vars: {path}")
    return True
