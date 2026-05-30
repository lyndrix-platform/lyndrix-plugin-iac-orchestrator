import argparse
import sys
import copy
from pathlib import Path
from typing import List, Dict, Any

from deepmerge import Merger

from services.loader import load_configuration
from services.renderer import render_templates
from gen.ansible_gen import generate_ansible_state
from gen.terraform import (
    build_terraform_state,
    write_terraform_state,
    TerraformSafetyError,
)
from validate.models import validate_configuration
from core.logger import get_logger

log = get_logger("IaC:Generator")

# Eigener Merger, der Listen dedupliziert anstatt sie wild aneinander zu hängen
def unique_list_merge(config, path, base, nxt):
    for item in nxt:
        if item not in base:
            base.append(item)
    return base

unique_merger = Merger(
    [(list, [unique_list_merge]), (dict, ["merge"]), (set, ["union"])],
    ["override"], ["override"]
)

def parse_arguments() -> argparse.Namespace:
    """Parses command line arguments."""
    parser = argparse.ArgumentParser(description="IaC Core Config Generator")
    parser.add_argument("--inventory-dir", type=Path, required=True, help="Path to iac-controller")
    parser.add_argument("--output-dir", type=Path, required=True, help="Root path for generated states")
    return parser.parse_args()

def discover_sites(inventory_dir: Path) -> List[str]:
    """Scans the inventory directory for available sites."""
    sites_path = inventory_dir / "environments" / "sites"
    if not sites_path.exists():
        return []
    return [d.name for d in sites_path.iterdir() if d.is_dir()]

def discover_stages(inventory_dir: Path, site: str) -> List[str]:
    """Scans a specific site directory for available stages."""
    stages_path = inventory_dir / "environments" / "sites" / site / "stages"
    if not stages_path.exists():
        return []
    return [d.name for d in stages_path.iterdir() if d.is_dir()]

def resolve_profiles(config: Dict[str, Any]) -> None:
    """Generic engine to merge profiles into nodes."""
    profiles = config.get("profiles", {})
    if not profiles:
        return

    def apply(node: Dict[str, Any]):
        if not isinstance(node, dict):
            return
        assigned = node.pop("profiles", [])
        if not assigned:
            return
        
        merged = {}
        for p in assigned:
            if p in profiles:
                # FIX 1: Deepcopy prevents shared memory mutation across multiple hosts
                unique_merger.merge(merged, copy.deepcopy(profiles[p]))
            else:
                log.warning(f"Profile '{p}' referenced but not found.")
        
        # Merge original host data ON TOP of profile data
        unique_merger.merge(merged, node)
        node.clear()
        node.update(merged)

    # Walk through all host sources
    for host in config.get("hosts", {}).values():
        apply(host)
    for hw in config.get("hardware_hosts", {}).values():
        if isinstance(hw, dict) and hw.get("is_cluster"):
            for n in hw.get("nodes", {}).values():
                apply(n)
        else:
            apply(hw)

def main() -> None:
    args = parse_arguments()
    if not args.inventory_dir.exists():
        log.error(f"Inventory directory {args.inventory_dir} does not exist.")
        raise RuntimeError(f"Inventory directory {args.inventory_dir} does not exist.")

    sites = discover_sites(args.inventory_dir)
    global_ansible_data = {
        "hardware_hosts": {},
        "hosts": {},
        "group_vars": {} 
    }
    error_count = 0
    # Terraform writes are DEFERRED: we build each environment's state in memory
    # during the loop, then only persist them after a fully clean pass. This
    # prevents a partial/failed run from committing a shrunken container map that
    # a downstream `terraform apply` would interpret as "destroy the rest".
    pending_terraform = []

    for site in sites:
        for stage in discover_stages(args.inventory_dir, site):
            log.info(f"--- Processing: {site} | {stage} ---")
            try:
                # 1. Load, Render, and Resolve Profiles
                raw_data = load_configuration(args.inventory_dir, site, stage)
                rendered_data = render_templates(raw_data)
                resolve_profiles(rendered_data)

                # 2. Validate
                validated_data = validate_configuration(rendered_data)
                
                # 3. Generate isolated states for this specific stage
                isolated_dir = args.output_dir / site / stage
                generate_ansible_state(validated_data, isolated_dir)
                # Build (pure, no I/O) now; write later once the whole run is clean.
                tf_state = build_terraform_state(validated_data, log)
                pending_terraform.append((isolated_dir, tf_state, f"{site}/{stage}"))

                # 4. SAFE AGGREGATION for Global Inventory
                group_vars = global_ansible_data["group_vars"]
                if validated_data.get("global_vars"):
                    group_vars["all"] = validated_data["global_vars"]
                if validated_data.get("site_vars"):
                    group_vars[f"site_{site}"] = validated_data["site_vars"]
                if validated_data.get("stage_vars"):
                    group_vars[f"stage_{stage}"] = validated_data["stage_vars"]

                def aggregate_hosts(source_dict: Dict[str, Any], target_dict: Dict[str, Any], is_hardware: bool = False):
                    # Prepare the context for THIS specific site/stage
                    env_context = {}
                    unique_merger.merge(env_context, validated_data.get("global_vars") or {})
                    unique_merger.merge(env_context, validated_data.get("site_vars") or {})
                    
                    # Nur Stage-Vars mergen, wenn es kein Hardware-Host ist
                    if not is_hardware:
                        unique_merger.merge(env_context, validated_data.get("stage_vars") or {})

                    for name, host_data in source_dict.items():
                        new_host = copy.deepcopy(host_data)
                        
                        # Initialize groups
                        groups = set(new_host.get("ansible_groups") or [])
                        groups.add(f"site_{site}")

                        if is_hardware:
                            # Hardware bleibt aus Stage-Gruppen raus
                            groups.add("physical_infrastructure")
                        else:
                            # Nur VMs bekommen die Stage-Zuweisung
                            groups.add(f"stage_{stage}")

                        new_host["ansible_groups"] = list(groups)
                        
                        # Merge environment vars into the host
                        final_host_data = copy.deepcopy(env_context)
                        unique_merger.merge(final_host_data, new_host)

                        if name in target_dict:
                            # Peaceful merge
                            existing_groups = set(target_dict[name].get("ansible_groups", []))
                            if not is_hardware:
                                existing_groups.update(new_host["ansible_groups"])
                            
                            unique_merger.merge(target_dict[name], final_host_data)
                            target_dict[name]["ansible_groups"] = list(existing_groups)
                        else:
                            target_dict[name] = final_host_data

                # --- Aufruf der Aggregation ---
                if "hardware_hosts" in validated_data:
                    aggregate_hosts(validated_data["hardware_hosts"], global_ansible_data["hardware_hosts"], is_hardware=True)
                
                if "hosts" in validated_data:
                    aggregate_hosts(validated_data["hosts"], global_ansible_data["hosts"], is_hardware=False)
                
            except Exception:
                log.exception(f"Failed processing {site}/{stage} due to an internal exception:")
                error_count += 1

    # 5. DEFERRED Terraform write phase.
    # Only persist Terraform state when EVERY stage parsed/validated cleanly, so a
    # half-processed run can never emit a destructive partial state. Each write is
    # additionally protected by the destroy-safety guard.
    if error_count == 0:
        for isolated_dir, tf_state, ctx in pending_terraform:
            try:
                write_terraform_state(tf_state, isolated_dir, context=ctx, log=log)
            except TerraformSafetyError as e:
                log.error(f"Terraform safety guard blocked {ctx}: {e}")
                error_count += 1
            except Exception:
                log.exception(f"Failed writing Terraform state for {ctx}:")
                error_count += 1
    else:
        log.error(
            f"Skipping ALL Terraform writes: generation had {error_count} error(s). "
            f"Refusing to emit a possibly-destructive partial state."
        )

    # 6. Final Global Generation
    has_hosts = len(global_ansible_data["hosts"]) > 0
    has_hw = len(global_ansible_data["hardware_hosts"]) > 0

    if error_count == 0 and (has_hosts or has_hw):
        log.info(f"Generating global inventory with {len(global_ansible_data['hosts'])} hosts...")
        generate_ansible_state(global_ansible_data, args.output_dir / "global")
    else:
        log.error(f"Global inventory generation aborted: {error_count} errors occurred or no hosts found.")
        raise RuntimeError(f"Global inventory generation aborted: {error_count} errors occurred or no hosts found.")

if __name__ == "__main__":
    main()