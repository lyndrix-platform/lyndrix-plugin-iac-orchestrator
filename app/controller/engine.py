import asyncio
import json
import sys
import os
import importlib
import logging
import shutil
import time
import uuid
import yaml
from hashlib import sha256
from pathlib import Path

from deepdiff import DeepDiff
import re

from ...stages.base import BaseStage
from .utils import StageResult, JobFileLogBridge
from ...stages.git import (
    SyncRepoStage,
    CommitPushStage,
    CloneServiceRepoStage,
    SyncAllServicesStage
)
from ...stages.ansible import (
    AnsiblePlaybookStage,
    AsyncBulkRolloutStage
)

log = logging.getLogger("IaC:Engine")

# --- LOCAL STAGE DEFINITIONS ---
class NativeGenerateStage:
    def __init__(self):
        self.name = "Native Artifact Generation"
    async def run(self, engine, context: dict) -> StageResult:
        try:
            await engine._execute_native_generation()
            await engine.emit_monitoring_inventory_sync()
            return StageResult(True, "Native artifacts generated.")
        except Exception as e:
            return StageResult(False, f"Native generation failed: {e}")


class TerraformProvisionStage(BaseStage):
    def __init__(self, host_name: str):
        super().__init__(f"Terraform Provision: {host_name or 'unknown-host'}")
        self.host_name = str(host_name or "").strip()

    async def run(self, engine, context: dict) -> StageResult:
        if not self.host_name:
            return StageResult(False, "Missing host_name for host_provision pipeline.")
        payload = context.get("payload") or {}
        force_apply = bool(payload.get("approve"))
        success, data = await engine.execute_terraform_provision(
            host_name=self.host_name,
            job_id=context.get("job_id", 0),
            force_apply=force_apply,
        )
        # Tell downstream stages whether a fresh container was actually (re)created,
        # so a complete reinstall can invalidate stale per-host state before rollout.
        if isinstance(data, dict):
            context["container_created"] = bool(data.get("container_created"))
        msg = "Terraform provisioning completed." if success else "Terraform provisioning failed."
        return StageResult(success, msg, data=data)


class InfraPlanStage(BaseStage):
    """Whole-infrastructure read-only plan ("Check Env"): runs ``tofu plan``
    across every non-empty environment without applying anything."""

    def __init__(self):
        super().__init__("Infra Plan: all environments")

    async def run(self, engine, context: dict) -> StageResult:
        success, data = await engine.execute_terraform_infra(
            mode="plan",
            job_id=context.get("job_id", 0),
            force_apply=False,
        )
        envs = data.get("environments", 0) if isinstance(data, dict) else 0
        msg = (
            f"Infrastructure plan completed across {envs} environment(s)."
            if success
            else "Infrastructure plan failed."
        )
        return StageResult(success, msg, data=data)


class InfraApplyStage(BaseStage):
    """Whole-infrastructure apply ("Deploy Infra"): runs ``tofu apply`` across
    every non-empty environment. Only reachable from explicit, operator-approved
    triggers (``approve`` payload flag) — never the automatic webhook path."""

    def __init__(self):
        super().__init__("Infra Deploy: all environments")

    async def run(self, engine, context: dict) -> StageResult:
        payload = context.get("payload") or {}
        force_apply = bool(payload.get("approve"))
        success, data = await engine.execute_terraform_infra(
            mode="apply",
            job_id=context.get("job_id", 0),
            force_apply=force_apply,
        )
        envs = data.get("environments", 0) if isinstance(data, dict) else 0
        msg = (
            f"Infrastructure deploy completed across {envs} environment(s)."
            if success
            else "Infrastructure deploy failed."
        )
        return StageResult(success, msg, data=data)


class ComplianceBootstrapStage(BaseStage):
    """Runs the baseline compliance playbook against a single freshly-provisioned
    host, connecting as root with the Terraform-injected key (Vault:
    ``iac_tf_ssh_private_key``) before the ansible-agent account exists."""

    def __init__(self, host_name: str, wait_for_ssh: bool = True):
        super().__init__(f"Compliance Bootstrap: {host_name or 'unknown-host'}")
        self.host_name = str(host_name or "").strip().lower()
        self.wait_for_ssh = wait_for_ssh

    async def run(self, engine, context: dict) -> StageResult:
        if not self.host_name:
            return StageResult(False, "Missing host_name for compliance bootstrap.")
        if not engine.ctx.get_secret("iac_tf_ssh_private_key"):
            return StageResult(
                False,
                "Missing root SSH private key. Set 'Root SSH Private Key' in the "
                "plugin Settings (Vault key iac_tf_ssh_private_key).",
            )
        if self.wait_for_ssh:
            await engine._await_host_ssh(self.host_name)
        inner = AnsiblePlaybookStage(
            name_override=self.name,
            playbook_path="playbooks/cd_playbooks/cd_compliance.yml",
            inventory_path="global/ansible/inventory.yml",
            limit=self.host_name,
            ssh_key_secret="iac_tf_ssh_private_key",
            remote_user="root",
        )
        return await inner.run(engine, context)

class InvalidateHostStateStage(BaseStage):
    """After a *complete reinstall* (Terraform actually re-created the LXC
    container), drop this host's entry from the persisted ``last_known_good``
    state. The subsequent host rollout's drift check then naturally sees the
    host's services as missing and redeploys exactly them on that host.

    No-op when Terraform made no changes, so ordinary idempotent re-runs keep
    the standard 'no drift -> deploy nothing' short-circuit untouched."""

    def __init__(self, host_name: str):
        super().__init__(f"Invalidate Host State: {host_name or 'unknown-host'}")
        self.host_name = str(host_name or "").strip().lower()

    async def run(self, engine, context: dict) -> StageResult:
        if not self.host_name:
            return StageResult(True, "No host_name; nothing to invalidate.")
        if not context.get("container_created"):
            return StageResult(True, "No fresh container created; preserving last_known_good.")

        record = engine.db.get_state("last_known_good")
        state = (record.get("data") if record else {}) or {}
        removed = False
        for key in list(state.keys()):
            if str(key).strip().lower() == self.host_name:
                state.pop(key, None)
                removed = True
        if removed:
            engine.db.update_state("last_known_good", state, "latest")
            return StageResult(
                True,
                f"Cleared '{self.host_name}' from last_known_good; host rollout will redeploy its services.",
            )
        return StageResult(True, f"Host '{self.host_name}' absent from last_known_good; nothing to clear.")

class TriggerHostRolloutStage(BaseStage):
    """Hands the freshly-provisioned + bootstrapped host off to the existing,
    already-coded host deployment path used by the Assignments tab 'Deploy Host'
    button: emit ``iac:webhook_verified`` with ``pipeline_type=rollout`` limited
    to this host. This runs as its own rollout job so the service deployment
    reuses the standard ansible-agent flow without duplicating any logic here."""

    def __init__(self, host_name: str):
        super().__init__(f"Trigger Host Rollout: {host_name or 'unknown-host'}")
        self.host_name = str(host_name or "").strip().lower()

    async def run(self, engine, context: dict) -> StageResult:
        if not self.host_name:
            return StageResult(False, "Missing host_name for host rollout hand-off.")
        # Emit exactly what the Assignments 'Deploy Host' button emits: a host-scoped
        # rollout that runs the per-host drift check and deploys only this host's
        # services on this host. Reuses the proven flow rather than duplicating it.
        engine.ctx.emit("iac:webhook_verified", {
            "pipeline_type": "rollout",
            "limit": self.host_name,
            "manual": True,
            "source": "host_provision_chain",
        })
        return StageResult(True, f"Host rollout queued for '{self.host_name}' (existing Deploy Host flow).")

class DynamicRuleExecutionStage:
    def __init__(self, pipeline_type: str):
        self.name = f"Dynamic Rules: {pipeline_type}"
        self.pipeline_type = pipeline_type
    async def run(self, engine, context: dict) -> StageResult:
        return StageResult(True, f"Dynamic rules evaluated for {self.pipeline_type}")

class DetectDriftStage(BaseStage):
    def __init__(self):
        super().__init__("Detect State Drift")

    def _load_current_state_from_git(self, engine):
        """Parses all YAML files to build the current desired state, folding in
        profile-inherited services so each host's desired service set matches
        what the Assignments tab shows (direct + profile). This makes a complete
        reinstall redeploy the host's *full* service set, and lets profile-driven
        service changes register as drift."""
        assignments = {}
        base_dir = engine.config.git_repos_dir / "iac_controller" / "environments"
        sites_dir = base_dir / "sites"
        if not sites_dir.exists(): return {}

        # Load profiles once (name -> service list) for inheritance resolution.
        profiles = {}
        profiles_file = base_dir / "global" / "03_profiles.yml"
        if profiles_file.exists():
            try:
                with open(profiles_file, 'r') as f:
                    profiles = (yaml.safe_load(f) or {}).get("profiles") or {}
            except Exception:
                profiles = {}

        for yaml_file in sites_dir.rglob("*.yml"):
            try:
                with open(yaml_file, 'r') as f:
                    data = yaml.safe_load(f) or {}
                    # Just using hostnames as keys for this example
                    hosts = {**data.get("hosts", {}), **data.get("hardware_hosts", {})}
                    for host_name, host_data in hosts.items():
                        if not isinstance(host_data, dict): continue
                        if host_name not in assignments: assignments[host_name] = {}
                        assignments[host_name].update(host_data)
                        # Fold profile-inherited services into the host's service
                        # list so drift accounts for them (only profile-using hosts
                        # are affected; direct-only hosts keep their exact shape).
                        svcs = assignments[host_name].get("services")
                        if not isinstance(svcs, list): svcs = []
                        have = {s.get("name") for s in svcs if isinstance(s, dict) and s.get("name")}
                        for p in (host_data.get("profiles") or []):
                            for s in (profiles.get(p, {}).get("services") or []):
                                nm = s.get("name") if isinstance(s, dict) else (s if isinstance(s, str) else None)
                                if nm and nm not in have:
                                    svcs.append({"name": nm}); have.add(nm)
                        assignments[host_name]["services"] = svcs
            except Exception:
                continue
        return assignments

    def _get_host_services(self, state_dict: dict, host_name: str) -> set:
        """Helper to extract a simple set of service names for a given host."""
        svcs = set()
        for s in state_dict.get(host_name, {}).get("services", []):
            if isinstance(s, dict) and s.get("name"): svcs.add(s["name"])
        return svcs

    async def run(self, engine, context: dict) -> StageResult:
        log.info("Comparing current desired state against last known good state...")
        
        current_desired_state = self._load_current_state_from_git(engine)
        if not current_desired_state:
            return StageResult(False, "Could not parse current desired state from Git.")

        # Save state to context so PersistStateStage can save it to the DB later
        context["current_desired_state"] = current_desired_state

        last_known_good_record = engine.db.get_state("last_known_good")
        last_known_good_state = last_known_good_record.get("data") if last_known_good_record else {}

        diff = DeepDiff(last_known_good_state, current_desired_state, ignore_order=True)

        if not diff:
            log.info("✅ No drift detected. Infrastructure is up to date.")
            context["stop_pipeline"] = True # Flag to stop the pipeline gracefully
            return StageResult(True, "No drift detected.")
        
        services_to_deploy = set()
        services_to_remove = set()

        # Intelligently parse the drift to find exactly WHICH services changed
        for change_type, changes in diff.items():
            paths = changes.keys() if isinstance(changes, dict) else changes
            for path in paths:
                m = re.match(r"root\['([^']+)'\](.*)", str(path))
                if not m: continue
                
                host_name, remainder = m.group(1), m.group(2)
                old_svcs = self._get_host_services(last_known_good_state, host_name)
                new_svcs = self._get_host_services(current_desired_state, host_name)

                if "['services']" in remainder:
                    # Only the services list changed on this host! 
                    # Find exactly which ones were added or removed
                    services_to_deploy.update(new_svcs - old_svcs)
                    services_to_remove.update(old_svcs - new_svcs)
                else:
                    # A core host property changed (like IP), we must redeploy all of its services
                    services_to_deploy.update(new_svcs)
        
        context["services_to_deploy"] = list(services_to_deploy)
        context["services_to_remove"] = list(services_to_remove)

        log.warning(f"DRIFT DETECTED: Deploying {len(services_to_deploy)} services, Cleaning up {len(services_to_remove)} services.")
        context["is_drift_run"] = True
        return StageResult(True, "Drift detected, proceeding with rollout.")

class CleanupOrphanedServicesStage(BaseStage):
    def __init__(self):
        super().__init__("Cleanup Orphaned Services")
        
    async def run(self, engine, context: dict) -> StageResult:
        to_remove = context.get("services_to_remove", [])
        if not to_remove:
            return StageResult(True, "No services require cleanup.")
            
        log.info(f"Placeholder: Would run cleanup playbook for removed services: {', '.join(to_remove)}")
        # FUTURE: await engine.execute_ansible_docker(playbook_subpath="playbooks/cleanup.yml", extra_vars={"services_to_kill": ",".join(to_remove)}, ...)
        return StageResult(True, "Placeholder cleanup completed.")

class PersistStateStage(BaseStage):
    def __init__(self):
        super().__init__("Persist State to DB")
    async def run(self, engine, context: dict) -> StageResult:
        log.info("Persisting new 'last_known_good' state to database...")
        new_state = context.get("current_desired_state")
        if new_state:
            # Use a placeholder 'latest' for commit hash for now
            engine.db.update_state("last_known_good", new_state, "latest")
        return StageResult(True, "State persisted.")

# --- THE ENGINE ---

class DeploymentEngine:
    def __init__(self, ctx, state, db, config, socket_client=None):
        self.ctx = ctx
        self.state = state
        self.db = db
        self.config = config
        self.socket_client = socket_client
        self.base_git_dir = self.config.git_repos_dir
        self.pending_syncs = {}
        self._pipeline_dispatch_lock = asyncio.Lock()
        self._active_single_service_keys: set[str] = set()
        self._active_terraform_host_keys: set[str] = set()
        # In-flight rollout keys ('rollout:<host>' or 'rollout:all'). Mirrors the
        # single_service/terraform guards above to close the TOCTOU window between
        # the concurrency check and create_job() marking the job RUNNING in the DB.
        self._active_rollout_keys: set[str] = set()
        # Only one OpenTofu container may run at a time.  Concurrent runs race
        # on provider downloads, share the S3 state backend lock, and can
        # confuse resource-targeting.  A second host_provision queued while one
        # is running will wait here rather than fail with a network timeout.
        self._terraform_run_semaphore = asyncio.Semaphore(1)
        ctx.subscribe("git:status_update")(self._on_git_status)

    def get_default_ansible_stages(self, pipeline_type: str = "connectivity"):
        if pipeline_type == "rollout":
            return [AsyncBulkRolloutStage(inventory_path="global/ansible/inventory.yml", limit="all")]
        return [
            AnsiblePlaybookStage(
                name_override="CONNECTIVITY TEST", 
                playbook_path="playbooks/cd_playbooks/cd_test_inventory.yml", 
                inventory_path="global/ansible/inventory.yml", 
                limit="docker-hydra"
            )
        ]

    def _load_generated_inventory(self) -> dict:
        inventory_path = self.base_git_dir / "inventory_state" / "global" / "ansible" / "inventory.yml"
        if not inventory_path.exists():
            return {}
        with open(inventory_path, "r", encoding="utf-8") as handle:
            return yaml.safe_load(handle) or {}

    def _load_service_docker_names(self) -> dict:
        """Build a slug→docker_name mapping by reading service.yml files in services_dir."""
        mapping: dict[str, str] = {}

        def _normalize_slug(raw: str) -> str:
            return "".join(ch for ch in str(raw or "").lower() if ch.isalnum())

        def _slug_aliases(raw: str) -> set[str]:
            # Accept common slug variants (hyphen/underscore/case) from generated inventory.
            base = str(raw or "").strip()
            if not base:
                return set()
            aliases = {
                base,
                base.lower(),
                base.replace("-", "_"),
                base.replace("_", "-"),
                base.lower().replace("-", "_"),
                base.lower().replace("_", "-"),
            }
            return {a for a in aliases if a}

        candidate_dirs: list[Path] = []
        for path_candidate in [
            self.config.services_dir,
            Path(getattr(self.config, "host_services_dir", "")),
            Path("/app/.dev/storage/services"),
            Path("/workspace/.dev/storage/services"),
            Path.cwd() / ".dev" / "storage" / "services",
        ]:
            if path_candidate and path_candidate.exists() and path_candidate.is_dir():
                candidate_dirs.append(path_candidate)

        # Deduplicate while preserving order so the configured path wins.
        seen_dirs: set[str] = set()
        unique_dirs: list[Path] = []
        for d in candidate_dirs:
            key = str(d.resolve())
            if key in seen_dirs:
                continue
            seen_dirs.add(key)
            unique_dirs.append(d)

        for services_dir in unique_dirs:
            for svc_yml in services_dir.rglob("service.yml"):
                try:
                    with open(svc_yml, "r", encoding="utf-8") as fh:
                        data = yaml.safe_load(fh) or {}
                    docker_name = (data.get("service") or {}).get("name")
                    slug = svc_yml.parent.name
                    if docker_name and slug:
                        for alias in _slug_aliases(slug):
                            mapping[alias] = docker_name
                        mapping[_normalize_slug(slug)] = docker_name
                except Exception:
                    continue

        if mapping:
            log.info(f"MONITORING: loaded {len(mapping)} service slug aliases for container-name mapping")
        return mapping

    def _build_monitoring_inventory_payload(self, inventory: dict) -> dict:
        all_hosts = (inventory.get("all") or {}).get("hosts") or {}
        children = (inventory.get("all") or {}).get("children") or {}
        host_groups: dict[str, list[str]] = {}
        for group_name, group_data in children.items():
            for host_name in (group_data.get("hosts") or {}).keys():
                host_groups.setdefault(host_name, []).append(group_name)

        docker_name_map = self._load_service_docker_names()

        def _resolve_docker_name(slug: str) -> str:
            direct = docker_name_map.get(slug)
            if direct:
                return direct
            normalized = "".join(ch for ch in str(slug or "").lower() if ch.isalnum())
            return docker_name_map.get(normalized) or slug

        hosts = []
        services = []
        for host_name, host_data in all_hosts.items():
            groups = sorted(host_groups.get(host_name, []))
            stage = next((group[len("stage_"):] for group in groups if group.startswith("stage_")), None)
            site = next((group[len("site_"):] for group in groups if group.startswith("site_")), None)
            hosts.append(
                {
                    "host_name": host_name,
                    "hostname": host_data.get("hostname") or host_name,
                    "address": host_data.get("ansible_host"),
                    "ansible_host": host_data.get("ansible_host"),
                    "groups": groups,
                    "ansible_groups": groups,
                    "baseline_roles": host_data.get("baseline_roles") or [],
                    "profiles": host_data.get("profiles") or [],
                    "terraform": host_data.get("terraform") or {},
                    "site": site,
                    "stage": stage,
                }
            )

            for service in host_data.get("services") or []:
                if not isinstance(service, dict) or not service.get("name"):
                    continue
                slug = str(service.get("name"))
                docker_name = _resolve_docker_name(slug)
                services.append(
                    {
                        "host_name": host_name,
                        "hostname": host_data.get("hostname") or host_name,
                        "address": host_data.get("ansible_host"),
                        "service_name": docker_name,
                        "service_slug": slug,
                        "name": docker_name,
                        "state": service.get("state"),
                        "desired_state": service.get("state"),
                        "deploy_type": service.get("deploy_type"),
                        "git_repo": service.get("git_repo"),
                        "git_version": service.get("git_version"),
                        "config": service.get("config") or {},
                        "groups": groups,
                        "ansible_groups": groups,
                        "site": site,
                        "stage": stage,
                    }
                )

        source_revision = sha256(
            json.dumps({"hosts": hosts, "services": services}, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()
        return {
            "owner_source": "orchestrator_service",
            "source_revision": source_revision,
            "hosts": hosts,
            "services": services,
        }

    async def emit_monitoring_inventory_sync(self):
        try:
            inventory = self._load_generated_inventory()
            if not inventory:
                return
            payload = self._build_monitoring_inventory_payload(inventory)
            if not payload["hosts"] and not payload["services"]:
                return
            self.ctx.emit("monitoring:inventory_sync", payload)
            log.info(
                f"MONITORING: Emitted generated inventory sync with {len(payload['hosts'])} hosts and {len(payload['services'])} services."
            )
        except Exception as exc:
            log.error(f"MONITORING: Failed to emit inventory sync: {exc}")

    async def run_pipeline(self, payload: dict):
        if not payload.get("pipeline_type") and payload.get("object_kind") == "push":
            name, ref = payload.get("project", {}).get("name"), payload.get("ref", "")
            if name and ref.startswith("refs/heads/"):
                payload.update({"pipeline_type": "single_service", "service_name": name, "service_branch": ref.replace("refs/heads/", "")})

        pipeline_type = payload.get("pipeline_type", "connectivity")
        # The provision pipeline also runs Ansible (root compliance bootstrap +
        # host rollout hand-off), so it is not solely Terraform: it is named
        # 'host_provision'. Accept the legacy 'terraform_provision' name for
        # backwards compatibility (old webhooks/buttons keep working).
        if pipeline_type == "terraform_provision":
            pipeline_type = "host_provision"
        payload["pipeline_type"] = pipeline_type
        single_key = None
        terraform_key = None
        rollout_key = None

        # Prevent concurrent bulk rollouts and duplicate single_service runs for the same service.
        async with self._pipeline_dispatch_lock:
            if pipeline_type == "rollout":
                incoming_limit = str(payload.get("limit") or "").strip().lower()
                rollout_key = f"rollout:{incoming_limit}" if (incoming_limit and incoming_limit != "all") else "rollout:all"
                # Combine DB RUNNING jobs with in-flight keys reserved this dispatch
                # but not yet marked RUNNING (closes the create_job TOCTOU window).
                active_rollout_types = {
                    str(j.pipeline_type or "")
                    for j in self.db.get_jobs_by_status("RUNNING")
                    if str(j.pipeline_type or "").startswith("rollout")
                } | set(self._active_rollout_keys)
                if active_rollout_types:
                    if not incoming_limit or incoming_limit == "all":
                        # Global (no-limit) rollout: block if ANY rollout is in flight.
                        log.warning("ENGINE: A rollout is already in progress — global rollout blocked.")
                        return
                    # Host-scoped rollout: block if the SAME host is already rolling out.
                    if rollout_key in active_rollout_types:
                        log.warning(f"ENGINE: Rollout for host '{incoming_limit}' is already running — duplicate blocked.")
                        return
                    # Also block if a global (no-limit) rollout is in flight to avoid conflicts.
                    if "rollout:all" in active_rollout_types or "rollout" in active_rollout_types:
                        log.warning(f"ENGINE: Global rollout in progress — host rollout for '{incoming_limit}' blocked.")
                        return
                self._active_rollout_keys.add(rollout_key)
            if pipeline_type == "single_service":
                service_name = str(payload.get("service_name") or "").strip().lower()
                if service_name:
                    payload["service_name"] = service_name
                    single_key = f"single_service:{service_name}"
                    if single_key in self._active_single_service_keys:
                        log.warning(f"ENGINE: Duplicate single_service pipeline ignored for '{service_name}' (in-memory lock).")
                        self.ctx.emit("system:notify", {
                            "title": "Pipeline Ignored",
                            "message": f"A deployment for {service_name} is already being started.",
                            "type": "warning",
                            "toast": True,
                        })
                        return

                    active_service_jobs = [
                        j for j in self.db.get_jobs_by_status("RUNNING")
                        if j.pipeline_type == single_key
                    ]
                    if active_service_jobs:
                        log.warning(f"ENGINE: Duplicate single_service pipeline ignored for '{service_name}'.")
                        self.ctx.emit("system:notify", {
                            "title": "Pipeline Ignored",
                            "message": f"A deployment for {service_name} is already running.",
                            "type": "warning",
                            "toast": True,
                        })
                        return

                    self._active_single_service_keys.add(single_key)
            if pipeline_type in ("host_provision", "bootstrap_compliance"):
                host_name = str(
                    payload.get("host_name")
                    or payload.get("test_host")
                    or payload.get("limit")
                    or ""
                ).strip().lower()
                if not host_name:
                    log.warning(f"ENGINE: {pipeline_type} ignored (missing host_name).")
                    self.ctx.emit("system:notify", {
                        "title": "Pipeline Ignored",
                        "message": f"{pipeline_type} requires host_name.",
                        "type": "warning",
                        "toast": True,
                    })
                    return
                payload["host_name"] = host_name
                terraform_key = f"{pipeline_type}:{host_name}"
                if terraform_key in self._active_terraform_host_keys:
                    log.warning(f"ENGINE: Duplicate {pipeline_type} ignored for '{host_name}' (in-memory lock).")
                    self.ctx.emit("system:notify", {
                        "title": "Pipeline Ignored",
                        "message": f"A {pipeline_type} run for {host_name} is already being started.",
                        "type": "warning",
                        "toast": True,
                    })
                    return

                active_tf_jobs = [
                    j for j in self.db.get_jobs_by_status("RUNNING")
                    if j.pipeline_type == terraform_key
                ]
                if active_tf_jobs:
                    log.warning(f"ENGINE: Duplicate {pipeline_type} ignored for '{host_name}'.")
                    self.ctx.emit("system:notify", {
                        "title": "Pipeline Ignored",
                        "message": f"A {pipeline_type} run for {host_name} is already running.",
                        "type": "warning",
                        "toast": True,
                    })
                    return
                self._active_terraform_host_keys.add(terraform_key)
            if pipeline_type in ("infra_plan", "infra_apply"):
                active_infra_jobs = [
                    j for j in self.db.get_jobs_by_status("RUNNING")
                    if str(j.pipeline_type or "").startswith(("infra_plan", "infra_apply"))
                ]
                if active_infra_jobs:
                    log.warning("ENGINE: An infrastructure plan/apply is already in progress.")
                    self.ctx.emit("system:notify", {
                        "title": "Pipeline Ignored",
                        "message": "An infrastructure plan/apply is already running.",
                        "type": "warning",
                        "toast": True,
                    })
                    return

        # Safely increment running job counter
        self.state["running_jobs"] = self.state.get("running_jobs", 0) + 1
        self.state["is_running"] = self.state["running_jobs"] > 0

        # Better tagging for filtering
        db_type = pipeline_type
        if pipeline_type == "single_service":
            db_type = f"single_service:{payload.get('service_name')}"
        elif pipeline_type == "host_provision":
            db_type = f"host_provision:{payload.get('host_name')}"
        elif pipeline_type == "bootstrap_compliance":
            db_type = f"bootstrap_compliance:{payload.get('host_name')}"
        elif pipeline_type == "rollout":
            # Host-scoped rollouts (e.g. the host_provision hand-off or the
            # Assignments 'Deploy Host' button) are tagged rollout:<host> so the
            # job list reads like the provision job that spawned it.
            _limit = str(payload.get("limit") or "").strip().lower()
            if _limit and _limit != "all":
                db_type = f"rollout:{_limit}"
            
        current_job_id = self.db.create_job(db_type)
        
        # FILE LOGGING SETUP
        bridge = JobFileLogBridge(self.config.get_log_path(current_job_id))
        logging.getLogger("IaC:Engine").addHandler(bridge)
        
        log.info("[SYSTEM] Pipeline Started")
        log.info(f"[SYSTEM] Job #{current_job_id} registered in database.")
        
        # Event-Driven Notification: Register a silent active task in the bell menu
        self.ctx.emit("system:notify", {"id": f"job_{current_job_id}", "title": f"Pipeline #{current_job_id}", "message": f"Running: {pipeline_type}", "type": "ongoing", "toast": False})

        context = {"payload": payload, "job_id": current_job_id}
        
        pipeline = [
            SyncRepoStage("iac_controller"),
            SyncRepoStage("inventory_state"),
            SyncRepoStage("config_engine"),
            SyncRepoStage("aac_factory"),
        ]
        if pipeline_type == "host_provision":
            pipeline.append(SyncRepoStage("infra_engine"))
        pipeline.extend([
            # Refresh inventory_state right before generation to minimize staleness windows.
            SyncRepoStage("inventory_state"),
            NativeGenerateStage(),
        ])

        # For single_service runs we do not persist generated inventory back to inventory_state.
        # This avoids unnecessary commit/push contention and keeps service deploys focused.
        # infra_plan is read-only (Check Env) and likewise must not commit state.
        if pipeline_type not in ("single_service", "infra_plan"):
            pipeline.append(CommitPushStage("inventory_state", "ci: automated state update"))
        
        if pipeline_type == "single_service":
            svc_name, svc_branch = payload.get("service_name"), payload.get("service_branch", "main")
            target_group = "stage_dev" if svc_branch == "dev" or str(svc_branch).endswith("-dev") else ("stage_test" if svc_branch == "test" else f"service_{str(svc_name).replace('-', '_')}")
            pipeline.extend([
                CloneServiceRepoStage(svc_name, svc_branch, payload), 
                AnsiblePlaybookStage(
                    name_override=f"Single Service: {svc_name} ({svc_branch})", 
                    playbook_path="playbooks/cd_playbooks/cd_rollout_single_service.yml", 
                    inventory_path="global/ansible/inventory.yml", 
                    limit=target_group, 
                    extra_vars={"SERVICE_BRANCH": svc_branch, "target_service": svc_name, "target_group": target_group, "LOCAL_SERVICES_DIR": str(self.config.services_dir)}
                )
            ])
        elif pipeline_type == "host_provision":
            pipeline.append(TerraformProvisionStage(host_name=payload.get("host_name")))
            # Ansible init: first compliance/baseline run as root (creates ansible-agent).
            if not payload.get("skip_bootstrap"):
                pipeline.append(ComplianceBootstrapStage(host_name=payload.get("host_name")))
            # Hand off to the existing host deployment (Assignments 'Deploy Host' = rollout limit=host).
            if not payload.get("skip_rollout"):
                # On a complete reinstall (fresh container), clear stale per-host state so
                # the rollout's drift check redeploys this host's services.
                pipeline.append(InvalidateHostStateStage(host_name=payload.get("host_name")))
                pipeline.append(TriggerHostRolloutStage(host_name=payload.get("host_name")))
            pipeline.append(DynamicRuleExecutionStage(pipeline_type))
        elif pipeline_type == "bootstrap_compliance":
            pipeline.extend([
                ComplianceBootstrapStage(host_name=payload.get("host_name")),
                DynamicRuleExecutionStage(pipeline_type),
            ])
        elif pipeline_type == "infra_plan":
            pipeline.extend([
                InfraPlanStage(),
                DynamicRuleExecutionStage(pipeline_type),
            ])
        elif pipeline_type == "infra_apply":
            pipeline.extend([
                InfraApplyStage(),
                DynamicRuleExecutionStage(pipeline_type),
            ])
        elif pipeline_type == "rollout":
            target_services = payload.get("target_services")
            if target_services is not None:
                log.info(
                    "ENGINE: rollout constrained to %s services on limit '%s'.",
                    len(target_services),
                    payload.get("limit", "all"),
                )
            pipeline.extend([
                DetectDriftStage(),
                SyncAllServicesStage(),
                AsyncBulkRolloutStage(
                    inventory_path="global/ansible/inventory.yml",
                    limit=payload.get("limit", "all"),
                    target_services=target_services,
                ),
                CleanupOrphanedServicesStage(),
                DynamicRuleExecutionStage(pipeline_type),
                PersistStateStage()
            ])
        else:
            pipeline.append(DynamicRuleExecutionStage(pipeline_type))

        try:
            total_stages = len(pipeline)
            for idx, stage in enumerate(pipeline):
                # PRE-ANSIBLE MACRO PROGRESS UPDATE (0% to 50%)
                pct = int((idx / total_stages) * 50)
                self.db.update_progress(current_job_id, progress=pct, current_step=f"Stage: {stage.name}")
                
                log.info(f"--- STAGE: {stage.name} ---")
                res = await stage.run(self, context)
                if not res.success:
                    raise RuntimeError(f"Stage '{stage.name}' failed: {res.message}")
                
                if context.get("stop_pipeline"):
                    log.info("[SYSTEM] Pipeline stopped gracefully by a stage.")
                    break
            
            log.info("[SYSTEM] Pipeline completed successfully.")
            self.state["last_deployment"] = "SUCCESS"
            self.db.update_progress(current_job_id, progress=100, current_step="Completed Successfully")
            
            # Clear the ongoing notification from the bell menu and send a standalone success toast
            self.ctx.emit("system:notify", {"id": f"job_{current_job_id}", "action": "clear"})
            self.ctx.emit("system:notify", {"title": f"Pipeline #{current_job_id}", "message": "Completed successfully.", "type": "positive", "toast": True})
        except Exception as e:
            log.error(f"!!! [FATAL] {str(e)}")
            self.state["last_deployment"] = "FAILED"
            self.db.update_progress(current_job_id, progress=None, current_step="Failed")
            
            # Update the existing ongoing notification in-place to an Error state
            self.ctx.emit("system:notify", {"id": f"job_{current_job_id}", "title": f"Pipeline #{current_job_id} Failed", "message": str(e), "type": "negative", "toast": True})
        finally:
            logging.getLogger("IaC:Engine").removeHandler(bridge)
            self.state["running_jobs"] = max(0, self.state.get("running_jobs", 0) - 1)
            self.state["is_running"] = self.state["running_jobs"] > 0
            self.db.update_job(job_id=current_job_id, status=self.state["last_deployment"])
            if single_key:
                async with self._pipeline_dispatch_lock:
                    self._active_single_service_keys.discard(single_key)
            if terraform_key:
                async with self._pipeline_dispatch_lock:
                    self._active_terraform_host_keys.discard(terraform_key)
            if rollout_key:
                async with self._pipeline_dispatch_lock:
                    self._active_rollout_keys.discard(rollout_key)

    async def resume_bulk_rollout(self, job_id: int, pending_services: list[str]):
        if not pending_services: return
        
        self.state["running_jobs"] = self.state.get("running_jobs", 0) + 1
        self.state["is_running"] = self.state["running_jobs"] > 0
        
        self.db.update_progress(job_id, progress=50, current_step="Resuming Bulk Rollout...")
        bridge = JobFileLogBridge(self.config.get_log_path(job_id))
        logging.getLogger("IaC:Engine").addHandler(bridge)
        log.info(f"[SYSTEM] Resuming {len(pending_services)} pending services from job #{job_id}")
        
        self.ctx.emit("system:notify", {"id": f"job_{job_id}", "title": f"Pipeline #{job_id}", "message": "Resuming Bulk Rollout...", "type": "ongoing", "toast": False})
        
        context = {"payload": {}, "job_id": job_id}
        stage = AsyncBulkRolloutStage(inventory_path="global/ansible/inventory.yml", limit="all", target_services=pending_services)

        try:
            res = await stage.run(self, context)
            log.info("[SYSTEM] Resumed Pipeline completed.")
            self.state["last_deployment"] = "SUCCESS" if res.success else "FAILED"
            if res.success: 
                self.db.update_progress(job_id, progress=100, current_step="Resume Completed")
                self.ctx.emit("system:notify", {"id": f"job_{job_id}", "action": "clear"})
                self.ctx.emit("system:notify", {"title": f"Pipeline #{job_id}", "message": "Resume completed successfully.", "type": "positive", "toast": True})
            else:
                self.ctx.emit("system:notify", {"id": f"job_{job_id}", "title": f"Pipeline #{job_id} Resume Failed", "message": "Stage failed.", "type": "negative", "toast": True})
        except Exception as e:
            log.error(f"!!! [FATAL] {str(e)}")
            self.state["last_deployment"] = "FAILED"
            self.db.update_progress(job_id, progress=None, current_step="Resume Failed")
            self.ctx.emit("system:notify", {"id": f"job_{job_id}", "title": f"Pipeline #{job_id} Resume Failed", "message": str(e), "type": "negative", "toast": True})
        finally:
            logging.getLogger("IaC:Engine").removeHandler(bridge)
            self.state["running_jobs"] = max(0, self.state.get("running_jobs", 0) - 1)
            self.state["is_running"] = self.state["running_jobs"] > 0
            self.db.update_job(job_id=job_id, status=self.state["last_deployment"])

    async def sync_core_repos(self):
        """Periodic/Startup task to keep core repositories up to date."""
        log.info("[SYSTEM] Initiating background sync for core repositories...")
        self.ctx.emit("system:notify", {"id": "sys_repo_sync", "title": "Repository Sync", "message": "Synchronizing core repositories...", "type": "ongoing", "toast": False})
        
        all_success = True
        for repo in ["iac_controller", "inventory_state", "config_engine", "aac_factory"]:
            success = await self.execute_git_sync(repo)
            if not success:
                log.warning(f"Failed to sync {repo} during background operation.")
                all_success = False
                
        self.ctx.emit("system:notify", {"id": "sys_repo_sync", "action": "clear"})
        if all_success:
            self.ctx.emit("system:notify", {"title": "Repository Sync", "message": "Core repositories synchronized successfully.", "type": "positive", "toast": True})
        else:
            self.ctx.emit("system:notify", {"title": "Repository Sync", "message": "Some repositories failed to sync. Check logs.", "type": "negative", "toast": True})

    async def _on_git_status(self, payload: dict):
        request_id = payload.get("request_id")
        if request_id:
            pending = self.pending_syncs.get(request_id)
            if pending:
                _, fut = pending
                if not fut.done():
                    fut.set_result(payload)
            self.pending_syncs.pop(request_id, None)
            return

        # Ignore legacy status events without request_id to prevent ambiguous correlation
        # when multiple git handlers emit updates for the same repo.
        status = payload.get("status")
        repo_id = payload.get("repo_id")
        if repo_id and status:
            log.debug(
                f"Ignoring uncorrelated git status for repo '{repo_id}' without request_id: {status}"
            )

    async def execute_git_sync(self, role_slug: str) -> bool:
        raw_config = self.ctx.get_secret(f"repo_{role_slug}_config")
        if not raw_config: return False
        config = json.loads(raw_config)
        if not config.get("url"): return True

        request_id = f"sync:{role_slug}:{uuid.uuid4().hex[:12]}"
        future = asyncio.get_event_loop().create_future()
        self.pending_syncs[request_id] = (role_slug, future)

        self.ctx.emit("git:sync", {
            "request_id": request_id,
            "repo_id": role_slug,
            "url": config.get("url"),
            "auth_type": "ssh" if "git@" in config.get("url") else "token",
            "secret_value": self.ctx.get_secret(config.get("token_key", "")) if config.get("token_key") else ""
        })
        try: return (await asyncio.wait_for(future, timeout=240.0)).get("status") == "synced"
        except asyncio.TimeoutError: return False
        finally:
            self.pending_syncs.pop(request_id, None)

    async def execute_git_commit_push(self, role_slug: str, message: str) -> str:
        request_id = f"commit:{role_slug}:{uuid.uuid4().hex[:12]}"
        future = asyncio.get_event_loop().create_future()
        self.pending_syncs[request_id] = (role_slug, future)

        self.ctx.emit("git:commit_push", {
            "request_id": request_id,
            "repo_id": role_slug,
            "message": message,
            "is_local": False
        })
        try: return (await asyncio.wait_for(future, timeout=240.0)).get("status", "failed")
        except asyncio.TimeoutError: return "timeout"
        finally:
            self.pending_syncs.pop(request_id, None)

    async def _execute_native_generation(self):
        # Traverse up 3 levels: engine.py → controller/ → app/ → plugin_root/
        plugin_root = Path(__file__).parent.parent.parent.resolve()
        generator_script = plugin_root / "iac_core" / "app" / "generator.py"
        generator_root = plugin_root / "iac_core" / "app"
        vendor_dir = plugin_root / "vendor"
        # The main Lyndrix application root (e.g., /app in Docker)
        app_root = plugin_root.parents[1]
        
        if not generator_script.exists():
            raise FileNotFoundError(f"Generator script not found at {generator_script}")

        # Inject the private vendor directory into the subprocess PYTHONPATH
        env = os.environ.copy()
        
        # Build a new PYTHONPATH, prioritizing the plugin's vendored libraries
        # and its own source root for relative imports.
        new_path_parts = []
        if vendor_dir.exists():
            new_path_parts.append(str(vendor_dir))
        if generator_root.exists():
            new_path_parts.append(str(generator_root))
        if app_root.exists():
            new_path_parts.append(str(app_root))
        if env.get("PYTHONPATH"):
            new_path_parts.append(env["PYTHONPATH"])
        env["PYTHONPATH"] = ":".join(new_path_parts)

        cmd = [
            sys.executable, str(generator_script),
            "--inventory-dir", str(self.base_git_dir / "iac_controller"),
            "--output-dir", str(self.base_git_dir / "inventory_state")
        ]
        
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT, env=env)
        stdout, _ = await proc.communicate()
        
        if proc.returncode != 0:
            raise RuntimeError(f"Native artifact generation failed:\n{stdout.decode('utf-8', errors='replace')}")

    async def reconcile_orphaned_runners(self, job_id=None):
        try:
            if self.socket_client is None:
                raise RuntimeError("Socket manager client is not configured.")
            payload = await self.socket_client.request("docker:runners", args={"prefix": "aac-runner-"})
            containers = []
            if isinstance(payload, dict):
                containers = payload.get("containers") or []
            if not containers:
                return

            log.info(f"Reconciliation: Found {len(containers)} orphaned runners. Reattaching...")
            self.state["is_running"] = True
            
            for container in containers:
                c_name = container.get("name") or ""
                labels = container.get("labels") or {}
                try:
                    recovered_job_id = int(labels.get("iac_job_id")) if labels.get("iac_job_id") else (job_id or 0)
                except ValueError:
                    recovered_job_id = job_id or 0
                    
                task_name = labels.get("iac_task_name") or c_name.replace("aac-runner-", "")
                
                if "active_tasks" not in self.state: self.state["active_tasks"] = {}
                if task_name not in self.state["active_tasks"]:
                    self.state["active_tasks"][task_name] = {
                        "status": "running_ansible", 
                        "logs": [],
                        "job_id": recovered_job_id
                    }
                
                self.state["running_jobs"] = self.state.get("running_jobs", 0) + 1
                self.ctx.create_task(
                    self._reconcile_and_finalize(c_name, task_name, recovered_job_id),
                    name=f"iac:reconcile:{recovered_job_id}:{task_name}"
                )
        except Exception as e: log.error(f"Failed to reconcile: {e}")

    async def _reconcile_and_finalize(self, c_name: str, task_name: str, job_id: int):
        """Wrapper to safely close out a recovered job in the database after the runner finishes."""
        success, _ = await self._watch_detached_runner(c_name, task_name, job_id)
        
        if job_id != 0:
            job = next((j for j in self.db.get_jobs_by_status("RUNNING") if j.id == job_id), None)
            if job:
                final_status = "SUCCESS" if success else "FAILED"
                self.state["last_deployment"] = final_status
                self.db.update_progress(job_id, progress=100 if success else None, current_step="Reconciled & Completed" if success else "Reconciled & Failed")
                self.db.update_job(job_id, final_status)
                self.ctx.emit("system:notify", {"id": f"job_{job_id}", "action": "clear"})
                self.ctx.emit("system:notify", {"title": f"Pipeline #{job_id}", "message": f"Recovered job finished.", "type": "positive" if success else "negative", "toast": True})
                
        self.state["running_jobs"] = max(0, self.state.get("running_jobs", 0) - 1)
        self.state["is_running"] = self.state["running_jobs"] > 0

    async def _watch_detached_runner(self, container_name: str, task_name: str, job_id: int):
        successful_hosts, failed_hosts = 0, 0
        containers_created = 0  # Terraform: count freshly (re)created LXC containers
        log_file = self.config.get_log_path(job_id)
        ansible_progress = 50.0  # Base progress for Ansible phase
        tf_already_running = False  # Proxmox "CT already running" is a non-fatal drift condition
        
        try:
            log_proc = await asyncio.create_subprocess_exec("docker", "logs", "-f", container_name, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
            while True:
                line = await log_proc.stdout.readline()
                if not line: break
                decoded = line.decode('utf-8', errors='replace').rstrip()
                if decoded:
                    # 1. WRITE TO DISK DIRECTLY (Solves the memory freeze)
                    with open(log_file, "a", encoding="utf-8") as f:
                        f.write(f"[{task_name}] {decoded}\n")

                    # 2. MICRO-PROGRESS SNIFFER
                    if "TASK [" in decoded:
                        ansible_task = decoded.split("TASK [")[1].split("]")[0]
                        ansible_progress = min(99.0, ansible_progress + 1.5)  # Increment slightly per task, capping at 99%
                        self.db.update_progress(job_id, progress=int(ansible_progress), current_step=f"Ansible: {ansible_task}")

                    # 3. LIGHTWEIGHT UI MEMORY (Keep only the last 50 lines for the active popup)
                    if "active_tasks" in self.state and task_name in self.state["active_tasks"]:
                        self.state["active_tasks"][task_name]["logs"].append(decoded)
                        if len(self.state["active_tasks"][task_name]["logs"]) > 50:
                            self.state["active_tasks"][task_name]["logs"].pop(0)

                    if "ok=" in decoded and "failed=" in decoded and ":" in decoded:
                        try:
                            sp = decoded.split(":")[1]
                            fc = int(sp.split("failed=")[1].split()[0])
                            uc = int(sp.split("unreachable=")[1].split()[0])
                            if fc > 0 or uc > 0: failed_hosts += 1
                            else: successful_hosts += 1
                        except Exception: pass

                    # Terraform: a fresh LXC container was actually (re)created.
                    if "proxmox_virtual_environment_container.ct[" in decoded and "Creation complete" in decoded:
                        containers_created += 1

                    # Terraform state drift: container exists and is already running on Proxmox.
                    # This is the desired end-state — treat it as a non-fatal warning, not a failure.
                    # Match the actual Proxmox error shape ("CT <id> ... already running") rather than
                    # loose substrings, to avoid masking unrelated failures as success.
                    if re.search(r"CT\s+\d+\b.*already running", decoded, re.IGNORECASE):
                        tf_already_running = True
                        log.warning("[TF] Proxmox state drift detected (%s): container is already running. Treating as success.", task_name)
                        with open(log_file, "a", encoding="utf-8") as f:
                            f.write(f"[{task_name}] [WARNING] State drift: container already running — desired state achieved, continuing.\n")

            wait_proc = await asyncio.create_subprocess_exec("docker", "wait", container_name, stdout=asyncio.subprocess.PIPE)
            stdout, _ = await wait_proc.communicate()
            exit_code = int(stdout.decode().strip())
            # Override failure if the only Terraform error was "already running" (desired state IS achieved)
            success = exit_code == 0 or tf_already_running
            
            if "active_tasks" in self.state and task_name in self.state["active_tasks"]:
                self.state["active_tasks"][task_name]["status"] = "success" if success else "failed"
            await asyncio.create_subprocess_exec("docker", "rm", "-f", container_name)
            return success, {"successful_hosts": successful_hosts, "failed_hosts": failed_hosts, "containers_created": containers_created}
            
        except Exception as e:
            if "active_tasks" in self.state and task_name in self.state["active_tasks"]:
                self.state["active_tasks"][task_name]["status"] = "error"
            return False, {"successful_hosts": 0, "failed_hosts": 0}

    def _resolve_host_ip(self, host_name: str) -> str | None:
        """Best-effort lookup of a host's ansible_host (IP) from generated inventory."""
        inv_path = self.base_git_dir / "inventory_state" / "global" / "ansible" / "inventory.yml"
        try:
            with open(inv_path, "r", encoding="utf-8") as fh:
                data = yaml.safe_load(fh) or {}
        except Exception:
            return None
        hosts = ((data.get("all") or {}).get("hosts") or {})
        host_vars = hosts.get(host_name)
        if isinstance(host_vars, dict):
            ip = host_vars.get("ansible_host")
            return str(ip).strip() if ip else None
        return None

    async def _await_host_ssh(self, host_name: str, timeout: float = 120.0):
        """Poll TCP/22 on the host until reachable so a freshly-booted guest is
        ready before the bootstrap playbook connects. Non-fatal on timeout."""
        ip = self._resolve_host_ip(host_name)
        if not ip:
            log.info(f"No ansible_host for '{host_name}'; skipping SSH readiness wait.")
            return
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                reader, writer = await asyncio.wait_for(asyncio.open_connection(ip, 22), timeout=5)
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass
                log.info(f"Host '{host_name}' ({ip}:22) is reachable; proceeding with bootstrap.")
                return
            except Exception:
                await asyncio.sleep(5)
        log.warning(f"Timed out waiting for SSH on '{host_name}' ({ip}:22); proceeding anyway.")

    def _find_terraform_context_for_host(self, host_name: str) -> dict:
        inventory_root = self.base_git_dir / "inventory_state"
        matches: list[dict] = []
        for tfvars_path in inventory_root.rglob("terraform/terraform.tfvars.json"):
            try:
                with open(tfvars_path, "r", encoding="utf-8") as fh:
                    data = json.load(fh) or {}
            except Exception:
                continue
            containers = data.get("containers") or {}
            host_cfg = containers.get(host_name)
            if not isinstance(host_cfg, dict):
                continue

            rel = tfvars_path.relative_to(inventory_root)
            parts = rel.parts
            if len(parts) < 4:
                continue
            site, stage = parts[0], parts[1]
            env_name = f"{site}_{stage}"
            node_name = str(host_cfg.get("node_name") or "").strip()
            if not node_name:
                continue
            matches.append(
                {
                    "tfvars_path": tfvars_path,
                    "site": site,
                    "stage": stage,
                    "env_name": env_name,
                    "node_name": node_name,
                }
            )

        if not matches:
            raise RuntimeError(f"No terraform.tfvars.json contains host '{host_name}'.")
        if len(matches) > 1:
            locations = ", ".join(f"{m['site']}/{m['stage']}" for m in matches)
            raise RuntimeError(f"Host '{host_name}' is ambiguous across environments: {locations}.")
        return matches[0]

    def _read_yaml_dict(self, path: Path, root_key: str) -> dict:
        if not path.exists():
            return {}
        try:
            with open(path, "r", encoding="utf-8") as fh:
                data = yaml.safe_load(fh) or {}
            value = data.get(root_key)
            return value if isinstance(value, dict) else {}
        except (OSError, yaml.YAMLError) as exc:
            logger.warning("Failed reading YAML config %s: %s", path, exc)
            return {}

    @staticmethod
    def _deep_get(data: dict, path: str):
        cur = data
        for part in path.split("."):
            if not isinstance(cur, dict):
                return None
            cur = cur.get(part)
        return cur

    @staticmethod
    def _first_non_empty(*values):
        for val in values:
            if val is None:
                continue
            text = str(val).strip()
            if text:
                return text
        return None

    @staticmethod
    def _normalize_ssh_public_key(value):
        """Collapse multi-line / PuTTY-exported public keys into a single
        valid authorized_keys line (`<type> <base64> [comment]`)."""
        if value is None:
            return None
        collapsed = " ".join(str(value).split())
        return collapsed or None

    def _build_repo_terraform_inputs(self, tf_context: dict) -> dict:
        site = str(tf_context.get("site") or "").strip()
        stage = str(tf_context.get("stage") or "").strip()
        node_name = str(tf_context.get("node_name") or "").strip()
        iac_root = self.base_git_dir / "iac_controller" / "environments"

        global_vars = self._read_yaml_dict(iac_root / "global" / "01_global_vars.yml", "global_vars")
        site_vars = self._read_yaml_dict(iac_root / "sites" / site / "site_vars.yml", "site_vars")
        stage_vars = self._read_yaml_dict(iac_root / "sites" / site / "stages" / stage / "stage_vars.yml", "stage_vars")
        hardware_hosts = self._read_yaml_dict(iac_root / "sites" / site / "hardware.yml", "hardware_hosts")

        tf_repo_vars: dict = {}
        for src in (global_vars, site_vars, stage_vars):
            for key in ("terraform_vars", "terraform"):
                val = src.get(key)
                if isinstance(val, dict):
                    tf_repo_vars.update(val)

        vault_vars = global_vars.get("vault_vars") if isinstance(global_vars.get("vault_vars"), dict) else {}
        node_tf = {}
        node_cfg = hardware_hosts.get(node_name)
        if isinstance(node_cfg, dict):
            tf_block = node_cfg.get("terraform")
            if isinstance(tf_block, dict):
                node_tf = tf_block

        node_username = self._first_non_empty(node_tf.get("username"))
        node_realm = self._first_non_empty(node_tf.get("realm"))
        node_full_user = (
            f"{node_username}@{node_realm}"
            if node_username and node_realm and "@" not in node_username
            else node_username
        )

        repo_backend = tf_repo_vars.get("backend") if isinstance(tf_repo_vars.get("backend"), dict) else {}
        repo_download = tf_repo_vars.get("download") if isinstance(tf_repo_vars.get("download"), dict) else {}
        repo_proxmox = tf_repo_vars.get("proxmox") if isinstance(tf_repo_vars.get("proxmox"), dict) else {}

        return {
            "proxmox_username": self._first_non_empty(
                tf_repo_vars.get("proxmox_username"),
                repo_proxmox.get("username"),
                node_full_user,
            ),
            "proxmox_password": self._first_non_empty(
                tf_repo_vars.get("proxmox_password"),
                repo_proxmox.get("password"),
                node_tf.get("password"),
            ),
            "ssh_key": self._normalize_ssh_public_key(self._first_non_empty(
                tf_repo_vars.get("ssh_key"),
                tf_repo_vars.get("public_ssh_key"),
                tf_repo_vars.get("root_pub_key"),
                tf_repo_vars.get("ansible_agent_pub_key"),
                vault_vars.get("root_pub_key"),
                vault_vars.get("ansible_agent_pub_key"),
            )),
            "root_password": self._first_non_empty(
                tf_repo_vars.get("root_password"),
                vault_vars.get("root_password"),
            ),
            "backend": {
                "endpoint": self._first_non_empty(
                    repo_backend.get("endpoint"),
                    tf_repo_vars.get("backend_endpoint"),
                ),
                "bucket": self._first_non_empty(
                    repo_backend.get("bucket"),
                    tf_repo_vars.get("backend_bucket"),
                ),
                "region": self._first_non_empty(
                    repo_backend.get("region"),
                    tf_repo_vars.get("backend_region"),
                ),
                "access_key": self._first_non_empty(
                    repo_backend.get("access_key"),
                    tf_repo_vars.get("backend_access_key"),
                ),
                "secret_key": self._first_non_empty(
                    repo_backend.get("secret_key"),
                    tf_repo_vars.get("backend_secret_key"),
                ),
            },
            "download": {
                "datastore_id": self._first_non_empty(
                    repo_download.get("datastore_id"),
                    tf_repo_vars.get("download_datastore_id"),
                ),
                "url": self._first_non_empty(
                    repo_download.get("url"),
                    tf_repo_vars.get("download_url"),
                ),
                "checksum": self._first_non_empty(
                    repo_download.get("checksum"),
                    tf_repo_vars.get("download_checksum"),
                ),
            },
        }

    def _build_terraform_secrets_payload(self, tf_context: dict) -> tuple[dict, list[str]]:
        missing: list[str] = []
        repo = self._build_repo_terraform_inputs(tf_context)

        def _required(repo_value, secret_key: str, missing_label: str) -> str:
            val = self._first_non_empty(repo_value, self.ctx.get_secret(secret_key))
            if val is None:
                missing.append(missing_label)
                return ""
            return val

        payload = {
            "proxmox_username": _required(
                repo.get("proxmox_username"),
                "iac_tf_proxmox_username",
                "terraform_vars.proxmox_username (or hardware terraform username/realm)",
            ),
            "proxmox_password": _required(
                repo.get("proxmox_password"),
                "iac_tf_proxmox_password",
                "terraform_vars.proxmox_password (or hardware terraform password)",
            ),
            "ssh_key": _required(
                repo.get("ssh_key"),
                "iac_tf_ssh_key",
                "terraform_vars.ssh_key (or global_vars.vault_vars.root_pub_key)",
            ),
            "root_password": _required(
                repo.get("root_password"),
                "iac_tf_root_password",
                "terraform_vars.root_password (or global_vars.vault_vars.root_password)",
            ),
            "backend": {
                "endpoint": self._first_non_empty(
                    self._deep_get(repo, "backend.endpoint"),
                    self.ctx.get_secret("iac_tf_backend_endpoint"),
                    "http://10.100.1.5:9000",
                ),
                "bucket": self._first_non_empty(
                    self._deep_get(repo, "backend.bucket"),
                    self.ctx.get_secret("iac_tf_backend_bucket"),
                    "tfstate",
                ),
                "region": self._first_non_empty(
                    self._deep_get(repo, "backend.region"),
                    self.ctx.get_secret("iac_tf_backend_region"),
                    "us-east-1",
                ),
                "access_key": self._first_non_empty(
                    self._deep_get(repo, "backend.access_key"),
                    self.ctx.get_secret("iac_tf_backend_access_key"),
                    "terraform",
                ),
                "secret_key": _required(
                    self._deep_get(repo, "backend.secret_key"),
                    "iac_tf_backend_secret_key",
                    "terraform_vars.backend.secret_key",
                ),
            },
            "download": {
                "datastore_id": self._first_non_empty(
                    self._deep_get(repo, "download.datastore_id"),
                    self.ctx.get_secret("iac_tf_download_datastore_id"),
                    "local",
                ),
                "url": _required(
                    self._deep_get(repo, "download.url"),
                    "iac_tf_download_url",
                    "terraform_vars.download.url",
                ),
                "checksum": _required(
                    self._deep_get(repo, "download.checksum"),
                    "iac_tf_download_checksum",
                    "terraform_vars.download.checksum",
                ),
            },
        }
        return payload, missing

    async def execute_terraform_docker(
        self,
        *,
        run_rel_dir: str,
        task_name: str,
        job_id: int,
        targets: list[str] | None = None,
        mode: str = "apply",
        force_apply: bool = False,
        host_name: str = "",
    ) -> tuple[bool, dict]:
        """Run OpenTofu/Terraform in an ephemeral Docker container against a
        rendered run dir.

        - ``targets`` limits the run to specific resource addresses (per-host
          provisioning). ``None`` runs the whole rendered environment.
        - ``mode="plan"`` only computes and prints a plan (read-only "check env",
          never applies). ``mode="apply"`` plans and then applies when allowed.
        - Apply happens only when ``force_apply`` (explicit operator approval) or
          the ``auto_apply`` config is set; otherwise the plan is shown but not
          applied — preserving the safe default for the webhook/auto path.
        """
        if "active_tasks" not in self.state:
            self.state["active_tasks"] = {}
        self.state["active_tasks"][task_name] = {
            "status": "pulling_image",
            "logs": [],
            "job_id": job_id,
        }

        safe_task_name = "".join(c if c.isalnum() or c in ".-_" else "-" for c in task_name).strip("-")
        c_name = f"iac-tf-{safe_task_name}"[:62]
        await asyncio.create_subprocess_exec(
            "docker", "rm", "-f", c_name,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        tf_bin = str(self.config.terraform_binary or "tofu").strip()
        target_flags = "".join(f" -target='{t}'" for t in (targets or []))
        run_dir = f"/data/storage/git_repos/{run_rel_dir}"
        do_apply = mode == "apply" and (force_apply or self.config.auto_apply)
        if mode == "plan":
            action_cmd = f"{tf_bin} plan -input=false -no-color{target_flags}"
        else:
            plan_cmd = (
                f"{tf_bin} plan -input=false -no-color -out=tfplan{target_flags}"
            )
            apply_cmd = (
                f"{tf_bin} apply -input=false -no-color -auto-approve tfplan"
                if do_apply
                else f"{tf_bin} show -no-color tfplan"
            )
            action_cmd = f"{plan_cmd}; {apply_cmd}"
        shell_cmd = (
            "set -euo pipefail; "
            f"cd '{run_dir}'; "
            f"{tf_bin} init -input=false -no-color; "
            f"{action_cmd}"
        )

        h_git = self.config.host_git_repos_dir
        mounts = [
            {
                "source": h_git,
                "target": "/data/storage/git_repos",
                "mode": "rw",
            }
        ]
        if getattr(self.config, "host_terraform_providers_dir", None):
            mounts.append(
                {
                    "source": self.config.host_terraform_providers_dir,
                    "target": "/data/storage/terraform-providers",
                    "mode": "rw",
                }
            )
        env_vars = [
            {"key": "IAC_JOB_ID", "value": str(job_id)},
            {"key": "TF_IN_AUTOMATION", "value": "1"},
            {"key": "CHECKPOINT_DISABLE", "value": "1"},
            {"key": "TF_PLUGIN_CACHE_DIR", "value": "/data/storage/terraform-providers"},
        ]
        spawn_request = {
            "image": self.config.terraform_docker_image,
            "name": c_name,
            "env_vars": env_vars,
            "mounts": mounts,
            "command": ["/bin/sh", "-lc", shell_cmd],
            "remove": True,
            "networks": [],
        }

        log.info(
            "Executing Terraform in Docker: scope=%s mode=%s apply=%s host=%s",
            "host" if targets else "environment",
            mode,
            do_apply,
            host_name or "(env)",
        )
        self.state["active_tasks"][task_name]["status"] = "running_terraform"

        async with self._terraform_run_semaphore:
            log.info("[TF] Acquired terraform run slot for '%s'.", host_name or "(env)")
            if self.socket_client is None:
                raise RuntimeError("Socket manager client is not configured.")
            spawn_result = await self.socket_client.spawn_runner(**spawn_request)
            if not isinstance(spawn_result, dict) or spawn_result.get("status") != "running":
                error_msg = "unknown error"
                if isinstance(spawn_result, dict):
                    error_msg = spawn_result.get("error") or error_msg
                raise RuntimeError(f"Failed to spawn Terraform runner container: {error_msg}")

            success, _stats = await self._watch_detached_runner(c_name, task_name, job_id)

        return success, {
            "successful_hosts": 1 if success else 0,
            "failed_hosts": 0 if success else 1,
            "container_created": bool(_stats.get("containers_created", 0)),
        }

    async def execute_terraform_provision(
        self, host_name: str, job_id: int, force_apply: bool = False
    ) -> tuple[bool, dict]:
        host_name = str(host_name or "").strip().lower()
        if not host_name:
            return False, {"successful_hosts": 0, "failed_hosts": 1}

        ctx = self._find_terraform_context_for_host(host_name)
        infra_repo = self.base_git_dir / "infra_engine"
        render_script = infra_repo / "scripts" / "python" / "render_environment.py"
        templates_dir = infra_repo / "terraform-templates"
        if not render_script.exists():
            raise RuntimeError(f"Missing render script: {render_script}")
        if not templates_dir.exists():
            raise RuntimeError(f"Missing templates dir: {templates_dir}")

        secrets_payload, missing = self._build_terraform_secrets_payload(ctx)
        if missing:
            raise RuntimeError(
                "Missing Terraform secret(s): " + ", ".join(sorted(set(missing)))
            )

        run_rel_dir = f"inventory_state/.terraform_runs/{ctx['env_name']}__{host_name}"
        run_dir = self.base_git_dir / run_rel_dir
        if run_dir.exists():
            shutil.rmtree(run_dir)
        run_dir.mkdir(parents=True, exist_ok=True)

        secrets_file = run_dir / "secrets.json"
        with open(secrets_file, "w", encoding="utf-8") as fh:
            json.dump(secrets_payload, fh)

        render_cmd = [
            sys.executable,
            str(render_script),
            "--templates", str(templates_dir),
            "--tfvars", str(ctx["tfvars_path"]),
            "--output", str(run_dir),
            "--env-name", str(ctx["env_name"]),
            "--secrets", str(secrets_file),
        ]
        log.info("Rendering Terraform env for host '%s' using %s", host_name, ctx["tfvars_path"])
        render_proc = await asyncio.create_subprocess_exec(
            *render_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        render_out, _ = await render_proc.communicate()
        render_text = render_out.decode("utf-8", errors="replace").strip()
        if render_text:
            for line in render_text.splitlines():
                log.info(f"[TF-Render] {line}")
        if render_proc.returncode != 0:
            raise RuntimeError("Terraform render failed before runner execution.")

        node_name = ctx["node_name"]
        targets = [
            f'module.download.proxmox_virtual_environment_download_file.tpl["{node_name}"]',
            f'module.lxc_{node_name}.proxmox_virtual_environment_container.ct["{host_name}"]',
        ]
        return await self.execute_terraform_docker(
            run_rel_dir=run_rel_dir,
            host_name=host_name,
            targets=targets,
            mode="apply",
            force_apply=force_apply,
            task_name=f"Terraform Provision: {host_name}",
            job_id=job_id,
        )

    def _iter_terraform_environments(self) -> list[dict]:
        """Enumerate rendered Terraform environments that have at least one
        container, returning a context per environment suitable for a
        whole-environment plan/apply (no -target scoping)."""
        environments: list[dict] = []
        inventory_root = self.base_git_dir / "inventory_state"
        if not inventory_root.exists():
            return environments
        seen: set[str] = set()
        for tfvars_path in inventory_root.glob("*/*/terraform/terraform.tfvars.json"):
            try:
                with open(tfvars_path, "r", encoding="utf-8") as fh:
                    tfvars = json.load(fh)
            except Exception as exc:  # noqa: BLE001
                log.warning("Skipping unreadable tfvars %s: %s", tfvars_path, exc)
                continue
            containers = tfvars.get("containers") or {}
            if not containers:
                continue
            try:
                stage = tfvars_path.parents[1].name
                site = tfvars_path.parents[2].name
            except IndexError:
                continue
            env_name = f"{site}_{stage}"
            if env_name in seen:
                continue
            first_host = next(iter(containers))
            first_cfg = containers.get(first_host) or {}
            node_name = first_cfg.get("node_name") or next(
                iter(tfvars.get("proxmox_nodes") or {}), ""
            )
            seen.add(env_name)
            environments.append(
                {
                    "env_name": env_name,
                    "site": site,
                    "stage": stage,
                    "tfvars_path": tfvars_path,
                    "node_name": node_name,
                    "host_count": len(containers),
                    "representative_host": first_host,
                }
            )
        return environments

    async def execute_terraform_environment(
        self, env_ctx: dict, mode: str, job_id: int, force_apply: bool = False
    ) -> tuple[bool, dict]:
        """Render and run OpenTofu for an entire environment (no -target)."""
        env_name = env_ctx["env_name"]
        ctx = self._find_terraform_context_for_host(env_ctx["representative_host"])

        infra_repo = self.base_git_dir / "infra_engine"
        render_script = infra_repo / "scripts" / "python" / "render_environment.py"
        templates_dir = infra_repo / "terraform-templates"
        if not render_script.exists():
            raise RuntimeError(f"Missing render script: {render_script}")
        if not templates_dir.exists():
            raise RuntimeError(f"Missing templates dir: {templates_dir}")

        secrets_payload, missing = self._build_terraform_secrets_payload(ctx)
        if missing:
            raise RuntimeError(
                "Missing Terraform secret(s): " + ", ".join(sorted(set(missing)))
            )

        run_rel_dir = f"inventory_state/.terraform_runs/{env_name}__{mode}"
        run_dir = self.base_git_dir / run_rel_dir
        if run_dir.exists():
            shutil.rmtree(run_dir)
        run_dir.mkdir(parents=True, exist_ok=True)

        secrets_file = run_dir / "secrets.json"
        with open(secrets_file, "w", encoding="utf-8") as fh:
            json.dump(secrets_payload, fh)

        render_cmd = [
            sys.executable,
            str(render_script),
            "--templates", str(templates_dir),
            "--tfvars", str(ctx["tfvars_path"]),
            "--output", str(run_dir),
            "--env-name", str(env_name),
            "--secrets", str(secrets_file),
        ]
        log.info("Rendering Terraform environment '%s' (%s)", env_name, mode)
        render_proc = await asyncio.create_subprocess_exec(
            *render_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        render_out, _ = await render_proc.communicate()
        render_text = render_out.decode("utf-8", errors="replace").strip()
        if render_text:
            for line in render_text.splitlines():
                log.info(f"[TF-Render] {line}")
        if render_proc.returncode != 0:
            raise RuntimeError("Terraform render failed before runner execution.")

        verb = "Plan" if mode == "plan" else "Apply"
        return await self.execute_terraform_docker(
            run_rel_dir=run_rel_dir,
            host_name=f"env:{env_name}",
            targets=None,
            mode=mode,
            force_apply=force_apply,
            task_name=f"Terraform {verb}: {env_name}",
            job_id=job_id,
        )

    async def execute_terraform_infra(
        self, mode: str, job_id: int, force_apply: bool = False
    ) -> tuple[bool, dict]:
        """Run a whole-infrastructure plan or apply across every non-empty
        Terraform environment."""
        environments = self._iter_terraform_environments()
        if not environments:
            log.warning("No Terraform environments with containers found; nothing to %s.", mode)
            return True, {"successful_hosts": 0, "failed_hosts": 0, "environments": 0}

        overall_ok = True
        succeeded = 0
        failed = 0
        skipped = 0
        for env_ctx in environments:
            log.info(
                "Infra %s: environment '%s' (%d host(s))",
                mode,
                env_ctx["env_name"],
                env_ctx["host_count"],
            )
            try:
                ok, _stats = await self.execute_terraform_environment(
                    env_ctx, mode, job_id, force_apply=force_apply
                )
            except RuntimeError as exc:
                # Environments that are not yet configured (missing secret/render
                # inputs) are skipped rather than failing the whole infra run, so
                # configured environments still plan/apply cleanly.
                msg = str(exc)
                if "Missing Terraform secret" in msg or "render failed" in msg.lower():
                    log.warning(
                        "Infra %s: skipping unconfigured env '%s' (%s).",
                        mode,
                        env_ctx["env_name"],
                        msg,
                    )
                    skipped += 1
                    continue
                log.error("Infra %s failed for env '%s': %s", mode, env_ctx["env_name"], exc)
                ok = False
            except Exception as exc:  # noqa: BLE001
                log.error("Infra %s failed for env '%s': %s", mode, env_ctx["env_name"], exc)
                ok = False
            if ok:
                succeeded += 1
            else:
                failed += 1
                overall_ok = False
        log.info(
            "Infra %s summary: %d ok, %d failed, %d skipped (of %d env).",
            mode, succeeded, failed, skipped, len(environments),
        )
        return overall_ok, {
            "successful_hosts": succeeded,
            "failed_hosts": failed,
            "skipped": skipped,
            "environments": len(environments),
        }

    async def execute_ansible_docker(self, playbook_subpath: str, inventory_subpath: str, limit: str = None, extra_vars: dict = None, task_name: str = "global", job_id: int = 0, ssh_key_secret: str = "ansible_ssh_key", remote_user: str = "ansible-agent"):
        key_path = None # Safe default so the 'finally' block doesn't crash on early exit
        
        if not shutil.which("docker"): return False, {"successful_hosts": 0, "failed_hosts": 0}

        if "active_tasks" not in self.state: self.state["active_tasks"] = {}
        self.state["active_tasks"][task_name] = {
            "status": "pulling_image", 
            "logs": [],
            "job_id": job_id 
        }

        reg_url, reg_user, reg_token = self.ctx.get_secret("ansible_registry_url"), self.ctx.get_secret("ansible_registry_user"), self.ctx.get_secret("ansible_registry_token")
        if reg_url and reg_user and reg_token:
            proc = await asyncio.create_subprocess_exec("docker", "login", reg_url, "-u", reg_user, "--password-stdin", stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
            await proc.communicate(input=reg_token.encode('utf-8'))

        ssh_key = self.ctx.get_secret(ssh_key_secret)
        if not ssh_key:
            log.error(f"Ansible run aborted: secret '{ssh_key_secret}' is not set in Vault.")
            return False, {"successful_hosts": 0, "failed_hosts": 0}

        # Use services_dir for temporary key exchange because its volume mount is proven to be correctly mapped
        key_filename = f"ansible_id_rsa_{uuid.uuid4().hex[:8]}"
        key_dir = self.config.services_dir / ".iac_keys"
        key_dir.mkdir(parents=True, exist_ok=True)
        key_path = key_dir / key_filename
        
        with open(key_path, "w") as f: f.write(ssh_key.replace('\\n', '\n').strip() + '\n')
        os.chmod(key_path, 0o600)

        try:
            # In a Docker-in-Docker setup, bind mounts require the physical host's path.
            h_git = self.config.host_git_repos_dir
            h_svc = self.config.host_services_dir
            
            safe_task_name = "".join(c if c.isalnum() or c in ".-_" else "-" for c in task_name).strip("-")
            # Include the target host in the container name to prevent conflicts when
            # the same service deploys to multiple hosts in parallel (e.g., docker-dev
            # and docker-devops both deploying aac-docker-to-dns at the same time).
            host_slug = ""
            if limit:
                # limit may be "docker-dev:&service_aac_docker_to_dns" — extract hostname part
                raw_host = limit.split(":")[0].strip()
                host_slug = "-" + "".join(c if c.isalnum() or c in ".-_" else "-" for c in raw_host).strip("-")
            c_name = f"aac-runner-{safe_task_name}{host_slug}"
            
            await asyncio.create_subprocess_exec("docker", "rm", "-f", c_name, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

            cmd = [
                "docker", "run", "-d", "--name", c_name, "--pull", "always",
                "--label", f"iac_job_id={job_id}", "--label", f"iac_task_name={task_name}",
                "-e", f"IAC_JOB_ID={job_id}",
                "-v", f"{h_git}:/data/storage/git_repos", "-v", f"{h_svc}:/data/storage/services",
                "-e", "ANSIBLE_HOST_KEY_CHECKING=False", "-e", "PYTHONUNBUFFERED=1", "-e", "ANSIBLE_NOCOLOR=1", "-e", "ANSIBLE_DEPRECATION_WARNINGS=0", "-e", "ANSIBLE_INTERPRETER_PYTHON=auto_silent",
                "-e", "ANSIBLE_ROLES_PATH=/data/storage/git_repos/config_engine/roles", "-e", "PYTHONPATH=/data/storage/git_repos/aac_factory/scripts",
                "--entrypoint", "",
                self.config.ansible_docker_image,
                "/bin/sh", "-c", 
                "mkdir -p /root/.ssh && cp \"$1\" /root/.ssh/id_rsa && chmod 600 /root/.ssh/id_rsa && shift && exec \"$@\"", 
                "--", f"/data/storage/services/.iac_keys/{key_filename}",
                "ansible-playbook", "-i", f"/data/storage/git_repos/inventory_state/{inventory_subpath}", f"/data/storage/git_repos/config_engine/{playbook_subpath}",
                "-u", remote_user, "--diff" 
            ]
            if limit: cmd.extend(["--limit", limit])
            if extra_vars:
                for k, v in extra_vars.items(): cmd.extend(["-e", f"{k}={v}"])
            if not self.config.auto_apply: cmd.append("--check")

            log.info(f"Executing: {playbook_subpath} (Limit: {limit or 'None'})")
            self.state["active_tasks"][task_name]["status"] = "running_ansible"

            proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
            stdout, _ = await proc.communicate()
            
            if proc.returncode != 0:
                error_msg = stdout.decode('utf-8', errors='replace').strip()
                raise RuntimeError(f"Failed to spawn Docker runner container: {error_msg}")

            return await self._watch_detached_runner(c_name, task_name, job_id)
        except Exception as e:
            log.error(f"Docker Execution Error: {e}")
            if "active_tasks" in self.state and task_name in self.state["active_tasks"]:
                self.state["active_tasks"][task_name]["status"] = "error"
            return False, {"successful_hosts": 0, "failed_hosts": 0}
        finally:
            if key_path and os.path.exists(key_path):
                try: os.remove(key_path)
                except Exception: pass
