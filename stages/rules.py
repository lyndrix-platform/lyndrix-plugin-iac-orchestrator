import asyncio
import fnmatch
import json
import re
import yaml
from pathlib import Path
from core.logger import get_logger

from .base import BaseStage
from ..utils import StageResult
from .ansible import AnsiblePlaybookStage, AsyncBulkRolloutStage

log = get_logger("IaC:Engine:Rules")


class DynamicRuleExecutionStage(BaseStage):
    """Maps changes in the generated ``inventory_state`` repo to precisely-scoped
    deploy actions, driven by ``config_engine/pipeline_rules.yml``.

    Two rule schemas are supported simultaneously:

    * **Legacy (v1)** -- an action with a ``playbook``/``inventory``/``limit``.
      Runs that playbook (or AsyncBulkRollout when it is ``cd_rollout_service``).
      Existing rules keep working unchanged.

    * **Scoped (v2)** -- an action with a ``type``:
        - ``terraform``       provision only the host(s) whose tfvars actually
                              changed. ``mode: plan`` (default, read-only) or
                              ``mode: apply`` (explicit opt-in -- recreates CTs).
        - ``service_rollout`` roll out only the changed services
                              (``scope: changed_services``) or all
                              (``scope: all``) to the captured ``limit``.
        - ``baseline``        run a baseline roles playbook at ``limit``.
        - ``connectivity``    run a read-only connectivity playbook.

      ``limit`` may contain capture variables ``{site}`` / ``{stage}`` derived
      from the changed path (e.g. ``onprem/dev/...`` -> site=onprem, stage=dev).
    """

    def __init__(self, pipeline_type: str):
        super().__init__("Dynamic Rule-Based Execution")
        self.pipeline_type = pipeline_type

    # ------------------------------------------------------------------ utils
    async def _git(self, engine, *args) -> tuple[int, str]:
        repo = engine.base_git_dir / "inventory_state"
        proc = await asyncio.create_subprocess_exec(
            "git", *args,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, cwd=repo,
        )
        out, err = await proc.communicate()
        return proc.returncode, (out.decode() if proc.returncode == 0 else err.decode())

    async def _get_changed_files(self, engine) -> list[str]:
        rc, out = await self._git(
            engine, "diff-tree", "--no-commit-id", "--name-only", "-r", "HEAD"
        )
        if rc != 0:
            if "bad object HEAD" in out:
                return []
            raise RuntimeError(f"Git diff failed: {out}")
        changed = out.strip()
        return [f for f in changed.split("\n") if f] if changed else []

    @staticmethod
    def _capture_path_vars(filepath: str) -> dict:
        """Derive {site}/{stage} from a path like ``onprem/dev/terraform/...``."""
        parts = filepath.split("/")
        vars_ = {}
        if len(parts) >= 2:
            vars_["site"] = parts[0]
            vars_["stage"] = parts[1]
        return vars_

    @staticmethod
    def _substitute(limit: str, path_vars: dict) -> str:
        if not limit:
            return limit
        out = limit
        for k, v in path_vars.items():
            out = out.replace("{" + k + "}", str(v))
        return out

    def _extract_changed_services(self, changed_files: list[str]) -> list[str]:
        changed_services = set()
        for filepath in changed_files:
            for part in filepath.split("/"):
                if part.startswith("aac-") or part in ["aria2", "renovate", "gitlab", "minio", "openbao"]:
                    changed_services.add(part)
                    break
        return list(changed_services)

    async def _changed_terraform_hosts(self, engine, tfvars_file: str) -> list[str]:
        """Diff the previous vs current ``terraform.tfvars.json`` and return the
        hostnames whose container definition actually changed (added or mutated).
        This is what keeps a terraform rule from reprovisioning a whole env."""
        rc_new, new_raw = await self._git(engine, "show", f"HEAD:{tfvars_file}")
        if rc_new != 0:
            return []
        rc_old, old_raw = await self._git(engine, "show", f"HEAD~1:{tfvars_file}")
        try:
            new_containers = (json.loads(new_raw) or {}).get("containers", {}) or {}
        except Exception:
            return []
        old_containers = {}
        if rc_old == 0:
            try:
                old_containers = (json.loads(old_raw) or {}).get("containers", {}) or {}
            except Exception:
                old_containers = {}
        changed = []
        for host, spec in new_containers.items():
            if host not in old_containers or old_containers.get(host) != spec:
                changed.append(host)
        return changed

    # --------------------------------------------------------------- actions
    async def _run_legacy_action(self, engine, context, action, changed_services):
        target = action.get("playbook", "")
        if "cd_rollout_service.yml" in target:
            stage = AsyncBulkRolloutStage(
                inventory_path=action.get("inventory"),
                limit=action.get("limit"),
                target_services=changed_services if changed_services else None,
            )
        else:
            stage = AnsiblePlaybookStage(
                name_override=action.get("name"),
                playbook_path=target,
                inventory_path=action.get("inventory"),
                limit=action.get("limit"),
            )
        return await stage.run(engine, context)

    async def _run_terraform_action(self, engine, context, action, changed_files, path_vars):
        mode = str(action.get("mode", "plan")).lower()
        apply = mode == "apply"
        hosts: list[str] = []
        for f in changed_files:
            if f.endswith("terraform/terraform.tfvars.json"):
                hosts.extend(await self._changed_terraform_hosts(engine, f))
        hosts = sorted(set(h for h in hosts if h))
        if not hosts:
            log.info("Terraform rule matched but no host-level changes detected; nothing to provision.")
            return StageResult(True, "Terraform: no changed hosts.")
        verb = "APPLY" if apply else "PLAN"
        log.info(f"Terraform rule -> {verb} for changed host(s): {', '.join(hosts)}")
        for host in hosts:
            try:
                ok, _ = await engine.execute_terraform_provision(
                    host_name=host,
                    job_id=context.get("job_id", 0),
                    force_apply=apply,  # plan-only unless the rule explicitly opts into apply
                )
            except Exception as e:
                return StageResult(False, f"Terraform {verb} failed for '{host}': {e}")
            if not ok:
                return StageResult(False, f"Terraform {verb} failed for host '{host}'.")
        return StageResult(True, f"Terraform {verb} completed for {len(hosts)} host(s).")

    async def _run_service_rollout_action(self, engine, context, action, changed_services, path_vars):
        scope = str(action.get("scope", "changed_services")).lower()
        limit = self._substitute(action.get("limit", "all"), path_vars)
        target_services = None
        if scope == "changed_services":
            if not changed_services:
                return StageResult(True, "Service rollout: no changed services.")
            target_services = changed_services
        already = set(context.get("_rules_deployed_services", []))
        if target_services is not None:
            target_services = [s for s in target_services if s not in already]
            if not target_services:
                return StageResult(True, "Service rollout: all changed services already deployed.")
        stage = AsyncBulkRolloutStage(
            inventory_path=action.get("inventory", "global/ansible/inventory.yml"),
            limit=limit,
            target_services=target_services,
        )
        res = await stage.run(engine, context)
        if target_services:
            context["_rules_deployed_services"] = list(already | set(target_services))
        return res

    async def _run_action(self, engine, context, action, changed_files, changed_services, path_vars):
        atype = str(action.get("type", "")).strip().lower()
        if not atype:
            return await self._run_legacy_action(engine, context, action, changed_services)
        if atype == "terraform":
            return await self._run_terraform_action(engine, context, action, changed_files, path_vars)
        if atype == "service_rollout":
            return await self._run_service_rollout_action(engine, context, action, changed_services, path_vars)
        if atype in ("baseline", "connectivity"):
            playbook = action.get("playbook") or (
                "playbooks/cd_playbooks/cd_baseline_roles.yml" if atype == "baseline"
                else "playbooks/cd_playbooks/cd_test_inventory.yml"
            )
            stage = AnsiblePlaybookStage(
                name_override=action.get("name", atype.title()),
                playbook_path=playbook,
                inventory_path=action.get("inventory", "global/ansible/inventory.yml"),
                limit=self._substitute(action.get("limit", "all"), path_vars),
            )
            return await stage.run(engine, context)
        log.warning(f"Unknown rule action type '{atype}', skipping.")
        return StageResult(True, f"Skipped unknown action type '{atype}'.")

    # ------------------------------------------------------------------- run
    async def run(self, engine, context: dict) -> StageResult:
        rules_file_path = engine.base_git_dir / "config_engine" / "pipeline_rules.yml"
        if not rules_file_path.exists():
            log.warning("pipeline_rules.yml not found. Falling back to default hardcoded behavior.")
            for stage in engine.get_default_ansible_stages(self.pipeline_type):
                res = await stage.run(engine, context)
                if not res.success:
                    return res
            return StageResult(True, "Fallback pipeline executed successfully.")
        with open(rules_file_path, "r") as f:
            try:
                rules_config = yaml.safe_load(f) or {}
            except yaml.YAMLError as e:
                return StageResult(False, f"Error parsing pipeline_rules.yml: {e}")

        if self.pipeline_type == "connectivity":
            actions = rules_config.get("connectivity_test", [])
            log.info("Executing 'connectivity_test' action from rules file.")
            for action in actions:
                res = await self._run_action(engine, context, action, [], [], {})
                if not res.success:
                    return res
            return StageResult(True, "Connectivity actions completed.")

        if context.get("inventory_state_commit_status") == "no_changes":
            log.info("No state changes generated. Bypassing execution.")
            return StageResult(True, "No state changes generated. Bypassing execution.")

        changed_files = await self._get_changed_files(engine)
        if not changed_files:
            return StageResult(True, "No changes in git diff, no actions required.")
        changed_services = self._extract_changed_services(changed_files)
        if changed_services:
            log.info(f"Differential deployment triggered for: {changed_services}")

        matched_rule = None
        path_vars: dict = {}
        for rule in rules_config.get("rules", []):
            patterns = rule.get("paths", [])
            for f in changed_files:
                if any(fnmatch.fnmatch(f, p) for p in patterns):
                    matched_rule = rule
                    path_vars = self._capture_path_vars(f)
                    break
            if matched_rule:
                break

        actions = matched_rule.get("actions", []) if matched_rule else rules_config.get("default", [])
        if not actions:
            return StageResult(True, "No actions performed.")

        for action in actions:
            res = await self._run_action(engine, context, action, changed_files, changed_services, path_vars)
            if not res.success:
                return res
        return StageResult(True, "Dynamic rule-based execution completed.")
