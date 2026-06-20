import asyncio
import fnmatch
import json
import yaml
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

    @staticmethod
    def _extract_host_baseline_roles(inventory: dict) -> dict:
        """Walk a generated Ansible inventory and return ``{hostname: set(roles)}``
        for every host that declares ``baseline_roles``, regardless of how deeply
        the host is nested under ``all`` / ``children`` groups."""
        result: dict[str, set] = {}

        def _walk(node):
            if not isinstance(node, dict):
                return
            hosts = node.get("hosts")
            if isinstance(hosts, dict):
                for host, hv in hosts.items():
                    if isinstance(hv, dict) and isinstance(hv.get("baseline_roles"), list):
                        result.setdefault(host, set()).update(
                            str(r) for r in hv["baseline_roles"]
                        )
            children = node.get("children")
            if isinstance(children, dict):
                for child in children.values():
                    _walk(child)

        _walk(inventory)
        if isinstance(inventory, dict):
            for top in inventory.values():
                _walk(top)
        return result

    async def _changed_baseline_roles(self, engine, inventory_file: str) -> dict:
        """Diff per-host ``baseline_roles`` between HEAD~1 and HEAD of the generated
        inventory. Returns ``{host: [added_roles]}`` for hosts that *gained* a role,
        so a single newly-assigned role deploys only that role on only that host."""
        rc_new, new_raw = await self._git(engine, "show", f"HEAD:{inventory_file}")
        if rc_new != 0:
            return {}
        try:
            new_inv = yaml.safe_load(new_raw) or {}
        except Exception:
            return {}
        old_inv = {}
        rc_old, old_raw = await self._git(engine, "show", f"HEAD~1:{inventory_file}")
        if rc_old == 0:
            try:
                old_inv = yaml.safe_load(old_raw) or {}
            except Exception:
                old_inv = {}
        new_map = self._extract_host_baseline_roles(new_inv)
        old_map = self._extract_host_baseline_roles(old_inv)
        changed: dict[str, list] = {}
        for host, new_roles in new_map.items():
            # Brand-new hosts (absent from the previous inventory) are provisioned by
            # the host_provision pipeline — don't try to role-patch an unprovisioned,
            # unreachable host here. Only patch hosts that already existed.
            if host not in old_map:
                continue
            added = sorted(new_roles - old_map[host])
            if added:
                changed[host] = added
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
        # Plan-by-default: a rule NEVER force-applies. mode:plan ALWAYS plans (a hard
        # gate, e.g. prod). mode:apply is merely *eligible* to apply, and only does so
        # when the operator has enabled the global auto_apply switch (or approves a
        # per-run). So Terraform can never fire on a dev host without a deliberate
        # opt-in — until then every change just yields a reviewable plan.
        verb = "apply-eligible" if apply else "plan"
        intent = "apply-eligible (gated by auto_apply)" if apply else "plan-only"
        log.info(f"Terraform rule -> {intent} for changed host(s): {', '.join(hosts)}")
        adopt = bool(action.get("adopt_existing", False))
        for host in hosts:
            try:
                ok, _ = await engine.execute_terraform_provision(
                    host_name=host,
                    job_id=context.get("job_id", 0),
                    mode=mode,            # 'plan' is a hard gate; 'apply' still needs auto_apply/approve
                    force_apply=False,    # never bypass the auto_apply guard from a rule
                    adopt_existing=adopt,
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

    async def _run_baseline_role_action(self, engine, context, action, changed_files):
        """Run ONLY the newly-added baseline role(s) on the host(s) that gained
        them, via ``cd_single_role_rollout.yml`` (``-e role_to_run``, ``--limit <host>``).
        Role names are passed through verbatim, exactly as ``cd_baseline_roles.yml``
        feeds them to ``include_role``."""
        inv_files = {f for f in changed_files if f.endswith("ansible/inventory.yml")}
        if not inv_files:
            inv_files = {"global/ansible/inventory.yml"}
        host_roles: dict[str, list] = {}
        for inv in inv_files:
            for host, roles in (await self._changed_baseline_roles(engine, inv)).items():
                bucket = host_roles.setdefault(host, [])
                for r in roles:
                    if r not in bucket:
                        bucket.append(r)
        if not host_roles:
            return StageResult(True, "Baseline roles: no host gained a role.")
        playbook = action.get("playbook", "playbooks/cd_playbooks/cd_single_role_rollout.yml")
        inventory = action.get("inventory", "global/ansible/inventory.yml")
        applied = 0
        for host, roles in host_roles.items():
            for role in roles:
                stage = AnsiblePlaybookStage(
                    name_override=f"Baseline role '{role}' -> {host}",
                    playbook_path=playbook,
                    inventory_path=inventory,
                    limit=host,
                    extra_vars={"role_to_run": role},
                )
                res = await stage.run(engine, context)
                if not res.success:
                    return res
                applied += 1
        return StageResult(
            True, f"Baseline roles: applied {applied} role(s) across {len(host_roles)} host(s)."
        )

    async def _run_action(self, engine, context, action, changed_files, changed_services, path_vars):
        atype = str(action.get("type", "")).strip().lower()
        if not atype:
            return await self._run_legacy_action(engine, context, action, changed_services)
        if atype == "terraform":
            return await self._run_terraform_action(engine, context, action, changed_files, path_vars)
        if atype == "service_rollout":
            return await self._run_service_rollout_action(engine, context, action, changed_services, path_vars)
        if atype == "baseline_role":
            return await self._run_baseline_role_action(engine, context, action, changed_files)
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

        # Collect EVERY rule whose paths match the change set, not just the first.
        # A single push (e.g. a new host) touches both terraform.tfvars.json and
        # inventory.yml, so the terraform AND baseline_role rules must both fire.
        matched_rules: list[tuple[dict, dict]] = []  # (rule, path_vars)
        for rule in rules_config.get("rules", []):
            patterns = rule.get("paths", [])
            rule_path_vars: dict = {}
            matched = False
            for f in changed_files:
                if any(fnmatch.fnmatch(f, p) for p in patterns):
                    matched = True
                    if not rule_path_vars:
                        rule_path_vars = self._capture_path_vars(f)
            if matched:
                matched_rules.append((rule, rule_path_vars))

        if matched_rules:
            log.info(f"{len(matched_rules)} rule(s) matched the change set: "
                     f"{', '.join(r.get('name', '?') for r, _ in matched_rules)}")
        else:
            default_actions = rules_config.get("default", [])
            if not default_actions:
                return StageResult(True, "No rules matched and no default actions.")
            matched_rules = [({"name": "default", "actions": default_actions}, {})]

        ran_any = False
        for rule, path_vars in matched_rules:
            for action in rule.get("actions", []):
                ran_any = True
                res = await self._run_action(
                    engine, context, action, changed_files, changed_services, path_vars
                )
                if not res.success:
                    return res
        if not ran_any:
            return StageResult(True, "No actions performed.")
        return StageResult(True, "Dynamic rule-based execution completed.")
