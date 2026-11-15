import asyncio
import fnmatch
import json
import yaml
from core.logger import get_logger

from .base import BaseStage
from ..utils import StageResult
from ..app.controller.offloop import off_loop
from .ansible import AnsiblePlaybookStage, AsyncBulkRolloutStage

log = get_logger("IaC:Engine:Rules")


class _MarkerUnavailable(RuntimeError):
    """The rule-engine marker could not be read (database error)."""


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

    **Which changes are evaluated (audit OC-06).** Not only the commit this
    pipeline published: everything between the *marker* — the inventory_state
    commit of the last rule stage that completed (``JobDatabase.rules_marker``)
    — and the state this pipeline pinned in its prefix. A pipeline that failed
    between publishing and its rule stage used to take its rule actions with it
    (baseline roles, service rollouts, Terraform plans for that controller
    change never ran, later pipelines saw "no changes"); now the next pipeline
    that runs the rule engine picks them up.

    * The marker moves only when the stage completed and the job is not a
      dry-run. The MODE of every action is decided exactly as before (job
      dry-run, ``auto_apply``, Terraform ``mode: plan``) — catching up never
      turns a check into a real run.
    * One rule stage at a time (``engine.rules_lock``); a waiter re-reads the
      marker and usually finds nothing left to do.
    * A backlog whose evaluation failed :data:`MAX_BACKLOG_ATTEMPTS` times is
      given up loudly (job log + notification with the files) instead of failing
      every later pipeline for ever: the stage then evaluates its own commit
      only — the behaviour before this change.
    * Without a marker (engine used without the job database) or when the
      marker is not part of the history any more, the stage evaluates its own
      commit only, as before.
    """

    #: Failed evaluations of one backlog before it is given up.
    MAX_BACKLOG_ATTEMPTS = 3

    def __init__(self, pipeline_type: str):
        super().__init__("Dynamic Rule-Based Execution")
        self.pipeline_type = pipeline_type
        # Commit to evaluate. run() pins it to the state this pipeline's
        # PublishInventoryStateStage saw, so a commit pushed by a concurrent
        # pipeline in between never changes which diff these rules act on.
        self._ref = "HEAD"
        # Old side of the diff: the marker; ``None`` = the parent of ``_ref``
        # (the pipeline's own commit only).
        self._base: str | None = None

    # ------------------------------------------------------------------ utils
    @staticmethod
    def _ansible_check_mode(engine, context: dict) -> bool:
        """--check for the Ansible actions this rule engine fires on its own.

        Rule actions are automatic infrastructure changes derived from a
        controller diff, so they stay gated by the global auto_apply switch
        (like Terraform ``mode: apply``). A job-level ``dry_run`` forces --check
        too. Explicit deploys outside the rule engine are NOT gated (1.5.6).
        """
        if context.get("dry_run"):
            return True
        if not bool(getattr(getattr(engine, "config", None), "auto_apply", False)):
            log.info("Rule action runs as DRY-RUN (--check): auto_apply is off for rule-engine changes.")
            return True
        return False

    @classmethod
    async def _check_mode(cls, engine, context: dict) -> bool:
        """:meth:`_ansible_check_mode` off the loop — ``auto_apply`` is a Vault read."""
        return await off_loop(cls._ansible_check_mode, engine, context)

    async def _git(self, engine, *args) -> tuple[int, str]:
        repo = engine.base_git_dir / "inventory_state"
        proc = await asyncio.create_subprocess_exec(
            "git", *args,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, cwd=repo,
        )
        out, err = await proc.communicate()
        return proc.returncode, (out.decode() if proc.returncode == 0 else err.decode())

    def _old(self, path: str) -> str:
        """``<rev>:<path>`` of the state the changes are measured against."""
        return f"{self._base or self._ref + '~1'}:{path}"

    async def _get_changed_files(self, engine) -> list[str]:
        if self._base:
            # --no-renames: list both paths of a moved file, like diff-tree below.
            rc, out = await self._git(engine, "diff", "--name-only", "--no-renames", self._base, self._ref)
        else:
            rc, out = await self._git(
                engine, "diff-tree", "--no-commit-id", "--name-only", "-r", self._ref
            )
        if rc != 0:
            if "bad object HEAD" in out:
                return []
            raise RuntimeError(f"Git diff failed: {out}")
        changed = out.strip()
        # Terraform run directories are versioned alongside the generated state
        # but are run artifacts: they must never trigger a rule.
        return [f for f in changed.split("\n")
                if f and not f.startswith(".terraform_runs/")] if changed else []

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
        rc_new, new_raw = await self._git(engine, "show", f"{self._ref}:{tfvars_file}")
        if rc_new != 0:
            return []
        rc_old, old_raw = await self._git(engine, "show", self._old(tfvars_file))
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
        """Diff per-host ``baseline_roles`` between the old side (marker, or the
        parent commit) and the evaluated commit of the generated inventory.
        Returns ``{host: [added_roles]}`` for hosts that *gained* a role,
        so a single newly-assigned role deploys only that role on only that host."""
        rc_new, new_raw = await self._git(engine, "show", f"{self._ref}:{inventory_file}")
        if rc_new != 0:
            return {}
        try:
            # The generated inventory is ~635 KB of YAML (about a second to parse).
            new_inv = await off_loop(yaml.safe_load, new_raw) or {}
        except Exception:
            return {}
        old_inv = {}
        rc_old, old_raw = await self._git(engine, "show", self._old(inventory_file))
        if rc_old == 0:
            try:
                old_inv = await off_loop(yaml.safe_load, old_raw) or {}
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
        check = await self._check_mode(engine, context)
        if "cd_rollout_service.yml" in target:
            stage = AsyncBulkRolloutStage(
                inventory_path=action.get("inventory"),
                limit=action.get("limit"),
                target_services=changed_services if changed_services else None,
                check_mode=check,
            )
        else:
            stage = AnsiblePlaybookStage(
                name_override=action.get("name"),
                playbook_path=target,
                inventory_path=action.get("inventory"),
                limit=action.get("limit"),
                check_mode=check,
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
        created_hosts: list[str] = []
        for host in hosts:
            try:
                ok, stats = await engine.execute_terraform_provision(
                    host_name=host,
                    job_id=context.get("job_id", 0),
                    mode=mode,            # 'plan' is a hard gate; 'apply' still needs auto_apply/approve
                    force_apply=False,    # never bypass the auto_apply guard from a rule
                    adopt_existing=adopt,
                    dry_run=bool(context.get("dry_run")),
                )
            except Exception as e:
                return StageResult(False, f"Terraform {verb} failed for '{host}': {e}")
            if not ok:
                rc = stats.get("exit_code") if isinstance(stats, dict) else None
                detail = f"exit code {rc}" if rc is not None else "exit code unknown"
                return StageResult(
                    False,
                    f"Terraform {verb} failed for host '{host}' ({detail}; tofu output is in the job log above).",
                )
            if isinstance(stats, dict) and stats.get("container_created"):
                created_hosts.append(host)
        # A rule-driven apply that actually created a CT must bootstrap it too —
        # a fresh container has no ansible-agent and would sit unmanaged otherwise.
        # (Lazy import: engine.py imports this module at load time.)
        if created_hosts:
            from ..app.controller.engine import ComplianceBootstrapStage, TriggerHostRolloutStage
            log.info(f"Terraform rule created {len(created_hosts)} host(s); auto-bootstrapping: {', '.join(created_hosts)}")
            for host in created_hosts:
                res = await ComplianceBootstrapStage(host_name=host).run(engine, context)
                if not res.success:
                    return StageResult(False, f"Auto-bootstrap failed for new host '{host}': {res.message}")
                await TriggerHostRolloutStage(host_name=host).run(engine, context)
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
            check_mode=await self._check_mode(engine, context),
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
                    check_mode=await self._check_mode(engine, context),
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
                check_mode=await self._check_mode(engine, context),
            )
            return await stage.run(engine, context)
        log.warning(f"Unknown rule action type '{atype}', skipping.")
        return StageResult(True, f"Skipped unknown action type '{atype}'.")

    # ------------------------------------------------------------------- run
    async def run(self, engine, context: dict) -> StageResult:
        rules_file_path = engine.base_git_dir / "config_engine" / "pipeline_rules.yml"
        if not rules_file_path.exists():
            log.warning("pipeline_rules.yml not found. Falling back to default hardcoded behavior.")
            check = await self._check_mode(engine, context)
            for stage in engine.get_default_ansible_stages(self.pipeline_type):
                if hasattr(stage, "check_mode"):
                    stage.check_mode = check
                res = await stage.run(engine, context)
                if not res.success:
                    return res
            return StageResult(True, "Fallback pipeline executed successfully.")
        def _load_rules() -> dict:
            with open(rules_file_path, "r") as f:
                return yaml.safe_load(f) or {}

        try:
            rules_config = await off_loop(_load_rules)
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

        return await self._run_for_pending_changes(engine, context, rules_config)

    # ------------------------------------------------------ pending changes
    _BYPASS = "No state changes generated. Bypassing execution."

    async def _run_for_pending_changes(self, engine, context: dict, rules_config: dict) -> StageResult:
        """Decide which state changes this run evaluates, evaluate them, and move
        the marker (see the class docstring)."""
        ref = str(context.get("inventory_state_head_sha") or "")
        db = getattr(engine, "db", None)
        if not ref or not callable(getattr(db, "rules_marker", None)):
            return await self._run_own_commit(engine, context, rules_config)

        try:
            if await self._nothing_pending(engine, db, ref):
                log.info(self._BYPASS)
                return StageResult(True, self._BYPASS)
            lock = getattr(engine, "rules_lock", None)
            if lock is None:
                return await self._run_tracked(engine, context, rules_config, db, ref)
            async with lock(context.get("job_id")):
                return await self._run_tracked(engine, context, rules_config, db, ref)
        except _MarkerUnavailable as exc:
            # Never guess: evaluating "only our own commit" here would move the
            # marker past changes nobody acted on.
            return StageResult(False, f"The rule-engine marker could not be read ({exc}) — the pending state "
                                      "changes were NOT evaluated. They stay pending; re-trigger the pipeline.")

    async def _read_marker(self, db) -> dict | None:
        try:
            return await off_loop(db.rules_marker)
        except Exception as exc:
            raise _MarkerUnavailable(str(exc)) from exc

    async def _is_ancestor(self, engine, older: str, newer: str) -> bool | None:
        """True/False, ``None`` when git cannot tell (unknown object)."""
        rc, _out = await self._git(engine, "merge-base", "--is-ancestor", older, newer)
        return True if rc == 0 else (False if rc == 1 else None)

    async def _nothing_pending(self, engine, db, ref: str) -> bool:
        """Has the marker reached (or passed) the state this run pinned?"""
        marker = await self._read_marker(db)
        if marker is None:
            return False
        return marker["sha"] == ref or bool(await self._is_ancestor(engine, ref, marker["sha"]))

    async def _run_tracked(self, engine, context: dict, rules_config: dict, db, ref: str) -> StageResult:
        """Evaluate marker..ref. Called with the rules lock held."""
        dry_run = bool(context.get("dry_run"))
        job_id = context.get("job_id")
        marker = await self._read_marker(db)
        if marker is not None and (marker["sha"] == ref or await self._is_ancestor(engine, ref, marker["sha"])):
            log.info("The rule engine already evaluated these state changes (another pipeline was faster).")
            return StageResult(True, self._BYPASS)

        base = marker["sha"] if marker else None
        if base and not await self._is_ancestor(engine, base, ref):
            log.warning(f"Rule engine: marker inventory_state@{base[:12]} is not part of the history of "
                        f"{ref[:12]} (history rewritten or repository replaced) — evaluating this pipeline's "
                        "own commit only.")
            base = None
        if base is None:
            # No usable marker: exactly the behaviour before the marker existed.
            res = await self._run_own_commit(engine, context, rules_config)
            if res.success and not dry_run:
                await self._advance(db, ref, job_id)
            return res

        failures = int(marker.get("failures") or 0)
        if failures >= self.MAX_BACKLOG_ATTEMPTS and not dry_run:
            skipped_to = await self._give_up_backlog(engine, context, db, marker)
            if skipped_to:
                base, failures = skipped_to, 0
                if base == ref:
                    return StageResult(True, self._BYPASS)

        self._ref, self._base = ref, base
        log.info(f"Rule engine: evaluating state changes inventory_state@{base[:12]}..{ref[:12]}"
                 + ("" if base == context.get("inventory_state_base_sha")
                    else " (includes changes of earlier pipelines whose rule stage did not complete)"))
        try:
            res = await self._evaluate(engine, context, rules_config)
        except Exception as exc:
            if not dry_run:
                await self._note_failure(db, base, failures, f"{type(exc).__name__}: {exc}", job_id)
            raise
        if dry_run:
            # A dry-run changes nothing — not the marker either: the same changes
            # are evaluated again by the next real run.
            log.info("Dry-run: the evaluated state changes stay pending for the next real run.")
        elif res.success:
            await self._advance(db, ref, job_id)
        else:
            await self._note_failure(db, base, failures, res.message, job_id)
            log.error(f"Rule engine: the state changes inventory_state@{base[:12]}..{ref[:12]} stay pending "
                      f"(failed evaluation {failures + 1}/{self.MAX_BACKLOG_ATTEMPTS}); the next pipeline that "
                      "runs the rule engine evaluates them again.")
        return res

    @staticmethod
    async def _advance(db, ref: str, job_id) -> None:
        try:
            await off_loop(db.set_rules_marker, ref, job_id=job_id)
        except Exception as exc:
            # The actions ran. Without the marker the next pipeline evaluates the
            # same changes once more (idempotent) — that is not worth failing this job.
            log.error(f"Rule engine: marker not moved to inventory_state@{ref[:12]} ({exc}); the next pipeline "
                      "evaluates these state changes again.")

    @staticmethod
    async def _note_failure(db, base: str, failures: int, error: str, job_id) -> None:
        try:
            await off_loop(db.set_rules_marker, base, job_id=job_id, failures=failures + 1,
                                    last_error=str(error or ""))
        except Exception as exc:
            log.warning(f"Rule engine: could not record the failed evaluation: {exc}")

    async def _give_up_backlog(self, engine, context: dict, db, marker: dict) -> str | None:
        """The changes earlier pipelines left behind failed too often: skip them —
        announced, with the files — and continue from the state before this
        pipeline's own commit. Returns that state, ``None`` when there is nothing
        to separate from the own commit (then the caller evaluates once more)."""
        base = marker["sha"]
        own_base = str(context.get("inventory_state_base_sha") or "")
        if not own_base or own_base == base or not await self._is_ancestor(engine, base, own_base):
            return None
        rc, out = await self._git(engine, "diff", "--name-only", "--no-renames", base, own_base)
        files = [f for f in out.splitlines() if f and not f.startswith(".terraform_runs/")] if rc == 0 else []
        shown = ", ".join(files[:20]) + (f" (+{len(files) - 20} more)" if len(files) > 20 else "")
        text = (f"The rule actions for the state changes inventory_state@{base[:12]}..{own_base[:12]} failed "
                f"{marker.get('failures')} times and are NOT retried any more. Last error: "
                f"{marker.get('last_error') or 'unknown'}. Changed files: {shown or 'none'}. "
                "Apply them by hand (baseline roles, service rollout, Terraform plan); later state changes "
                "are evaluated normally.")
        log.error(f"Rule engine: {text}")
        notify = getattr(engine, "_notify", None)
        if callable(notify):
            try:
                notify("Rule actions skipped", text[:900], "negative", broadcast=True)
            except Exception as exc:
                log.debug(f"give-up notification not sent: {exc}")
        await off_loop(db.set_rules_marker, own_base, job_id=context.get("job_id"))
        return own_base

    async def _run_own_commit(self, engine, context: dict, rules_config: dict) -> StageResult:
        """Evaluate the commit this pipeline published — nothing else."""
        if context.get("inventory_state_commit_status") == "no_changes":
            log.info(self._BYPASS)
            return StageResult(True, self._BYPASS)
        self._ref, self._base = str(context.get("inventory_state_commit_sha") or "HEAD"), None
        return await self._evaluate(engine, context, rules_config)

    async def _evaluate(self, engine, context: dict, rules_config: dict) -> StageResult:
        """Match the changed files of ``_base``..``_ref`` against the rules and run
        the actions."""
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
