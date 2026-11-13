import asyncio
import fnmatch
import json
import yaml
from core.logger import get_logger

from .base import BaseStage
from ..utils import StageResult
from .ansible import AnsiblePlaybookStage, AsyncBulkRolloutStage
from . import service_diff as sdiff

log = get_logger("IaC:Engine:Rules")

#: Operator modes of the ``service_reconcile`` action (setting ``service_reconcile_mode``).
SERVICE_RECONCILE_MODES = ("off", "notify", "dry_run", "deploy")
#: Trigger name the dispatched single_service jobs carry (maintenance policy applies to it).
SERVICE_RECONCILE_TRIGGER = "controller-diff"
#: Shell deep link to the plugin UI (deploy buttons live there).
_PLUGIN_UI_PATH = "/apps/lyndrix-plugin-iac_orchestrator/iac"


def _is_duplicate_job(exc: BaseException) -> bool:
    """``queue.DuplicateJob`` matched by name + ``key`` attribute: this module is loaded
    standalone in tests and must not import the queue (sqlalchemy, core.api)."""
    return type(exc).__name__ == "DuplicateJob" and hasattr(exc, "key")


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
        - ``service_reconcile`` (1.7) diff the generated stage inventories host by
                              host and queue ONE ``single_service`` job per changed
                              service and stage, limited to the hosts whose rendered
                              entry changed. Mode via setting ``service_reconcile_mode``
                              (``off`` default / ``notify`` / ``dry_run`` / ``deploy``);
                              above ``service_reconcile_max_hosts`` /
                              ``service_reconcile_max_services`` the change waits for
                              operator approval (``/reconcile/pending``). Services in the
                              setting ``service_reconcile_exclude_services`` (default
                              ``aac-hermes-agent``) or the rule parameter
                              ``exclude_services`` are only reported, never dispatched.
                              Never fails the job.

      ``limit`` may contain capture variables ``{site}`` / ``{stage}`` derived
      from the changed path (e.g. ``onprem/dev/...`` -> site=onprem, stage=dev).
    """

    def __init__(self, pipeline_type: str):
        super().__init__("Dynamic Rule-Based Execution")
        self.pipeline_type = pipeline_type
        # Commit to evaluate. run() pins it to the SHA this pipeline's
        # PublishInventoryStateStage created, so a commit pushed by a concurrent
        # pipeline in between never changes which diff these rules act on.
        self._ref = "HEAD"

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
        rc_old, old_raw = await self._git(engine, "show", f"{self._ref}~1:{tfvars_file}")
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
        rc_new, new_raw = await self._git(engine, "show", f"{self._ref}:{inventory_file}")
        if rc_new != 0:
            return {}
        try:
            new_inv = yaml.safe_load(new_raw) or {}
        except Exception:
            return {}
        old_inv = {}
        rc_old, old_raw = await self._git(engine, "show", f"{self._ref}~1:{inventory_file}")
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
        check = self._ansible_check_mode(engine, context)
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
            check_mode=self._ansible_check_mode(engine, context),
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
                    check_mode=self._ansible_check_mode(engine, context),
                )
                res = await stage.run(engine, context)
                if not res.success:
                    return res
                applied += 1
        return StageResult(
            True, f"Baseline roles: applied {applied} role(s) across {len(host_roles)} host(s)."
        )

    # ------------------------------------------------- service reconcile (1.7)
    @staticmethod
    def _reconcile_mode(engine) -> str:
        raw = str(getattr(getattr(engine, "config", None), "service_reconcile_mode", "off") or "off").strip().lower()
        return raw if raw in SERVICE_RECONCILE_MODES else "off"

    @staticmethod
    def _reconcile_limit(engine, attr: str, default: int) -> int:
        try:
            return max(1, int(getattr(getattr(engine, "config", None), attr, default) or default))
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _reconcile_excluded(engine, action) -> set[str]:
        """Services this action never dispatches: the setting
        ``service_reconcile_exclude_services`` (default ``aac-hermes-agent``; an operator who
        clears it excludes nothing) plus the rule parameter ``exclude_services`` (list or comma
        string in ``pipeline_rules.yml``). Normalised, case-insensitive."""
        cfg = getattr(engine, "config", None)
        if cfg is not None and hasattr(cfg, "service_reconcile_exclude_services"):
            excluded = sdiff.normalize_service_names(getattr(cfg, "service_reconcile_exclude_services"))
        else:
            excluded = sdiff.normalize_service_names(sdiff.DEFAULT_EXCLUDE_SERVICES)
        if isinstance(action, dict):
            excluded |= sdiff.normalize_service_names(action.get("exclude_services"))
        return excluded

    async def _load_inventory_pair(self, engine, path: str) -> tuple[dict, dict] | None:
        """``(old, new)`` render of one inventory file at ``ref~1`` / ``ref``; ``None`` when
        either side is missing or unparsable (first commit → nothing to diff)."""
        rc_new, new_raw = await self._git(engine, "show", f"{self._ref}:{path}")
        if rc_new != 0:
            return None
        rc_old, old_raw = await self._git(engine, "show", f"{self._ref}~1:{path}")
        if rc_old != 0:
            return None
        try:
            return (yaml.safe_load(old_raw) or {}), (yaml.safe_load(new_raw) or {})
        except Exception:
            return None

    async def _detect_service_changes(self, engine, changed_files: list[str]) -> tuple[dict, dict]:
        """Diff every changed *stage* inventory. Returns ``(plan, removed)`` with
        ``plan = {(service, stage): {service, stage, site, sites, hosts, changed_keys}}`` and
        ``removed = {service: [hosts]}`` (reported only). Values never leave the diff — only
        key paths such as ``config.vars.WAZUH_MANAGER``."""
        plan: dict[tuple[str, str], dict] = {}
        removed_all: dict[str, list[str]] = {}
        for path in changed_files:
            scope = sdiff.inventory_path_scope(path)
            if scope is None:
                continue
            site, stage = scope
            pair = await self._load_inventory_pair(engine, path)
            if pair is None:
                continue
            changed, removed = sdiff.diff_host_services(*pair)
            for name, change in changed.items():
                entry = plan.setdefault((name, stage), {
                    "service": name, "stage": stage, "site": site, "sites": [], "hosts": [], "changed_keys": [],
                })
                if site not in entry["sites"]:
                    entry["sites"].append(site)
                for host in change.hosts:
                    if host not in entry["hosts"]:
                        entry["hosts"].append(host)
                for key in change.changed_keys:
                    if key not in entry["changed_keys"]:
                        entry["changed_keys"].append(key)
            for name, hosts in removed.items():
                bucket = removed_all.setdefault(name, [])
                bucket.extend(h for h in hosts if h not in bucket)
        return plan, removed_all

    @staticmethod
    def _reconcile_payload(entry: dict, mode: str, context: dict) -> dict:
        """The ``single_service`` job for one (service, stage) — same shape every other
        trigger emits, plus ``target_hosts`` (host limit) and the controller-diff provenance."""
        return {
            "pipeline_type": "single_service",
            "service_name": entry["service"],
            "service_branch": sdiff.branch_for_stage(entry["stage"]),
            "target_hosts": sorted(entry["hosts"]),
            "stage": entry["stage"],
            "site": entry["site"],
            "trigger": SERVICE_RECONCILE_TRIGGER,
            "source": "service_reconcile",
            "dry_run": mode == "dry_run",
            "changed_keys": list(entry["changed_keys"]),
            "controller_sha": context.get("iac_controller_sha"),
            "inventory_commit": context.get("inventory_state_commit_sha"),
            "source_job_id": context.get("job_id"),
        }

    async def _reconcile_enqueue(self, engine, payload: dict) -> tuple[str, object]:
        """Queue one job. ``("queued", job_id)`` / ``("duplicate", existing_job_id)`` /
        ``("error", message)``. Uses the service's ``enqueue_pipeline`` (set on the engine)
        so a ``DuplicateJob`` is visible; without it (older wiring) the bus path is used and
        duplicates are dropped by the webhook handler."""
        enqueue = getattr(engine, "enqueue_pipeline", None)
        if callable(enqueue):
            try:
                job_id = await asyncio.to_thread(enqueue, dict(payload), requested_by=SERVICE_RECONCILE_TRIGGER)
                return "queued", job_id
            except Exception as exc:
                if _is_duplicate_job(exc):
                    return "duplicate", getattr(exc, "existing_id", None)
                return "error", f"{type(exc).__name__}: {exc}"
        engine.ctx.emit("iac:webhook_verified", dict(payload))
        return "queued", None

    @staticmethod
    def _reconcile_notify(engine, *, mode: str, entries: list[dict], removed: dict, outcomes: dict,
                          inventory_commit: str | None, note: str, excluded: list[dict] | None = None) -> None:
        """One bell/Discord notification per controller change. Service, stage, host count,
        key *names* and the outcome — never a value (profiles carry plaintext secrets).
        ``excluded`` entries are listed with outcome ``excluded`` so the operator sees the
        change exists but was deliberately not deployed."""
        excluded = excluded or []
        lines = []

        def _keys(entry: dict) -> str:
            return ", ".join(entry["changed_keys"][:8]) + (" …" if len(entry["changed_keys"]) > 8 else "")

        for entry in entries:
            key = (entry["service"], entry["stage"])
            outcome = outcomes.get(key, note)
            lines.append(f"• {entry['service']} ({entry['stage']}, {len(entry['hosts'])} host(s)) — {_keys(entry) or '-'} → {outcome}")
        for entry in excluded:
            lines.append(f"• {entry['service']} ({entry['stage']}, {len(entry['hosts'])} host(s)) — {_keys(entry) or '-'} "
                         f"→ excluded (never deployed automatically — deploy by hand if intended)")
        for name, hosts in sorted(removed.items()):
            lines.append(f"• {name}: removed on {len(hosts)} host(s) — no automatic cleanup")
        sha = str(inventory_commit or "")[:7] or "n/a"
        title = f"Service changes detected ({len(entries) + len(excluded)}) — inventory@{sha}"
        body = "\n".join(lines) + f"\nMode: {mode}. Deploy / approve: {_PLUGIN_UI_PATH}"
        attention = (mode == "notify" or bool(excluded)
                     or any(str(v).startswith(("pending", "deferred", "notify")) for v in outcomes.values()))
        severity = "warning" if attention else "info"
        payload = {
            "message": body,
            "mode": mode,
            "inventory_commit": inventory_commit,
            "services": [{"service": e["service"], "stage": e["stage"], "hosts": len(e["hosts"]),
                          "changed_keys": list(e["changed_keys"]),
                          "outcome": outcomes.get((e["service"], e["stage"]), note)} for e in entries]
                        + [{"service": e["service"], "stage": e["stage"], "hosts": len(e["hosts"]),
                            "changed_keys": list(e["changed_keys"]), "outcome": "excluded"} for e in excluded],
            "excluded": sorted({e["service"] for e in excluded}),
            "removed": {k: len(v) for k, v in removed.items()},
            "ui_path": _PLUGIN_UI_PATH,
        }
        notify = getattr(getattr(engine, "ctx", None), "notify", None)
        if callable(notify):
            try:
                notify("service_reconcile", payload=payload, notification_id=f"service_reconcile_{sha}",
                       title=title, body=body, severity=severity)
                return
            except Exception as exc:
                log.debug(f"Service reconcile: ctx.notify failed ({exc}); falling back to messaging.")
        fallback = getattr(engine, "_notify", None)
        if callable(fallback):
            fallback(title, body, severity, notification_id=f"service_reconcile_{sha}", broadcast=True)

    async def _run_service_reconcile_action(self, engine, context, action, changed_files):
        """Never fails the job: a broken detection or dispatch is logged and reported, the
        infra pipeline that carried the rule finishes normally."""
        try:
            return await self._service_reconcile(engine, context, action, changed_files)
        except Exception as exc:
            log.error(f"Service reconcile failed (ignored, pipeline continues): {type(exc).__name__}: {exc}")
            return StageResult(True, f"Service reconcile: error ignored ({type(exc).__name__}).")

    async def _service_reconcile(self, engine, context, action, changed_files):
        mode = self._reconcile_mode(engine)
        plan, removed = await self._detect_service_changes(engine, changed_files)
        if not plan and not removed:
            return StageResult(True, "Service reconcile: no service entries changed.")
        entries = sorted(plan.values(), key=lambda e: (e["service"], e["stage"]))
        # Excluded services (setting / rule parameter) leave the dispatch path right here:
        # they never become a job, a pending record or a deferred one — only a log line and an
        # "excluded" item in the notification. They also do not count against max_services.
        excluded_names = self._reconcile_excluded(engine, action)
        excluded = [e for e in entries if sdiff.is_excluded(e["service"], excluded_names)]
        entries = [e for e in entries if not sdiff.is_excluded(e["service"], excluded_names)]
        for entry in entries:
            log.info(f"Service reconcile: {entry['service']} ({entry['stage']}) changed on {len(entry['hosts'])} host(s): "
                     f"{', '.join(entry['hosts'])}; keys: {', '.join(entry['changed_keys']) or '-'}")
        for entry in excluded:
            log.warning(f"Service reconcile: {entry['service']} ({entry['stage']}) changed on {len(entry['hosts'])} host(s) "
                        f"({', '.join(entry['hosts'])}); keys: {', '.join(entry['changed_keys']) or '-'} — EXCLUDED from "
                        f"automatic deploys (service_reconcile_exclude_services / rule exclude_services), nothing dispatched.")
        for name, hosts in sorted(removed.items()):
            log.warning(f"Service reconcile: {name} removed on {len(hosts)} host(s) ({', '.join(hosts)}) — orphan cleanup is not implemented.")
        excluded_note = f", {len(excluded)} excluded" if excluded else ""
        if mode == "off":
            return StageResult(True, f"Service reconcile: {len(entries)} service change(s) detected{excluded_note} — mode 'off', nothing dispatched.")

        inventory_commit = context.get("inventory_state_commit_sha")
        outcomes: dict[tuple[str, str], str] = {}
        if not entries:
            if excluded:
                self._reconcile_notify(engine, mode=mode, entries=[], excluded=excluded, removed=removed, outcomes=outcomes,
                                       inventory_commit=inventory_commit, note="n/a")
                return StageResult(True, f"Service reconcile: {len(excluded)} excluded service change(s) only — nothing to deploy.")
            return StageResult(True, "Service reconcile: only removals detected — nothing to deploy.")
        if mode == "notify" or self.pipeline_type == "rollout":
            note = "notify only" if mode == "notify" else "notify only (rollout pipeline deploys inline)"
            self._reconcile_notify(engine, mode=mode, entries=entries, excluded=excluded, removed=removed, outcomes=outcomes,
                                   inventory_commit=inventory_commit, note=note)
            return StageResult(True, f"Service reconcile: {len(entries)} service change(s) → {note}{excluded_note}.")

        max_hosts = self._reconcile_limit(engine, "service_reconcile_max_hosts", 10)
        max_services = self._reconcile_limit(engine, "service_reconcile_max_services", 3)
        store = getattr(engine, "reconcile_store", None)
        if store is None:
            log.warning("Service reconcile: no record store on this engine — changes above the thresholds are only reported.")
        too_many_services = len(entries) > max_services
        queued = deferred = pending = 0
        for entry in entries:
            key = (entry["service"], entry["stage"])
            payload = self._reconcile_payload(entry, mode, context)
            reason = None
            if too_many_services:
                reason = f"{len(entries)} services in one controller change > max_services ({max_services})"
            elif len(entry["hosts"]) > max_hosts:
                reason = f"{len(entry['hosts'])} hosts > max_hosts ({max_hosts})"
            if reason:
                pending += 1
                outcomes[key] = f"pending approval ({reason})"
                if store is not None:
                    await asyncio.to_thread(store.add, payload=payload, status="pending", reason=reason,
                                            inventory_commit=inventory_commit, source_job_id=context.get("job_id"))
                log.warning(f"Service reconcile: {entry['service']} ({entry['stage']}) needs approval — {reason}.")
                continue
            # Record first, dispatch second: a failing dispatch must not lose a detection whose
            # inventory diff is already consumed by the publish commit.
            record = None
            if store is not None:
                record = await asyncio.to_thread(store.add, payload=payload, status="pending", reason="dispatching",
                                                 inventory_commit=inventory_commit, source_job_id=context.get("job_id"))
            status, detail = await self._reconcile_enqueue(engine, payload)
            if status == "queued":
                queued += 1
                outcomes[key] = f"queued job #{detail}" if detail else "queued"
                if record is not None:
                    await asyncio.to_thread(store.remove, record["id"])
                log.info(f"Service reconcile: {entry['service']} ({entry['stage']}) → single_service job "
                         f"{'#' + str(detail) if detail else 'queued'} on {len(entry['hosts'])} host(s){' [dry-run]' if payload['dry_run'] else ''}.")
            elif status == "duplicate":
                deferred += 1
                outcomes[key] = f"deferred (job #{detail} active)" if detail else "deferred (duplicate job active)"
                if record is not None:
                    await asyncio.to_thread(store.update, record["id"], status="deferred",
                                            reason=f"waiting for job #{detail}" if detail else "waiting for the active job",
                                            waiting_for_job=detail)
                log.warning(f"Service reconcile: {entry['service']} ({entry['stage']}) deferred — a job for this service is active"
                            f"{f' (#{detail})' if detail else ''}; re-queued when it ends.")
            else:
                pending += 1
                outcomes[key] = f"pending approval (dispatch failed: {detail})"
                if record is not None:
                    await asyncio.to_thread(store.update, record["id"], reason=f"dispatch failed: {detail}")
                log.error(f"Service reconcile: dispatch of {entry['service']} ({entry['stage']}) failed: {detail} — kept as pending.")
        self._reconcile_notify(engine, mode=mode, entries=entries, excluded=excluded, removed=removed, outcomes=outcomes,
                               inventory_commit=inventory_commit, note="n/a")
        return StageResult(True, f"Service reconcile ({mode}): {queued} queued, {deferred} deferred, {pending} pending approval{excluded_note}.")

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
        if atype == "service_reconcile":
            return await self._run_service_reconcile_action(engine, context, action, changed_files)
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
                check_mode=self._ansible_check_mode(engine, context),
            )
            return await stage.run(engine, context)
        log.warning(f"Unknown rule action type '{atype}', skipping.")
        return StageResult(True, f"Skipped unknown action type '{atype}'.")

    # ------------------------------------------------------------------- run
    async def run(self, engine, context: dict) -> StageResult:
        rules_file_path = engine.base_git_dir / "config_engine" / "pipeline_rules.yml"
        if not rules_file_path.exists():
            log.warning("pipeline_rules.yml not found. Falling back to default hardcoded behavior.")
            check = self._ansible_check_mode(engine, context)
            for stage in engine.get_default_ansible_stages(self.pipeline_type):
                if hasattr(stage, "check_mode"):
                    stage.check_mode = check
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

        self._ref = str(context.get("inventory_state_commit_sha") or "HEAD")
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
