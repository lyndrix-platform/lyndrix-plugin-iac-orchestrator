import asyncio
import yaml
from core.logger import get_logger

from .base import BaseStage
from ..utils import StageResult

log = get_logger("IaC:Engine:Ansible")

class AnsiblePlaybookStage(BaseStage):
    def __init__(self, playbook_path: str, inventory_path: str, limit: str = None, name_override: str = None, extra_vars: dict = None, ssh_key_secret: str = "ansible_ssh_key", remote_user: str = "ansible-agent"):
        self.display_name = name_override or f"Ansible: {playbook_path}"
        super().__init__(self.display_name)
        self.playbook_path = playbook_path
        self.inventory_path = inventory_path
        self.limit = limit
        self.extra_vars = extra_vars or {}
        self.ssh_key_secret = ssh_key_secret
        self.remote_user = remote_user

    async def run(self, engine, context: dict) -> StageResult:
        success, stats = await engine.execute_ansible_docker(
            playbook_subpath=self.playbook_path, 
            inventory_subpath=self.inventory_path, 
            limit=self.limit,
            extra_vars=self.extra_vars,
            task_name=self.display_name,
            job_id=context.get("job_id", 0),
            ssh_key_secret=self.ssh_key_secret,
            remote_user=self.remote_user,
        )
        msg = "Ansible execution completed." if success else "Ansible execution failed."
        return StageResult(success, msg, data=stats)

class AsyncBulkRolloutStage(BaseStage):
    def __init__(self, inventory_path: str, limit: str = "all", target_services: list[str] = None):
        super().__init__(f"Async Bulk Rollout (Limit: {limit})")
        self.inventory_path = inventory_path
        self.limit = limit
        self.target_services = target_services

    async def run(self, engine, context: dict) -> StageResult:
        job_id = context.get("job_id", 0)
        if self.target_services is not None:
            svc_names = self.target_services
        elif context.get("services_to_deploy") is not None:
            # Triggered dynamically by DetectDriftStage
            svc_names = context.get("services_to_deploy")
        else:
            catalog_file = engine.base_git_dir / "iac_controller" / "environments" / "global" / "02_service_catalog.yml"
            if not catalog_file.exists(): return StageResult(False, "Source service_catalog.yml missing.")
            try:
                with open(catalog_file, 'r') as f:
                    catalog_data = yaml.safe_load(f) or {}
                    services = catalog_data.get("service_catalog", {}).get("services", [])
                    svc_names = [svc.get("name") for svc in services if svc.get("name")]
            except Exception as e: return StageResult(False, f"Raw catalog parse failed: {e}")
            
        if not svc_names: 
            log.info("No active services queued for deployment in this rollout.")
            return StageResult(True, "Skipped: No services to deploy.")
            
        log.info(f"Initiating Async Rollout for {len(svc_names)} services (Limit: {self.limit})...")
        pending_queue = list(svc_names)
        total_services = len(pending_queue)
        try:
            if hasattr(engine.db, 'update_pending_tasks'): engine.db.update_pending_tasks(job_id, pending_queue)
        except Exception: pass
        sem = asyncio.Semaphore(5)
        report = {}
        failed_services = []
        async def bounded_deploy(svc_name):
            async with sem:
                sanitized_name = str(svc_name).replace("-", "_")
                svc_group = f"service_{sanitized_name}"
                eff_limit = f"{self.limit}:&{svc_group}" if self.limit and self.limit != "all" else svc_group
                stage = AnsiblePlaybookStage(name_override=svc_name, playbook_path="playbooks/cd_playbooks/cd_rollout_single_service.yml", inventory_path=self.inventory_path, limit=eff_limit, extra_vars={"target_service": svc_name, "target_group": eff_limit, "LOCAL_SERVICES_DIR": str(engine.config.services_dir)})
                res = await stage.run(engine, context)
                report[svc_name] = {"success": res.success, "successful_hosts": res.data.get("successful_hosts", 0), "failed_hosts": res.data.get("failed_hosts", 0)}
                if not res.success: failed_services.append(svc_name)
                if svc_name in pending_queue:
                    pending_queue.remove(svc_name)
                    try:
                        if hasattr(engine.db, 'update_pending_tasks'):
                            engine.db.update_pending_tasks(job_id, pending_queue)
                            completed = total_services - len(pending_queue)
                            engine.db.update_progress(job_id, progress=None, current_step=f"Bulk Deploying ({completed}/{total_services})")
                    except Exception: pass
        await asyncio.gather(*[bounded_deploy(s) for s in svc_names])
        summary = ["\n" + "="*60, "🚀 BULK ROLLOUT DEPLOYMENT SUMMARY", "="*60]
        tot_succ, tot_fail = 0, 0
        for svc, stats in sorted(report.items()):
            status_icon = "✅" if stats['success'] else "❌"
            sh, fh = stats['successful_hosts'], stats['failed_hosts']
            host_str = f"{sh} hosts" + (f" | FAILED on {fh} hosts" if fh > 0 else "")
            summary.append(f"{status_icon} {svc.ljust(35)} : {host_str}")
            tot_succ += sh; tot_fail += fh
        summary.extend(["="*60, f"📊 TOTALS: {len(svc_names)} Services | {tot_succ} Successes | {tot_fail} Failures", "="*60 + "\n"])
        for line in summary: log.info(line)
        if failed_services: return StageResult(False, f"Bulk rollout had failures. See summary.", data=report)
        return StageResult(True, "Async bulk rollout completed successfully.", data=report)
class ServiceDeployStage(BaseStage):
    """Deploy all services to a SINGLE HOST (scoped rollout for host_provision pipeline)."""
    def __init__(self, host_name: str):
        super().__init__(f"Service Deployment: {host_name}")
        self.host_name = str(host_name or "").strip()

    def _get_host_services(self, engine, host_name: str) -> list[str]:
        """Return the service names assigned to host_name via service_* inventory groups."""
        inv_path = engine.base_git_dir / "inventory_state" / "global" / "ansible" / "inventory.yml"
        try:
            with open(inv_path, 'r') as f:
                inv = yaml.safe_load(f) or {}
        except Exception as e:
            log.warning(f"Could not read inventory for host service lookup: {e}")
            return []
        assigned = []
        for grp, val in (inv.get("all", {}).get("children", {}) or {}).items():
            if not grp.startswith("service_"):
                continue
            hosts = list((val or {}).get("hosts", {}).keys()) if val else []
            if host_name in hosts:
                # service_aac_docker_proxy → aac-docker-proxy
                assigned.append(grp.removeprefix("service_").replace("_", "-"))
        return assigned

    async def run(self, engine, context: dict) -> StageResult:
        if not self.host_name:
            return StageResult(False, "Host name is required for service deployment.")
        
        job_id = context.get("job_id", 0)

        # Only deploy services this host is actually assigned to (via inventory service_* groups)
        assigned_svcs = self._get_host_services(engine, self.host_name)
        if not assigned_svcs:
            log.info(f"No services assigned to host '{self.host_name}' in inventory.")
            return StageResult(True, "Skipped: No services assigned to this host.")
        
        # Intersect with catalog to keep canonical service names
        catalog_file = engine.base_git_dir / "iac_controller" / "environments" / "global" / "02_service_catalog.yml"
        catalog_names: set[str] = set()
        if catalog_file.exists():
            try:
                with open(catalog_file, 'r') as f:
                    catalog_data = yaml.safe_load(f) or {}
                    catalog_names = {svc.get("name") for svc in catalog_data.get("service_catalog", {}).get("services", []) if svc.get("name")}
            except Exception as e:
                log.warning(f"Could not parse service catalog: {e}")
        
        svc_names = [s for s in assigned_svcs if s in catalog_names] if catalog_names else assigned_svcs
        if not svc_names:
            log.info("No active services to deploy.")
            return StageResult(True, "Skipped: No services configured.")
        
        log.info(f"Deploying {len(svc_names)} services to host '{self.host_name}'...")
        
        sem = asyncio.Semaphore(3)
        report = {}
        failed_services = []
        
        async def bounded_deploy(svc_name):
            async with sem:
                sanitized_name = str(svc_name).replace("-", "_")
                svc_group = f"service_{sanitized_name}"
                eff_limit = f"{self.host_name}:&{svc_group}"
                stage = AnsiblePlaybookStage(
                    name_override=svc_name, 
                    playbook_path="playbooks/cd_playbooks/cd_rollout_single_service.yml", 
                    inventory_path="global/ansible/inventory.yml", 
                    limit=eff_limit, 
                    extra_vars={"target_service": svc_name, "target_group": eff_limit, "LOCAL_SERVICES_DIR": str(engine.config.services_dir)}
                )
                res = await stage.run(engine, context)
                report[svc_name] = {"success": res.success, "successful_hosts": res.data.get("successful_hosts", 0), "failed_hosts": res.data.get("failed_hosts", 0)}
                if not res.success:
                    failed_services.append(svc_name)
        
        await asyncio.gather(*[bounded_deploy(s) for s in svc_names])
        
        summary = ["\n" + "="*60, f"🚀 SERVICE DEPLOYMENT FOR {self.host_name}", "="*60]
        tot_deployed, tot_skipped, tot_failed = 0, 0, 0
        for svc, stats in sorted(report.items()):
            sh = stats['successful_hosts']
            if stats['success'] and sh > 0:
                status_icon, label = "✅", "deployed"
                tot_deployed += sh
            elif stats['success'] and sh == 0:
                status_icon, label = "⏭️ ", "skipped (not applicable)"
                tot_skipped += 1
            else:
                status_icon, label = "⚠️ ", "failed"
                tot_failed += 1
            summary.append(f"{status_icon} {svc.ljust(35)} : {label}")
        summary.extend(["="*60, f"📊 TOTALS: {len(svc_names)} Services | {tot_deployed} Deployed | {tot_skipped} Skipped | {tot_failed} Failed", "="*60 + "\n"])
        for line in summary: log.info(line)
        
        # Service deployment is best-effort: hosts that aren't in a service group simply skip it.
        # Only actual ansible failures (exit != 0) are counted as failed, but they don't abort the job.
        if failed_services:
            log.warning(f"⚠️  Service deployment on '{self.host_name}': {tot_deployed} deployed, {tot_skipped} skipped, {tot_failed} failed (non-fatal)")
        return StageResult(True, f"Service deployment completed: {tot_deployed} deployed, {tot_skipped} skipped, {tot_failed} failed", data=report)
