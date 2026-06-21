import hmac
import yaml
import time
import re
import asyncio
from fastapi import APIRouter, Request, Header, HTTPException
from pydantic import BaseModel, Field

from .gitlab_webhooks import sync_group_webhooks_from_ctx, WebhookConfigError

iac_api_router = APIRouter(prefix="/api/iac", tags=["IaC Orchestrator"])

# Internal references to be set by init_api
_ctx = None
_engine = None
_service = None
_recent_events = {}
_EVENT_TTL_SECONDS = 90

def init_api(ctx, service):
    """Initializes the API module with the required context and execution engine."""
    global _ctx, _engine, _service
    _ctx = ctx
    _service = service
    _engine = service.engine


def _notify_ctx(ctx, title: str, body: str, severity: str, *, broadcast: bool = False) -> None:
    """Emit a notification via the Messaging Gateway from module-level code (no self)."""
    from core.api import OutboundMessage, MessageSeverity
    _sev = {
        "positive": MessageSeverity.SUCCESS,
        "negative": MessageSeverity.ERROR,
        "warning":  MessageSeverity.WARNING,
        "info":     MessageSeverity.INFO,
    }
    msg = OutboundMessage(
        title=title,
        body=body,
        severity=_sev.get(severity, MessageSeverity.INFO),
        source_plugin_id="lyndrix.plugin.iac_orchestrator",
        target_provider=None if broadcast else "system",
        metadata={"toast": True, "persist": True},
    )
    ctx.emit("messaging:outbound", msg.model_dump(mode="json"))


def _emit_webhook_verified(event_payload: dict) -> None:
    """Notify the central router that a webhook was accepted and dispatched."""
    if _ctx is None:
        return
    try:
        _ctx.notify(
            "webhook_verified",
            payload={
                "pipeline_type": event_payload.get("pipeline_type"),
                "trigger": event_payload.get("trigger"),
                "manual": event_payload.get("manual", False),
                "service_name": event_payload.get("service_name"),
                "host_name": event_payload.get("host_name"),
            },
            title="IaC webhook verified",
            body=f"Pipeline '{event_payload.get('pipeline_type') or 'unknown'}' triggered.",
            severity="info",
        )
    except Exception as exc:
        _ctx.log.debug(f"NOTIFY: webhook_verified notify failed: {exc}")


def _require_gitlab_token(x_gitlab_token: str | None):
    if not _ctx:
        raise HTTPException(status_code=500, detail="API Context not initialized")
    expected_token = _ctx.get_secret("gitlab_webhook_token")
    if not expected_token:
        _ctx.log.error("SECURITY HALT: Webhook token missing in Vault.")
        raise HTTPException(status_code=500, detail="Configuration Error")
    if not x_gitlab_token or not hmac.compare_digest(x_gitlab_token, expected_token):
        _ctx.log.warning("SECURITY REJECTION: Unauthorized webhook attempt.")
        raise HTTPException(status_code=401, detail="Unauthorized")



def _prune_recent_events(now: float):
    expired = [k for k, ts in _recent_events.items() if now - ts > _EVENT_TTL_SECONDS]
    for key in expired:
        _recent_events.pop(key, None)


def _event_key(payload: dict) -> str:
    object_kind = (payload.get("object_kind") or "unknown").lower()
    project = payload.get("project", {})
    project_path = project.get("path_with_namespace") or project.get("name") or "unknown-project"

    if object_kind == "pipeline":
        attrs = payload.get("object_attributes") or {}
        pipeline_id = attrs.get("id") or attrs.get("iid") or "unknown"
        status = (attrs.get("status") or "unknown").lower()
        return f"pipeline:{project_path}:{pipeline_id}:{status}"

    if object_kind == "push":
        after_sha = payload.get("after") or payload.get("checkout_sha") or "unknown"
        return f"push:{project_path}:{after_sha}"

    if object_kind == "build":
        build_id = payload.get("build_id") or "unknown"
        build_status = (payload.get("build_status") or "unknown").lower()
        return f"build:{project_path}:{build_id}:{build_status}"

    if object_kind == "merge_request":
        attrs = payload.get("object_attributes") or {}
        mr_iid = attrs.get("iid") or attrs.get("id") or "unknown"
        action = (attrs.get("action") or "unknown").lower()
        state = (attrs.get("state") or "unknown").lower()
        return f"merge_request:{project_path}:{mr_iid}:{action}:{state}"

    return f"{object_kind}:{project_path}"


def _should_trigger_orchestrator(payload: dict) -> tuple[bool, str]:
    object_kind = (payload.get("object_kind") or "").lower()

    if object_kind in {"build", "job", "push", "tag_push", "merge_request", "issue"}:
        return False, f"ignored object_kind={object_kind}"

    if object_kind == "pipeline":
        attrs = payload.get("object_attributes") or {}
        status = (attrs.get("status") or "").lower()
        if status != "success":
            return False, f"ignored pipeline status={status or 'unknown'}"

        source = (attrs.get("source") or "").lower()
        if source != "push":
            return False, f"ignored pipeline source={source or 'unknown'}"

        commit_message = (
            (payload.get("commit") or {}).get("message")
            or (payload.get("commit") or {}).get("title")
            or ""
        ).lower()
        if "ci: automated state update" in commit_message:
            return False, "ignored orchestrator auto-commit pipeline"

        return True, "pipeline success"

    return False, f"ignored object_kind={object_kind or 'unknown'}"


def _notification_type_from_pipeline_status(status: str) -> str:
    normalized = (status or "").lower()
    if normalized in {"success", "passed"}:
        return "positive"
    if normalized in {"failed", "error"}:
        return "negative"
    if normalized in {"canceled", "cancelled", "skipped", "manual"}:
        return "warning"
    if normalized in {"running", "pending", "created", "preparing", "waiting_for_resource"}:
        return "ongoing"
    return "info"


def _emit_internal_notification(payload: dict):
    if not _ctx:
        return

    object_kind = (payload.get("object_kind") or "").lower()
    project = payload.get("project", {})
    attrs = payload.get("object_attributes", {})

    if object_kind == "pipeline":
        status = attrs.get("status", "unknown")
        pipeline_id = attrs.get("id") or attrs.get("iid") or "unknown"
        ref = attrs.get("ref", "unknown")
        source = attrs.get("source", "unknown")
        project_name = project.get("path_with_namespace") or project.get("name") or "unknown-project"
        notif_type = _notification_type_from_pipeline_status(status)
        should_emit_outbound = notif_type in {"positive", "negative", "warning"}

        if notif_type == "ongoing":
            _ctx.emit("system:notify", {
                "id": f"gitlab:pipeline:{project_name}:{pipeline_id}",
                "title": f"GitLab Pipeline #{pipeline_id}",
                "message": f"{project_name} | {status.upper()} | ref={ref} | source={source}",
                "type": "ongoing",
                "toast": False,
                "emit_outbound": False,
            })
        else:
            _notify_ctx(
                _ctx,
                f"GitLab Pipeline #{pipeline_id}",
                f"{project_name} | {status.upper()} | ref={ref} | source={source}",
                notif_type,
                broadcast=should_emit_outbound,
            )
        return

    if object_kind == "merge_request":
        action = (attrs.get("action") or "").lower()
        state = (attrs.get("state") or "").lower()
        merged_at = attrs.get("merged_at")

        # Send outbound notifications only when the MR was actually merged.
        if action != "merge" and state != "merged" and not merged_at:
            return

        mr_iid = attrs.get("iid") or attrs.get("id") or "unknown"
        source_branch = attrs.get("source_branch") or "unknown"
        target_branch = attrs.get("target_branch") or "unknown"
        title = attrs.get("title") or "Merge Request"
        url = attrs.get("url") or ""
        project_name = project.get("path_with_namespace") or project.get("name") or "unknown-project"

        message = f"{project_name} | !{mr_iid} merged | {source_branch} -> {target_branch} | {title}"
        if url:
            message = f"{message} | {url}"

        _notify_ctx(_ctx, f"GitLab MR !{mr_iid} Merged", message, "positive", broadcast=True)
        return

    # Ignore non-pipeline webhook kinds on the notification bus to avoid spam.
    return


def _build_orchestrator_event(payload: dict) -> tuple[dict | None, str]:
    """Build a normalized orchestrator event payload from a verified webhook."""
    object_kind = (payload.get("object_kind") or "").lower()
    if object_kind != "pipeline":
        return None, f"unsupported object_kind={object_kind or 'unknown'}"

    attrs = payload.get("object_attributes") or {}
    project = payload.get("project") or {}
    project_path = (
        project.get("path_with_namespace")
        or project.get("path")
        or project.get("name")
        or ""
    ).strip()
    if not project_path:
        return None, "missing project path"

    # The service slug maps to the repo name segment in path_with_namespace.
    service_name = project_path.split("/")[-1].strip().lower()
    if not service_name:
        return None, "missing service name"

    ref = (attrs.get("ref") or project.get("default_branch") or "main").strip()
    if not ref:
        ref = "main"

    pipeline_id = attrs.get("id") or attrs.get("iid")
    return {
        "pipeline_type": "single_service",
        "service_name": service_name,
        "service_branch": ref,
        "manual": False,
        "trigger": "gitlab_pipeline_success",
        "source_pipeline_id": pipeline_id,
        "project_path": project_path,
    }, "single_service pipeline event"


@iac_api_router.get("/health")
async def health_check():
    """Returns a lightweight health snapshot for the orchestrator plugin."""
    if not _ctx or not _engine:
        raise HTTPException(status_code=503, detail="Orchestrator not initialized")

    return {
        "status": "healthy",
        "version": getattr(_ctx.manifest, "version", "unknown"),
        "db_connected": bool(getattr(_engine.db, "engine", None)),
        "vault_ready": _ctx.get_secret("iac_auto_apply") is not None,
        "active_workers": len((_engine.state or {}).get("active_tasks", {})),
    }

@iac_api_router.post("/webhook/gitlab")
async def gitlab_webhook(request: Request, x_gitlab_token: str = Header(None)):
    """
    Endpoint for GitLab webhooks. Validates security tokens and triggers
    the internal event bus for processing.
    """
    if not _ctx:
        raise HTTPException(status_code=500, detail="API Context not initialized")

    # 1. Security Check
    _require_gitlab_token(x_gitlab_token)

    # 2. Payload Processing
    try:
        payload = await request.json()
        project_name = payload.get("project", {}).get("name", "unknown")
        _ctx.log.info(f"WEBHOOK: Verified push for project '{project_name}'.")

        now = time.time()
        _prune_recent_events(now)
        ev_key = _event_key(payload)
        if ev_key in _recent_events:
            _ctx.log.info(f"WEBHOOK: Duplicate delivery ignored ({ev_key}).")
            return {"status": "ignored", "reason": "duplicate", "event_key": ev_key}
        _recent_events[ev_key] = now

        # Bridge into the central notification engine via the internal event bus.
        _emit_internal_notification(payload)

        should_trigger, reason = _should_trigger_orchestrator(payload)
        if not should_trigger:
            _ctx.log.info(f"WEBHOOK: Accepted but did not trigger orchestrator ({reason}).")
            return {"status": "accepted", "triggered": False, "reason": reason}

        event_payload, event_reason = _build_orchestrator_event(payload)
        if not event_payload:
            _ctx.log.info(
                f"WEBHOOK: Accepted but did not trigger orchestrator ({event_reason})."
            )
            return {
                "status": "accepted",
                "triggered": False,
                "reason": event_reason,
            }

        # 3. Emit event to decouple request from execution
        _ctx.emit("iac:webhook_verified", event_payload)
        _emit_webhook_verified(event_payload)

        return {
            "status": "accepted",
            "triggered": True,
            "reason": f"{reason}; {event_reason}",
        }
    except Exception as e:
        _ctx.log.error(f"WEBHOOK ERROR: {str(e)}")
        raise HTTPException(status_code=400, detail="Malformed JSON payload")


@iac_api_router.post("/webhook/sync")
async def sync_webhooks(x_gitlab_token: str = Header(None)):
    """Token-protected, idempotent re-registration of the group webhooks.

    Lets iac-controller CI onboard newly created service repos (registers the
    merge-request hook) without a manual Settings-UI click. Scans the whole
    group, so it does not need to know which repo is new.
    """
    if not _ctx:
        raise HTTPException(status_code=500, detail="API Context not initialized")
    _require_gitlab_token(x_gitlab_token)
    try:
        result = await asyncio.to_thread(sync_group_webhooks_from_ctx, _ctx)
    except WebhookConfigError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        _ctx.log.error(f"WEBHOOK SYNC ERROR: {exc}")
        raise HTTPException(status_code=502, detail=f"Webhook sync failed: {exc}")
    _ctx.log.info(
        f"WEBHOOK SYNC: projects={result['projects_total']} "
        f"created={result['created']} updated={result['updated']} failed={result['failed']}"
    )
    return {"status": "ok", **result}


# --- NEW EXPOSED CONTROL ENDPOINTS ---

@iac_api_router.get("/catalog")
async def get_service_catalog():
    """Returns the parsed global service catalog."""
    if not _engine: raise HTTPException(status_code=500, detail="Engine offline")
    catalog_file = _engine.config.git_repos_dir / "iac_controller" / "environments" / "global" / "02_service_catalog.yml"
    if catalog_file.exists():
        with open(catalog_file, 'r') as f:
            data = yaml.safe_load(f) or {}
            return data.get("service_catalog", {}).get("services", [])
    return []

class DeployRequest(BaseModel):
    branch: str = "main"


class TestHostDeployRequest(BaseModel):
    services: list[str] = Field(default_factory=list)


_HOST_LIMIT_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")


def _load_generated_inventory_hosts() -> set[str]:
    if not _engine:
        return set()
    inventory_path = _engine.config.git_repos_dir / "inventory_state" / "global" / "ansible" / "inventory.yml"
    if not inventory_path.exists():
        return set()
    try:
        with open(inventory_path, "r", encoding="utf-8") as handle:
            inv = yaml.safe_load(handle) or {}
    except Exception:
        return set()
    return set(((inv.get("all") or {}).get("hosts") or {}).keys())

@iac_api_router.post("/deploy/service/{service_name}")
async def trigger_service_deployment(service_name: str, payload: DeployRequest):
    """Triggers a targeted single-service deployment."""
    if not _ctx:
        raise HTTPException(status_code=500, detail="Context offline")
    normalized_service = str(service_name or "").strip().lower()
    branch = str(payload.branch or "main").strip() or "main"
    event_payload = {
        "pipeline_type": "single_service",
        "service_name": normalized_service,
        "service_branch": branch,
        "manual": True,
    }
    _ctx.emit("iac:webhook_verified", event_payload)
    _emit_webhook_verified(event_payload)
    return {"status": "accepted", "message": f"Deployment queued for {normalized_service}"}


@iac_api_router.post("/deploy/service/{service_name}/gitlab")
async def trigger_service_deployment_gitlab(
    service_name: str,
    payload: DeployRequest,
    x_gitlab_token: str = Header(None),
):
    """GitLab-token-protected single-service trigger endpoint for CI pipelines."""
    _require_gitlab_token(x_gitlab_token)
    return await trigger_service_deployment(service_name, payload)


@iac_api_router.get("/deploy/service/{service_name}/status")
async def get_service_deployment_status(service_name: str, since_epoch: int = 0):
    """Returns the latest deployment status for a service, optionally after an epoch timestamp."""
    if not _engine:
        raise HTTPException(status_code=500, detail="Engine offline")
    normalized_service = str(service_name or "").strip().lower()
    pipeline_type = f"single_service:{normalized_service}"
    job = _engine.db.get_latest_job_for_pipeline_type(pipeline_type, since_epoch=since_epoch)
    if not job:
        return {
            "status": "pending",
            "service_name": normalized_service,
            "pipeline_type": pipeline_type,
            "found": False,
            "since_epoch": since_epoch,
        }

    job_status = str(job.get("status") or "").upper()
    if job_status == "SUCCESS":
        status = "success"
    elif job_status in {"FAILED", "ERROR", "ABORTED"}:
        status = "failed"
    elif job_status in {"RUNNING", "PENDING"}:
        status = "running"
    else:
        status = "unknown"

    return {
        "status": status,
        "found": True,
        "service_name": normalized_service,
        "pipeline_type": pipeline_type,
        "job_id": job.get("id"),
        "job_status": job_status,
        "progress": job.get("progress") or 0,
        "current_step": job.get("current_step") or "",
    }


@iac_api_router.get("/deploy/service/{service_name}/gitlab/status")
async def get_service_deployment_status_gitlab(
    service_name: str,
    since_epoch: int = 0,
    x_gitlab_token: str = Header(None),
):
    """GitLab-token-protected deployment status endpoint for CI wait loops."""
    _require_gitlab_token(x_gitlab_token)
    return await get_service_deployment_status(service_name, since_epoch=since_epoch)


@iac_api_router.post("/deploy/test-host/{host_name}")
async def trigger_test_host_deployment(host_name: str, payload: TestHostDeployRequest):
    """
    Triggers a guarded host_provision run (Terraform + Ansible bootstrap +
    host rollout hand-off) for exactly one host.

    Safety constraints:
    - host must be an exact hostname token (no Ansible patterns/wildcards),
    - host must be listed in `PLUGIN_IAC_ORCHESTRATOR_TEST_DEPLOY_ALLOWED_HOSTS`
      (or Vault key `iac_test_deploy_allowed_hosts`),
    - host must exist in the generated inventory.
    """
    if not _ctx or not _engine:
        raise HTTPException(status_code=500, detail="Orchestrator offline")

    host = str(host_name or "").strip()
    if not host or not _HOST_LIMIT_PATTERN.fullmatch(host):
        raise HTTPException(
            status_code=400,
            detail="Invalid host_name: only exact host tokens [A-Za-z0-9._-] are allowed.",
        )

    allowed_hosts = _engine.config.test_deploy_allowed_hosts
    if not allowed_hosts:
        raise HTTPException(
            status_code=403,
            detail=(
                "Test-host deploy is disabled. Configure "
                "PLUGIN_IAC_ORCHESTRATOR_TEST_DEPLOY_ALLOWED_HOSTS or "
                "Vault key iac_test_deploy_allowed_hosts."
            ),
        )
    if host not in allowed_hosts:
        raise HTTPException(
            status_code=403,
            detail=f"Host '{host}' is not in test-deploy allowlist.",
        )

    known_hosts = _load_generated_inventory_hosts()
    if host not in known_hosts:
        raise HTTPException(
            status_code=404,
            detail=f"Host '{host}' not found in generated inventory_state.",
        )

    event_payload = {
        "pipeline_type": "host_provision",
        "host_name": host,
        "manual": True,
        "trigger": "manual_test_host",
        "test_host": host,
    }

    _ctx.emit("iac:webhook_verified", event_payload)
    _emit_webhook_verified(event_payload)
    return {
        "status": "accepted",
        "message": f"Host provisioning queued for host '{host}'.",
        "host_name": host,
        "services": [str(s).strip() for s in (payload.services or []) if str(s).strip()],
    }


@iac_api_router.post("/webhook/gitlab/test-host/{host_name}")
async def gitlab_test_host_webhook(
    host_name: str,
    payload: TestHostDeployRequest,
    x_gitlab_token: str = Header(None),
):
    """
    GitLab-token-protected webhook variant for test-host rollout triggering.
    Useful when CI should only call a Lyndrix webhook endpoint.
    """
    if not _ctx:
        raise HTTPException(status_code=500, detail="API Context not initialized")

    _require_gitlab_token(x_gitlab_token)

    return await trigger_test_host_deployment(host_name, payload)


@iac_api_router.post("/bootstrap/{host_name}")
async def trigger_host_bootstrap(host_name: str, payload: TestHostDeployRequest):
    """
    Triggers a guarded compliance/bootstrap run (cd_compliance.yml) for exactly
    one host, connecting as root with the Terraform-injected key
    (Vault: iac_tf_ssh_private_key).

    Safety constraints mirror the test-host deploy:
    - host must be an exact hostname token (no Ansible patterns/wildcards),
    - host must be listed in the test-deploy allowlist,
    - host must exist in the generated inventory.
    """
    if not _ctx or not _engine:
        raise HTTPException(status_code=500, detail="Orchestrator offline")

    host = str(host_name or "").strip()
    if not host or not _HOST_LIMIT_PATTERN.fullmatch(host):
        raise HTTPException(
            status_code=400,
            detail="Invalid host_name: only exact host tokens [A-Za-z0-9._-] are allowed.",
        )

    allowed_hosts = _engine.config.test_deploy_allowed_hosts
    if not allowed_hosts:
        raise HTTPException(
            status_code=403,
            detail=(
                "Host bootstrap is disabled. Configure "
                "PLUGIN_IAC_ORCHESTRATOR_TEST_DEPLOY_ALLOWED_HOSTS or "
                "Vault key iac_test_deploy_allowed_hosts."
            ),
        )
    if host not in allowed_hosts:
        raise HTTPException(
            status_code=403,
            detail=f"Host '{host}' is not in the allowlist.",
        )

    known_hosts = _load_generated_inventory_hosts()
    if host not in known_hosts:
        raise HTTPException(
            status_code=404,
            detail=f"Host '{host}' not found in generated inventory_state.",
        )

    event_payload = {
        "pipeline_type": "bootstrap_compliance",
        "host_name": host,
        "manual": True,
        "trigger": "manual_bootstrap",
    }

    _ctx.emit("iac:webhook_verified", event_payload)
    _emit_webhook_verified(event_payload)
    return {
        "status": "accepted",
        "message": f"Compliance bootstrap queued for host '{host}'.",
        "host_name": host,
    }

@iac_api_router.post("/infra/plan")
async def trigger_infra_plan():
    """Triggers a read-only whole-infrastructure Terraform plan ("Check Env").

    Renders and runs ``tofu plan`` across every non-empty environment without
    applying anything, so the operator can compare the live infrastructure
    against the desired Terraform plan.
    """
    if not _ctx:
        raise HTTPException(status_code=500, detail="Context offline")
    infra_plan_payload = {"pipeline_type": "infra_plan", "manual": True, "trigger": "manual_infra_plan"}
    _ctx.emit("iac:webhook_verified", infra_plan_payload)
    _emit_webhook_verified(infra_plan_payload)
    return {"status": "accepted", "message": "Infrastructure plan (Check Env) queued."}


@iac_api_router.post("/infra/apply")
async def trigger_infra_apply():
    """Triggers a whole-infrastructure Terraform apply ("Deploy Infra").

    Runs ``tofu apply`` across every non-empty environment. The explicit
    ``approve`` flag forces the apply regardless of the ``auto_apply`` config so
    this manual, operator-initiated action always applies — while the automatic
    webhook path stays gated by ``auto_apply``.
    """
    if not _ctx:
        raise HTTPException(status_code=500, detail="Context offline")
    infra_apply_payload = {
        "pipeline_type": "infra_apply",
        "approve": True,
        "manual": True,
        "trigger": "manual_infra_apply",
    }
    _ctx.emit("iac:webhook_verified", infra_apply_payload)
    _emit_webhook_verified(infra_apply_payload)
    return {"status": "accepted", "message": "Infrastructure deploy queued."}


@iac_api_router.get("/jobs")
async def list_orchestrator_jobs(limit: int = 20):
    """Returns a list of recent and active jobs."""
    if not _engine: raise HTTPException(status_code=500, detail="Engine offline")
    return _engine.db.get_recent_jobs(limit)

    
    
