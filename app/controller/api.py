import hmac
import yaml
import time
import re
from fastapi import APIRouter, Request, Header, HTTPException
from pydantic import BaseModel, Field

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

        _ctx.emit("system:notify", {
            "id": f"gitlab:pipeline:{project_name}:{pipeline_id}",
            "title": f"GitLab Pipeline #{pipeline_id}",
            "message": f"{project_name} | {status.upper()} | ref={ref} | source={source}",
            "type": notif_type,
            "toast": notif_type != "ongoing",
            "emit_outbound": should_emit_outbound,
        })
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

        _ctx.emit("system:notify", {
            "id": f"gitlab:mr:{project_name}:{mr_iid}",
            "title": f"GitLab MR !{mr_iid} Merged",
            "message": message,
            "type": "positive",
            "toast": True,
            "emit_outbound": True,
        })
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
    expected_token = _ctx.get_secret("gitlab_webhook_token")
    if not expected_token:
        _ctx.log.error("SECURITY HALT: Webhook token missing in Vault.")
        raise HTTPException(status_code=500, detail="Configuration Error")

    if not x_gitlab_token or not hmac.compare_digest(x_gitlab_token, expected_token):
        _ctx.log.warning("SECURITY REJECTION: Unauthorized webhook attempt.")
        raise HTTPException(status_code=401, detail="Unauthorized")

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

        return {
            "status": "accepted",
            "triggered": True,
            "reason": f"{reason}; {event_reason}",
        }
    except Exception as e:
        _ctx.log.error(f"WEBHOOK ERROR: {str(e)}")
        raise HTTPException(status_code=400, detail="Malformed JSON payload")


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
    if not _ctx: raise HTTPException(status_code=500, detail="Context offline")
    event_payload = {"pipeline_type": "single_service", "service_name": service_name, "service_branch": payload.branch, "manual": True}
    _ctx.emit("iac:webhook_verified", event_payload)
    return {"status": "accepted", "message": f"Deployment queued for {service_name}"}


@iac_api_router.post("/deploy/test-host/{host_name}")
async def trigger_test_host_deployment(host_name: str, payload: TestHostDeployRequest):
    """
    Triggers a guarded terraform_provision run for exactly one host.

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
        "pipeline_type": "terraform_provision",
        "host_name": host,
        "manual": True,
        "trigger": "manual_test_host",
        "test_host": host,
    }

    _ctx.emit("iac:webhook_verified", event_payload)
    return {
        "status": "accepted",
        "message": f"Terraform test deployment queued for host '{host}'.",
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

    expected_token = _ctx.get_secret("gitlab_webhook_token")
    if not expected_token:
        _ctx.log.error("SECURITY HALT: Webhook token missing in Vault.")
        raise HTTPException(status_code=500, detail="Configuration Error")
    if not x_gitlab_token or not hmac.compare_digest(x_gitlab_token, expected_token):
        _ctx.log.warning("SECURITY REJECTION: Unauthorized webhook test-host trigger attempt.")
        raise HTTPException(status_code=401, detail="Unauthorized")

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
    return {
        "status": "accepted",
        "message": f"Compliance bootstrap queued for host '{host}'.",
        "host_name": host,
    }

@iac_api_router.get("/jobs")
async def list_orchestrator_jobs(limit: int = 20):
    """Returns a list of recent and active jobs."""
    if not _engine: raise HTTPException(status_code=500, detail="Engine offline")
    return _engine.db.get_recent_jobs(limit)

    
    
