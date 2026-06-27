"""Shared IaC Orchestrator API logic.

This module holds the plugin's API *state* (the ctx/engine/service handles set
via :func:`init_api`), all helper functions, request models, and the concrete
handler logic (the ``do_*`` coroutines). The actual routers are assembled
elsewhere:

  * ``app/controller/webhook_router.py`` — the PUBLIC ``/api/iac`` router, mounted
    DIRECTLY on the FastAPI app (never via the plugin registry, which would force
    header auth and break external GitLab callers). Its handlers validate the
    ``x_gitlab_token`` themselves.
  * ``app/api.py`` — the AUTH'D plugin router (mounted via ``ctx.register_routes``;
    the registry already wraps every route with ``require_api_auth``) plus the SSE
    stream router (mounted directly so ``EventSource`` can authenticate via a
    ``?token=`` query parameter instead of a Bearer header).

Keeping the logic here means both routers share one implementation and one set of
helpers — there is exactly one place that knows how to verify a webhook token,
build an orchestrator event, or read a job log file.
"""
import hmac
import json
import yaml
import time
import re
import secrets
import asyncio
from pathlib import Path

from fastapi import Request, Header, HTTPException
from pydantic import BaseModel, Field

from .settings_schema import OrchestratorSettings

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


def _resolve_stream_identity(token: str | None):
    """Resolve a core ``ApiIdentity`` from a bearer ``token`` query parameter.

    EventSource and browser tab/download requests cannot send an Authorization
    header, so the live job stream and raw-log download take the bearer token
    (the ``lyk_...`` value the React app stores as ``lyndrix_token``) via a query
    parameter. We feed it into core's standard ``authenticate_request`` through a
    synthetic request so the system key, the per-user key store and per-key scopes
    are all honoured exactly as for normal header auth — without reaching into any
    private ``core.components.*`` internals.
    """
    if not token:
        return None
    try:
        from starlette.requests import Request as _StarletteRequest
        from core.api import authenticate_request
        scope = {
            "type": "http",
            "headers": [(b"authorization", f"Bearer {token.strip()}".encode())],
        }
        return authenticate_request(_StarletteRequest(scope))
    except Exception as exc:
        if _ctx:
            _ctx.log.debug(f"STREAM: token verification failed: {exc}")
        return None


def _validate_stream_token(token: str | None, permission: str = "api:read") -> bool:
    """Validate a stream/raw-log ``?token=`` and enforce a permission scope.

    Mirrors the authorization the rest of the API enforces via
    ``require_permission`` — a syntactically valid key is not enough; the resolved
    identity must also be *granted* ``permission`` (``api:read`` by default). This
    closes the gap where any low-privilege key could stream live state or download
    raw logs that the equivalent ``require_permission("api:read")`` REST route
    would deny.
    """
    identity = _resolve_stream_identity(token)
    return identity is not None and identity.allows(permission)


# ── Short-lived stream tickets ───────────────────────────────────────────────
# Process-lifetime signing key for stream tickets. Tickets are only ever valid
# within this process, which is fine given their short TTL: a restart simply
# invalidates outstanding tickets and the client transparently mints a new one.
_STREAM_TICKET_KEY = secrets.token_bytes(32)
_STREAM_TICKET_TTL = 60  # seconds


def issue_stream_ticket(permission: str = "api:read") -> str:
    """Mint a short-lived, signed ticket authorizing a stream/raw-log read.

    Minted only after the caller has already passed ``require_permission`` on the
    authed endpoint, so the granted scope is baked into the ticket. EventSource
    and browser tab/download requests then present this opaque ticket as
    ``?ticket=`` instead of the long-lived bearer token, keeping the real
    credential out of URLs, reverse-proxy access logs and browser history.
    """
    import base64
    exp = int(time.time()) + _STREAM_TICKET_TTL
    body = f"{exp}:{permission}"
    sig = hmac.new(_STREAM_TICKET_KEY, body.encode(), "sha256").hexdigest()
    return base64.urlsafe_b64encode(f"{body}:{sig}".encode()).decode()


def _validate_stream_ticket(ticket: str | None, permission: str = "api:read") -> bool:
    """Constant-time verify a stream ticket and check it is unexpired and in-scope."""
    if not ticket:
        return False
    try:
        import base64
        raw = base64.urlsafe_b64decode(ticket.encode()).decode()
        exp_s, perm, sig = raw.rsplit(":", 2)
        expected = hmac.new(_STREAM_TICKET_KEY, f"{exp_s}:{perm}".encode(), "sha256").hexdigest()
        if not hmac.compare_digest(sig, expected):
            return False
        if int(exp_s) < int(time.time()):
            return False
        return perm == permission
    except Exception:
        return False


def _authorize_stream_request(token: str | None, ticket: str | None,
                              permission: str = "api:read") -> bool:
    """Accept either a short-lived stream ticket (preferred) or a bearer token.

    The React client now mints a ticket via ``POST /stream/ticket`` and passes it
    as ``?ticket=``; the bearer-token path is retained for non-browser/API callers
    that cannot pre-mint a ticket. Both paths enforce ``permission``.
    """
    if _validate_stream_ticket(ticket, permission):
        return True
    return _validate_stream_token(token, permission)


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


# ── Request models ──────────────────────────────────────────────────────────

class DeployRequest(BaseModel):
    branch: str = "main"


class TestHostDeployRequest(BaseModel):
    services: list[str] = Field(default_factory=list)


# Generic operation trigger. Mirrors the NiceGUI Assignments-tab buttons, which
# all run ``ctx.emit("iac:webhook_verified", {pipeline_type, limit|host_name, ...})``.
_PIPELINE_TYPES = {"bootstrap_compliance", "adopt_host", "rollout", "init_host", "compliance"}


class PipelineRequest(BaseModel):
    pipeline_type: str
    limit: str | None = None       # "all" | "<site>" | "<host>"
    host_name: str | None = None   # exact host token for host-scoped actions


class SettingsRequest(BaseModel):
    auto_apply: bool | None = None
    test_deploy_allowed_hosts: str | None = None
    gitlab_url: str | None = None
    group_id: str | None = None
    lyndrix_base_url: str | None = None
    gitlab_token_key: str | None = None
    autosync_enabled: bool | None = None
    sync_interval: int | None = None


def _safe_int(value, default: int) -> int:
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return default


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


# ── Handler logic (router-agnostic) ─────────────────────────────────────────

async def do_health():
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


async def do_gitlab_webhook(request: Request, x_gitlab_token: str | None):
    """Verify a GitLab webhook and bridge it into the internal event bus."""
    if not _ctx:
        raise HTTPException(status_code=500, detail="API Context not initialized")

    _require_gitlab_token(x_gitlab_token)

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
            return {"status": "accepted", "triggered": False, "reason": event_reason}

        _ctx.emit("iac:webhook_verified", event_payload)
        _emit_webhook_verified(event_payload)

        return {"status": "accepted", "triggered": True, "reason": f"{reason}; {event_reason}"}
    except HTTPException:
        raise
    except Exception as e:
        _ctx.log.error(f"WEBHOOK ERROR: {str(e)}")
        raise HTTPException(status_code=400, detail="Malformed JSON payload")


async def do_sync_webhooks(x_gitlab_token: str | None):
    """Token-protected, idempotent re-registration of the group webhooks."""
    if not _ctx:
        raise HTTPException(status_code=500, detail="API Context not initialized")
    _require_gitlab_token(x_gitlab_token)
    from .gitlab_webhooks import sync_group_webhooks_from_ctx, WebhookConfigError
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


async def do_get_catalog():
    """Returns the parsed global service catalog."""
    if not _engine:
        raise HTTPException(status_code=500, detail="Engine offline")
    catalog_file = _engine.config.git_repos_dir / "iac_controller" / "environments" / "global" / "02_service_catalog.yml"
    if catalog_file.exists():
        with open(catalog_file, 'r') as f:
            data = yaml.safe_load(f) or {}
            return data.get("service_catalog", {}).get("services", [])
    return []


async def do_trigger_service_deployment(service_name: str, payload: DeployRequest):
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


async def do_get_service_status(service_name: str, since_epoch: int = 0):
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


async def do_trigger_test_host_deployment(host_name: str, payload: TestHostDeployRequest):
    """Triggers a guarded host_provision run for exactly one allow-listed host."""
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
        raise HTTPException(status_code=403, detail=f"Host '{host}' is not in test-deploy allowlist.")

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


async def do_trigger_host_bootstrap(host_name: str, payload: TestHostDeployRequest):
    """Triggers a guarded compliance/bootstrap run for exactly one allow-listed host."""
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
        raise HTTPException(status_code=403, detail=f"Host '{host}' is not in the allowlist.")

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


async def do_trigger_infra_plan():
    """Triggers a read-only whole-infrastructure Terraform plan ("Check Env")."""
    if not _ctx:
        raise HTTPException(status_code=500, detail="Context offline")
    infra_plan_payload = {"pipeline_type": "infra_plan", "manual": True, "trigger": "manual_infra_plan"}
    _ctx.emit("iac:webhook_verified", infra_plan_payload)
    _emit_webhook_verified(infra_plan_payload)
    return {"status": "accepted", "message": "Infrastructure plan (Check Env) queued."}


async def do_trigger_infra_apply():
    """Triggers a whole-infrastructure Terraform apply ("Deploy Infra")."""
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


async def do_list_jobs(limit: int = 20):
    """Returns a list of recent and active jobs."""
    if not _engine:
        raise HTTPException(status_code=500, detail="Engine offline")
    return _engine.db.get_recent_jobs(limit)


# Default amount of trailing log to seed an offset=0 tail request with, so the UI
# gets recent context without ever reading a multi-hundred-MB file into memory.
_LOG_TAIL_BYTES = 256 * 1024
# Hard cap on grep matches returned in one response (grep streams the file line by
# line, so memory stays flat regardless of file size; this just bounds the payload).
_LOG_GREP_MAX = 5000


async def do_job_logs(
    job_id: int,
    tail: int = 200,
    grep: str | None = None,
    offset: int = 0,
):
    """Incrementally tail a job's on-disk log file (memory-bounded).

    The log file is append-only, so callers track a byte ``offset`` and fetch only
    the new bytes since their last poll — O(new data) instead of re-reading the
    whole file. Behaviour:

    * ``offset > 0`` (and within the file): return only the bytes after ``offset``.
    * ``offset > size`` (file rotated / new job / truncated): reset and tail.
    * ``offset == 0``: seed with the last ``_LOG_TAIL_BYTES`` (drop the partial
      first line) so the viewer opens with recent context.
    * ``grep``: stream the file line by line (never loaded whole) and return up to
      ``_LOG_GREP_MAX`` matching lines from the end.

    Always returns the new ``offset`` (== current file size) and ``size`` so the
    caller can resume. The full, unbounded log is served separately by
    :func:`do_job_log_raw`.
    """
    if not _engine:
        raise HTTPException(status_code=500, detail="Engine offline")

    log_path = _engine.config.get_log_path(job_id)
    if not log_path or not Path(log_path).exists():
        # Fall back to any legacy logs stored in the DB row.
        return {
            "job_id": job_id,
            "lines": _engine.db.get_job_logs(job_id),
            "source": "db",
            "offset": 0,
            "size": 0,
            "tail": tail,
            "grep": grep,
        }

    try:
        size = log_path.stat().st_size

        # grep: stream-scan the whole file line by line, keep only the last N matches.
        if grep:
            term = grep.lower()
            from collections import deque
            matches: deque = deque(maxlen=_LOG_GREP_MAX)
            with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                for ln in f:
                    if term in ln.lower():
                        matches.append(ln.rstrip("\n"))
            return {
                "job_id": job_id,
                "lines": list(matches),
                "source": "file",
                "offset": size,
                "size": size,
                "tail": tail,
                "grep": grep,
            }

        # Incremental / tail read (no whole-file load).
        start = 0
        drop_partial = False
        if offset and 0 < offset <= size:
            start = offset
        elif offset > size:
            start = max(0, size - _LOG_TAIL_BYTES)
            drop_partial = start > 0
        else:  # offset == 0
            start = max(0, size - _LOG_TAIL_BYTES)
            drop_partial = start > 0

        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            f.seek(start)
            chunk = f.read()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to read log: {exc}")

    lines = chunk.split("\n")
    # Drop a trailing empty element from a chunk that ended on a newline.
    if lines and lines[-1] == "":
        lines.pop()
    # When we seeked into the middle of the file, the first line is partial.
    if drop_partial and lines:
        lines = lines[1:]

    return {
        "job_id": job_id,
        "lines": lines,
        "source": "file",
        "offset": size,
        "size": size,
        "tail": tail,
        "grep": grep,
    }


async def do_job_log_raw(job_id: int, download: bool = False):
    """Stream the ENTIRE job log file to the browser without loading it into memory.

    Backs the "Open full log" / "Download" actions. ``FileResponse`` streams the
    file in chunks, so a 100 MB+ log costs ~constant server memory. Served from the
    stream router with ``?token=`` auth because the browser opens this URL directly
    (a new tab / download) and cannot send a Bearer header.
    """
    from fastapi.responses import FileResponse

    if not _engine:
        raise HTTPException(status_code=500, detail="Engine offline")

    log_path = _engine.config.get_log_path(job_id)
    if not log_path or not Path(log_path).exists():
        raise HTTPException(status_code=404, detail="Log file not found")

    disposition = "attachment" if download else "inline"
    return FileResponse(
        str(log_path),
        media_type="text/plain; charset=utf-8",
        headers={
            "Content-Disposition": f'{disposition}; filename="job_{job_id}.log"',
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


async def do_job_runners(job_id: int):
    """Return the in-memory active runner tasks attributed to a job."""
    if not _engine:
        raise HTTPException(status_code=500, detail="Engine offline")
    active = (_engine.state or {}).get("active_tasks", {}) or {}
    runners = {
        name: data
        for name, data in active.items()
        if isinstance(data, dict) and data.get("job_id") == job_id
    }
    return {"job_id": job_id, "runners": runners}


# ── Generic operation trigger / abort ───────────────────────────────────────

async def do_run_pipeline(payload: PipelineRequest):
    """Generic operation trigger replacing the ~10 NiceGUI global/site/host buttons.

    Ports the exact path those handlers use: emit ``iac:webhook_verified`` with the
    same payload shape (``pipeline_type`` plus ``limit`` and/or ``host_name``).
    """
    if not _ctx:
        raise HTTPException(status_code=500, detail="Context offline")

    pipeline_type = str(payload.pipeline_type or "").strip()
    if pipeline_type not in _PIPELINE_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported pipeline_type. Allowed: {sorted(_PIPELINE_TYPES)}",
        )

    limit = str(payload.limit or "").strip()
    host = str(payload.host_name or "").strip()
    if not limit and not host:
        raise HTTPException(
            status_code=400,
            detail="Provide either 'limit' (all|<site>|<host>) or 'host_name'.",
        )
    if host and not _HOST_LIMIT_PATTERN.fullmatch(host):
        raise HTTPException(status_code=400, detail="Invalid host_name token.")
    if limit and not _HOST_LIMIT_PATTERN.fullmatch(limit):
        raise HTTPException(status_code=400, detail="Invalid limit token.")

    event_payload: dict = {"pipeline_type": pipeline_type, "manual": True, "trigger": "ui_pipeline"}
    if limit:
        event_payload["limit"] = limit
    if host:
        event_payload["host_name"] = host

    _ctx.emit("iac:webhook_verified", event_payload)
    _emit_webhook_verified(event_payload)
    target = host or limit
    return {
        "status": "accepted",
        "message": f"Pipeline '{pipeline_type}' queued for '{target}'.",
        "pipeline_type": pipeline_type,
        "limit": limit or None,
        "host_name": host or None,
    }


async def do_abort():
    """Abort the running execution: kill runner containers, mark RUNNING jobs ABORTED.

    Ports the NiceGUI ``abort_execution`` handler (dashboard.py).
    """
    if not _ctx or not _engine or not _service:
        raise HTTPException(status_code=503, detail="Orchestrator not initialized")

    state = _service.state
    _ctx.log.warning("API: ABORT SEQUENCE INITIATED BY USER.")

    for task_name in list((state.get("active_tasks", {}) or {}).keys()):
        safe_task_name = "".join(
            ch if ch.isalnum() or ch in ".-_" else "-" for ch in task_name
        ).strip("-")
        try:
            proc = await asyncio.create_subprocess_exec(
                "docker", "rm", "-f", f"aac-runner-{safe_task_name}",
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await proc.wait()
        except Exception:
            pass

    aborted: list[int] = []
    try:
        running_jobs = _engine.db.get_jobs_by_status("RUNNING")
    except Exception:
        running_jobs = []
    for job in running_jobs:
        job_id = getattr(job, "id", None)
        if job_id is None:
            continue
        _engine.db.update_job(job_id, "ABORTED")
        _engine.db.update_progress(job_id, progress=None, current_step="Aborted by User")
        aborted.append(job_id)

    if aborted:
        try:
            from core.api import OutboundMessage, MessageSeverity
            ids = ", ".join(f"#{j}" for j in aborted)
            abort_msg = OutboundMessage(
                title="Pipeline Aborted",
                body=f"Execution aborted by user (jobs {ids}).",
                severity=MessageSeverity.WARNING,
                source_plugin_id="lyndrix.plugin.iac_orchestrator",
                target_provider="system",
                metadata={"toast": True, "persist": True},
            )
            _ctx.emit("messaging:outbound", abort_msg.model_dump(mode="json"))
        except Exception as exc:
            _ctx.log.debug(f"ABORT: notification failed: {exc}")

    state["is_running"] = False
    state["active_tasks"] = {}
    return {"status": "ok", "aborted_jobs": aborted}


# ── Infrastructure inventory (assignments + terraform hosts) ─────────────────

def _load_assignments() -> list:
    """Flatten site→stage→host service assignments. Ports dashboard.load_assignments."""
    if not _engine:
        return []
    config = _engine.config
    assignments: list[dict] = []
    base_dir = config.git_repos_dir / "iac_controller" / "environments"
    sites_dir = base_dir / "sites"
    profiles_file = base_dir / "global" / "03_profiles.yml"

    profiles: dict = {}
    if profiles_file.exists():
        try:
            with open(profiles_file, "r", encoding="utf-8") as f:
                p_data = yaml.safe_load(f) or {}
                profiles = p_data.get("profiles") or {}
        except Exception as exc:
            _ctx and _ctx.log.error(f"API: Failed to parse profiles YAML: {exc}")

    if not sites_dir.exists():
        return []

    for yaml_file in sites_dir.rglob("*.yml"):
        parts = yaml_file.parts
        try:
            site = parts[parts.index("sites") + 1]
            stage = parts[parts.index("stages") + 1] if "stages" in parts else "common"
            with open(yaml_file, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            all_hosts = {**(data.get("hosts") or {}), **(data.get("hardware_hosts") or {})}
            for host_name, host_data in all_hosts.items():
                if not isinstance(host_data, dict):
                    continue
                host_svcs: set = set()
                direct_services = host_data.get("services") or []
                if isinstance(direct_services, list):
                    for s in direct_services:
                        if isinstance(s, dict) and s.get("name"):
                            host_svcs.add(s.get("name"))
                host_profiles = host_data.get("profiles") or []
                if isinstance(host_profiles, list):
                    for p in host_profiles:
                        profile_services = profiles.get(p, {}).get("services") or []
                        if isinstance(profile_services, list):
                            for s in profile_services:
                                if isinstance(s, dict) and s.get("name"):
                                    host_svcs.add(s.get("name"))
                if host_svcs:
                    assignments.append({
                        "site": site, "stage": stage, "host": host_name,
                        "services": sorted(host_svcs),
                    })
        except (ValueError, IndexError):
            continue
        except Exception as exc:
            _ctx and _ctx.log.error(f"API: Failed to parse assignment YAML {yaml_file}: {exc}")

    unique = {f"{a['site']}-{a['stage']}-{a['host']}": a for a in assignments}
    return sorted(unique.values(), key=lambda x: (x["site"], x["stage"], x["host"]))


async def do_load_assignments():
    """Return the flat site→stage→host service assignment list."""
    if not _engine:
        raise HTTPException(status_code=500, detail="Engine offline")
    return _load_assignments()


_TF_PROVIDER = "bpg/proxmox"
_TF_JOB_PREFIXES = ("init_host", "host_provision", "adopt_host")


def _tfvars_host_index(config) -> dict:
    """Index hosts from rendered terraform.tfvars.json. Ports terraform._tfvars_host_index."""
    index: dict[str, dict] = {}
    inv_root = config.git_repos_dir / "inventory_state"
    if not inv_root.exists():
        return index
    for tfvars_path in inv_root.glob("*/*/terraform/terraform.tfvars.json"):
        try:
            with open(tfvars_path, "r", encoding="utf-8") as fh:
                data = json.load(fh) or {}
        except Exception:
            continue
        try:
            stage = tfvars_path.parents[1].name
            site = tfvars_path.parents[2].name
        except IndexError:
            continue
        for host_name, cfg in (data.get("containers") or {}).items():
            if not isinstance(cfg, dict):
                continue
            node = str(cfg.get("node_name") or "").strip()
            index[host_name] = {"node": node, "env": f"{site}_{stage}"}
    return index


def _host_state_from_jobs(jobs: list, host: str) -> str:
    """Derive a tile status from the most recent terraform-type job for ``host``."""
    for job in jobs:  # newest-first
        ptype = str(job.get("pipeline_type") or "")
        base, _, target = ptype.partition(":")
        if base not in _TF_JOB_PREFIXES or target.strip().lower() != host.lower():
            continue
        status = str(job.get("status") or "").upper()
        if status == "SUCCESS":
            return "created"
        if status in ("FAILED", "ERROR", "ABORTED"):
            return "failed"
        if status == "RUNNING":
            return "provisioning"
        return "unknown"
    return "not provisioned"


def _scan_terraform_hosts() -> list:
    """Parse site host YAML → per-host terraform metadata. Ports terraform._scan_terraform_hosts."""
    config = _engine.config
    base_dir = config.git_repos_dir / "iac_controller" / "environments"
    sites_dir = base_dir / "sites"
    results: list[dict] = []
    if not sites_dir.exists():
        return results

    tf_index = _tfvars_host_index(config)
    try:
        recent_jobs = _engine.db.get_jobs_for_stats(500)
    except Exception:
        recent_jobs = []

    for yaml_file in sites_dir.rglob("*.yml"):
        parts = yaml_file.parts
        try:
            site = parts[parts.index("sites") + 1]
            stage = parts[parts.index("stages") + 1] if "stages" in parts else "common"
            with open(yaml_file, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            all_hosts = {**(data.get("hosts") or {}), **(data.get("hardware_hosts") or {})}
            for host_name, host_data in all_hosts.items():
                if not isinstance(host_data, dict):
                    continue
                tf = host_data.get("terraform") if isinstance(host_data.get("terraform"), dict) else {}
                tf_meta = tf_index.get(host_name)
                managed = bool(tf) or tf_meta is not None
                if tf_meta is not None:
                    node = tf_meta.get("node") or ""
                    provider = _TF_PROVIDER
                    resource = (
                        f'module.lxc_{node}.proxmox_virtual_environment_container.ct["{host_name}"]'
                        if node else 'proxmox_virtual_environment_container.ct'
                    )
                    state = _host_state_from_jobs(recent_jobs, host_name)
                else:
                    provider = tf.get("provider", "—")
                    resource = tf.get("resource", tf.get("type", "—"))
                    state = tf.get("state", "unknown")
                results.append({
                    "site": site, "stage": stage, "host": host_name,
                    "ansible_host": host_data.get("ansible_host") or host_data.get("address") or "—",
                    "managed": managed, "provider": provider, "resource": resource,
                    "workspace": tf.get("workspace", "default"), "state": state,
                })
        except (ValueError, IndexError):
            continue
        except Exception as exc:
            _ctx and _ctx.log.error(f"API: Terraform scan failed for {yaml_file}: {exc}")

    return sorted(results, key=lambda x: (not x["managed"], x["site"], x["stage"], x["host"]))


async def do_terraform_hosts():
    """Return per-host Terraform metadata + status (tfvars index + job-derived state)."""
    if not _engine:
        raise HTTPException(status_code=500, detail="Engine offline")
    try:
        return _scan_terraform_hosts()
    except Exception as exc:
        _ctx and _ctx.log.error(f"API: terraform host scan failed: {exc}")
        raise HTTPException(status_code=500, detail="Terraform host scan failed")


# ── Aggregated overview stats ────────────────────────────────────────────────

async def do_get_stats():
    """Aggregated Overview stats (KPIs, host-lifecycle phases, status breakdown)."""
    if not _engine:
        raise HTTPException(status_code=500, detail="Engine offline")
    from . import stats as _stats
    try:
        jobs = _engine.db.get_jobs_for_stats(500)
    except Exception:
        jobs = []
    try:
        tasks = _engine.db.get_job_tasks_for_stats(1500)
    except Exception:
        tasks = []
    s = _stats.compute(jobs, tasks)

    def _ser_recent(j: dict) -> dict:
        return {
            "id": j.get("id"),
            "pipeline_type": j.get("pipeline_type"),
            "type_label": j.get("type_label"),
            "phase": j.get("phase"),
            "icon": j.get("icon"),
            "color": j.get("color"),
            "status": j.get("status"),
            "progress": j.get("progress", 0),
            "duration_s": j.get("duration_s"),
            "duration_human": _stats.humanize_duration(j.get("duration_s")),
            "start_label": j.get("start_label"),
        }

    return {
        "total": s.total,
        "success": s.success,
        "failed": s.failed,
        "running": s.running,
        "finished": s.finished,
        "success_rate": s.success_rate,
        "avg_duration_s": s.avg_duration_s,
        "avg_duration_human": _stats.humanize_duration(s.avg_duration_s),
        "last_deployment_status": s.last_deployment_status,
        "last_deployment_at": s.last_deployment_at.isoformat() if s.last_deployment_at else None,
        "by_status": s.by_status,
        "by_phase": [
            {
                "phase": p.phase, "label": p.label, "icon": p.icon, "color": p.color,
                "total": p.total, "success": p.success, "failed": p.failed,
                "running": p.running, "success_rate": p.success_rate,
            }
            for p in s.by_phase
        ],
        "recent": [_ser_recent(j) for j in s.recent],
    }


async def do_service_history(service_name: str):
    """Recent jobs touching a service. Ports db.get_service_history."""
    if not _engine:
        raise HTTPException(status_code=500, detail="Engine offline")
    name = str(service_name or "").strip()
    return _engine.db.get_service_history(name)


# ── Settings (Vault-backed) ──────────────────────────────────────────────────

async def do_get_settings():
    """Return the orchestrator settings surface. Ports settings.py reads."""
    if not _ctx or not _service:
        raise HTTPException(status_code=503, detail="Orchestrator not initialized")
    return {
        "auto_apply": bool(_service.state.get("auto_apply_enabled", False)),
        "test_deploy_allowed_hosts": _ctx.get_secret("iac_test_deploy_allowed_hosts") or "",
        "gitlab_url": _ctx.get_secret("iac_gitlab_url") or "https://gitlab.int.fam-feser.de",
        "group_id": _ctx.get_secret("iac_gitlab_group_id") or "",
        "lyndrix_base_url": _ctx.get_secret("iac_lyndrix_base_url") or "http://10.1.10.31:8081",
        "gitlab_token_key": _ctx.get_secret("iac_gitlab_api_token_key") or "",
        "autosync_enabled": (_ctx.get_secret("iac_webhook_autosync_enabled") or "true").lower() != "false",
        "sync_interval": _safe_int(_ctx.get_secret("iac_webhook_sync_interval_seconds"), 1800),
        "webhook_endpoint": (
            f"{str(_ctx.get_secret('iac_lyndrix_base_url') or 'http://10.1.10.31:8081').rstrip('/')}"
            "/api/iac/webhook/gitlab"
        ),
    }


async def do_save_settings(payload: SettingsRequest):
    """Persist the orchestrator settings to Vault. Ports settings.py writes."""
    if not _ctx or not _service:
        raise HTTPException(status_code=503, detail="Orchestrator not initialized")

    if payload.auto_apply is not None:
        _service.state["auto_apply_enabled"] = bool(payload.auto_apply)
        _ctx.set_secret("iac_auto_apply", str(bool(payload.auto_apply)))
    if payload.test_deploy_allowed_hosts is not None:
        _ctx.set_secret(
            "iac_test_deploy_allowed_hosts",
            str(payload.test_deploy_allowed_hosts or "").strip(),
        )
    if payload.gitlab_url is not None:
        _ctx.set_secret("iac_gitlab_url", str(payload.gitlab_url or "").strip())
    if payload.group_id is not None:
        _ctx.set_secret("iac_gitlab_group_id", str(payload.group_id or "").strip())
    if payload.lyndrix_base_url is not None:
        _ctx.set_secret("iac_lyndrix_base_url", str(payload.lyndrix_base_url or "").strip())
    if payload.gitlab_token_key is not None:
        _ctx.set_secret("iac_gitlab_api_token_key", str(payload.gitlab_token_key or "").strip())
    if payload.autosync_enabled is not None:
        _ctx.set_secret(
            "iac_webhook_autosync_enabled",
            "true" if payload.autosync_enabled else "false",
        )
    if payload.sync_interval is not None:
        interval = max(300, _safe_int(payload.sync_interval, 1800))
        _ctx.set_secret("iac_webhook_sync_interval_seconds", str(interval))

    return await do_get_settings()


# ── Settings: schema-driven, fully API-controllable surface ──────────────────
#
# These three endpoints expose EVERY operator-tunable setting (pipeline, GitLab
# webhooks, Ansible, Terraform, repository roles) as a typed schema + values, so
# everything the NiceGUI offers is reachable over the API and renderable by the
# React UI. Persistence reuses the existing Vault keys via OrchestratorSettings;
# secrets are masked on read and "blank = keep" on write.

class SettingsValuesRequest(BaseModel):
    values: dict = Field(default_factory=dict)


class CredentialRequest(BaseModel):
    alias: str
    secret: str


# Git credential aliases double as Vault keys, so they are constrained to a safe
# character set and may not collide with the plugin's own security-critical
# secrets. Any ``iac_*`` key (engine configuration) is additionally reserved.
_CREDENTIAL_ALIAS_RE = re.compile(r"^[a-z0-9_]+$")
_RESERVED_CREDENTIAL_KEYS = frozenset({
    "gitlab_webhook_token",
    "ansible_ssh_key",
    "iac_tf_ssh_private_key",
    "iac_token_registry",
    "system_api_key",
})


def _settings_mgr() -> OrchestratorSettings:
    if not _ctx or not _service:
        raise HTTPException(status_code=503, detail="Orchestrator not initialized")
    return OrchestratorSettings(_ctx, _service)


async def do_get_settings_schema():
    """Return the typed field schema (grouped by category, secrets flagged)."""
    mgr = _settings_mgr()
    return {"schema": mgr.get_schema()}


async def do_get_settings_values():
    """Return current values (secrets masked, ``<key>__configured`` flags added)."""
    mgr = _settings_mgr()
    return {"values": mgr.get_values()}


async def do_save_settings_values(payload: SettingsValuesRequest):
    """Persist provided settings. Unknown keys ignored; blank secrets keep existing.

    Settings shadowed by an OS env var are NOT written (env always wins) and are
    reported in ``locked`` with a ``warning`` status so the UI never claims a
    no-op succeeded.
    """
    mgr = _settings_mgr()
    result = mgr.save_values(payload.values or {})
    saved = result["saved"]
    locked = result["locked"]
    return {
        "status": "warning" if locked else "ok",
        "saved": saved,
        "locked": locked,
        "warning": (
            f"Locked by environment (not saved): {', '.join(locked)}" if locked else None
        ),
        "values": mgr.get_values(),
    }


async def do_list_credentials():
    """List stored Git credential aliases (names only — never the secret values)."""
    mgr = _settings_mgr()
    return {"credentials": mgr.credential_aliases()}


async def do_add_credential(payload: CredentialRequest):
    """Store a Git credential (token/key) under an alias and index it in the registry."""
    if not _ctx:
        raise HTTPException(status_code=503, detail="Context offline")
    alias = (payload.alias or "").strip()
    secret_val = (payload.secret or "").strip()
    if not alias or not secret_val:
        raise HTTPException(status_code=422, detail="Both 'alias' and 'secret' are required.")
    # Validate the alias before using it as a Vault key. Without this an operator
    # (or compromised api:write identity) could pass the name of a security-
    # critical engine secret (webhook token, SSH keys, the registry itself) and
    # silently overwrite it through the credential form.
    if not _CREDENTIAL_ALIAS_RE.match(alias):
        raise HTTPException(
            status_code=422,
            detail="Alias must match ^[a-z0-9_]+$ (lowercase letters, digits, underscore).",
        )
    if alias in _RESERVED_CREDENTIAL_KEYS or alias.startswith("iac_"):
        raise HTTPException(
            status_code=409,
            detail=f"Alias '{alias}' is reserved for engine configuration and cannot be used.",
        )
    _ctx.set_secret(alias, secret_val)
    raw = _ctx.get_secret("iac_token_registry")
    try:
        registry = list(json.loads(raw)) if raw else []
    except Exception:
        registry = []
    if alias not in registry:
        registry.append(alias)
        _ctx.set_secret("iac_token_registry", json.dumps(registry))
    return {"status": "ok", "alias": alias, "credentials": registry}


async def do_delete_credential(alias: str):
    """Remove a credential alias from the registry (the underlying secret is left in Vault)."""
    if not _ctx:
        raise HTTPException(status_code=503, detail="Context offline")
    alias = (alias or "").strip()
    raw = _ctx.get_secret("iac_token_registry")
    try:
        registry = list(json.loads(raw)) if raw else []
    except Exception:
        registry = []
    if alias in registry:
        registry = [a for a in registry if a != alias]
        _ctx.set_secret("iac_token_registry", json.dumps(registry))
    return {"status": "ok", "credentials": registry}


async def do_get_webhook_token():
    """Return whether the webhook token is set, masked (never the raw value)."""
    if not _ctx:
        raise HTTPException(status_code=503, detail="Context offline")
    token = _ctx.get_secret("gitlab_webhook_token")
    return {
        "configured": bool(token),
        "masked": "•" * 32 if token else "",
    }


async def do_generate_webhook_token():
    """Generate + store a new GitLab webhook token. Returns the raw value once."""
    if not _ctx:
        raise HTTPException(status_code=503, detail="Context offline")
    new_token = secrets.token_urlsafe(32)
    _ctx.set_secret("gitlab_webhook_token", new_token)
    _ctx.log.info("API: New GitLab webhook token generated and stored in Vault.")
    return {"status": "ok", "token": new_token}


async def do_sync_webhooks_authed():
    """Auth'd, idempotent re-registration of the group webhooks (Settings button)."""
    if not _ctx:
        raise HTTPException(status_code=500, detail="API Context not initialized")
    from .gitlab_webhooks import sync_group_webhooks_from_ctx, WebhookConfigError
    try:
        result = await asyncio.to_thread(sync_group_webhooks_from_ctx, _ctx)
    except WebhookConfigError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        _ctx.log.error(f"WEBHOOK SYNC ERROR: {exc}")
        raise HTTPException(status_code=502, detail=f"Webhook sync failed: {exc}")
    return {"status": "ok", **result}


def _job_stream_snapshot() -> dict:
    """Build a single live snapshot of orchestrator state for the SSE stream."""
    if not _engine:
        return {"jobs": [], "active_tasks": {}, "is_running": False, "ts": time.time()}
    jobs = _engine.db.get_recent_jobs(30)
    state = _engine.state or {}
    return {
        "jobs": jobs,
        "active_tasks": state.get("active_tasks", {}) or {},
        "is_running": bool(state.get("is_running", False)),
        "ts": time.time(),
    }


async def stream_jobs(request: Request, token: str | None, ticket: str | None = None):
    """SSE generator factory for ``GET /stream/jobs?ticket=...``.

    Returns a StreamingResponse emitting a JSON snapshot whenever the orchestrator
    state changes, plus a ~1s keep-alive tick. Authorization is validated
    in-handler because EventSource cannot send an Authorization header — it
    accepts a short-lived ``ticket`` (preferred) or a bearer ``token``, and both
    must carry the ``api:read`` permission.
    """
    from fastapi.responses import StreamingResponse

    if not _authorize_stream_request(token, ticket, "api:read"):
        raise HTTPException(status_code=401, detail="Unauthorized")

    async def generator():
        last_hash = None
        ticks = 0
        try:
            while True:
                if await request.is_disconnected():
                    break
                snapshot = _job_stream_snapshot()
                # Hash on everything except the wall-clock timestamp so we only
                # push a new frame when the orchestrator state actually changes.
                change_key = json.dumps(
                    {k: v for k, v in snapshot.items() if k != "ts"},
                    sort_keys=True,
                    default=str,
                )
                snap_hash = hash(change_key)
                ticks += 1
                # Emit on change, on first connect, or at least every ~15s so the
                # client keeps a fresh snapshot even on a quiet system.
                if snap_hash != last_hash or ticks % 15 == 0:
                    last_hash = snap_hash
                    yield f"data: {json.dumps(snapshot, default=str)}\n\n"
                else:
                    yield ": keep-alive\n\n"
                await asyncio.sleep(1.0)
        except (asyncio.CancelledError, GeneratorExit):
            pass

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
