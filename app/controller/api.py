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
from collections import OrderedDict, deque
from pathlib import Path

from fastapi import Request, Header, HTTPException
from pydantic import BaseModel, Field

from .settings_schema import (
    OrchestratorSettings, SettingsPermissionError, SettingsValidationError,
    TIER_CREDENTIALS, TIER_INFRA_APPLY, TIER_OPERATOR, TIER_SETTINGS,
    is_valid_credential_alias,
)
from .queue import DuplicateJob as _DuplicateJob, SubmitResult
from .job_payload import ENT_APPROVE, ENT_TRIGGER, InvalidJobPayload
from . import branch_policy, plan_binding
from . import maintenance_resolver as _mres
from .offloop import off_loop
from .public_projection import public_catalog_entry, public_scalar

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



def _actor(identity) -> str | None:
    """Name recorded as the job's ``requested_by`` for an authenticated caller."""
    name = getattr(identity, "username", None)
    return str(name)[:100] if isinstance(name, str) and name else None


async def _submit(payload: dict, *entitlements: str, actor: str | None = None) -> SubmitResult:
    """Queue a trigger of this API (validated, with the entitlements of the calling
    route) and return what became of it; an invalid payload is a 400. Triggers no
    longer travel over the bus: the bus topic ``iac:webhook_verified`` has no
    sender identity (audit OS-09).

    The "webhook verified" notification is sent only for a trigger that was
    queued — a refused one already produced "Pipeline Ignored"."""
    if not _service:
        raise HTTPException(status_code=503, detail="Orchestrator not initialized")
    try:
        # ``actor`` = authenticated username (recorded with the job, audit OS-13);
        # token/CI triggers are recorded under their trigger name.
        result = await _service.submit_job_ex(payload, entitlements=entitlements, requested_by=actor)
    except InvalidJobPayload as exc:
        raise HTTPException(status_code=400, detail=f"Invalid job payload: {exc}")
    if result.queued:
        _emit_webhook_verified(payload)
    return result


def _trigger_response(result: SubmitResult, message: str, **extra) -> dict:
    """Body of a trigger endpoint. It used to say ``accepted`` / "… queued" whatever
    happened, also for a trigger that was dropped (audit OC-05). Now:

    * ``status``: ``accepted`` when a job will run the trigger; ``duplicate`` (or
      ``unavailable``) when it was NOT queued. HTTP stays 200 — GitLab disables a
      hook that answers 4xx and the existing CI template only looks at the code;
      what happened is in the body;
    * ``job_id``: the job to poll — also for a trigger merged into a queued job
      (``outcome: coalesced``) or waiting behind a running one (``follow_up``,
      ``waiting_for``); ``queued`` false and ``duplicate_of`` when refused.
    """
    return {
        "status": "accepted" if result.queued else result.outcome,
        "message": result.describe(message),
        **result.as_dict(),
        **extra,
    }

# ── Webhook token check (public routes) ──────────────────────────────────────
# Runs BEFORE authentication succeeded, for anyone who reaches the port. It used
# to do a synchronous, uncached Vault round trip on the event loop per request
# (audit OS-11): a loop of bogus calls stalled every other request and — on the
# runner — the cluster heartbeat. Now the expected token is cached for a short
# time and read off the loop; a mismatch may trigger ONE fresh read every few
# seconds (so a token rotated on another instance is picked up quickly), never
# one per request.
_TOKEN_TTL_SECONDS = 30.0
_TOKEN_REFRESH_MIN_SECONDS = 5.0
_token_cache: dict = {"value": None, "at": 0.0, "forced_at": 0.0, "loaded": False, "purpose": {}}

# ── One token per purpose (audit OS-04, option A — backward compatible) ───────
# The ONE shared token (``gitlab_webhook_token``) sits in every project hook and
# in two CI variables and authorised every public action: a token leaked from a
# hook could deploy. Each purpose now has its own secret slot. While a slot is
# empty the shared token keeps authorising that purpose exactly as before (with
# a deprecation warning); once it is set, ONLY that token is accepted for the
# purpose — not the shared one, not the token of another purpose. Nothing here
# ever writes, moves or deletes the shared token.
SHARED_SLOT = "gitlab_webhook_token"
PURPOSE_HOOK = "hook"                    # POST /webhook/gitlab (group hooks: merge request events)
PURPOSE_SERVICE_CI = "service_ci"        # deploy/service/<svc>/gitlab (+ status)
PURPOSE_CONTROLLER_CI = "controller_ci"  # infra/plan/gitlab, webhook/sync, test-host, infra/apply/gitlab
PURPOSE_TOKEN_KEYS = {
    PURPOSE_HOOK: "gitlab_hook_token",
    PURPOSE_SERVICE_CI: "gitlab_service_ci_token",
    PURPOSE_CONTROLLER_CI: "gitlab_controller_ci_token",
}
MODE_DEDICATED, MODE_SHARED = "dedicated", "shared"
#: The "shared token still in use" warning is logged once per purpose per hour.
_DEPRECATION_LOG_SECONDS = 3600.0
_deprecation_logged: dict[str, float] = {}

# ── Failure rate limit per source address ──────────────────────────────────────
# Counts REJECTED calls only, and a valid token is always accepted whatever the
# count: the limit cannot be turned into a lock-out of the legitimate CI (all
# GitLab runners share one egress address — one project with a stale token
# variable polling the status route must not stop every other pipeline). What
# the limit does: after ``_FAIL_LIMIT`` rejected calls within
# ``_FAIL_WINDOW_SECONDS`` from one address, further rejected calls from it are
# answered 429 + Retry-After and are not logged one by one (the Vault re-read
# is limited for the whole instance anyway). In-process and bounded (``_FAIL_TRACKED_ADDRESSES``, least recently
# failing address dropped first); each instance counts for itself.
_FAIL_LIMIT = 20
_FAIL_WINDOW_SECONDS = 60.0
_FAIL_TRACKED_ADDRESSES = 2048
_failures_by_address: "OrderedDict[str, deque]" = OrderedDict()
#: Rejected token-route calls on this instance (health/diagnostics, audit OC-16:
#: a rotated token that CI still uses otherwise fails without a trace).
webhook_auth_failures: dict = {"count": 0, "last_at": None}


def _reset_token_cache(value: str | None = None) -> None:
    _token_cache.update(value=value, at=time.monotonic() if value else 0.0, loaded=bool(value))


def _read_tokens() -> tuple[str | None, dict]:
    """The shared token and the purpose tokens from Vault (sync: thread)."""
    def read(key: str) -> str | None:
        value = _ctx.get_secret(key)
        return value if isinstance(value, str) and value else None

    return read(SHARED_SLOT), {purpose: read(key) for purpose, key in PURPOSE_TOKEN_KEYS.items()}


async def _load_tokens(*, force: bool = False) -> tuple[str | None, dict]:
    """``(shared token, {purpose: dedicated token | None})`` — cached, read off
    the event loop in one go."""
    now = time.monotonic()
    if not force and _token_cache["loaded"] and now - _token_cache["at"] < _TOKEN_TTL_SECONDS:
        return _token_cache["value"], _token_cache.get("purpose") or {}
    shared, dedicated = await off_loop(_read_tokens)
    _token_cache.update(value=shared, purpose=dedicated, at=time.monotonic(), loaded=True)
    return shared, dedicated


async def _expected_webhook_token(*, force: bool = False) -> str | None:
    """The shared webhook token (kept for callers that need only that)."""
    return (await _load_tokens(force=force))[0]


def _token_matches(presented: str | None, expected: str | None) -> bool:
    if not presented or not expected:
        return False
    # Bytes, so a non-ASCII header is a plain mismatch (compare_digest raises on
    # non-ASCII str — that used to be a 500).
    return hmac.compare_digest(str(presented).encode("utf-8", "replace"), str(expected).encode("utf-8", "replace"))


def _accepted_token(purpose: str | None, shared: str | None, dedicated: dict) -> tuple[str | None, str]:
    """``(the one token accepted for this purpose, mode)``. A purpose whose slot
    is set accepts that token only; an empty slot (or no purpose) the shared one."""
    own = (dedicated or {}).get(purpose) if purpose else None
    return (own, MODE_DEDICATED) if own else (shared, MODE_SHARED)


def _source_address(request) -> str:
    try:
        return str(request.client.host or "unknown")[:64]
    except Exception:
        return "unknown"


def _failures_in_window(address: str, now: float) -> int:
    stamps = _failures_by_address.get(address)
    if not stamps:
        return 0
    while stamps and now - stamps[0] > _FAIL_WINDOW_SECONDS:
        stamps.popleft()
    if not stamps:
        _failures_by_address.pop(address, None)
        return 0
    return len(stamps)


def _record_failure(address: str, now: float) -> None:
    stamps = _failures_by_address.get(address)
    if stamps is None:
        stamps = _failures_by_address[address] = deque(maxlen=_FAIL_LIMIT)
        while len(_failures_by_address) > _FAIL_TRACKED_ADDRESSES:
            _failures_by_address.popitem(last=False)
    stamps.append(now)
    _failures_by_address.move_to_end(address)


def _warn_shared_token(purpose: str) -> None:
    now = time.monotonic()
    last = _deprecation_logged.get(purpose)
    if last is not None and now - last < _DEPRECATION_LOG_SECONDS:
        return
    _deprecation_logged[purpose] = now
    _ctx.log.warning(
        f"DEPRECATED: a '{purpose}' call was authorised by the shared webhook token. Store a token of its own "
        f"in the secret slot '{PURPOSE_TOKEN_KEYS[purpose]}' (documentation.md → Webhook-Tokens); from then on "
        "only that token is accepted for this purpose.")


async def token_modes() -> dict:
    """``{purpose: "dedicated" | "shared" | "unconfigured"}`` — for the plugin health."""
    try:
        shared, dedicated = await _load_tokens()
    except Exception:
        return {purpose: "unknown" for purpose in PURPOSE_TOKEN_KEYS}
    return {purpose: (MODE_DEDICATED if dedicated.get(purpose) else (MODE_SHARED if shared else "unconfigured"))
            for purpose in PURPOSE_TOKEN_KEYS}


async def _require_gitlab_token(x_gitlab_token: str | None, purpose: str | None = None, request=None) -> str:
    """Authorise a call of a public route for ``purpose``. Returns the mode it
    was accepted in (``dedicated`` — the purpose's own token — or ``shared``).
    Raises 401, 429 for an address over the failure limit, 500 when no token is
    configured at all."""
    if not _ctx:
        raise HTTPException(status_code=500, detail="API Context not initialized")
    shared, dedicated = await _load_tokens()
    expected, mode = _accepted_token(purpose, shared, dedicated)
    address, now = _source_address(request), time.monotonic()
    throttled = _failures_in_window(address, now) >= _FAIL_LIMIT
    if not _token_matches(x_gitlab_token, expected) \
            and now - _token_cache["forced_at"] >= _TOKEN_REFRESH_MIN_SECONDS:
        # Unknown or changed? One fresh read at most every few seconds (for the
        # whole instance, whoever asks) — so a token rotated on another instance
        # is accepted quickly, also from an address that is over its failure limit.
        _token_cache["forced_at"] = now
        shared, dedicated = await _load_tokens(force=True)
        expected, mode = _accepted_token(purpose, shared, dedicated)
    if _token_matches(x_gitlab_token, expected):
        if mode == MODE_SHARED and purpose in PURPOSE_TOKEN_KEYS:
            _warn_shared_token(purpose)
        return mode
    if not expected:
        _ctx.log.error("SECURITY HALT: Webhook token missing in Vault.")
        raise HTTPException(status_code=500, detail="Configuration Error")
    webhook_auth_failures["count"] += 1
    webhook_auth_failures["last_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    if throttled:
        retry_after = max(1, int(_FAIL_WINDOW_SECONDS - (now - _failures_by_address[address][0])) + 1)
        raise HTTPException(status_code=429, detail="Too many rejected calls from this address",
                            headers={"Retry-After": str(retry_after)})
    _record_failure(address, now)
    if _failures_in_window(address, now) >= _FAIL_LIMIT:
        _ctx.log.warning(f"SECURITY REJECTION: {_FAIL_LIMIT} unauthorized webhook attempts from {address} within "
                         f"{int(_FAIL_WINDOW_SECONDS)}s — further rejected calls from it get 429.")
    else:
        _ctx.log.warning(f"SECURITY REJECTION: Unauthorized webhook attempt ({purpose or 'token'} route, {address}).")
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


# ── Short-lived stream tickets ───────────────────────────────────────────────
# Signing key for stream tickets. Derived from STORAGE_SECRET so a ticket minted
# on one cluster instance validates on every other one (the load balancer may
# send the EventSource request to a different pod than the POST that minted it).
# Without a STORAGE_SECRET (dev) it falls back to a process-lifetime random key.
def _stream_ticket_key() -> bytes:
    import hashlib
    import os

    secret = (os.environ.get("STORAGE_SECRET") or "").strip()
    if secret and secret != "dev_secret_only":
        return hashlib.sha256(b"iac-orchestrator:stream-ticket:" + secret.encode()).digest()
    return secrets.token_bytes(32)


_STREAM_TICKET_KEY = _stream_ticket_key()
_STREAM_TICKET_TTL = 60  # seconds
#: Ticket scopes: ``api:read`` = the job stream (status, progress); ``logs`` = job
#: output (log lines in the stream, raw log download) — needs PERM_LOGS.
SCOPE_READ = "api:read"
SCOPE_LOGS = "logs"


def issue_stream_ticket(permission: str = "api:read", *, logs: bool = False) -> str:
    """Mint a short-lived, signed ticket authorizing a stream/raw-log read.

    Minted only after the caller has already passed ``require_permission`` on the
    authed endpoint, so the granted scopes are baked into the ticket (``logs``
    only for a caller holding the log permission). EventSource and browser
    tab/download requests then present this opaque ticket as ``?ticket=`` instead
    of the long-lived bearer token, keeping the real credential out of URLs,
    reverse-proxy access logs and browser history.
    """
    import base64
    exp = int(time.time()) + _STREAM_TICKET_TTL
    scopes = permission + (f"+{SCOPE_LOGS}" if logs else "")
    body = f"{exp}:{scopes}"
    sig = hmac.new(_STREAM_TICKET_KEY, body.encode(), "sha256").hexdigest()
    return base64.urlsafe_b64encode(f"{body}:{sig}".encode()).decode()


def _ticket_scopes(ticket: str | None) -> set[str]:
    """Scopes of a valid, unexpired ticket (constant-time signature check); empty
    for anything else."""
    if not ticket:
        return set()
    try:
        import base64
        raw = base64.urlsafe_b64decode(ticket.encode()).decode()
        # Split from the right once to isolate the sig, then split from the left
        # once for exp — the scope string itself contains colons ("api:read").
        body, sig = raw.rsplit(":", 1)
        exp_s, scopes = body.split(":", 1)
        expected = hmac.new(_STREAM_TICKET_KEY, body.encode(), "sha256").hexdigest()
        if not hmac.compare_digest(sig.encode("utf-8", "replace"), expected.encode()):
            return set()
        if int(exp_s) < int(time.time()):
            return set()
        return {part for part in scopes.split("+") if part}
    except Exception:
        return set()


def _validate_stream_ticket(ticket: str | None, permission: str = "api:read") -> bool:
    """Is ``ticket`` valid and does it carry scope ``permission``?"""
    return permission in _ticket_scopes(ticket)


def _stream_access(token: str | None, ticket: str | None) -> tuple[bool, bool]:
    """``(may_read, may_see_logs)`` for a stream/raw-log request.

    A short-lived ticket (preferred — the React client mints one via
    ``POST /stream/ticket``) carries its scopes; the bearer-token path is kept for
    non-browser callers and resolves both permissions from the identity.
    """
    scopes = _ticket_scopes(ticket)
    if SCOPE_READ in scopes:
        return True, SCOPE_LOGS in scopes
    identity = _resolve_stream_identity(token)
    if identity is None:
        return False, False
    try:
        return bool(identity.allows("api:read")), bool(identity.allows(PERM_LOGS))
    except Exception:
        return False, False


def _authorize_stream_request(token: str | None, ticket: str | None,
                              permission: str = "api:read") -> bool:
    """Compatibility wrapper: may the caller read the job stream?"""
    return _stream_access(token, ticket)[0]


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
        # Loop guard for the (future) SSoT-Manager maintenance-default switch: its
        # controller commit syncs the tree but must never trigger a deploy.
        if _mres.is_maintenance_commit(commit_message):
            return False, "ignored maintenance default commit"

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
    if not _is_safe_git_ref_token(service_name):
        return None, f"invalid service_name derived from project path: {service_name!r}"

    ref = (attrs.get("ref") or project.get("default_branch") or "main").strip()
    if not ref:
        ref = "main"
    if not _is_safe_git_ref_token(ref):
        return None, f"invalid branch/ref: {ref!r}"

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
    # Explicit dry-run: ansible --check, nothing changed. Deploys are real by
    # default; the global auto_apply switch does not affect them (1.5.6).
    dry_run: bool = False


class TestHostDeployRequest(BaseModel):
    services: list[str] = Field(default_factory=list)


# Generic operation trigger. Mirrors the NiceGUI Assignments-tab buttons, which
# all run ``ctx.emit("iac:webhook_verified", {pipeline_type, limit|host_name, ...})``.
_PIPELINE_TYPES = {"bootstrap_compliance", "adopt_host", "rollout", "init_host", "compliance"}


class PipelineRequest(BaseModel):
    pipeline_type: str
    limit: str | None = None       # "all" | "<site>" | "<host>"
    host_name: str | None = None   # exact host token for host-scoped actions
    dry_run: bool = False          # ansible --check for every playbook of the job


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


def _is_safe_git_ref_token(value: str) -> bool:
    """True if `value` is safe to use as a service-repo directory name and a
    git ref (branch), e.g. via ``services_dir / service_name`` and
    ``git fetch/checkout/reset/clone -b origin/{branch}`` in stages/git.py.

    Reuses the host-token charset (`_HOST_LIMIT_PATTERN`) but additionally
    rejects values the plain charset still lets through unsafely: a leading
    '-' (git/argv would parse it as an option => option injection / RCE via
    e.g. ``--upload-pack=``), a leading '.', and any '..' segment (path
    traversal out of ``services_dir``).
    """
    if not value or not _HOST_LIMIT_PATTERN.fullmatch(value):
        return False
    if value.startswith("-") or value.startswith("."):
        return False
    if ".." in value:
        return False
    return True


def _load_generated_inventory_hosts() -> set[str]:
    if not _engine:
        return set()
    inventory_path = _engine.config.git_repos_dir / "inventory_state" / "global" / "ansible" / "inventory.yml"
    if not inventory_path.exists():
        # No working tree on this instance (control plane only) → the runner's snapshot.
        return set(_snapshot_items("inventory_hosts") or [])
    try:
        with open(inventory_path, "r", encoding="utf-8") as handle:
            inv = yaml.safe_load(handle) or {}
    except Exception:
        return set()
    return set(((inv.get("all") or {}).get("hosts") or {}).keys())


# ── Repo-derived read models shared with instances that have no git working trees ──
# The runner (the only instance with iac_controller / inventory_state checked out)
# publishes these after every pipeline and repo sync; every other instance serves
# them from the database instead of answering "empty".

_SNAPSHOT_PREFIX = "snapshot:"


def _has_repo_checkout() -> bool:
    return bool(_engine) and (_engine.config.git_repos_dir / "iac_controller").exists()


def _snapshot_items(name: str):
    """Snapshot ``name`` from the shared DB (``None`` when there is none)."""
    if not _engine:
        return None
    try:
        rec = _engine.db.get_state(f"{_SNAPSHOT_PREFIX}{name}")
    except Exception:
        return None
    if not rec or not isinstance(rec.get("data"), dict):
        return None
    return rec["data"].get("items")


def publish_repo_snapshots() -> dict:
    """Store catalog, assignments, terraform host metadata and generated inventory
    hosts in the database. Called on the executing instance (which has the repos).
    Returns the item counts; a no-op when this instance has no checkout."""
    if not _engine or not _has_repo_checkout():
        return {}
    counts = {}
    for name, loader in (
        ("catalog", _read_catalog_from_disk),
        ("assignments", _load_assignments_from_disk),
        ("terraform_hosts", _collect_terraform_hosts_from_disk),
        ("inventory_hosts", lambda: sorted(_load_generated_inventory_hosts())),
        # Branch → target policy of the catalog (audit OS-05): instances without a
        # checkout judge a trigger's branch by it.
        ("branch_policy", lambda: branch_policy.load(_engine.config.git_repos_dir) or {}),
    ):
        try:
            items = loader()
            _engine.db.update_state(f"{_SNAPSHOT_PREFIX}{name}", {"items": items}, commit_hash="")
            counts[name] = len(items)
        except Exception as exc:
            _ctx and _ctx.log.debug(f"API: snapshot '{name}' not published: {exc}")
    return counts


# ── Handler logic (router-agnostic) ─────────────────────────────────────────

_HEALTH_TTL_SECONDS = 5.0
_health_cache: dict = {"at": 0.0, "value": None}


async def do_health():
    """Public liveness snapshot (``GET /api/iac/health``, no authentication).

    Minimal on purpose and cheap for the instance (audit OS-11): no version, no
    instance id, no queue depth for anonymous callers; built off the event loop
    and cached for a few seconds, so hammering it costs nothing. The detailed
    health is the authenticated plugin health of the core.
    """
    if not _ctx or not _engine:
        raise HTTPException(status_code=503, detail="Orchestrator not initialized")
    now = time.monotonic()
    if _health_cache["value"] is not None and now - _health_cache["at"] < _HEALTH_TTL_SECONDS:
        return _health_cache["value"]

    def _probe() -> dict:
        db_ok = True
        try:
            _engine.db.get_recent_jobs(1)
        except Exception:
            db_ok = False
        return {
            "status": "healthy" if db_ok else "degraded",
            "db_connected": db_ok,
            "runner_online": bool(_service.runner_online()) if _service else True,
        }

    value = await off_loop(_probe)
    _health_cache.update(at=time.monotonic(), value=value)
    return value


async def do_gitlab_webhook(request: Request, x_gitlab_token: str | None):
    """Verify a GitLab webhook and bridge it into the internal event bus."""
    if not _ctx:
        raise HTTPException(status_code=500, detail="API Context not initialized")

    mode = await _require_gitlab_token(x_gitlab_token, PURPOSE_HOOK, request)

    try:
        payload = await request.json()
        project_name = payload.get("project", {}).get("name", "unknown")
        _ctx.log.info(f"WEBHOOK: Verified push for project '{project_name}'.")
        if mode == MODE_DEDICATED and str(payload.get("object_kind") or "").lower() == "pipeline":
            # The dedicated hook token is for what the group hooks really send:
            # merge request events (gitlab_webhooks registers them with
            # pipeline_events/push_events/job_events off). A ``pipeline`` body on
            # this route is hand-built — it could start a deploy, which is the
            # service CI's job with its own token. 200, so a hook that does send
            # one is not disabled by GitLab; nothing is triggered or announced.
            _ctx.log.warning(f"WEBHOOK: pipeline event for '{project_name}' ignored — the hook token does not "
                             "trigger deployments.")
            return {"status": "accepted", "triggered": False,
                    "reason": "pipeline events are not accepted with the hook token; deployments are triggered "
                              "by the service CI"}

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

        try:
            result = await _submit(event_payload, ENT_TRIGGER)
        except HTTPException as exc:
            if exc.status_code != 400:
                raise
            # A refused event (e.g. a pipeline of a branch that may not be
            # deployed, audit OS-05) is still a delivered hook: 200, not triggered.
            _ctx.log.warning(f"WEBHOOK: event not triggered — {exc.detail}")
            return {"status": "accepted", "triggered": False, "reason": str(exc.detail)}
        # Always 200 for GitLab (a hook that answers 4xx gets disabled); whether a
        # job runs the event is in ``triggered`` / ``outcome``.
        return {"status": "accepted", "triggered": result.queued,
                "reason": f"{reason}; {event_reason}" if result.queued else result.describe(""),
                **{k: v for k, v in result.as_dict().items() if k != "reason"}}
    except HTTPException:
        raise
    except Exception as e:
        _ctx.log.error(f"WEBHOOK ERROR: {str(e)}")
        raise HTTPException(status_code=400, detail="Malformed JSON payload")


async def do_sync_webhooks(x_gitlab_token: str | None, request=None):
    """Token-protected, idempotent re-registration of the group webhooks."""
    if not _ctx:
        raise HTTPException(status_code=500, detail="API Context not initialized")
    await _require_gitlab_token(x_gitlab_token, PURPOSE_CONTROLLER_CI, request)
    from .gitlab_webhooks import sync_group_webhooks_from_ctx, WebhookConfigError
    try:
        result = await off_loop(sync_group_webhooks_from_ctx, _ctx)
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


def _read_catalog_from_disk() -> list:
    catalog_file = _engine.config.git_repos_dir / "iac_controller" / "environments" / "global" / "02_service_catalog.yml"
    if catalog_file.exists():
        with open(catalog_file, 'r') as f:
            data = yaml.safe_load(f) or {}
            # Allowlisted fields only: the result is stored as a snapshot and
            # served to every reader — never pass raw SSoT entries through.
            entries = (data.get("service_catalog") or {}).get("services") or []
            return [e for e in (public_catalog_entry(x) for x in entries) if e]
    return []


async def do_get_catalog():
    """Returns the parsed global service catalog (disk on the runner, snapshot elsewhere)."""
    if not _engine:
        raise HTTPException(status_code=500, detail="Engine offline")
    if _has_repo_checkout():
        return _read_catalog_from_disk()
    return _snapshot_items("catalog") or []


async def do_trigger_service_deployment(service_name: str, payload: DeployRequest, *, trigger: str = "ui",
                                        identity=None, prod_only: bool = False):
    """Triggers a targeted single-service deployment.

    ``trigger`` tells the maintenance policy stage where the deploy comes from:
    ``gitlab-ci`` (service CI, public route) follows the service's ``deploy_policy``
    and may be parked as SCHEDULED; ``ui`` (catalog button) always runs at once and
    promotes a parked job for the same service ("run now").
    """
    if not _ctx:
        raise HTTPException(status_code=500, detail="Context offline")
    normalized_service = str(service_name or "").strip().lower()
    branch = str(payload.branch or "main").strip() or "main"
    require_prod = False
    if prod_only:
        # The dedicated service-CI token deploys the catalog's production branch
        # of the service and nothing else (audit OS-04/OS-05): the branch in the
        # body is not a choice the token holder has. Without a policy on this
        # instance yet, the runner enforces it (``require_prod_branch``).
        policy = await off_loop(_service.branch_policy) if _service is not None else None
        if policy:
            forced = branch_policy.prod_branch(policy, normalized_service)
            if forced != branch:
                _ctx.log.info(f"API: service-CI trigger for '{normalized_service}' asked for branch '{branch}' — "
                              f"deploying the production branch '{forced}'.")
            branch = forced
        else:
            require_prod = True
    if not _is_safe_git_ref_token(normalized_service):
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid service_name: only exact tokens [A-Za-z0-9._-] are allowed, "
                "and it must not start with '-'/'.' or contain '..'."
            ),
        )
    if not _is_safe_git_ref_token(branch):
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid branch: only exact tokens [A-Za-z0-9._-] are allowed, "
                "and it must not start with '-'/'.' or contain '..'."
            ),
        )
    event_payload = {
        "pipeline_type": "single_service",
        "service_name": normalized_service,
        "service_branch": branch,
        "manual": True,
        "trigger": str(trigger or "ui"),
    }
    if payload.dry_run:
        event_payload["dry_run"] = True
    if require_prod:
        event_payload["require_prod_branch"] = True
    result = await _submit(event_payload, ENT_TRIGGER, actor=_actor(identity))
    kind = "Dry-run (--check)" if payload.dry_run else "Deployment"
    return _trigger_response(result, f"{kind} queued for {normalized_service}", dry_run=bool(payload.dry_run))


def map_service_status(job: dict | None, *, accept: str | None = None) -> dict:
    """CI-facing status of the newest job of a service (pure; ``job`` from
    ``get_latest_job_for_pipeline_type``).

    Maintenance windows (1.6): a parked job is ``SCHEDULED``. Only a caller that
    sends ``accept=scheduled`` gets ``status: "scheduled"`` (+ ``not_before``); the
    old CI template polls for success/failed and would turn red on anything else, so
    without it the compatibility mode answers ``status: "success"`` with
    ``job_status: "SCHEDULED"`` (decision D / finding 7). ``QUEUED`` is ``running``.
    """
    accepted = {a.strip().lower() for a in str(accept or "").split(",") if a.strip()}
    job_status = str((job or {}).get("status") or "").upper()
    if job_status == "SUCCESS":
        status = "success"
    elif job_status in {"FAILED", "ERROR", "ABORTED"}:
        status = "failed"
    elif job_status in {"RUNNING", "PENDING", "QUEUED"}:
        status = "running"
    elif job_status == "SCHEDULED":
        status = "scheduled" if "scheduled" in accepted else "success"
    else:
        status = "unknown"
    out: dict = {"status": status, "job_status": job_status}
    not_before = (job or {}).get("not_before")
    if job_status == "SCHEDULED" and not_before is not None:
        from .queue import from_queue_clock
        from core.api import maintenance as _mw

        aware = from_queue_clock(not_before)
        out["not_before"] = _mw.iso(aware) if aware else None
    return out


async def do_get_service_status(service_name: str, since_epoch: int = 0, accept: str | None = None):
    """Returns the latest deployment status for a service, optionally after an epoch
    timestamp. ``accept`` is a comma list of extra statuses the caller understands
    (today: ``scheduled``) — see :func:`map_service_status`."""
    if not _engine:
        raise HTTPException(status_code=500, detail="Engine offline")
    normalized_service = str(service_name or "").strip().lower()
    pipeline_type = f"single_service:{normalized_service}"
    # Polled every 10 s by each running CI job — never on the event loop.
    job = await off_loop(_engine.db.get_latest_job_for_pipeline_type, pipeline_type, since_epoch)
    if not job:
        return {
            "status": "pending",
            "service_name": normalized_service,
            "pipeline_type": pipeline_type,
            "found": False,
            "since_epoch": since_epoch,
        }

    mapped = map_service_status(job, accept=accept)
    return {
        **mapped,
        "found": True,
        "service_name": normalized_service,
        "pipeline_type": pipeline_type,
        "job_id": job.get("id"),
        "progress": job.get("progress") or 0,
        "current_step": job.get("current_step") or "",
    }


# ── Maintenance windows (plugin 1.6): scheduled deploys + effective windows ──

def _iso(value) -> str | None:
    if value is None:
        return None
    from core.api import maintenance as _mw

    try:
        return _mw.iso(value)
    except Exception:
        return str(value)


async def do_list_schedules():
    """Parked (SCHEDULED) deploys waiting for their maintenance window."""
    if not _service:
        raise HTTPException(status_code=503, detail="Orchestrator not initialized")
    rows = await off_loop(_service.queue.scheduled_jobs)
    for r in rows:
        r["not_before"] = _iso(r.get("not_before"))
        r["start_time"] = _iso(r.get("start_time"))
    return {"schedules": rows, "count": len(rows)}


async def do_run_schedule_now(job_id: int):
    """"Run now": promote a SCHEDULED job to QUEUED (``force``), wake the runners.
    The maintenance override is announced when the job actually runs."""
    if not _service:
        raise HTTPException(status_code=503, detail="Orchestrator not initialized")
    ok = await off_loop(_service.promote_scheduled, int(job_id))
    if not ok:
        raise HTTPException(status_code=404, detail=f"Job #{job_id} is not a scheduled deployment")
    return {"status": "ok", "job_id": int(job_id), "message": f"Scheduled job #{job_id} queued to run now"}


async def do_cancel_schedule(job_id: int):
    """Cancel a parked deploy (SCHEDULED → ABORTED)."""
    if not _service:
        raise HTTPException(status_code=503, detail="Orchestrator not initialized")
    ok = await off_loop(_service.queue.abort_scheduled, int(job_id))
    if not ok:
        raise HTTPException(status_code=404, detail=f"Job #{job_id} is not a scheduled deployment")
    return {"status": "ok", "job_id": int(job_id), "message": f"Scheduled job #{job_id} cancelled"}


def _maintenance_snapshot() -> dict:
    """The runner-published read model (``snapshot:maintenance_effective``), empty when none."""
    if not _engine:
        return {}
    try:
        rec = _engine.db.get_state(_engine.MAINTENANCE_SNAPSHOT)
    except Exception:
        rec = None
    data = (rec or {}).get("data")
    return data if isinstance(data, dict) else {}


def _running_overrides():
    """Our own running deploy overrides as ``core.api.maintenance.Override`` objects
    (what this plugin knows; other plugins' overrides live in their read models)."""
    from datetime import timedelta
    from core.api import maintenance as _mw

    queue = getattr(_service, "queue", None)
    if queue is None or not _engine:
        return []
    try:
        rows = queue.running_overrides()
    except Exception:
        return []
    max_minutes = int(_engine.config.maintenance_max_override_minutes)
    linger = int(_engine.config.maintenance_linger_minutes)
    out = []
    for r in rows:
        started = r.get("claimed_at") or _mw.utcnow()
        out.append(_mw.Override(
            override_id=str(r["override_id"]), ci_ids=tuple(r.get("window_ci_ids") or []),
            until=started + timedelta(minutes=max_minutes), started_at=started,
            linger_minutes=linger, reason=f"IaC deploy job #{r['id']}", source=_mres.SOURCE,
        ))
    return out


async def do_maintenance_windows(site: str | None = None, stage: str | None = None,
                                 kind: str | None = None, q: str | None = None):
    """Effective maintenance windows (snapshot) with the evaluated state per item."""
    if not _engine:
        raise HTTPException(status_code=500, detail="Engine offline")
    from core.api import maintenance as _mw

    snap = await off_loop(_maintenance_snapshot)
    items = [it for it in (snap.get("items") or []) if isinstance(it, dict)]
    term = str(q or "").strip().lower()
    if site:
        items = [it for it in items if it.get("site") == site]
    if stage:
        items = [it for it in items if it.get("stage") == stage]
    if kind:
        items = [it for it in items if it.get("kind") == kind]
    if term:
        items = [it for it in items if term in str(it.get("name") or "").lower()
                 or term in str(it.get("host") or "").lower()]
    overrides = await off_loop(_running_overrides)
    now = _mw.utcnow()
    horizon = int(_engine.config.maintenance_schedule_horizon_days)
    out = []
    for it in items:
        row = dict(it)
        row["state"] = _mres.effective_state_of(it, overrides, now, horizon_days=horizon)
        out.append(row)
    all_items = [it for it in (snap.get("items") or []) if isinstance(it, dict)]
    return {
        "items": out,
        "count": len(out),
        "published_at": snap.get("published_at"),
        "now": _mw.iso(now),
        "sites": sorted({str(it.get("site")) for it in all_items if it.get("site")}),
        "stages": sorted({str(it.get("stage")) for it in all_items if it.get("stage")}),
        "kinds": sorted({str(it.get("kind")) for it in all_items if it.get("kind")}),
    }


async def do_maintenance_default():
    """Read-only: ``maintenance_default`` per site/stage from the controller snapshot
    (decision C — changed in the iac-controller repo, never from here)."""
    if not _engine:
        raise HTTPException(status_code=500, detail="Engine offline")
    snap = await off_loop(_maintenance_snapshot)
    return {
        "defaults": [d for d in (snap.get("defaults") or []) if isinstance(d, dict)],
        "published_at": snap.get("published_at"),
        "read_only": True,
        "hint": "Edit maintenance_default (block under stage_vars: / site_vars:) in the iac-controller "
                "repository; the next sync republishes it.",
    }


async def do_trigger_test_host_deployment(host_name: str, payload: TestHostDeployRequest, identity=None):
    """Triggers a guarded host_provision run for exactly one allow-listed host."""
    if not _ctx or not _engine:
        raise HTTPException(status_code=500, detail="Orchestrator offline")
    _require_infra_apply(identity, "host_provision")

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
    result = await _submit(event_payload, ENT_TRIGGER, actor=_actor(identity))
    return _trigger_response(
        result, f"Host provisioning queued for host '{host}'.", host_name=host,
        services=[str(s).strip() for s in (payload.services or []) if str(s).strip()],
    )


async def do_trigger_host_bootstrap(host_name: str, payload: TestHostDeployRequest, identity=None):
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
    result = await _submit(event_payload, ENT_TRIGGER, actor=_actor(identity))
    return _trigger_response(result, f"Compliance bootstrap queued for host '{host}'.", host_name=host)


async def do_trigger_infra_plan(identity=None):
    """Triggers a read-only whole-infrastructure Terraform plan ("Check Env")."""
    if not _ctx:
        raise HTTPException(status_code=500, detail="Context offline")
    infra_plan_payload = {"pipeline_type": "infra_plan", "manual": True, "trigger": "manual_infra_plan"}
    result = await _submit(infra_plan_payload, ENT_TRIGGER, actor=_actor(identity))
    return _trigger_response(result, "Infrastructure plan (Check Env) queued.")


async def do_trigger_infra_apply_by_token(x_gitlab_token: str | None, request=None):
    """``POST /api/iac/infra/apply/gitlab``: fleet apply for a token holder.

    Disabled unless ``token_infra_apply_enabled`` is set (audit OS-03): the one
    static webhook token sits in every project hook and CI variable, and no CI
    pipeline calls this route — the reviewed path is plan → approve in the
    dashboard. Checked before the token so a disabled route costs nothing.
    """
    if not _ctx or not _engine:
        raise HTTPException(status_code=500, detail="Context offline")
    enabled = await off_loop(lambda: bool(_engine.config.token_infra_apply_enabled))
    if not enabled:
        raise HTTPException(
            status_code=404,
            detail="Token-triggered infrastructure apply is disabled (setting token_infra_apply_enabled).",
        )
    await _require_gitlab_token(x_gitlab_token, PURPOSE_CONTROLLER_CI, request)
    return await do_trigger_infra_apply()


async def do_trigger_infra_apply(identity=None):
    """Triggers a whole-infrastructure Terraform apply ("Deploy Infra")."""
    if not _ctx:
        raise HTTPException(status_code=500, detail="Context offline")
    infra_apply_payload = {
        "pipeline_type": "infra_apply",
        "approve": True,
        "manual": True,
        "trigger": "manual_infra_apply",
    }
    # Only the two apply routes (permission api:infra_apply) may set ``approve``.
    result = await _submit(infra_apply_payload, ENT_TRIGGER, ENT_APPROVE, actor=_actor(identity))
    return _trigger_response(result, "Infrastructure deploy queued.")


# ── Pending infra plan (review-and-approve flow) ────────────────────────────
# A successful fleet plan with real resource changes persists a
# "pending_infra_plan" record (see engine.execute_terraform_infra). The UIs
# surface it with an Apply button; a successful apply clears it automatically.

async def do_get_pending_plan():
    """Returns the pending infra plan awaiting operator approval (if any)."""
    if not _engine:
        raise HTTPException(status_code=503, detail="Orchestrator not initialized")
    record = await off_loop(_engine.db.get_state, "pending_infra_plan")
    data = (record or {}).get("data") or {}
    if not data.get("envs"):
        return {"pending": False}
    return {
        "pending": True,
        "job_id": data.get("job_id"),
        "planned_at": data.get("planned_at"),
        "envs": data.get("envs") or {},
        # What an approval is bound to (audit OS-12). ``approvable`` is False for a
        # plan recorded without the controller commit: plan again before approving.
        "controller_sha": data.get("controller_sha") or None,
        "approvable": plan_binding.approval_payload(data) is not None,
    }


async def do_apply_pending_plan(identity=None):
    """Approves the pending plan: triggers the fleet apply (approve=True)."""
    if not _ctx or not _engine:
        raise HTTPException(status_code=503, detail="Orchestrator not initialized")
    record = await off_loop(_engine.db.get_state, "pending_infra_plan")
    data = (record or {}).get("data") or {}
    if not data.get("envs"):
        raise HTTPException(status_code=409, detail="No pending infrastructure plan to apply")
    # The approval names the plan that was reviewed — controller commit and a
    # fingerprint per environment. The runner applies that plan or nothing
    # (audit OS-12: it used to apply whatever the controller was by then).
    payload = plan_binding.approval_payload(data)
    if payload is None:
        raise HTTPException(
            status_code=409,
            detail="The pending plan does not record the iac-controller commit it was made from (it was created "
                   "by an older plugin version) and cannot be approved. Run Check Env again and approve that plan.")
    result = await _submit(payload, ENT_TRIGGER, ENT_APPROVE, actor=_actor(identity))
    return _trigger_response(result, "Approved — infrastructure deploy queued.", plan_job_id=data.get("job_id"),
                             plan_controller_sha=payload["plan_controller_sha"])


async def do_dismiss_pending_plan():
    """Dismisses the pending plan without applying it."""
    if not _engine:
        raise HTTPException(status_code=503, detail="Orchestrator not initialized")
    await off_loop(_engine.db.update_state, "pending_infra_plan", {}, "latest")
    return {"status": "ok"}


#: Upper bounds of the list/tail parameters (a reader must not be able to make the
#: instance load an arbitrary amount of rows).
_JOBS_LIMIT_MAX = 500
_LOG_TAIL_MAX = 5000


def _with_wait_hints(jobs: list, runner_state: str) -> list:
    """QUEUED jobs say what they are waiting for while no runner executes them —
    instead of looking like a job that hangs at 0 % ("Queued by gitlab-ci")."""
    if runner_state not in ("absent", "restarting"):
        return jobs
    hint = "Waiting for runner (restarting)" if runner_state == "restarting" else "Waiting for runner — none online"
    out = []
    for job in jobs:
        if isinstance(job, dict) and job.get("status") == "QUEUED":
            job = {**job, "waiting_for": "runner",
                   "current_step": f"{hint} · {job.get('current_step') or 'Queued'}"[:250]}
        out.append(job)
    return out


async def do_list_jobs(limit: int = 20):
    """Returns a list of recent and active jobs."""
    if not _engine:
        raise HTTPException(status_code=500, detail="Engine offline")
    limit = max(1, min(int(limit or 20), _JOBS_LIMIT_MAX))
    jobs = await off_loop(_engine.db.get_recent_jobs, limit)
    return _with_wait_hints(jobs, _service.runner_state()) if _service else jobs


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
    tail = max(1, min(int(tail or 200), _LOG_TAIL_MAX))

    # Cluster: the executing runner streams every line into the shared database;
    # serve from there first so any instance can answer, regardless of which host
    # holds the log file.
    queue = getattr(_service, "queue", None)
    if queue is not None:
        try:
            has_db_log = await off_loop(queue.has_log, job_id)
        except Exception:
            has_db_log = False
        if has_db_log:
            res = await off_loop(
                queue.read_log, job_id, offset=offset, tail=max(1, tail or 600), grep=grep, grep_max=_LOG_GREP_MAX,
            )
            return {
                "job_id": job_id,
                "lines": res["lines"],
                "source": "db",
                "offset": res["offset"],
                "size": res["size"],
                "tail": tail,
                "grep": grep,
            }

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
    from fastapi.responses import FileResponse, StreamingResponse

    if not _engine:
        raise HTTPException(status_code=500, detail="Engine offline")

    disposition = "attachment" if download else "inline"
    headers = {
        "Content-Disposition": f'{disposition}; filename="job_{job_id}.log"',
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
    }

    # Shared database log (cluster): stream it in chunks from whichever instance answers.
    queue = getattr(_service, "queue", None)
    if queue is not None and await off_loop(queue.has_log, job_id):
        def _iter():
            for line in queue.iter_log(job_id):
                yield line + "\n"

        return StreamingResponse(_iter(), media_type="text/plain; charset=utf-8", headers=headers)

    log_path = _engine.config.get_log_path(job_id)
    if not log_path or not Path(log_path).exists():
        raise HTTPException(status_code=404, detail="Log file not found")

    return FileResponse(str(log_path), media_type="text/plain; charset=utf-8", headers=headers)


def _live_summary() -> dict:
    """``active_tasks`` / ``is_running`` / ``queued`` from the shared database (the
    runner publishes them), merged with this process' own in-memory state when it
    is the executing instance (fresher by up to a second)."""
    summary = {"active_tasks": {}, "is_running": False, "queued": 0}
    queue = getattr(_service, "queue", None)
    if queue is not None:
        try:
            summary = queue.live_summary()
        except Exception as exc:
            _ctx and _ctx.log.debug(f"API: live summary from DB failed: {exc}")
    state = (_engine.state if _engine else None) or {}
    local = state.get("active_tasks") or {}
    if local:
        merged = dict(summary.get("active_tasks") or {})
        merged.update({k: v for k, v in local.items() if isinstance(v, dict)})
        summary["active_tasks"] = merged
    summary["is_running"] = bool(summary.get("is_running")) or bool(state.get("is_running"))
    return summary


async def do_job_runners(job_id: int, include_logs: bool = True):
    """Return the runner tasks attributed to a job (shared DB snapshot, local state on the runner).
    ``include_logs=False`` (caller without the log permission) strips the log tails."""
    if not _engine:
        raise HTTPException(status_code=500, detail="Engine offline")
    runners: dict = {}
    queue = getattr(_service, "queue", None)
    if queue is not None:
        try:
            runners = await off_loop(queue.runners_for, job_id)
        except Exception:
            runners = {}
    active = (_engine.state or {}).get("active_tasks", {}) or {}
    for name, data in active.items():
        if isinstance(data, dict) and data.get("job_id") == job_id:
            runners[name] = data
    if not include_logs:
        runners = _without_logs(runners)
    return {"job_id": job_id, "runners": runners, "logs_included": bool(include_logs)}


# ── Generic operation trigger / abort ───────────────────────────────────────

async def do_run_pipeline(payload: PipelineRequest, identity=None):
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
    if pipeline_type in _APPLY_CAPABLE_TYPES and not payload.dry_run:
        _require_infra_apply(identity, pipeline_type)

    event_payload: dict = {"pipeline_type": pipeline_type, "manual": True, "trigger": "ui_pipeline"}
    if limit:
        event_payload["limit"] = limit
    if host:
        event_payload["host_name"] = host
    if payload.dry_run:
        event_payload["dry_run"] = True

    result = await _submit(event_payload, ENT_TRIGGER, actor=_actor(identity))
    target = host or limit
    suffix = " (dry-run, --check)" if payload.dry_run else ""
    return _trigger_response(
        result, f"Pipeline '{pipeline_type}' queued for '{target}'{suffix}.",
        dry_run=bool(payload.dry_run), pipeline_type=pipeline_type, limit=limit or None, host_name=host or None,
    )


def _close_aborted_row(job_id: int) -> None:
    """ABORTED for a RUNNING job nobody executes any more (sync: thread)."""
    if _engine.db.update_job(job_id, "ABORTED"):
        _engine.db.update_progress(job_id, progress=None, current_step="Aborted by User")
    _engine.end_override_for(job_id)  # maintenance override of a dead runner's job


def _forget_stopped_tasks(state: dict, stopped_job_ids) -> None:
    """Remove the in-memory runner tasks of ``stopped_job_ids`` (and entries that
    belong to no job) from ``state["active_tasks"]``; everything else stays."""
    stopped = {int(j) for j in stopped_job_ids}
    tasks = state.get("active_tasks")
    if not isinstance(tasks, dict):
        tasks = state["active_tasks"] = {}
    for name in list(tasks):
        task = tasks[name]
        job_id = task.get("job_id") if isinstance(task, dict) else None
        if not job_id or job_id == -1 or job_id in stopped:
            tasks.pop(name, None)
    state["is_running"] = bool(tasks) or int(state.get("running_jobs") or 0) > 0


async def do_abort():
    """Abort the running execution: kill runner containers, mark RUNNING jobs ABORTED.

    Ports the NiceGUI ``abort_execution`` handler (dashboard.py).
    """
    if not _ctx or not _engine or not _service:
        raise HTTPException(status_code=503, detail="Orchestrator not initialized")

    state = _service.state
    _ctx.log.warning("API: ABORT SEQUENCE INITIATED BY USER.")

    aborted: list[int] = []
    queue = getattr(_service, "queue", None)

    # 1. Nothing executes a QUEUED job yet — close those rows directly.
    if queue is not None:
        try:
            aborted.extend(await off_loop(queue.abort_queued))
        except Exception as exc:
            _ctx.log.debug(f"ABORT: abort_queued failed: {exc}")

    # 2. RUNNING jobs: the owning runner cancels its pipeline task and kills its
    #    containers. Ask every instance (cluster bus; the local bus delivers to this
    #    process too), and close rows whose owner is no longer alive ourselves.
    try:
        running = await off_loop(queue.running_jobs) if queue is not None else [
            {"id": getattr(j, "id", None), "owner_instance": getattr(j, "owner_instance", None)}
            for j in _engine.db.get_jobs_by_status("RUNNING")
        ]
    except Exception:
        running = []
    running_ids = [int(r["id"]) for r in running if r.get("id") is not None]
    if running_ids:
        try:
            _ctx.emit("iac:job_abort_requested", {"job_ids": running_ids}, cluster=True)
        except TypeError:
            _ctx.emit("iac:job_abort_requested", {"job_ids": running_ids})
        # Jobs whose containers Docker would not stop (review C1): their abort is
        # being retried by the runner — the row stays as it is until then.
        unstopped = set(await _service.on_abort_requested({"job_ids": running_ids}) or [])
        alive = None
        instances = getattr(_ctx, "cluster_instances", None)
        if instances:
            try:
                alive = {i["instance_id"] for i in instances if i.get("alive")}
            except Exception:
                alive = None
        for r in running:
            job_id = int(r["id"])
            owner = r.get("owner_instance")
            local = any(isinstance(t, dict) and t.get("job_id") == job_id
                        for t in (state.get("active_tasks") or {}).values())
            if job_id in unstopped:
                continue
            if local or not owner or (alive is not None and owner not in alive):
                # Conditional on the row still being RUNNING: a job its owner
                # finished meanwhile keeps its result. Off the loop (database).
                await off_loop(_close_aborted_row, job_id)
        aborted.extend(j for j in running_ids if j not in unstopped)
    else:
        unstopped = set()

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

    # Forget the tasks of the jobs that were stopped — and only those. This used
    # to be ``state["active_tasks"] = {}``: on the runner that also dropped the
    # tasks of jobs whose containers could NOT be stopped (and of a job that
    # started meanwhile). Those tasks are what keeps such a job attended and
    # heartbeated; without them the worker closed it as "lost track" while its
    # containers were still running.
    _forget_stopped_tasks(state, aborted)
    result: dict = {"status": "ok", "aborted_jobs": aborted}
    if unstopped:
        # Additive field: Docker on the runner did not answer, the abort is retried.
        result["unstopped_jobs"] = sorted(unstopped)
    return result


# ── Infrastructure inventory (assignments + terraform hosts) ─────────────────

def _load_assignments() -> list:
    """Flatten site→stage→host service assignments (disk on the runner, snapshot elsewhere)."""
    if not _engine:
        return []
    if not _has_repo_checkout():
        return _snapshot_items("assignments") or []
    return _load_assignments_from_disk()


def _load_assignments_from_disk() -> list:
    """Ports dashboard.load_assignments — reads the iac_controller working tree."""
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
    """Per-host terraform metadata + job-derived state. The static part comes from
    the working tree (runner) or the published snapshot (other instances); the
    ``state`` of tfvars-managed hosts is always derived from the shared job table."""
    hosts = _collect_terraform_hosts_from_disk() if _has_repo_checkout() else (_snapshot_items("terraform_hosts") or [])
    try:
        from .stats import STATS_JOB_WINDOW
        recent_jobs = _engine.db.get_jobs_for_stats(STATS_JOB_WINDOW)
    except Exception:
        recent_jobs = []
    results = []
    for entry in hosts:
        entry = dict(entry)
        if entry.pop("_state_from_jobs", False):
            entry["state"] = _host_state_from_jobs(recent_jobs, entry["host"])
        results.append(entry)
    return sorted(results, key=lambda x: (not x["managed"], x["site"], x["stage"], x["host"]))


def _collect_terraform_hosts_from_disk() -> list:
    """Parse site host YAML → per-host terraform metadata. Ports terraform._scan_terraform_hosts.
    Entries of tfvars-managed hosts carry ``_state_from_jobs=True`` instead of a state."""
    config = _engine.config
    base_dir = config.git_repos_dir / "iac_controller" / "environments"
    sites_dir = base_dir / "sites"
    results: list[dict] = []
    if not sites_dir.exists():
        return results

    tf_index = _tfvars_host_index(config)

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
                state_from_jobs = False
                if tf_meta is not None:
                    node = tf_meta.get("node") or ""
                    provider = _TF_PROVIDER
                    resource = (
                        f'module.lxc_{node}.proxmox_virtual_environment_container.ct["{host_name}"]'
                        if node else 'proxmox_virtual_environment_container.ct'
                    )
                    state = "unknown"
                    state_from_jobs = True
                else:
                    provider = public_scalar(tf.get("provider")) or "—"
                    resource = public_scalar(tf.get("resource", tf.get("type"))) or "—"
                    state = public_scalar(tf.get("state")) or "unknown"
                results.append({
                    "site": site, "stage": stage, "host": host_name,
                    "ansible_host": public_scalar(host_data.get("ansible_host") or host_data.get("address")) or "—",
                    "managed": managed, "provider": provider, "resource": resource,
                    "workspace": public_scalar(tf.get("workspace")) or "default", "state": state,
                    "_state_from_jobs": state_from_jobs,
                })
        except (ValueError, IndexError):
            continue
        except Exception as exc:
            _ctx and _ctx.log.error(f"API: Terraform scan failed for {yaml_file}: {exc}")

    return results


async def do_terraform_hosts():
    """Return per-host Terraform metadata + status (tfvars index + job-derived state)."""
    if not _engine:
        raise HTTPException(status_code=500, detail="Engine offline")
    # Disk scan + job-window read (STATS_JOB_WINDOW rows) are sync — keep them off the loop.
    try:
        return await off_loop(_scan_terraform_hosts)
    except Exception as exc:
        _ctx and _ctx.log.error(f"API: terraform host scan failed: {exc}")
        raise HTTPException(status_code=500, detail="Terraform host scan failed")


# ── Aggregated overview stats ────────────────────────────────────────────────

async def do_get_stats():
    """Aggregated Overview stats (KPIs, host-lifecycle phases, status breakdown)."""
    if not _engine:
        raise HTTPException(status_code=500, detail="Engine offline")
    from . import stats as _stats
    # Sync DB reads + aggregation run off the event loop (window is 5000 rows now).
    try:
        jobs = await off_loop(_engine.db.get_jobs_for_stats, _stats.STATS_JOB_WINDOW)
    except Exception:
        jobs = []
    try:
        tasks = await off_loop(_engine.db.get_job_tasks_for_stats, _stats.STATS_TASK_WINDOW)
    except Exception:
        tasks = []
    s = await off_loop(_stats.compute, jobs, tasks)

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
    # Read through to Vault (shared) — the in-memory flag is per instance.
    return {
        "auto_apply": bool(_service.config.auto_apply),
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


async def do_save_settings(payload: SettingsRequest, identity=None):
    """Persist the general settings. Same validation, tiers and "only what changed"
    rule as :func:`do_save_settings_values` (the keys are schema keys)."""
    if not _ctx or not _service:
        raise HTTPException(status_code=503, detail="Orchestrator not initialized")
    values = {k: v for k, v in payload.model_dump().items() if v is not None}
    await _save_settings(values, identity)
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


class SecretSlotRequest(BaseModel):
    value: str


# Git credential aliases double as Vault keys, so they are constrained to a safe
# character set (letters, digits, underscore, hyphen — the operator's GitLab
# namespace is hyphenated) and may not be the name of anything else the plugin
# stores: every setting/secret slot and any ``iac_*`` key is reserved
# (settings_schema.is_valid_credential_alias — one helper for every surface).
_CREDENTIAL_ALIAS_RE = re.compile(r"^[A-Za-z0-9_-]+$")

# Permission ids behind the settings tiers. They are custom permissions on
# purpose: the core grants ``plugin:<id>:api:write`` to every INT_USER and lets
# the global ``api:write`` satisfy it, so "write" cannot protect anything that
# decides which image runs with the fleet key or where tokens are sent.
_PLUGIN_PERM = "plugin:lyndrix.plugin.iac_orchestrator:"
PERM_SETTINGS_ADMIN = _PLUGIN_PERM + "iac:settings_admin"
PERM_CREDENTIALS_ADMIN = _PLUGIN_PERM + "iac:credentials_admin"
PERM_INFRA_APPLY = _PLUGIN_PERM + "api:infra_apply"
# Job output (log lines) — its own permission: logs can contain what the tools
# print about the managed systems, and plain api:read is every viewer (OS-07).
PERM_LOGS = _PLUGIN_PERM + "iac:logs_read"
_TIER_PERMISSIONS = {
    TIER_SETTINGS: PERM_SETTINGS_ADMIN,
    TIER_CREDENTIALS: PERM_CREDENTIALS_ADMIN,
    TIER_INFRA_APPLY: PERM_INFRA_APPLY,
}


def _allowed_tiers(identity) -> set[str] | None:
    """Settings tiers ``identity`` may change (``None`` = trusted in-process caller)."""
    if identity is None:
        return None
    tiers = {TIER_OPERATOR}
    for tier, permission in _TIER_PERMISSIONS.items():
        try:
            if identity.allows(permission):
                tiers.add(tier)
        except Exception:
            pass
    return tiers


# Pipeline types whose purpose is to create or change infrastructure through
# Terraform. Triggering them for real needs the apply permission; a dry-run
# (plan only, enforced by the executor) stays available to every operator.
_APPLY_CAPABLE_TYPES = frozenset({"init_host", "host_provision"})


def _require_infra_apply(identity, what: str) -> None:
    """403 unless ``identity`` holds the infra-apply permission (``None`` = trusted
    in-process caller). Used wherever an action can lead to a Terraform apply
    without passing the gated /infra/apply routes (audit OS-03)."""
    if identity is None:
        return
    allowed = False
    try:
        allowed = bool(identity.allows(PERM_INFRA_APPLY))
    except Exception:
        allowed = False
    if not allowed:
        raise HTTPException(
            status_code=403,
            detail=f"Missing required permission: {PERM_INFRA_APPLY} — {what} can apply infrastructure changes "
                   "(a dry-run only plans and does not need it).",
        )


async def _save_settings(values: dict, identity) -> dict:
    """Validated, authorised, all-or-nothing settings write (off the event loop)."""
    mgr = _settings_mgr()
    try:
        result = await off_loop(mgr.save_values, values, allowed_tiers=_allowed_tiers(identity))
    except SettingsValidationError as exc:
        raise HTTPException(status_code=422, detail=f"Invalid setting — {exc}")
    except SettingsPermissionError as exc:
        raise HTTPException(
            status_code=403,
            detail=f"Missing required permission: {_TIER_PERMISSIONS.get(exc.tier, exc.tier)} — {exc}",
        )
    if result["saved"]:
        _ctx.log.info(
            f"SETTINGS: {getattr(identity, 'username', None) or 'internal'} changed "
            f"{', '.join(result['saved'])}"
        )
    return result


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


async def do_save_settings_values(payload: SettingsValuesRequest, identity=None):
    """Persist provided settings (validated per key, all or nothing). Unknown keys
    and unchanged values are ignored; secret slots are refused here (422 — they
    have their own route); a change outside the caller's tiers is a 403.

    Settings shadowed by an OS env var are NOT written (env always wins) and are
    reported in ``locked`` with a ``warning`` status so the UI never claims a
    no-op succeeded.
    """
    mgr = _settings_mgr()
    result = await _save_settings(payload.values or {}, identity)
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


async def do_set_secret_slot(key: str, payload: SecretSlotRequest, identity=None):
    """Store secret material (SSH key, password, token) in its slot. The value is
    never echoed; the route requires the credentials permission."""
    mgr = _settings_mgr()
    try:
        await off_loop(mgr.set_secret_slot, key, payload.value)
    except SettingsValidationError as exc:
        raise HTTPException(status_code=422, detail=f"Invalid setting — {exc}")
    _ctx.log.info(f"SETTINGS: {getattr(identity, 'username', None) or 'internal'} replaced secret slot '{key}'")
    return {"status": "ok", "key": key, "configured": True}


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
            detail="Alias must match ^[A-Za-z0-9_-]+$ (letters, digits, underscore, hyphen).",
        )
    # Reserved = every Vault key the plugin uses for a setting or secret slot plus
    # the "iac_" prefix, compared case-insensitively ("Iac_foo" must not slip past).
    if not is_valid_credential_alias(alias):
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


async def do_generate_webhook_token(identity=None):
    """Generate + store a new GitLab webhook token. Returns the raw value once."""
    if not _ctx:
        raise HTTPException(status_code=503, detail="Context offline")
    new_token = secrets.token_urlsafe(32)
    _ctx.set_secret("gitlab_webhook_token", new_token)
    _reset_token_cache(new_token)
    _ctx.log.info(f"API: New GitLab webhook token generated by {_actor(identity) or 'internal'} and stored in Vault.")
    return {"status": "ok", "token": new_token}


async def do_clear_stats(identity=None):
    """Clear job history records (keeps RUNNING jobs intact) and reset last_deployment state."""
    if not _service or not _engine:
        raise HTTPException(status_code=503, detail="Orchestrator not initialized")
    deleted = await off_loop(_engine.db.clear_all_jobs, True)
    if deleted < 0:
        raise HTTPException(status_code=500, detail="Failed to clear statistics (see logs)")
    _ctx.log.warning(f"API: job history cleared by {_actor(identity) or 'internal'} ({deleted} job record(s) and their logs).")
    _service.state["last_deployment"] = "N/A"
    return {"status": "ok", "deleted": deleted}


async def do_clear_failed_jobs(identity=None):
    """Delete only failed/errored/aborted job records, leaving successful and running jobs intact."""
    if not _service or not _engine:
        raise HTTPException(status_code=503, detail="Orchestrator not initialized")
    deleted = await off_loop(_engine.db.clear_failed_jobs)
    if deleted < 0:
        raise HTTPException(status_code=500, detail="Failed to clear failed jobs (see logs)")
    _ctx.log.warning(f"API: failed job records cleared by {_actor(identity) or 'internal'} ({deleted} record(s) and their logs).")
    return {"status": "ok", "deleted": deleted}


async def do_sync_repos(identity=None):
    """Trigger a background sync of all core repositories (iac_controller, inventory_state, …)."""
    if not _ctx or not _service or not _engine:
        raise HTTPException(status_code=503, detail="Orchestrator not initialized")
    # Runs on a runner instance (the one with the working trees) via the queue, like
    # every other piece of execution; the job shows up in the history as "repo_sync".
    try:
        job_id = await off_loop(_service.enqueue_pipeline, {"pipeline_type": "repo_sync", "manual": True},
                                         requested_by=_actor(identity) or "ui")
    except _DuplicateJob as exc:
        raise HTTPException(status_code=409, detail=f"A repository sync is already queued/running (job #{exc.existing_id})")
    return {"status": "accepted", "message": "Repository sync queued", "job_id": job_id}


async def do_sync_webhooks_authed():
    """Auth'd, idempotent re-registration of the group webhooks (Settings button)."""
    if not _ctx:
        raise HTTPException(status_code=500, detail="API Context not initialized")
    from .gitlab_webhooks import sync_group_webhooks_from_ctx, WebhookConfigError
    try:
        result = await off_loop(sync_group_webhooks_from_ctx, _ctx)
    except WebhookConfigError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        _ctx.log.error(f"WEBHOOK SYNC ERROR: {exc}")
        raise HTTPException(status_code=502, detail=f"Webhook sync failed: {exc}")
    return {"status": "ok", **result}


def _without_logs(tasks: dict) -> dict:
    """Runner tasks without their log tails (status only)."""
    return {name: ({**info, "logs": []} if isinstance(info, dict) else info) for name, info in (tasks or {}).items()}


def _job_stream_snapshot() -> dict:
    """Build a single live snapshot of orchestrator state for the SSE stream."""
    if not _engine:
        return {"jobs": [], "active_tasks": {}, "is_running": False, "ts": time.time()}
    runner_state = _service.runner_state() if _service else "online"
    jobs = _with_wait_hints(_engine.db.get_recent_jobs(30), runner_state)
    live = _live_summary()
    return {
        "jobs": jobs,
        "active_tasks": live.get("active_tasks") or {},
        "is_running": bool(live.get("is_running", False)),
        "queued": live.get("queued", 0),
        "scheduled": live.get("scheduled", 0),
        "runner_online": runner_state == "online",
        "runner_state": runner_state,
        "ts": time.time(),
    }


_SNAPSHOT_MAX_AGE_SECONDS = 0.9
_snapshot_cache: dict = {"at": 0.0, "value": None, "task": None}


async def _shared_job_stream_snapshot() -> dict:
    """The live snapshot for the SSE streams: built at most about once per second
    per process, in a worker thread, and shared by every open stream (audit
    OC-08 B1). Before, each dashboard tab ran its own five synchronous queries
    on the event loop every second — a slow database froze the instance, a dead
    pooled socket stopped its cluster heartbeat. Treat the result as read-only."""
    now = time.monotonic()
    if _snapshot_cache["value"] is not None and now - _snapshot_cache["at"] < _SNAPSHOT_MAX_AGE_SECONDS:
        return _snapshot_cache["value"]
    task = _snapshot_cache["task"]
    if task is None or task.done():
        task = asyncio.ensure_future(off_loop(_job_stream_snapshot))
        _snapshot_cache["task"] = task
    value = await asyncio.shield(task)
    _snapshot_cache.update(at=time.monotonic(), value=value)
    return value


async def stream_jobs(request: Request, token: str | None, ticket: str | None = None):
    """SSE generator factory for ``GET /stream/jobs?ticket=...``.

    Returns a StreamingResponse emitting a JSON snapshot whenever the orchestrator
    state changes, plus a ~1s keep-alive tick. Authorization is validated
    in-handler because EventSource cannot send an Authorization header — it
    accepts a short-lived ``ticket`` (preferred) or a bearer ``token``, and both
    must carry the ``api:read`` permission. The log tails of running tasks are
    only included for a caller with the log permission (ticket scope ``logs``).
    """
    from fastapi.responses import StreamingResponse

    may_read, may_see_logs = _stream_access(token, ticket)
    if not may_read:
        raise HTTPException(status_code=401, detail="Unauthorized")

    async def generator():
        last_hash = None
        ticks = 0
        try:
            while True:
                if await request.is_disconnected():
                    break
                snapshot = dict(await _shared_job_stream_snapshot())   # shared: copy before changing
                if not may_see_logs:
                    snapshot["active_tasks"] = _without_logs(snapshot.get("active_tasks") or {})
                    snapshot["logs_included"] = False
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
