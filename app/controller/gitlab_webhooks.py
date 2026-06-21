import json
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError


def _request_json(base_url: str, token: str, method: str, path: str, query: dict | None = None, payload: dict | None = None):
    api_base = f"{base_url.rstrip('/')}/api/v4"
    url = f"{api_base}{path}"
    if query:
        url = f"{url}?{urlencode(query)}"

    headers = {
        "PRIVATE-TOKEN": token,
        "Content-Type": "application/json",
    }
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")

    req = Request(url, data=data, headers=headers, method=method)
    try:
        with urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8") if resp.length != 0 else ""
            body = json.loads(raw) if raw else None
            return resp.status, body, resp.headers
    except HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        raise RuntimeError(f"{method} {path} failed ({exc.code}): {raw or exc.reason}") from exc
    except URLError as exc:
        raise RuntimeError(f"{method} {path} failed: {exc.reason}") from exc


def _list_group_projects(gitlab_url: str, gitlab_token: str, group_id: int) -> list[dict]:
    projects: list[dict] = []
    page = 1
    while True:
        _, rows, headers = _request_json(
            gitlab_url,
            gitlab_token,
            "GET",
            f"/groups/{group_id}/projects",
            query={
                "include_subgroups": "true",
                "per_page": 100,
                "page": page,
            },
        )
        projects.extend(rows or [])
        next_page = headers.get("X-Next-Page", "")
        if not next_page:
            break
        page = int(next_page)
    return projects


def upsert_gitlab_group_webhooks(
    gitlab_url: str,
    gitlab_token: str,
    group_id: int,
    lyndrix_base_url: str,
    webhook_token: str,
) -> dict:
    webhook_url = f"{lyndrix_base_url.rstrip('/')}/api/iac/webhook/gitlab"
    payload = {
        "url": webhook_url,
        "push_events": False,
        "tag_push_events": False,
        "issues_events": False,
        "confidential_issues_events": False,
        "merge_requests_events": True,
        "job_events": False,
        # Deployments are triggered by the service CI's prod-trigger-orchestrator
        # job (LYNDRIX_DIRECT_TRIGGER_ENABLED). Enabling pipeline_events here would
        # double-trigger (and fire on dev/test too — no branch filter). Keep off;
        # this hook only carries merge_request events for notifications.
        "pipeline_events": False,
        "wiki_page_events": False,
        "enable_ssl_verification": True,
    }
    if webhook_token:
        payload["token"] = webhook_token

    projects = _list_group_projects(gitlab_url, gitlab_token, group_id)
    created = 0
    updated = 0
    failed = 0
    errors: list[str] = []

    for project in projects:
        project_id = project.get("id")
        project_name = project.get("path_with_namespace") or str(project_id)
        try:
            _, hooks, _ = _request_json(
                gitlab_url,
                gitlab_token,
                "GET",
                f"/projects/{project_id}/hooks",
                query={"per_page": 100},
            )
            existing = next((h for h in (hooks or []) if h.get("url") == webhook_url), None)
            if existing:
                _request_json(
                    gitlab_url,
                    gitlab_token,
                    "PUT",
                    f"/projects/{project_id}/hooks/{existing.get('id')}",
                    payload=payload,
                )
                updated += 1
            else:
                _request_json(
                    gitlab_url,
                    gitlab_token,
                    "POST",
                    f"/projects/{project_id}/hooks",
                    payload=payload,
                )
                created += 1
        except Exception as exc:
            failed += 1
            errors.append(f"{project_name}: {exc}")

    return {
        "group_id": group_id,
        "webhook_url": webhook_url,
        "projects_total": len(projects),
        "created": created,
        "updated": updated,
        "failed": failed,
        "errors": errors,
    }


class WebhookConfigError(ValueError):
    """Raised when GitLab webhook sync is not (fully) configured in Vault."""


def sync_group_webhooks_from_ctx(ctx) -> dict:
    """Read the GitLab webhook config from Vault and run the idempotent group
    upsert. Shared by the Settings UI, the ``/webhook/sync`` endpoint, and the
    self-healing background loop so they can never diverge.

    Raises WebhookConfigError when not fully configured (callers may skip/400);
    propagates RuntimeError from the API layer on transport failure.
    """
    gitlab_url = (ctx.get_secret("iac_gitlab_url") or "https://gitlab.int.fam-feser.de").strip()
    group_id_raw = (ctx.get_secret("iac_gitlab_group_id") or "").strip()
    lyndrix_base_url = (ctx.get_secret("iac_lyndrix_base_url") or "").strip()
    token_key = (ctx.get_secret("iac_gitlab_api_token_key") or "").strip()
    webhook_token = ctx.get_secret("gitlab_webhook_token")

    if not group_id_raw.isdigit():
        raise WebhookConfigError("GitLab Group ID is not set or not numeric.")
    if not lyndrix_base_url:
        raise WebhookConfigError("Lyndrix Base URL is not set.")
    if not token_key:
        raise WebhookConfigError("No GitLab API credential (Vault key) selected.")
    gitlab_token = ctx.get_secret(token_key)
    if not gitlab_token:
        raise WebhookConfigError(f"Vault key '{token_key}' has no secret value.")
    if not webhook_token:
        raise WebhookConfigError("No GitLab webhook token generated yet.")

    return upsert_gitlab_group_webhooks(
        gitlab_url, gitlab_token, int(group_id_raw), lyndrix_base_url, webhook_token
    )
