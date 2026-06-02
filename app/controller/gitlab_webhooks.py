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
