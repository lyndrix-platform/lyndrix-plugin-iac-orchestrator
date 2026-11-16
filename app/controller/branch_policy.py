"""Which branch of a service may be deployed where (audit OS-05, option A).

A ``single_service`` deploy takes its target from the branch: ``dev`` /
``*-dev`` → the hosts of ``stage_dev``, ``test`` → ``stage_test``, EVERYTHING
ELSE → ``service_<svc>``, i.e. every host of the service, production included.
Any branch name a token holder (or a pipeline of an unreviewed branch) sent was
therefore rolled out fleet-wide.

Now only the catalog's production branch goes to ``service_<svc>``; the dev and
test branches keep their stages; any other branch is refused — when the trigger
arrives (HTTP 400, "bus trigger rejected") and again on the runner, which has
the freshly synced catalog.

The branch names come from the service catalog of the iac-controller
(``service_catalog.remote_git_management``: ``remote_git_default_branch_prod``,
``…_dev``, ``…_test`` — the live catalog spells the latter two ``remove_git_…``;
both spellings are read). A service entry may carry its own
``remote_git_default_branch_prod``.

# TODO(agent): option B of the audit — "the deployed commit must be an ancestor
# of the PROTECTED branch". After the repo sync the checkout IS
# ``origin/<branch>``, so locally that is trivially true; what is missing is the
# fact that the branch is protected in GitLab (and was not re-pointed or
# re-created). That needs the GitLab API (``/projects/:id/protected_branches``)
# with the stored GitLab credential, or a CI ID token whose ``ref_protected``
# claim says so (see OS-04 option B) — data the plugin does not have today.
"""
from __future__ import annotations

from pathlib import Path

DEFAULT_PROD, DEFAULT_DEV, DEFAULT_TEST = "main", "dev", "test"
CATALOG_RELATIVE = Path("iac_controller") / "environments" / "global" / "02_service_catalog.yml"


def from_catalog(catalog_data: dict | None) -> dict:
    """The policy as plain data (it is stored as a snapshot for instances
    without a checkout): ``{"prod", "dev", "test", "services": {name: prod}}``."""
    catalog = (catalog_data or {}).get("service_catalog") or {}
    mgmt = catalog.get("remote_git_management") or {}

    def name(*keys: str, default: str) -> str:
        for key in keys:
            value = mgmt.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return default

    services = {}
    for entry in catalog.get("services") or []:
        if isinstance(entry, dict) and entry.get("name"):
            own = entry.get("remote_git_default_branch_prod")
            if isinstance(own, str) and own.strip():
                services[str(entry["name"]).strip().lower()] = own.strip()
    return {
        "prod": name("remote_git_default_branch_prod", default=DEFAULT_PROD),
        "dev": name("remote_git_default_branch_dev", "remove_git_default_branch_dev", default=DEFAULT_DEV),
        "test": name("remote_git_default_branch_test", "remove_git_default_branch_test", default=DEFAULT_TEST),
        "services": services,
    }


def load(git_repos_dir) -> dict | None:
    """The policy from the iac-controller checkout below ``git_repos_dir``
    (``None`` without one). Synchronous YAML read — call it off the loop."""
    import yaml

    catalog_file = Path(git_repos_dir) / CATALOG_RELATIVE
    if not catalog_file.is_file():
        return None
    with open(catalog_file, "r", encoding="utf-8") as handle:
        return from_catalog(yaml.safe_load(handle) or {})


def prod_branch(policy: dict, service: str | None) -> str:
    own = (policy.get("services") or {}).get(str(service or "").strip().lower())
    return str(own or policy.get("prod") or DEFAULT_PROD)


def stage_of(policy: dict, service: str | None, branch: str | None) -> str | None:
    """``"prod"`` (→ ``service_<svc>``), ``"dev"`` (→ ``stage_dev``), ``"test"``
    (→ ``stage_test``) or ``None`` — a branch that may not be deployed."""
    branch = str(branch or "").strip()
    if not branch:
        return None
    # dev/test first, exactly as the engine picks the target group: ``dev`` and
    # ``*-dev`` go to stage_dev, ``test`` to stage_test — whatever else the
    # catalog calls its dev/test branch goes there as well.
    if branch == "dev" or branch.endswith("-dev") or branch == policy.get("dev"):
        return "dev"
    if branch == "test" or branch == policy.get("test"):
        return "test"
    if branch == prod_branch(policy, service):
        return "prod"
    return None


def violation(policy: dict | None, payload: dict | None) -> str | None:
    """Why this ``single_service`` payload must not be deployed (``None``: fine,
    or there is no policy to judge by)."""
    p = payload or {}
    if policy is None or p.get("pipeline_type") != "single_service":
        return None
    service = str(p.get("service_name") or "")
    branch = str(p.get("service_branch") or "main")
    prod = prod_branch(policy, service)
    stage = stage_of(policy, service, branch)
    if stage is None:
        return (f"branch '{branch}' of '{service}' may not be deployed: only the production branch '{prod}' "
                f"goes to all hosts of the service, and the dev/test branches ('{policy.get('dev')}', "
                f"'*-dev', '{policy.get('test')}') to stage_dev/stage_test")
    if p.get("require_prod_branch") and stage != "prod":
        return (f"this trigger may only deploy the production branch '{prod}' of '{service}', not '{branch}'")
    return None
