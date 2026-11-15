"""An approval is for the plan that was reviewed (audit OS-12).

"Approve" on a pending infrastructure plan used to queue a plain ``infra_apply``:
the job re-rendered from whatever the iac-controller was by then and ran
``plan`` + ``apply`` in one go. A controller change pushed between the review and
the click — a host to destroy, say — was applied unseen.

The pending plan now records what was reviewed: the iac-controller commit and,
per environment, a *fingerprint* of the plan (tofu's ``Plan: …`` line plus the
hosts it creates and destroys). The approval carries both
(``plan_controller_sha`` / ``plan_expected``); the runner applies only when the
controller is still at that commit and a fresh plan has the same fingerprints —
and then applies exactly the plan files it just compared (``tofu apply tfplan``;
tofu itself refuses a saved plan whose state changed meanwhile).
"""
from __future__ import annotations

import hashlib
import json

#: ``trigger`` of an approval queued by the pending-plan routes/buttons.
APPROVAL_TRIGGER = "pending_plan_approval"


def fingerprint(env_plan: dict | None) -> str:
    """Fingerprint of one environment's plan: the summary line (readable) and a
    digest over summary + hosts to create + hosts to destroy. ``""`` for an
    environment without changes."""
    plan = env_plan or {}
    summary = " ".join(str(plan.get("summary") or "").split())
    if not summary:
        return ""
    canonical = json.dumps({
        "summary": summary,
        "create": sorted(str(h) for h in plan.get("hosts_to_create") or []),
        "destroy": sorted(str(h) for h in plan.get("hosts_to_destroy") or []),
    }, sort_keys=True)
    return f"{summary[:150]} #{hashlib.sha256(canonical.encode()).hexdigest()[:16]}"


def fingerprints(envs: dict | None) -> dict[str, str]:
    """``{env: fingerprint}`` of the environments that have changes."""
    out = {str(env): fingerprint(plan) for env, plan in (envs or {}).items() if isinstance(plan, dict)}
    return {env: fp for env, fp in out.items() if fp}


def with_fingerprints(envs: dict | None) -> dict:
    """The per-environment plan records as they are stored with the pending plan."""
    return {str(env): {**plan, "fingerprint": fingerprint(plan)}
            for env, plan in (envs or {}).items() if isinstance(plan, dict)}


def approval_payload(pending: dict | None) -> dict | None:
    """The ``infra_apply`` payload that approves ``pending`` (the stored pending
    plan) — ``None`` when that plan cannot be bound: it was recorded without the
    controller commit (an older plugin version, or the commit was unreadable)."""
    data = pending or {}
    expected = fingerprints(data.get("envs"))
    sha = str(data.get("controller_sha") or "")
    if not expected or not sha:
        return None
    return {
        "pipeline_type": "infra_apply",
        "approve": True,
        "manual": True,
        "trigger": APPROVAL_TRIGGER,
        "plan_job_id": data.get("job_id"),
        "plan_controller_sha": sha,
        "plan_expected": expected,
    }


def is_approval(payload: dict | None) -> bool:
    """Is this ``infra_apply`` the approval of a reviewed plan (as opposed to an
    explicit "Deploy Infra")?"""
    p = payload or {}
    return bool(p.get("trigger") == APPROVAL_TRIGGER or p.get("plan_job_id") is not None
                or p.get("plan_controller_sha") or p.get("plan_expected"))


def differences(expected: dict | None, fresh: dict | None) -> list[str]:
    """Human-readable differences between the reviewed and the fresh
    fingerprints (``[]`` = the same plan)."""
    expected, fresh = dict(expected or {}), dict(fresh or {})
    out = []
    for env in sorted(set(expected) | set(fresh)):
        was, now = expected.get(env), fresh.get(env)
        if was == now:
            continue
        if was is None:
            out.append(f"{env}: no changes when reviewed, now '{now}'")
        elif now is None:
            out.append(f"{env}: reviewed '{was}', now no changes")
        else:
            out.append(f"{env}: reviewed '{was}', now '{now}'")
    return out
