"""Job payload validation (audit OS-09).

A job payload ends up as ``services_dir / service_name`` (and ``rmtree`` of it),
as ``--limit``/``-e`` arguments of ansible-playbook, as ``-target`` of tofu and
as the switch that turns a plan into an apply. It arrives from HTTP handlers,
from the bus (any plugin can emit ``iac:webhook_verified``) and — at claim time —
from a database row any instance can write. None of that is trusted:

* :func:`validate_structure` — allow-listed pipeline types and keys, strict name
  tokens (no path separators, no ``..``, nothing that starts like an option),
  typed flags. Runs for **every** row at enqueue (``JobQueue.enqueue_ex``) and
  again when a runner claims the job. A field this version does not know is
  REFUSED, not dropped: it may be what limits the job (a host list, a mode) —
  written by a newer plugin version on another instance, or by a trigger that
  was added without teaching this module. Dropping it would run the job wider
  than its trigger asked for.
* :func:`strip_unentitled` — ``approve`` (plan → apply), ``force`` (ignore the
  maintenance window) and ``trigger`` (decides whether the deploy policy applies)
  are honoured only from code paths that are entitled to set them. Everything
  else — in particular the bus — gets them removed.
"""
from __future__ import annotations

import re
from typing import Any, Iterable

ENT_APPROVE = "approve"
ENT_FORCE = "force"
ENT_TRIGGER = "trigger"
ALL_ENTITLEMENTS = frozenset({ENT_APPROVE, ENT_FORCE, ENT_TRIGGER})

#: ``trigger`` of a job whose origin could not vouch for one. It is a policy
#: trigger (maintenance_resolver.POLICY_TRIGGERS): such a deploy follows the
#: service's deploy policy and can never claim "run now".
UNTRUSTED_TRIGGER = "bus"

PIPELINE_TYPES = frozenset({
    "single_service", "host_provision", "init_host", "state_check", "adopt_host",
    "bootstrap_compliance", "compliance", "infra_plan", "infra_apply", "rollout",
    "repo_sync", "connectivity",
})
#: Types that may carry ``approve`` / ``force`` at all.
_APPROVABLE = frozenset({"infra_apply", "host_provision"})
_FORCEABLE = frozenset({"single_service"})

# First character alphanumeric: never an option ("-x"), never hidden/relative
# (".", ".."); no "/" or "\\" anywhere; ".." rejected separately.
_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_LABEL_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.:-]{0,63}$")
_PROJECT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,254}$")
_SHA_RE = re.compile(r"^[0-9a-fA-F]{7,64}$")
_MAX_TARGET_SERVICES = 500

_NAME_KEYS = ("service_name", "service_branch", "host_name", "test_host", "limit")
_BOOL_KEYS = ("dry_run", "manual", "adopt_existing", "skip_bootstrap", "skip_service_deploy",
              "skip_rollout", "approve", "force")
_LABEL_KEYS = ("trigger", "source")
_INT_KEYS = ("plan_job_id", "source_pipeline_id", "followup_of")
_SHA_KEYS = ("plan_controller_sha",)
#: Every field a job payload may carry. Anything else is refused (see the module
#: docstring) — except ``_``-prefixed transport fields of the bus (``_origin``).
KNOWN_KEYS = frozenset({"pipeline_type", "target_services", "project_path", "plan_expected",
                        *_NAME_KEYS, *_BOOL_KEYS, *_LABEL_KEYS, *_INT_KEYS, *_SHA_KEYS})


class InvalidJobPayload(ValueError):
    """The payload must not become (or be executed as) a job."""


def is_safe_name(value: Any) -> bool:
    """Is ``value`` usable as a service/host/branch/limit token?"""
    text = str(value) if value is not None else ""
    return bool(_NAME_RE.fullmatch(text)) and ".." not in text


def _name(value: Any, what: str) -> str:
    text = str(value).strip() if value is not None else ""
    if not is_safe_name(text):
        raise InvalidJobPayload(f"invalid {what} {text[:60]!r}: only [A-Za-z0-9._-], "
                                "starting with a letter or digit, no '..'")
    return text


def _flag(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def strip_unentitled(payload: dict, entitlements: Iterable[str] = ()) -> tuple[dict, list[str]]:
    """Copy of ``payload`` without the privileged fields its origin may not set.

    Returns ``(payload, dropped)``. Without ``trigger`` entitlement the trigger
    becomes :data:`UNTRUSTED_TRIGGER`, so the job is governed by the deploy
    policy whatever the sender claimed.
    """
    ents = set(entitlements or ())
    out = dict(payload or {})
    dropped: list[str] = []
    for key, ent in (("approve", ENT_APPROVE), ("force", ENT_FORCE)):
        if key in out and ent not in ents:
            if _flag(out.get(key)):
                dropped.append(key)
            out.pop(key, None)
    if ENT_TRIGGER not in ents:
        if out.get("trigger") not in (None, "", UNTRUSTED_TRIGGER):
            dropped.append("trigger")
        out["trigger"] = UNTRUSTED_TRIGGER
    return out, dropped


def validate_structure(payload: Any) -> dict:
    """Normalised, allow-listed copy of ``payload``; raises :class:`InvalidJobPayload`.

    Unknown fields are refused (never silently dropped — see the module
    docstring). Idempotent, so the claim-time re-validation of a stored payload
    yields the same payload.
    """
    from .queue import normalize_payload  # local: queue imports this module

    if not isinstance(payload, dict):
        raise InvalidJobPayload("payload must be an object")
    # No silent default: an empty or type-less event must not become a job (the
    # only type-less form accepted is the legacy raw GitLab push, normalised below).
    legacy_push = not payload.get("pipeline_type") and payload.get("object_kind") == "push"
    if not payload.get("pipeline_type") and not legacy_push:
        raise InvalidJobPayload("pipeline_type is required")
    if not legacy_push:  # a raw GitLab body is all foreign fields; only what is derived from it is kept
        unknown = sorted(str(k) for k in payload if k not in KNOWN_KEYS and not str(k).startswith("_"))
        if unknown:
            raise InvalidJobPayload(
                f"unknown field(s) {', '.join(repr(k[:40]) for k in unknown[:5])} — refused rather than dropped: "
                "a field this plugin version does not know may be what limits the job")
    src = normalize_payload(payload)
    ptype = str(src.get("pipeline_type") or "")
    if ptype not in PIPELINE_TYPES:
        raise InvalidJobPayload(f"unsupported pipeline_type {ptype[:40]!r}")
    out: dict = {"pipeline_type": ptype}

    for key in _NAME_KEYS:
        if src.get(key) not in (None, ""):
            out[key] = _name(src[key], key)
    if "target_services" in src and src["target_services"] is not None:
        services = src["target_services"]
        if not isinstance(services, (list, tuple)) or len(services) > _MAX_TARGET_SERVICES:
            raise InvalidJobPayload("target_services must be a list of service names")
        out["target_services"] = [_name(s, "target_services entry") for s in services]
    for key in _BOOL_KEYS:
        if key in src and src[key] is not None:
            out[key] = _flag(src[key])
    for key in _LABEL_KEYS:
        if src.get(key) not in (None, ""):
            text = str(src[key])
            if not _LABEL_RE.fullmatch(text):
                raise InvalidJobPayload(f"invalid {key} {text[:40]!r}")
            out[key] = text
    for key in _INT_KEYS:
        if src.get(key) not in (None, ""):
            try:
                out[key] = int(src[key])
            except (TypeError, ValueError):
                raise InvalidJobPayload(f"{key} must be a number")
    for key in _SHA_KEYS:
        if src.get(key) not in (None, ""):
            if not _SHA_RE.fullmatch(str(src[key])):
                raise InvalidJobPayload(f"{key} must be a commit id")
            out[key] = str(src[key])
    if src.get("project_path") not in (None, ""):
        text = str(src["project_path"])
        if not _PROJECT_RE.fullmatch(text) or ".." in text:
            raise InvalidJobPayload("invalid project_path")
        out["project_path"] = text
    if isinstance(src.get("plan_expected"), dict):
        # {env: "Plan: …" summary} of the reviewed plan (set by the approve route).
        out["plan_expected"] = {
            _name(env, "plan environment"): str(summary)[:200]
            for env, summary in list(src["plan_expected"].items())[:200]
        }

    # Privileged flags only where they mean something; a flag on any other type
    # is dropped rather than carried along for a later stage to misread.
    if out.get("approve") and ptype not in _APPROVABLE:
        out.pop("approve")
    if out.get("force") and ptype not in _FORCEABLE:
        out.pop("force")
    if ptype == "single_service":
        if not out.get("service_name"):
            raise InvalidJobPayload("single_service requires service_name")
        out["service_name"] = out["service_name"].lower()
        out.setdefault("service_branch", "main")
    return out


__all__ = [
    "ENT_APPROVE", "ENT_FORCE", "ENT_TRIGGER", "ALL_ENTITLEMENTS", "UNTRUSTED_TRIGGER",
    "PIPELINE_TYPES", "KNOWN_KEYS", "InvalidJobPayload", "is_safe_name", "strip_unentitled", "validate_structure",
]
