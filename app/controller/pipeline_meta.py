"""
Pipeline taxonomy / phase metadata.

Single source of truth that classifies a raw ``pipeline_type`` string (as stored
on :class:`IaCJob`) into a higher-level *deployment phase*. This is the modular
seam that lets new execution backends (Terraform today, others later) plug into
the dashboard without touching the UI: add a ``PhaseDef`` / matcher here and the
statistics, KPI cards and pipeline visualization pick it up automatically.

The orchestrator vision is a three-phase host lifecycle::

    1. PROVISION  (Terraform)  — bring the bare host into existence
    2. CONFIGURE  (Ansible)    — bring the host up to spec / connectivity
    3. DEPLOY     (Services)   — roll out the application services

``connectivity`` and ``rollout`` are existing Ansible-era types; ``single_service``
is a per-service deploy. Terraform types are defined ahead of implementation so
the UI is already able to display them.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List


# Canonical phase identifiers.
PHASE_PROVISION = "provision"   # Terraform
PHASE_CONFIGURE = "configure"   # Ansible
PHASE_DEPLOY = "deploy"         # Services
PHASE_OTHER = "other"


@dataclass(frozen=True)
class PhaseDef:
    id: str
    label: str
    icon: str
    color: str          # tailwind colour stem, e.g. "violet"
    description: str = ""


# Ordered so the dashboard can render the lifecycle left-to-right.
PHASES: List[PhaseDef] = [
    PhaseDef(PHASE_PROVISION, "Provision", "dns", "violet",
             "Terraform brings the host into existence."),
    PhaseDef(PHASE_CONFIGURE, "Configure", "tune", "sky",
             "Ansible brings the host up to spec."),
    PhaseDef(PHASE_DEPLOY, "Deploy", "rocket_launch", "emerald",
             "Application services are rolled out."),
]

_PHASE_BY_ID: Dict[str, PhaseDef] = {p.id: p for p in PHASES}
_PHASE_OTHER = PhaseDef(PHASE_OTHER, "Other", "category", "zinc", "Uncategorised pipeline.")


@dataclass(frozen=True)
class PipelineTypeDef:
    """Describes a concrete ``pipeline_type`` value and the phase it belongs to."""
    label: str
    phase: str
    icon: str
    color: str


# Known pipeline types. ``single_service`` is stored as ``single_service:<name>``
# so classification uses a prefix match (see :func:`classify`).
_KNOWN_TYPES: Dict[str, PipelineTypeDef] = {
    # --- Provision (Terraform + Ansible host bootstrap) ---
    # host_provision is the full host lifecycle pipeline: Terraform brings the
    # container into existence, then Ansible runs the root compliance bootstrap
    # and hands off to the host rollout. terraform_provision is kept as a legacy
    # alias so historical jobs still classify correctly.
    "host_provision":      PipelineTypeDef("Host Provisioning", PHASE_PROVISION, "dns", "violet"),
    "terraform_provision": PipelineTypeDef("Host Provisioning", PHASE_PROVISION, "dns", "violet"),
    # adopt_host imports an existing CT into Terraform state (then plans to verify),
    # bringing a manually-created container under management without recreating it.
    "adopt_host":          PipelineTypeDef("Adopt Existing", PHASE_PROVISION, "move_to_inbox", "violet"),
    "terraform_plan":      PipelineTypeDef("Terraform Plan", PHASE_PROVISION, "preview", "violet"),
    "terraform_destroy":   PipelineTypeDef("Terraform Destroy", PHASE_PROVISION, "delete_forever", "rose"),
    # infra_plan / infra_apply are the whole-infrastructure operations driven from
    # the Provision tab: "Check Env" (read-only plan across all environments) and
    # "Deploy Infra" (apply across all environments).
    "infra_plan":          PipelineTypeDef("Infra Plan", PHASE_PROVISION, "preview", "violet"),
    "infra_apply":         PipelineTypeDef("Infra Deploy", PHASE_PROVISION, "dns", "violet"),
    # --- Configure (Ansible) ---
    "connectivity": PipelineTypeDef("Connectivity Check", PHASE_CONFIGURE, "lan", "sky"),
    "rollout":      PipelineTypeDef("Full Rollout", PHASE_CONFIGURE, "public", "sky"),
    "bootstrap_compliance": PipelineTypeDef("Compliance Bootstrap", PHASE_CONFIGURE, "verified_user", "sky"),
    # --- Deploy (Services) ---
    "single_service": PipelineTypeDef("Service Deploy", PHASE_DEPLOY, "rocket", "emerald"),
}


def get_phases() -> List[PhaseDef]:
    """Return the ordered lifecycle phases for rendering."""
    return list(PHASES)


def get_phase(phase_id: str) -> PhaseDef:
    return _PHASE_BY_ID.get(phase_id, _PHASE_OTHER)


def classify(pipeline_type: str) -> PipelineTypeDef:
    """
    Map a raw ``pipeline_type`` string to its :class:`PipelineTypeDef`.

    Handles the ``single_service:<name>`` storage convention via prefix match and
    falls back to a generic "other" descriptor for unknown types so the UI never
    breaks on a new value.
    """
    raw = (pipeline_type or "").strip()
    if not raw:
        return PipelineTypeDef("Unknown", PHASE_OTHER, "help", "zinc")

    key = raw.split(":", 1)[0].lower()
    if key in _KNOWN_TYPES:
        return _KNOWN_TYPES[key]

    # Heuristic fallbacks keep forward compatibility with new naming.
    if key.startswith("terraform") or key.startswith("tf_"):
        return PipelineTypeDef(raw, PHASE_PROVISION, "dns", "violet")
    if key.startswith("ansible") or key in ("provision_config", "spec"):
        return PipelineTypeDef(raw, PHASE_CONFIGURE, "tune", "sky")
    if "service" in key or "deploy" in key:
        return PipelineTypeDef(raw, PHASE_DEPLOY, "rocket", "emerald")

    return PipelineTypeDef(raw, PHASE_OTHER, "category", "zinc")


def phase_of(pipeline_type: str) -> str:
    """Convenience: return just the phase id for a pipeline type."""
    return classify(pipeline_type).phase


# Maps IaCJobTask.task_name values (from host_provision sub-tasks) to lifecycle phases.
TASK_NAME_TO_PHASE: Dict[str, str] = {
    "terraform_provision": PHASE_PROVISION,
    "compliance_bootstrap": PHASE_CONFIGURE,
    "service_rollout": PHASE_DEPLOY,
}
