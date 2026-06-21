"""
Single source of truth for the generated Terraform container/provider shape.

The schema mirrors the ``proxmox_lxc`` module contract used by the downstream
infra-engine (see the reference ``infra-stack`` repo: ``modules/proxmox_lxc``).
Keeping the field list + defaults here means the mapper, the writer and any
future module only ever agree on one definition.

Secrets are intentionally NOT part of the generated state. The downstream
Terraform root injects ``ssh_key`` / ``root_password`` from its own secret vars
(exactly like the reference ``main.tf`` does:
``merge(c, { ssh_key = var.ssh_key, root_password = var.root_password })``).
This keeps credentials out of the generated, version-controlled inventory_state.
"""
from __future__ import annotations

# Sensible defaults for any proxmox LXC field the SSoT host does not specify.
# These match the reference module's expectations so a host that only declares
# node_name/vm_id/ip still produces a fully valid container object.
CONTAINER_DEFAULTS = {
    "arch": "amd64",
    "cores": 1,
    "memory": 2048,
    "swap": 512,
    "unprivileged": False,
    "keyctl": False,
    "nesting": True,
    "fuse": False,
    "mount": [],
    "start_on_boot": True,
    "ostype": "debian",
    "disk_storage": "local-lvm",
    "disk_size": 10,
    "bridge": "vmbr0",
    "mac": "",
    "nameserver": "",
    "searchdomain": "ad.fam-feser.de",
    "tags": [],
    "description": "",
}

# Fields a managed container MUST resolve to (from the host's terraform block)
# before it can be safely emitted. A host missing any of these is skipped with a
# warning rather than emitting a half-formed resource that could break a plan.
CONTAINER_REQUIRED = ("node_name", "vm_id", "ip")

# Optional fields copied straight from the host terraform block when present.
CONTAINER_PASSTHROUGH = (
    "node_name", "vm_id", "ip", "gateway", "vlan",
    "arch", "cores", "memory", "swap", "unprivileged", "keyctl",
    "nesting", "fuse", "mount", "start_on_boot", "ostype",
    "disk_storage", "disk_size", "bridge", "mac", "nameserver",
    "searchdomain", "tags", "mtu", "startup_order",
)

# Provider connection metadata that is safe to write to the generated state.
# Credentials (password/token/ssh_key) are deliberately excluded and must be
# supplied by the Terraform root via secret vars.
PROVIDER_PUBLIC_FIELDS = (
    "provider", "endpoint", "username", "realm", "auth_type", "ssh_agent",
)

# Field names that must never be serialised into the generated state file.
SECRET_FIELDS = ("password", "token", "ssh_key", "root_password", "secret")

# Top-level keys of the generated terraform.tfvars.json.
# NOTE: ``providers`` is a RESERVED Terraform variable name, so the provider
# connection map is exposed as ``proxmox_nodes`` to remain natively consumable
# as a tfvars variable by the downstream infra-engine.
KEY_PROVIDERS = "proxmox_nodes"
KEY_CONTAINERS = "containers"
