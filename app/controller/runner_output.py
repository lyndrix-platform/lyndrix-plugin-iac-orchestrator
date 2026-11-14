"""Verdict of a runner container (audit OS-14).

A job's success is the exit code of its container, cross-checked with Ansible's
PLAY RECAP — never the mere presence of a phrase in the output. Managed hosts and
services print into that output (task results, command stdout), so a substring
match let them turn a failed run into SUCCESS: ``CT 1 is already running`` inside
any Ansible task forced success, and so did ``no hosts to target``.

Two narrow, legitimate cases of "non-zero exit, still fine" remain, each tied to
the exact tool, step and message shape:

* **Terraform apply, Proxmox "CT <id> already running"** — the container exists
  and runs, which is the desired state. Accepted only for a tofu runner that was
  really applying, when the step that failed is ``apply`` (the runner shell's EXIT
  trap names it) and *every* ``Error:`` block of tofu is that error (and names
  the targeted host, for a host-scoped run).
* **Ansible, empty ``--limit`` intersection** — ansible-playbook refuses to start
  ("Specified inventory, host pattern and/or --limit leaves us with no hosts to
  target") before any play. Accepted only as that start-up error line, when no
  task ran and no recap was printed.
"""
from __future__ import annotations

import re

_RECAP_HOST_RE = re.compile(
    r"^(?P<host>\S+)\s*:\s*ok=(?P<ok>\d+)\s+changed=(?P<changed>\d+)\s+unreachable=(?P<unreachable>\d+)\s+failed=(?P<failed>\d+)"
)
_NO_HOSTS_RE = re.compile(
    r"^(ERROR!|\[ERROR\]:?)\s*Specified inventory, host pattern and/or --limit leaves us with no hosts to target"
)
_TF_ERROR_RE = re.compile(r"^[│|]?\s*Error: ")
_TF_STEP_FAILED_RE = re.compile(r"^\[TF\] step <(?P<step>[a-z_]+)> failed with exit code (?P<rc>\d+)")
_TF_ALREADY_RUNNING_RE = re.compile(r"CT\s+\d+\b.*already running", re.IGNORECASE)
# ``module.lxc_<node>.proxmox_virtual_environment_container.ct["<host>"]``
_TF_CT_HOST_RE = re.compile(r'proxmox_virtual_environment_container\.ct\["([^"]+)"\]')


class RunnerOutputParser:
    """Consumes the output lines of one runner container and decides its result."""

    def __init__(self, kind: str, *, tolerate_already_running: bool = False, tf_host: str | None = None):
        self.kind = "terraform" if kind == "terraform" else "ansible"
        self.tolerate_already_running = bool(tolerate_already_running) and self.kind == "terraform"
        self.tf_host = (tf_host or "").strip().lower() or None
        # Ansible
        self.successful_hosts = 0
        self.failed_hosts = 0
        self._in_recap = False
        self._recap_lines = 0
        self._tasks_seen = 0
        self._no_hosts_error = False
        # Terraform
        self.containers_created = 0
        self.created_hosts: list[str] = []
        self.hosts_to_create: list[str] = []
        self.hosts_to_destroy: list[str] = []
        self.plan_summary = ""
        self._tf_errors: list[dict] = []
        self._tf_block_open = False
        self._tf_failed_step: str | None = None

    # ------------------------------------------------------------------ input
    def feed(self, line: str) -> None:
        stripped = line.strip()
        if self.kind == "ansible":
            self._feed_ansible(stripped)
        else:
            self._feed_terraform(line, stripped)

    def _feed_ansible(self, stripped: str) -> None:
        if stripped.startswith("PLAY RECAP"):
            self._in_recap = True
            return
        if stripped.startswith(("TASK [", "RUNNING HANDLER [")):
            self._tasks_seen += 1
            self._in_recap = False
            return
        if stripped.startswith("PLAY ["):
            self._in_recap = False
            return
        if self._in_recap:
            m = _RECAP_HOST_RE.match(stripped)
            if m:
                self._recap_lines += 1
                if int(m.group("failed")) > 0 or int(m.group("unreachable")) > 0:
                    self.failed_hosts += 1
                else:
                    self.successful_hosts += 1
            return
        if self._tasks_seen == 0 and _NO_HOSTS_RE.match(stripped):
            self._no_hosts_error = True

    def _feed_terraform(self, line: str, stripped: str) -> None:
        step = _TF_STEP_FAILED_RE.match(stripped)
        if step:
            self._tf_failed_step = step.group("step")
            self._tf_block_open = False
            return
        if _TF_ERROR_RE.match(stripped):
            self._tf_errors.append({"already_running": bool(_TF_ALREADY_RUNNING_RE.search(stripped)), "hosts": set()})
            self._tf_block_open = True
        elif self._tf_block_open:
            if stripped.startswith(("│", "|")):
                block = self._tf_errors[-1]
                if _TF_ALREADY_RUNNING_RE.search(stripped):
                    block["already_running"] = True
                host = _TF_CT_HOST_RE.search(stripped)
                if host:
                    block["hosts"].add(host.group(1).lower())
            else:
                self._tf_block_open = False

        if "proxmox_virtual_environment_container.ct[" in line:
            host = _TF_CT_HOST_RE.search(line)
            name = host.group(1) if host else None
            if "Creation complete" in line:
                self.containers_created += 1
                if name and name not in self.created_hosts:
                    self.created_hosts.append(name)
            if name and not self._tf_block_open:
                if "will be created" in line or "must be replaced" in line:
                    if name not in self.hosts_to_create:
                        self.hosts_to_create.append(name)
                elif "will be destroyed" in line and name not in self.hosts_to_destroy:
                    self.hosts_to_destroy.append(name)
        if stripped.startswith("Plan:") and " to add" in stripped:
            self.plan_summary = stripped

    # ---------------------------------------------------------------- verdict
    def verdict(self, exit_code: int | None) -> tuple[bool, str | None]:
        """``(success, note)`` — ``note`` explains a result that differs from the
        plain exit code (for the job log)."""
        if exit_code is None:
            return False, None
        if exit_code == 0:
            if self.kind == "ansible" and self.failed_hosts > 0:
                return False, ("exit code 0 but the play recap reports failed/unreachable hosts "
                               f"({self.failed_hosts}) — treated as failed")
            return True, None
        if self.kind == "terraform" and self._tolerated_already_running():
            return True, ("State drift: Proxmox reports the container as already running — "
                          "the desired state is reached, continuing")
        if (self.kind == "ansible" and self._no_hosts_error and self._tasks_seen == 0
                and self._recap_lines == 0):
            return True, "matched no hosts (empty --limit) — skipped, nothing was run"
        return False, None

    def _tolerated_already_running(self) -> bool:
        if not self.tolerate_already_running or self._tf_failed_step != "apply" or not self._tf_errors:
            return False
        for block in self._tf_errors:
            if not block["already_running"]:
                return False
            if self.tf_host and block["hosts"] and self.tf_host not in block["hosts"]:
                return False
        return True

    def stats(self, exit_code: int | None) -> dict:
        return {
            "successful_hosts": self.successful_hosts,
            "failed_hosts": self.failed_hosts,
            "containers_created": self.containers_created,
            "created_hosts": list(self.created_hosts),
            "hosts_to_create": list(self.hosts_to_create),
            "hosts_to_destroy": list(self.hosts_to_destroy),
            "plan_summary": self.plan_summary,
            "exit_code": exit_code,
        }
