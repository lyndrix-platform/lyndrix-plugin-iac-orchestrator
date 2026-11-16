"""Job-log redaction (audit OS-07).

Job output is stored in the database, written to a host file, streamed to the
dashboard and quoted in notifications. It contains whatever the tools print:
tofu errors with provider arguments, Ansible ``--diff`` of rendered files, the
occasional ``-vvv``. Every line therefore passes a :class:`SecretRedactor`
*before* it is stored or streamed:

* **known values** — the secrets the job itself was given (Vault secret slots,
  git credentials, Terraform secrets, the credential-like values of the
  generated inventory / rendered service env). Exact-value replacement, longest
  first, so nothing about the secret's shape has to be guessed;
* **patterns** — credential shapes that are recognisable without knowing the
  value (URL userinfo, Authorization / PRIVATE-TOKEN headers, GitLab tokens).

Redaction is the second line of defence. The first one is not to print secrets
(``--diff`` is off for real runs, see ``ansible_diff_mode``); the third is the
log permission on the routes that serve job output.
"""
from __future__ import annotations

import re
from typing import Any, Iterable

from .public_projection import is_secret_key

MASK = "***"
#: Shorter values are not registered: masking every "admin" or "1234" in a log
#: destroys it without protecting anything worth the name.
MIN_SECRET_LENGTH = 8
#: Lines of a multi-line secret (PEM) are registered individually from this length.
MIN_SECRET_LINE_LENGTH = 16
_NEVER = frozenset({"changeme", "change_me", "password", "undefined", "localhost"})

_PATTERNS: tuple[tuple[re.Pattern, str], ...] = (
    (re.compile(r"(?<=://)[^/\s:@]+:[^@\s/]+(?=@)"), f"{MASK}:{MASK}"),
    (re.compile(r"(?i)(authorization\"?\s*[:=]\s*\"?(?:basic|bearer)\s+)[A-Za-z0-9._~+/=-]+"), r"\1" + MASK),
    (re.compile(r"(?i)(private-token\"?\s*[:=]\s*\"?)[A-Za-z0-9._~+/=-]+"), r"\1" + MASK),
    (re.compile(r"\bgl(?:pat|dt|rt|ptt|cbt|ft|soat|agent)-[0-9A-Za-z_-]{20,}"), MASK),
)


def secret_values_of(node: Any, _inside: bool = False, _depth: int = 0) -> list[str]:
    """String leaves of ``node`` that sit under a credential-like key (``secrets``,
    ``*_password``, ``vault_vars`` …) at any depth — the secret values of a
    generated inventory or a rendered service definition."""
    out: list[str] = []
    if _depth > 12:
        return out
    if isinstance(node, dict):
        for key, value in node.items():
            out.extend(secret_values_of(value, _inside or is_secret_key(key), _depth + 1))
    elif isinstance(node, (list, tuple)):
        for value in node:
            out.extend(secret_values_of(value, _inside, _depth + 1))
    elif _inside and isinstance(node, str) and node:
        out.append(node)
    return out


class SecretRedactor:
    """Replaces known secret values and credential-shaped text in log lines."""

    def __init__(self, values: Iterable[Any] = ()):
        self._values: set[str] = set()
        self._ordered: list[str] = []
        self.add_many(values)

    def __len__(self) -> int:
        return len(self._values)

    def add(self, value: Any) -> None:
        self.add_many([value])

    def add_many(self, values: Iterable[Any]) -> None:
        changed = False
        for value in values or ():
            if not isinstance(value, str):
                continue
            text = value.strip()
            candidates = {text}
            if "\n" in text or "\\n" in text:
                # A PEM key travels as real or escaped newlines and is printed
                # line by line: every body line is a secret of its own.
                unescaped = text.replace("\\n", "\n")
                candidates.add(unescaped.replace("\n", "\\n"))
                candidates.update(
                    line.strip() for line in unescaped.splitlines()
                    if len(line.strip()) >= MIN_SECRET_LINE_LENGTH and not line.strip().startswith("-----")
                )
            for candidate in candidates:
                if (len(candidate) >= MIN_SECRET_LENGTH and "\n" not in candidate
                        and candidate.lower() not in _NEVER and candidate not in self._values):
                    self._values.add(candidate)
                    changed = True
        if changed:
            self._ordered = sorted(self._values, key=len, reverse=True)

    def redact(self, text: str) -> str:
        if not isinstance(text, str) or not text:
            return text
        for value in self._ordered:
            if value in text:
                text = text.replace(value, MASK)
        for pattern, replacement in _PATTERNS:
            text = pattern.sub(replacement, text)
        return text


__all__ = ["MASK", "SecretRedactor", "secret_values_of"]
