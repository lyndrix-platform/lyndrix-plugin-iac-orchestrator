"""Explicit environments for the processes the plugin starts (audit OS-06).

The generator (``iac_core/app/generator.py``) and the infra-engine render script
execute content of repositories — Jinja templates of the iac-controller, the
templates of the infra engine. They used to inherit the whole environment of the
core process (``os.environ.copy()`` or no ``env`` at all): ``DB_PASSWORD``,
``STORAGE_SECRET``, ``LYNDRIX_MASTER_KEY``, ``LYNDRIX_SYSTEM_API_KEY``, Vault
tokens, … were one ``{{ … }}`` (or one line of a script in a repo) away.

Nothing is inherited implicitly any more. A child gets the variables named
here and what its caller adds on purpose.
"""
from __future__ import annotations

import os

#: What a child process needs to RUN — none of these carries a secret:
#:   PATH, HOME, USER, LOGNAME   find executables, a home for caches/dotfiles
#:   LANG, LANGUAGE, LC_*, TZ    stable text encoding, sorting and timestamps
#:   TMPDIR, TEMP, TMP           where temporary files go
#:   PYTHONPATH, PYTHONHOME, VIRTUAL_ENV, LD_LIBRARY_PATH
#:                               the interpreter finds its libraries (jinja2, yaml)
#:   PYTHONDONTWRITEBYTECODE, PYTHONUNBUFFERED, PYTHONIOENCODING
#:                               interpreter behaviour, as the core runs
BASE_VARIABLES = (
    "PATH", "HOME", "USER", "LOGNAME", "LANG", "LANGUAGE", "TZ", "TMPDIR", "TEMP", "TMP",
    "PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV", "LD_LIBRARY_PATH",
    "PYTHONDONTWRITEBYTECODE", "PYTHONUNBUFFERED", "PYTHONIOENCODING",
)
BASE_PREFIXES = ("LC_",)

#: Settings the bundled generator reads from its environment
#: (``iac_core/app/gen/terraform/writer.py``): the destroy guard of the tfvars.
GENERATOR_VARIABLES = (
    "PLUGIN_IAC_ORCHESTRATOR_TF_ALLOW_DESTROY",
    "PLUGIN_IAC_ORCHESTRATOR_TF_MAX_DESTROY_RATIO",
)

#: What git (and the ssh/curl it starts) additionally needs to reach a remote:
#: trust anchors of the internal CA and proxies. A proxy URL may carry
#: credentials — it is passed to git only, never to code from a repository.
GIT_VARIABLES = (
    "SSL_CERT_FILE", "SSL_CERT_DIR", "CURL_CA_BUNDLE", "REQUESTS_CA_BUNDLE",
    "GIT_SSL_CAINFO", "GIT_SSL_CAPATH", "GIT_SSL_NO_VERIFY",
    "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "ALL_PROXY",
    "http_proxy", "https_proxy", "no_proxy", "all_proxy",
)


def minimal_env(*, allow: tuple[str, ...] = (), extra: dict | None = None, source: dict | None = None) -> dict:
    """A fresh environment: the base variables, the ``allow``-listed ones (when
    set) and ``extra``. Everything else of the core's environment stays behind."""
    src = os.environ if source is None else source
    env = {name: src[name] for name in (*BASE_VARIABLES, *allow) if name in src}
    env.update({name: value for name, value in src.items() if name.startswith(BASE_PREFIXES)})
    env.update({str(k): str(v) for k, v in (extra or {}).items()})
    return env


def generator_env() -> dict:
    """Environment of the bundled generator."""
    return minimal_env(allow=GENERATOR_VARIABLES)


def render_env() -> dict:
    """Environment of the infra-engine render script (repository code)."""
    return minimal_env()


def git_env(extra: dict | None = None) -> dict:
    """Environment of a git process: no prompt, plus trust anchors and proxies."""
    return minimal_env(allow=GIT_VARIABLES, extra={"GIT_TERMINAL_PROMPT": "0", **(extra or {})})


def git_safety_args(repo) -> list[str]:
    """``-c …`` options for a git command the plugin runs in ``repo`` (audit
    OS-06 D); they go right after ``git``.

    * ``safe.directory=<this repository>`` — for this one command. The plugin
      used to write ``safe.directory=*`` into the global git config of the core
      process' user: every repository on the host counted as trusted, for every
      later git call of anyone.
    * No hooks, no fsmonitor program, no credential helper, no ``ext::``
      transport: programs a repository's own ``.git/config`` or ``.git/hooks``
      could name. This narrows what a tampered checkout can make git execute; it
      is NOT a complete defence against a hostile ``.git/config`` (filters,
      for instance) — the defence is that runner containers cannot write the
      checkouts at all (read-only mounts, setting ``runner_readonly_mounts``).
    """
    from pathlib import Path

    given = str(repo)
    paths = [given]
    try:
        resolved = str(Path(repo).resolve())
        if resolved != given:
            paths.append(resolved)                 # git compares with the real path
    except OSError:
        pass
    args: list[str] = []
    for path in paths:
        args += ["-c", f"safe.directory={path}"]
    return args + ["-c", "core.hooksPath=/dev/null", "-c", "core.fsmonitor=false",
                   "-c", "credential.helper=", "-c", "protocol.ext.allow=never"]
