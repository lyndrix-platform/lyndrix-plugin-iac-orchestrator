---
name: run-iac-orchestrator
description: Run, launch, and screenshot the IaC Orchestrator /iac dashboard plugin. This plugin has NO standalone run path — its only way to run is to boot the lyndrix-core dev stack with this repo volume-mounted and enabled, then drive the in-process NiceGUI /iac UI headless. Use to start/screenshot the /iac Overview/Provision/Service Catalog/Assignments/History dashboard or verify an iac-orchestrator UI change in the actually-running app.
---

# Run the IaC Orchestrator (/iac) plugin

This repo is a **lyndrix-core plugin**, not an app. There is no `main`, no server
of its own. The *only* way to run it is to boot the **lyndrix-core** dev stack
(FastAPI + NiceGUI + Vault + MariaDB) with this repo **volume-mounted** as
`/app/plugins/iac_orchestrator` and the plugin **enabled**, then open its UI route
**`/iac`**. NiceGUI renders over a websocket, so `curl` only sees an empty shell —
to *see* `/iac` you need a real browser. `driver.py` (a copy of the core skill's
Playwright/Chromium driver) logs in and screenshots `/iac`.

This is the **same harness** as the `run-lyndrix-core` skill — same compose stack,
same venv, same driver — just pointed at `/iac`.

Paths below are relative to this plugin repo root (`lyndrix-plugin-iac-orchestrator/`).
The core repo is its sibling `../lyndrix-core/`.

## Prerequisites

The driver runtime (Python venv + Chromium) lives in the **core** repo and is
shared. `node`/`chromium-cli` are NOT used (chromium-cli is not available on this
host). Create it once if missing:

```bash
python3 -m venv ../lyndrix-core/.dev/run-venv
. ../lyndrix-core/.dev/run-venv/bin/activate
pip install playwright
python -m playwright install chromium
sudo $(which python) -m playwright install-deps chromium   # system libs (libnspr4/libnss3/...)
```

## Bring up the stack (with this plugin mounted)

The plugin is already wired into `../lyndrix-core/docker/docker-compose.dev.yml`:

```
- ../../lyndrix-plugin-iac-orchestrator:/app/plugins/iac_orchestrator
```

Bring the stack up from the **core** repo (if `docker ps` already shows
`lyndrix-core-dev` on `:8081`, skip this):

```bash
docker compose -f ../lyndrix-core/docker/docker-compose.dev.yml up -d --build
```

## Ensure the plugin is enabled

The manifest sets `auto_enable_on_install=False` (`entrypoint.py`), so on a fresh
DB the plugin is discovered but starts **inactive** and `/iac` will not render.
Confirm it is active — it appears in `/api/health` when enabled:

```bash
curl -s http://localhost:8081/api/health | python3 -m json.tool | grep iac_orchestrator
# "lyndrix.plugin.iac_orchestrator": {
```

If it is **not** listed, enable it once via the Plugin Manager UI: open
`http://localhost:8081/plugins`, log in as `admin`, and toggle **"IaC Orchestrator"**
to **Active**. The activation state persists in the MariaDB volume
(`../lyndrix-core/.dev/db_data`), so it survives restarts. (Alternatively, add the
repo to `LYNDRIX_PLUGINS_DESIRED` in `../lyndrix-core/docker/.env.dev`.)

## Run (agent path) — screenshot /iac

```bash
cd .claude/skills/run-iac-orchestrator
. ../../../../lyndrix-core/.dev/run-venv/bin/activate
export LYNDRIX_ADMIN_PASSWORD="$(grep -E '^LYNDRIX_ADMIN_PASSWORD=' ../../../../lyndrix-core/docker/.env.dev | cut -d= -f2-)"
python driver.py --routes /iac --no-mobile --outdir shots
```

Output → `shots/iac.desktop.png` (plus `shots/login.desktop.png`). **Open the PNG
and look**: a correct `/iac` shot shows the cyan **"IAC ORCHESTRATOR"** header with
the **Overview / Provision / Service Catalog / Assignments / History & Logs** tabs,
deployment KPI cards (Total Deployments, Success Rate, Avg Duration, Last
Deployment), a Host Lifecycle (Provision → Configure → Deploy) panel, a Status
Breakdown bar, and a Recent Deployments list. If you instead see the generic
dashboard or a login card, the plugin is not enabled (see above) or the password
is wrong.

Useful variants:

```bash
python driver.py --routes /iac /plugins --no-mobile --outdir shots   # /iac + Plugin Manager
python driver.py --health-only                                       # no browser; print /api/health
```

## Also reachable as a React bundle (lyndrix-ui shell)

Besides the NiceGUI `/iac`, this plugin now ships a **React bundle**
(`src/ui/PluginApp.tsx` → `ui_static/ui_bundle.js`, manifest **v0.8.0**) that
renders inside the **lyndrix-ui** shell (same running stack, no extra setup).
Drive it with the sibling `run-lyndrix-ui` driver (run from this repo root):

```bash
node ../lyndrix-ui/.claude/skills/run-lyndrix-ui/driver.mjs \
  '/apps/lyndrix-plugin-iac_orchestrator/iac' /tmp/iac-react.png
```

> **safeId gotcha:** the shell route segment is the plugin id with dots → dashes
> *only* — `lyndrix.plugin.iac_orchestrator` → `lyndrix-plugin-iac_orchestrator`
> (the underscore stays). Using `…-iac-orchestrator` silently bounces to the
> Dashboard. Settings is `…/iac_orchestrator/iac/settings`.

A correct React shot shows the Overview with the **Provision / Configure / Deploy
phase tiles as standalone KPI-style tiles** (un-nested), whereas the NiceGUI
`/iac` still wraps the same three in a titled **Host Lifecycle** card — a handy
way to tell the two front-ends apart.

## Run (human path)

`docker compose ... up`, then browse to `http://localhost:8081/iac` and log in as
`admin`. Useless headless — that is what the driver is for.

## Gotchas

- **No standalone run.** This plugin cannot boot on its own; it always rides the
  core stack. Don't look for a server entrypoint here.
- **`/iac` renders the dashboard only when the plugin is Active** — `auto_enable_on_install=False`
  means a fresh DB needs a one-time toggle in `/plugins`.
- **Host edits are live.** The repo is bind-mounted into the container and uvicorn
  runs `--reload`, so editing `app/ui/...` here updates `/iac` without a rebuild.
- **NiceGUI is a websocket SPA.** `curl http://localhost:8081/iac` returns 200 with
  an empty shell, not the UI — only the browser driver shows real content. The
  driver waits ~2 s after login (NiceGUI session race) and retries once if a route
  bounces to `/login`.
- **Secrets stay out of the repo.** The driver refuses to run without
  `LYNDRIX_ADMIN_PASSWORD`; source it from `../lyndrix-core/docker/.env.dev`.
- **`shots/` + the venv are gitignored** — only `SKILL.md` and `driver.py` are committed.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `/iac` shot shows the generic dashboard or a login card | Plugin not Active — toggle "IaC Orchestrator" on in `/plugins`; verify with the `/api/health` grep above. |
| `error: set LYNDRIX_ADMIN_PASSWORD ...` | `export LYNDRIX_ADMIN_PASSWORD="$(grep -E '^LYNDRIX_ADMIN_PASSWORD=' ../../../../lyndrix-core/docker/.env.dev \| cut -d= -f2-)"`. |
| `libnspr4.so: cannot open shared object file` | `sudo $(which python) -m playwright install-deps chromium`. |
| `curl: connection refused` on :8081 | Stack not up — `docker compose -f ../lyndrix-core/docker/docker-compose.dev.yml up -d`. |

## Stop

```bash
docker compose -f ../lyndrix-core/docker/docker-compose.dev.yml down
```

Keeps the DB/Vault volumes (and thus the plugin's Active state) intact. Add `-v`
only for a fully clean boot (you will then re-enable the plugin and re-unseal Vault).
