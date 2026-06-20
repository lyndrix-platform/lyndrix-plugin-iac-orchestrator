# Session notes — authoring run-iac-orchestrator / run-lyndrix-core

Log of what was hit while building + verifying the shared harness (2026-06-20).

## Environment / harness reality
- **`chromium-cli` does not exist here.** Not on PATH, and `npm view chromium-cli`
  returns 404 (no such package). The task framed the driver as a "chromium-cli
  heredoc"; the actual working harness in this repo set is the **Playwright/Chromium
  `driver.py`** already shipped in `lyndrix-core/.claude/skills/run-lyndrix-core/`.
  Used that (it is the established, committed tool) rather than inventing a
  non-existent CLI. `node`/`npx` exist but are not needed.
- Playwright venv already present at `lyndrix-core/.dev/run-venv`, Chromium already
  installed under `~/.cache/ms-playwright`. `pip install playwright` +
  `playwright install chromium` re-run clean (idempotent).

## Stack state
- The dev stack was **already running** (`docker ps`: `lyndrix-core-dev` :8081,
  `lyndrix-vault-dev` :8200, `lyndrix-db-dev`, `lyndrix-docs-dev` :8000) — no Vault
  unseal / DB-readiness fight this session. Vault stays unsealed across restarts via
  `LYNDRIX_MASTER_KEY` in `docker/.env.dev` + the persisted `.dev/vault_data` volume.
- `/api/health`: `core_version=0.1.3 api_version=1.2.0`, 5 active plugins including
  `lyndrix.plugin.iac_orchestrator`.

## Plugin enable
- Manifest: `id=lyndrix.plugin.iac_orchestrator`, `version=0.5.1`, `ui_route=/iac`,
  `auto_enable_on_install=False`.
- It was **already Active** in the persisted dev DB (toggled in a prior session via
  the Plugin Manager UI), so `/iac` rendered immediately — no enable step needed
  this run. On a fresh DB it would start inactive; enable via `/plugins` toggle
  (persists in `.dev/db_data`) or `LYNDRIX_PLUGINS_DESIRED`. No `LYNDRIX_PLUGINS_DESIRED`
  is set in `.env.dev`.

## Auth
- Driver needs `LYNDRIX_ADMIN_PASSWORD`; sourced from `docker/.env.dev` (26 chars).
  Login succeeded first try; no password fight.

## Result
- `/iac` loaded fully: IAC ORCHESTRATOR header + Overview/Provision/Service
  Catalog/Assignments/History tabs, KPI cards, Host Lifecycle, Status Breakdown,
  Recent Deployments. Real screenshot saved to `shots/iac.desktop.png` and viewed.
- No error→fix loop required: the stack and harness were both already in place; the
  work was verifying them end-to-end and writing the two SKILL.md files.
