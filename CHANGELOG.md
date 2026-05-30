# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Overview statistics dashboard** — modern KPI row (total deployments, success rate, average duration, last deployment) plus a status breakdown and a recent-deployments feed at the top of the Overview tab. Auto-refreshes as jobs progress.
- **Host Lifecycle pipeline** visualization (Provision → Configure → Deploy) with per-phase health, so Terraform runs surface automatically once they exist.
- **Provision (Terraform) tab** — a modular, display-only readiness panel that scans site/host YAML for `terraform:` blocks and lists Terraform-managed vs. unmanaged hosts. Provision actions are present but disabled (clearly marked "coming soon") pending the engine stage.
- `app/controller/pipeline_meta.py` — single-source taxonomy classifying `pipeline_type` values into lifecycle phases (terraform/ansible/service); the modular seam for adding Terraform without touching the UI.
- `app/controller/stats.py` — pure, testable `DeploymentStats` aggregation (totals, success rate, durations, per-phase + per-status breakdown, recent feed).
- `app/ui/components.py` — reusable modern UI helpers (KPI cards, status badges, progress bars, section headers) shared across the dashboard.
- `app/ui/overview_dashboard.py` and `app/ui/terraform.py` — the new view modules.
- `JobDatabase.get_jobs_for_stats()` — lightweight, raw-typed job slice for statistics/duration math.
- **Modular Terraform state generator** (`iac_core/app/gen/terraform/`) — replaces the thin
  passthrough `terraform_gen.py` with a proper package (`schema` / `mapper` / `safety` / `writer`).
  Modelled on the reference `infra-stack` repo (proxmox_lxc module + per-environment tfvars), it
  maps every SSoT host with `terraform.is_managed: true` into a fully-defaulted LXC container object
  (roles/services carried through for the Ansible bridge) and every `hardware_host` with
  `terraform.is_used: true` into a provider connection entry. Output is the structured
  `terraform/terraform.tfvars.json` (`{ "proxmox_nodes": {...}, "containers": {...} }`) per environment.
- **Destroy-safety guard** — the generator now refuses to emit a Terraform state that would tear
  down live infrastructure. A wipe (non-empty → empty) or a removal beyond
  `PLUGIN_IAC_ORCHESTRATOR_TF_MAX_DESTROY_RATIO` (default 50%) raises and aborts the write unless
  `PLUGIN_IAC_ORCHESTRATOR_TF_ALLOW_DESTROY=true`. Writes are atomic (temp file + `os.replace`) and
  Terraform output is now **deferred** until every stage has parsed cleanly, so a racy or partial
  generation run can never commit a destructive partial state.
- **Secrets stay out of generated state** — `ssh_key` / `root_password` / `password` / `token` are
  never serialised; the downstream Terraform root injects them from its own secret vars.
- `iac_core/tests/test_terraform_gen.py` — unit coverage for the mapper and the destroy-safety guard.

### Changed
- `iac_core/app/generator.py` — Terraform generation is built per-stage in memory and written in a
  single guarded phase after a fully clean pass (gated on `error_count == 0`).
- `iac_core/app/gen/terraform_gen.py` — now a backwards-compatible shim re-exporting the new package.

## [0.3.0] - 2026-05-26

### Changed
- Refactored to the new Lyndrix Core plugin standard (`./app/` sub-package layout).
- `entrypoint.py` is now a pure wiring layer (manifest + lifecycle hooks only).
- All business logic consolidated behind a single `IaCService` controller (`app/controller/service.py`).
- Manifest `repo_url` corrected to the canonical `lyndrix-platform` repository URL.
- `app/ui/dashboard.py` and `app/ui/settings.py` now receive the `IaCService` object instead of raw engine/state/config arguments.
- `app/controller/api.py` `init_api` now accepts an `IaCService` instead of a raw `DeploymentEngine`.
- Replaced internal `core.logger` and `core.bus` imports with standard Python `logging` and `ctx.subscribe` respectively.

### Added
- `app/controller/service.py` — single shared service object composing `IaCConfig`, `JobDatabase`, and `DeploymentEngine`.
- `app/model/` — SQLAlchemy models and DB session helpers.
- `app/controller/` — business logic layer (engine, config, API router, utils, service).
- `app/ui/` — NiceGUI pages and widgets.
- `app/ui/widget.py` — extracted dashboard widget from `entrypoint.py`.
- `CHANGELOG.md`.
- `requirements-dev.txt` with the core development toolchain (pytest, pytest-asyncio, pytest-cov, mypy, ruff, black).
- `tests/` scaffold with a smoke test for `IaCService` construction.
- `examples/` directory with sample operator-provided configuration files.

### Fixed
- `repo_url` previously pointed to a personal fork (`marvin1309/lyndrix-iac-orchestrator`); now points to the canonical `lyndrix-platform/lyndrix-plugin-iac-orchestrator`.

## [0.2.9] - earlier

- Last release on the legacy flat layout.
