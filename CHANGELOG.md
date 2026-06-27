# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.9.0] - 2026-06-27

### Security / Changed
- **Production hardening pass** across the API, controller and React UI layers (input validation, auth scoping, safe git sync, engine robustness).
- **`POST /infra/apply` now requires the dedicated `iac:infra_apply` permission** (in addition to generic `api:write`), so operating a single service can never implicitly authorise a fleet-wide `tofu apply`/destroy. The master system key and superadmin role bypass both, as everywhere. (Confirmed consistent — B4.)

### Notes
- **External-services coupling is conceptual only** — this plugin makes no HTTP (or in-process) call to the external-services plugin, so the external-services API move to `/api/plugins/lyndrix.plugin.external_services/...` with required auth needs no caller-side change here (B3). No call site exists to migrate.

## [Unreleased]

### Added
- **All orchestrator settings are now fully API-controllable** — a single schema source of truth (`app/controller/settings_schema.py`) exposes every operator-tunable setting (Pipeline, GitLab Webhooks, Ansible, Terraform, and all six Repository Roles) as a typed schema, so nothing is NiceGUI-only anymore:
  - `GET /api/plugins/<id>/settings/schema` — typed field schema (kind/category/sensitive/options, credential selects resolved dynamically).
  - `GET /api/plugins/<id>/settings/values` — current values; **secrets are masked** and reported via a `<key>__configured` flag (never returned in plaintext).
  - `POST /api/plugins/<id>/settings/values` — persist values; a blank/sentinel secret **keeps** the stored value (never clobbers).
  - `GET/POST /api/plugins/<id>/settings/credentials` + `DELETE …/credentials/{alias}` — manage the Git credential registry that backs the credential select-boxes.
  - Persistence reuses the existing Vault keys (`iac_*`, `ansible_*`, `repo_<slug>_config`), so engine read-sites and behaviour are unchanged.
- **React settings UI for the new surface** — the orchestrator's React bundle (`/iac/settings`) now renders the Ansible, Terraform and Repository-Roles sections plus a Git Credential Manager generically from `/settings/schema`, matching the `lx-*` design system (sensitive fields show a "set — overwrite to change" placeholder).

### Fixed
- **Live log viewer no longer freezes the dashboard** — the job-log poller did synchronous file reads (up to ~1 MB on grep, 200 KB on seed) directly on the NiceGUI event loop every second and on every search keystroke, which blocked the loop ("connection lost"/slow UI). Disk I/O now runs in a worker thread (`asyncio.to_thread`); the 1 s poll is scoped to the open dialog (stops on close) and is job-aware (no stale-job lines after switching); the filter input is debounced.

### Added
- **Provision tab is now operational** — the *Provisioning (Terraform)* tab's construction notice is gone and its actions are wired:
  - **Check Env** (`infra_plan`): a read-only `tofu plan` across every Terraform environment (no `-target`), comparing the live infrastructure against the desired plan without changing anything. Unconfigured environments (missing secret/render inputs) are skipped, not failed, so configured environments still report cleanly.
  - **Deploy Infra** (`infra_apply`): an operator-approved `tofu apply` across every environment, guarded by a confirm dialog.
  - **Provision** (per host): the previously disabled per-host button now triggers a `host_provision` run (Terraform + Ansible compliance bootstrap + host rollout) behind a confirm dialog. Buttons are disabled while a pipeline is running.
- **`POST /api/iac/infra/plan` and `POST /api/iac/infra/apply`** — API endpoints for the whole-infrastructure plan/apply. `infra/apply` emits the explicit `approve` flag.
- **Settings → "Run Test Deploy" button** — below the *Test Deploy Allowed Hosts* input; provisions the allowlisted test host(s) (`host_provision`, approved) behind a confirm dialog for quick smoke testing.
- **Explicit-approval apply gate** — manual UI/API actions thread an `approve` flag that maps to `force_apply`, so operator-initiated applies run regardless of the `auto_apply` config, while the automatic webhook path stays gated by `auto_apply` (auto-enrollment remains disabled by default). `execute_terraform_docker` now accepts `targets`/`mode`/`force_apply`; new engine helpers `_iter_terraform_environments`, `execute_terraform_environment`, `execute_terraform_infra` and the `InfraPlanStage`/`InfraApplyStage` stages back the whole-infra operations.
- `app/controller/pipeline_meta.py` — added `infra_plan` ("Infra Plan") and `infra_apply` ("Infra Deploy") to the Provision phase taxonomy.
- **"Clear All Stats" button (Settings → Maintenance)** — new maintenance card in the plugin Settings UI with a confirmation dialog that wipes deployment job history (the data behind the Overview KPIs and recent-deployments feed) via the new `JobDatabase.clear_all_jobs()`. Currently RUNNING jobs are preserved so an active pipeline isn't disrupted.
- **Complete-reinstall redeploy** — when Terraform actually (re)creates a host's LXC container, the provision chain now clears that host's entry from the persisted `last_known_good` state (`InvalidateHostStateStage`) before handing off to the host rollout. The standard per-host drift check then sees the host's services as missing and redeploys exactly them on that host. Ordinary idempotent re-runs (no container created) are unaffected and keep the normal "no drift → deploy nothing" short-circuit. This fixes a freshly wiped-and-reprovisioned host coming back up with **zero** services because the stale `last_known_good` still listed them.
- **Drift now resolves profile-inherited services** — `DetectDriftStage` folds each host's `profiles:` service lists (from `global/03_profiles.yml`) into its desired state, matching the Assignments tab. A complete reinstall therefore redeploys the host's **full** service set (direct + profile-inherited), not just directly-assigned ones, and profile-driven service changes now register as drift. Only profile-using hosts are affected; direct-only hosts keep their exact prior behavior.
- **First-boot compliance bootstrap (root)** — after a Terraform provision the orchestrator now auto-runs the baseline compliance playbook (`cd_compliance.yml`) against the new host, connecting **as root** with the Terraform-injected key before the `ansible-agent` account exists. Once the host is bootstrapped it is handed off to the existing **Deploy Host** flow (`rollout` limited to that host — the same already-coded function the Assignments tab uses, including its per-host drift check) to deploy that host's services. Also exposed as a guarded standalone trigger `POST /api/iac/bootstrap/{host_name}`. Auto-chain can be skipped per run with `skip_bootstrap: true` / `skip_rollout: true`.
- **Root SSH private key in Settings UI** — new "Root SSH Private Key (bootstrap / first compliance run)" field on the Terraform Provisioning Secrets card, stored in Vault as `iac_tf_ssh_private_key`. It is the private counterpart of the injected public key and is used only for the initial root bootstrap connection.
- `execute_ansible_docker` / `AnsiblePlaybookStage` now accept `ssh_key_secret` and `remote_user`, so playbooks can run under different identities (e.g. root for bootstrap vs. ansible-agent for steady-state) without duplicating the runner logic.
- A best-effort TCP/22 readiness wait before the bootstrap run so a freshly-booted guest is reachable before Ansible connects.
- **Terraform guest credentials auto-sourced from `iac-controller` global vars** — the injected root public key (`ssh_key`) and `root_password` now resolve from `global_vars.vault_vars.root_pub_key` / `root_password` (the same values Ansible already uses), with multi-line/PuTTY-exported keys normalized into a single valid `authorized_keys` line.
- **Terraform provisioning secrets in Settings UI** — optional "Terraform Provisioning Secrets" card to store the root SSH public key (`iac_tf_ssh_key`) and root password (`iac_tf_root_password`) in Vault as a fallback when not present in `terraform_vars`/`global_vars`.
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
- **Guarded test-host deploy API** — new `POST /api/iac/deploy/test-host/{host_name}` trigger and
  GitLab-token webhook variant `POST /api/iac/webhook/gitlab/test-host/{host_name}`. Both start a
  `terraform_provision` run constrained to exactly one host (`host_name=<host>`). They refuse
  wildcard/pattern limits and only allow hosts explicitly
  listed in `PLUGIN_IAC_ORCHESTRATOR_TEST_DEPLOY_ALLOWED_HOSTS` (or Vault key
  `iac_test_deploy_allowed_hosts`), then validate that the host exists in generated inventory.
- **Pipeline settings UI** now includes `Test Deploy Allowed Hosts (comma-separated)` to manage the
  same allowlist from the dashboard.

### Removed
- **Legacy pre-`app/` root modules** — deleted the old flat-layout source files that were fully superseded by the `./app/` sub-package refactor (per the core Plugin Development Guide): `api.py`, `config.py`, `database.py`, `engine.py`, `models.py`, `ui_dashboard.py`, `ui_settings.py`. Their live counterparts are `app/controller/{api,config,engine}.py`, `app/model/{database,models}.py`, and `app/ui/{dashboard,settings}.py`. Also removed the never-wired `aac-client.py` (AWX/AAC `AACClient`, unreferenced) and a stray empty `lyndrix-core.code-workspace`. The root `utils.py` and `stages/` are retained — they are still imported by `app/controller/engine.py`.

### Changed
- **Overview & Provision tiles now match the app design system** — the KPI/lifecycle/breakdown/feed tiles (Overview) and host tiles (Provision) were rebuilt on the shared app card chrome used by the core dashboard and the IaC **Assignments** tab: sharp `lyndrix-card` surfaces with a top accent gradient stripe, accent-colored `font-mono` KPI numbers, and section headers led by the signature vertical gradient accent bar (replacing the soft `rounded-xl` pills and icon-only headers). Centralized via a new `components.tile()` helper and an updated `kpi_card`/`section_header`.
- **Renamed `terraform_provision` pipeline to `host_provision`** — the pipeline isn't solely Terraform: after the Terraform apply it also runs the Ansible root compliance bootstrap and hands off to the host rollout. Display label is now "Host Provisioning". The legacy `terraform_provision` `pipeline_type` is still accepted (normalized to `host_provision`) and still classifies correctly for historical jobs, so existing webhooks/buttons keep working.
- **Host-scoped rollouts are tagged `rollout:<host>`** — a rollout triggered with a specific host limit (the `host_provision` chain hand-off, or the Assignments "Deploy Host" button) is now recorded as `rollout:docker-dev` instead of a bare `rollout`, so the job list reads like the provision job that spawned it. Full/site rollouts keep their `rollout` / `rollout:<site>` tags. The one-rollout-at-a-time guard now matches any `rollout*` tag.
- `iac_core/app/generator.py` — Terraform generation is built per-stage in memory and written in a
  single guarded phase after a fully clean pass (gated on `error_count == 0`).
- `iac_core/app/gen/terraform_gen.py` — now a backwards-compatible shim re-exporting the new package.
- Rollout dispatch now accepts an optional `target_services` payload key and passes it into
  `AsyncBulkRolloutStage`, allowing safe single-host smoke tests without forcing full catalog rollout.
- `terraform_provision` is now executable in the engine via a spawned Docker runner (same model as
  Ansible): syncs `infra_engine`, renders a per-host Terraform root from generated tfvars
  (`render_environment.py`), then runs `init/plan` and (when auto-apply is enabled) `apply` with
  strict `-target` to the selected host resource.

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
