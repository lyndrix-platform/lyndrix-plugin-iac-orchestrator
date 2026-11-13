# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.7.0] - 2026-11-11
Service reconcile: an iac-controller change that alters a service's **rendered** entry (profile vars, secrets, host overrides) now reaches the service. Before, a controller push only ran the Terraform and baseline-role rules; `hosts.yml`/profile edits of service *configuration* triggered nothing (the rollout drift check compares service names only), e.g. the WAZUH_MANAGER change had to be deployed by hand on every host. Default **off** — double opt-in via the `service_reconcile` rule in `config_engine/pipeline_rules.yml` (iac-ansible-automation) plus the plugin setting. Recommended rollout: `notify` for a week, then `dry_run`, then `deploy`.

### Added
- Rule action `type: service_reconcile` (`stages/rules.py`, pure diff in `stages/service_diff.py`): after the publish commit, every changed `<site>/<stage>/ansible/inventory.yml` is diffed host by host (`all.hosts[*].services`, canonical JSON → order noise never triggers; the global inventory is skipped so no host counts twice). Result per changed service and stage: the hosts whose entry changed and the changed **key paths** (`config.vars.WAZUH_MANAGER`, `+service` for a new entry) — never values, the profiles carry plaintext secrets and job logs/notifications go to Discord. Hosts absent from the previous render are skipped (host_provision owns them); removed entries are reported only (orphan cleanup is still not implemented). The action never fails the carrying job.
- Setting category **Service Reconcile** (`IaCConfig.service_reconcile_*`, env `PLUGIN_IAC_ORCHESTRATOR_SERVICE_RECONCILE_*` wins over Vault): `service_reconcile_mode` `off` (default) / `notify` (bell + Discord, endpoint `service_reconcile`) / `dry_run` (jobs with `dry_run: true`, the labelled `--check` path of 1.5.6) / `deploy`; `service_reconcile_max_hosts` (10) and `service_reconcile_max_services` (3). Deliberately independent of `auto_apply` (infra only since 1.5.6).
- Dispatch (`dry_run`/`deploy`): **one `single_service` job per (service, stage)** with `trigger: controller-diff`, `target_hosts`, `changed_keys`, `inventory_commit`, `source_job_id`; branch `main` for prod, `dev`/`test` otherwise. `SingleServiceStrategy` honours `target_hosts` → `--limit h1:h2:&service_<name>` (also as `target_group` for the playbooks' `hosts:`), so the blast radius is exactly the changed hosts. `MaintenancePolicyStage` evaluates policy and override for those hosts only. `controller-diff` is in `POLICY_TRIGGERS`: prod services with `deploy_policy: window` are parked until the window ("run now" stays available). Queued through the service (`engine.enqueue_pipeline`), not the bus, so a `DuplicateJob` is visible. Jobs are not dispatched from a `rollout` pipeline (it deploys inline) — notify only there.
- Blast-radius guard + approval: more hosts than `max_hosts` in one job, or more services than `max_services` in one controller change → the change is stored as **pending** (`iac_orchestrator_state` row `pending_service_reconcile`, `app/controller/reconcile_store.py`, advisory-locked when the core offers it) instead of deployed. API: `GET /reconcile/pending`, `POST /reconcile/pending/{id}/approve` (body `{"dry_run": bool, "force": bool}`), `POST /reconcile/pending/{id}/dismiss`, `POST /reconcile/pending/approve-all`. The record is written **before** the dispatch, so a failing dispatch keeps the detection (status `pending`, reason `dispatch failed: …`) — the inventory diff that produced it is already consumed by the publish commit.
- Deferred queue: a job refused as `DuplicateJob` (the same service is queued/running — e.g. a CI deploy) is kept as **deferred** (`waiting_for_job`) and re-queued by `DeploymentEngine.drain_deferred_reconcile()` when a `single_service` job ends (pipeline end, and via the queue's `on_terminal` hook also orphan sweep). DB-based, works on every instance; a still colliding record stays deferred.
- Exclude list: setting `service_reconcile_exclude_services` (comma-separated, case-insensitive, default **`aac-hermes-agent`**; env `PLUGIN_IAC_ORCHESTRATOR_SERVICE_RECONCILE_EXCLUDE_SERVICES`) plus the optional rule parameter `exclude_services` (list or comma string in `pipeline_rules.yml`). An excluded service is never dispatched by service reconcile — not from the rule (no job, no pending/deferred record, does not count against `max_services`), not via `POST /reconcile/pending/{id}/approve` / `approve-all` (409, record stays for `/dismiss`), not from the deferred drain (record dropped). It is only logged and listed as `excluded` in the notification (`payload.excluded`, item outcome `excluded`, severity warning). Background: the single_service playbook tears the whole compose stack down and recreates it (`compose down` + `docker rm -f` by name prefix); for the AI agent on docker-gpu that is an outage mid-session, so a controller diff must not deploy it on its own. Clearing the setting excludes nothing. `GET /reconcile/pending` returns `excluded_services`. Note: the deploy chain itself has no per-service protection flag — `deployment_strategy.<branch>.enabled` (→ `deployment_enabled` gatekeeper in `cd_rollout_single_service.yml`) is `true` for aac-hermes-agent on `main`, so only this exclude list stands between a controller diff and a recreate.
- Notification endpoint `service_reconcile` (default on, internal + external): one message per controller change listing service, stage, host count, key names and outcome (queued job #, deferred, pending approval, notify only, excluded) plus the UI path.
- Tests: `tests/test_service_diff.py`, `tests/test_reconcile_store.py`, `tests/test_service_reconcile_engine.py`, service-reconcile cases in `tests/test_rules.py` (modes, grouping, branch mapping, thresholds, DuplicateJob → deferred, dispatch error never fails the stage, no values in logs/notifications, rollout pipeline notify-only, bus fallback).
- Docs: `docs/usage.md` (operating guide incl. reconcile), `ai/INSTRUCTIONS.md` (trigger model + pending approvals). Companion change: rule `Service Reconcile (changed service entries)` in iac-ansible-automation `pipeline_rules.yml`.

Based on 1.6.1 (includes every 1.5.x port and the runner-presence fix; the queue-clock test fix moved there).

## [1.6.1] - 2026-11-11
Forward-port of the complete 1.5.x hotfix line (1.5.1–1.5.7, core < 1.11) plus the runner-presence fix from the 2026-11-11 incident. Requires core ≥ 1.11.0 (API 1.13) like 1.6.0; no schema migration (the queue columns are added idempotently by `JobQueue.ensure_schema` as before).

### Added
- Explicit dry-run: job payload `dry_run: true` (`POST /deploy/service/{service}`, its `/gitlab` CI variant and `POST /pipeline` accept `"dry_run": true`) runs every Ansible stage of the job with `--check`. Dry-runs are labelled: job log banner `DRY-RUN`, `[DRY-RUN --check]` on each playbook, dry-run summaries, final step "Completed (Dry-Run, nothing changed)", "(Dry-Run)" in the notification title. Terraform is not affected (`infra_plan`/`state_check` are plan/read-only already). (Port of 1.5.6.)

### Changed
- Statistics read window 500 → 5000 jobs / 1500 → 15000 tasks (`STATS_JOB_WINDOW`/`STATS_TASK_WINDOW` in `app/controller/stats.py`): Overview KPIs, lifecycle phases and Terraform host states looked only at the newest 500 jobs. Job history itself is never pruned; only the read window grows. `GET /stats` and `GET /terraform/hosts` run their sync DB reads, disk scan and aggregation via `asyncio.to_thread`.

### Security
- UI build toolchain (dev dependencies only): `vite` 5.4.21 → 6.4.5, `@vitejs/plugin-react` range ^4.7.0 (already resolved 4.7.0), transitive `postcss` 8.5.32, `esbuild` 0.25.12, `browserslist` 4.29.5, `baseline-browser-mapping` 2.11.35, `source-map-js` 1.2.2, `nanoid` 3.3.20 — closes all 14 Dependabot alerts (GHSA-rq7h-c2jc-7f22, -698v-4r3x-3vcv, -587h-7wmj-h5pm, -v6wh-96g9-6wx3, -fx2h-pf6j-xcff, -4w7w-66w2-5vf9, -68fv-2mgg-jv7q, -w5vr-8v7q-w6rv, -73wf-gq98-2v4g, -fxqj-rqcc-2cmp, -r28c-9q8g-f849, -67mh-4wv8-2f99); `npm audit` clean. The shipped `ui_static/ui_bundle.js` builds byte-identical, so nothing changes at runtime. React stays on 18 (provided by the shell via window globals).

### Fixed
- **Runner reported dead while it was blocked on the database** (prod 2026-11-11, job #1387 `single_service:aac-vscode`): `run_pipeline` read the RUNNING jobs for its dispatch guard with a synchronous query on the event loop. The pooled connection's peer (DB VIP 10.100.1.160) had gone silent, so the query only returned after the kernel's TCP retransmit timeout — 16 minutes (`Lost connection to MySQL server during query ([Errno 110] Connection timed out)`). During that time the core's cluster heartbeat on the same loop stopped too: after the 30 s lease TTL every instance saw `docker-auto-runner` as stale, the plugin went `degraded — No runner instance online`, the ai_assistant preflight reported no `runner` role, and the job showed "Claimed by docker-auto-runner" without a single log line. The guard now reads the RUNNING rows once per dispatch via `asyncio.to_thread`, so a stalled connection stalls only that job, never the heartbeat. Companion core fix (pymysql `read_timeout`/`write_timeout` on the pool) is tracked in lyndrix-core.
- Terraform runner: tofu errors and exit codes are no longer swallowed. The TF container is spawned without auto-remove (the watcher removes it), stderr is folded into the job log, an EXIT trap names the failing step (init/import/plan/apply/show) with its exit code, and an unreadable `docker wait` result or a watcher exception is logged instead of failing silently. (Port of 1.5.1.)
- Adopt/import step ("step <import> failed with exit code 1" with nothing else in the job log, e.g. job #1335 docker-cerberus, adopt job #1321 onp-netp-iot): the guard piped `tofu state list` into `grep -q` under `set -euo pipefail`. grep quitting early SIGPIPEs tofu (measured ~1 in 13 runs on the runner) and a `$(… | grep)` without a match aborted the script — so every adopt of an untracked CT and a share of apply-eligible runs died silently since 1.4.3. The state list is now snapshotted to a file once, no pipeline can abort the step, and every decision is echoed into the job log (`[adopt] …`) together with tofu's own output. (Port of 1.5.2.)
- Node migration: a CT tracked under another node's module, or at the right address but with a stale `node_name` in state, is now `state rm` + re-imported from the live node instead of `state mv`'d. `state mv` kept the old `node_name`, so the refresh looked for the CT on the old node, dropped it and planned a create (onp-netp-prv). A failed re-import is fatal so an apply never tries to create an existing CT. (Port of 1.5.2.)
- `iac_core` generator (`resolve_profiles`): a host's partial `services[]` entry was appended next to the profile entry of the same name (the list unique-merge compares whole dicts), so the single-service rollout picked the profile entry (`| first`, override lost) while the full host rollout deployed both in sequence. Services are now merged by name — host (and later profiles) deep-merge on top. Generated inventory is unchanged today (no host carries duplicate names). (PR #28.)
- `_read_yaml_config` raised `NameError: logger` instead of logging when a YAML file could not be read (undefined name in the `except` branch).
- Test suite: `test_outside_window_parks_job_as_scheduled` pinned the queue clock — it turned red on its own once the calendar passed the fixed 2026-10-25 window.
- Every rollout started from the job queue aborted at once with `Skipped: rollout for '<host>' already running` (or `a rollout is already in progress` for a global rollout) and no logs: the rollout dispatch guard counted the job's own row, which the runner had already claimed and set RUNNING with tag `rollout:<host>`. It now excludes its own job id, like the single_service / host / infra guards do. This also broke the hand-off from `infra_apply` / `host_provision` to the host rollout of freshly created hosts: they were bootstrapped but never got their services. In-memory dispatch keys are released in an outer `finally`, so an error or cancel during pipeline set-up no longer blocks that target until the process restarts. (Port of 1.5.7.)
- Service deploys changed nothing while `auto_apply` was off: the Ansible runner appended `--check` to EVERY playbook run whenever the global `auto_apply` switch was off, so `single_service` deploys from GitLab CI/UI, rollouts, compliance and bootstrap ran as dry-runs. Some reported SUCCESS without changing anything (e.g. aac-kea-dhcp); others failed because check mode skips the local `rsync` in `cd_rollout_single_service.yml` (`lookup('file') … service.yml` missing, e.g. aac-renovate). `auto_apply` now gates only infrastructure applies: Terraform `apply` (`infra_apply`, rule `mode: apply`) and the Ansible actions the rule engine fires on its own (`baseline_role`, `baseline`, legacy rule playbooks). Explicit deploys (`single_service`, `rollout`, `host_provision` service deploy, `compliance`, `bootstrap_compliance`, bootstrap of newly created hosts) always run for real. A `--check` rollout no longer persists `last_known_good`. (Port of 1.5.6.)
- inventory_state reaches GitLab after every iac-controller change: an iac-controller push on main triggers `infra_plan` (controller CI `trigger-orchestrator-infra-plan`), which generated but never committed — iac-inventory-state had no commit since 2026-10-12 and the generated files were reset by the next sync. `infra_plan` now publishes like `rollout`/`infra_apply`/`host_provision`/`compliance`.
- The rule engine of `infra_plan` diffed the last commit some *other* pipeline had made (HEAD/HEAD~1) instead of the change it just generated. Rules now diff exactly the commit their own pipeline published.
- Single writer: sync → generate → commit/push run as one critical section per instance (`Prepare State` stage). Before, a concurrent pipeline's `Sync Repo: inventory_state` (reset --hard origin) could wipe another pipeline's generated files before they were committed.
- `.terraform_runs/` (incl. rendered `secrets.auto.tfvars`) is versioned in inventory_state with the generated tree — the repo lives on the on-prem GitLab only (not reachable from outside); SOPS encryption follows later. Only the terraform provider cache (`.terraform/`) is kept out via `.git/info/exclude`; the `.terraform_runs/` exclude rule 1.5.3 wrote there is removed on the next publish. The rule engine ignores `.terraform_runs/` paths, so run artifacts never trigger a rule. (Same as 1.5.4; supersedes the 1.5.3 un-tracking.)
- Commit only on a real diff; the message carries the iac-controller SHA, job id, pipeline and trigger (`ci: automated state update — iac-controller@<sha>`). A rejected push is detected (the core path reported `pushed` on a non-fast-forward reject), rebased onto the remote and retried; a conflicting concurrent generation fails the stage instead of silently diverging.
- `single_service`, `state_check` and `adopt_host` deliberately still do not commit: they do not run the rule engine, and committing there would consume an iac-controller change so the next `infra_plan`/`rollout` saw `no_changes` and skipped its rules. Supersedes the best-effort single_service commit of PR #16.
- "Clear Failed Jobs" and "Clear Statistics" failed with "see logs" (MariaDB 1451): since 1.4 every job has rows in `iac_orchestrator_job_logs` (FK on the job), which were never deleted before the job itself. Both now delete logs → tasks → jobs, in committed batches of 100 jobs (stays under the DB statement cap, retries on 1020/1205/1213).

## [1.6.0] - 2026-10-18

Maintenance windows (core API 1.13, `core.api.maintenance`; plan `lyndrix-core/features/maintenance-windows/plan-iac-orchestrator.md`). Requires core ≥ 1.11.0.

### Added
- **Effective maintenance windows are resolved on the runner and published.** `app/controller/maintenance_resolver.py` reads the iac-controller tree (`site_vars`, `hardware.yml`, `stage_vars`, `hosts.yml`) plus the cloned `service.yml` files and resolves windows, `deploy_policy` and incident rules per hypervisor, host and service instance (chain: `service.yml` + `stage_overrides` → controller `services[].maintenance` → host `maintenance` unless `inherit: false` → stage `maintenance_default` if `enabled` → site default → none; lists are replaced; invalid blocks are logged and skipped). After every `NativeGenerateStage` and on runner boot the engine emits **`maintenance:windows_published`** (`source: iac-orchestrator`, `replaces: true`, locally — never `cluster=True`) and stores the read model as snapshot `maintenance_effective` for every instance.
- **Service CIs.** `entity_registry:identify` now also carries one `service` item per host/service instance (`iac.service = "<host>/<service>"`, display `"<service> @ <host>"`; needs registry ≥ 0.3.0, an older registry simply yields no id) and the engine emits **`entity_registry:relate`** `runs_on` service → host. Requests are chunked (500 items). `iac:inventory_updated` gains `services[]` (with `ci_id`/`host_ci_id`) and the hosts' `maintenance`/`incidents` blocks; the monitoring inventory sync carries `ci_id` on hosts and services plus `host_ci_id` on services. `NativeGenerateStage` order: registry ids → monitoring sync → inventory event → maintenance publish.
- **Scheduled deployments (job status `SCHEDULED`).** New `MaintenancePolicyStage` right after `CloneServiceRepoStage`: a CI/webhook-triggered deploy (`trigger: gitlab-ci|webhook`) whose effective `deploy_policy` is `window` (default on stage `prod`) and that is not inside a window is **parked** until the next window start within `maintenance_schedule_horizon_days` (`JobQueue.park`: status `SCHEDULED`, `not_before`, `window_ci_ids`, `schedule_reason`, claim released). The claim takes due jobs again (`not_before <= now`, re-clone → fresh `service.yml`). No window within the horizon → warning in the job log, deploy at once **and** `maintenance:window_missing` (decision H; setting `maintenance_report_missing_window`). UI deploys, `force` and the reached `not_before` skip the evaluation. A newer trigger for a SCHEDULED job is **coalesced** (payload replaced, `start_time = now`); a UI deploy **promotes** it to run now.
- **Every executed deploy is a maintenance override** (`maintenance_deploy_marks_maintenance`, decision B): `maintenance:override_started` for the service + host CIs with `until = now + maintenance_max_override_minutes` and `override_ended` exactly once on every terminal path (pipeline end, abort, orphan sweep, container recovery, startup reconciliation).
- API: `GET /schedules`, `POST /schedules/{id}/run-now`, `DELETE /schedules/{id}`, `GET /maintenance-windows?site=&stage=&kind=&q=` (snapshot + evaluated state), `GET /maintenance-default` (read-only). Public CI status `GET /api/iac/deploy/service/{svc}/gitlab/status?accept=scheduled` → `{"status":"scheduled","not_before":…}`; without `accept` the compatibility mode answers `status: "success"` with `job_status: "SCHEDULED"` (old templates never turn red); `QUEUED` maps to `running` (was `unknown`).
- Settings category **Maintenance**: `maintenance_deploy_marks_maintenance` (true), `maintenance_max_override_minutes` (240), `maintenance_linger_minutes` (15), `maintenance_schedule_horizon_days` (30), `maintenance_report_missing_window` (true); env `PLUGIN_IAC_ORCHESTRATOR_MAINTENANCE_*` wins.
- React tab **Wartung / Maintenance**: scheduled deployments (run now / cancel), effective windows with filters and state badges, read-only default-window card per site/stage pointing to the controller file. `SCHEDULED` badge uses `--lx-state-maintenance` (falls back to paused). Overview tile + "scheduled: n" note on Active Pipelines.
- `ServiceDefinition` (`iac_core`) keeps unknown keys (`extra="allow"`) so `services[].maintenance` survives validation.
- Loop guard: a controller commit starting with `ci: maintenance` (reserved for the future SSoT-Manager default switch) syncs but never triggers a deploy.
- **Boot publish waits for the Entity Registry.** The runner's boot chain (registry ids → monitoring sync → `windows_published`) is retried with back-off (15/30/60/120/300 s) while the registry still answers `ok: false` ("database not ready"); meanwhile `windows_published` is **held back** instead of sending an empty `replaces: true` set that would wipe every consumer's windows (the UI snapshot is still refreshed). Seen on the dev bench: 108 items resolved, 0 published after a core restart.

### Changed
- `JobQueue.enqueue` is now `enqueue_ex(...).job_id`; `abort_queued()` keeps SCHEDULED jobs (`include_scheduled=True` to cancel them); `clear_all_jobs(keep_running=True)` keeps SCHEDULED; `live_summary`/SSE snapshot/health carry `scheduled`; `get_latest_job_for_pipeline_type` prefers a SCHEDULED job regardless of the time window.
- Decision C: the orchestrator stays execution-only — the `maintenance_default.enabled` switch is **displayed** from the controller snapshot, never written back from here.

## [1.5.0] - 2026-10-16
### Added
- AI assistant (core API 1.12): manifest `ai_instructions` (purpose) + `ai_instructions_route`; the full operating guide is served live at `GET /ai/instructions` from `ai/INSTRUCTIONS.md` (concepts, trigger model, adopt/plan/apply operations, guardrails).

## [1.4.3] - 2026-10-16
### Fixed
- Adopt/import of a container that moved to another Proxmox node: the guarded import step now finds the CT under its old per-node module (`module.lxc_<old>…ct["<host>"]`) and runs `tofu state mv` to the new address instead of importing a second entry — the old orphan broke `output "info"` on refresh (`var.containers[name]` → Invalid index) and would have been planned for destruction. Import still happens only when the CT is tracked nowhere.
- "Adopt Host" with a site name as `host_name` (job "Adopt Host onprem" → "No terraform.tfvars.json contains host 'onprem'") now runs the site-scoped adopt.

## [1.4.2] - 2026-10-15

### Fixed
- **MariaDB error 1020 on the job row** — progress updates (per Ansible task), the worker
  heartbeat and the runner snapshot write the same `iac_orchestrator_jobs` row concurrently;
  under `innodb_snapshot_isolation=ON` that failed with "Record has changed since last read"
  and dropped progress updates. Every job/queue session now runs READ COMMITTED and the short
  writers retry on 1020/1205/1213 (`app/model/database.py`: `read_committed_session`,
  `retry_write`).

## [1.4.1] - 2026-10-15

### Fixed
- **A job interrupted by a runner restart (self-deploy of the Lyndrix stack) is no longer
  marked FAILED although its Ansible run succeeded.** The worker's orphan sweep ran six
  seconds after boot, before the coordinator listed the restarted instance, and judged the
  instance's own job orphaned. Now: own jobs are never orphan candidates, the first sweep
  waits 90 s, the live-instance set is only trusted once it contains this instance, and a
  job whose runner process died is closed from its ``aac-runner-*`` containers (re-attach
  while running, exit code + log tail when finished — `finalize_from_containers`) before
  "Runner lost" is ever written. A successful container also wins over a row that was
  closed as FAILED meanwhile.

## [1.4.0] - 2026-10-15

### Changed
- **Cluster control plane** (`placement="any"`, core API ≥ 1.7): the REST API, React data,
  GitLab webhooks/CI triggers, SSE stream and job logs now run on every core instance from the
  shared database. Triggers no longer execute in-process: they enqueue a job (`QUEUED`, with
  `payload` and a `dedupe_key`) and runner instances (`LYNDRIX_INSTANCE_ROLES` incl. `runner`)
  claim it atomically (`app/controller/queue.py`: `JobQueue`, `JobWorker`). Heartbeats every
  10 s; jobs of a dead runner are marked `FAILED` ("Runner lost").
- Job logs are streamed into `iac_orchestrator_job_logs` (the file on the runner host stays);
  `GET /jobs/{id}/logs` and the raw download serve from the database. Runner/task state is
  published to `iac_orchestrator_jobs.runners`; the SSE snapshot keeps its shape.
- Catalog, assignments, terraform hosts and generated inventory hosts are published as
  snapshots (`iac_orchestrator_state` `snapshot:*`) so instances without git working trees
  answer the same data.
- Stream tickets are signed with a key derived from `STORAGE_SECRET` (valid on every instance).
- `POST /abort` aborts queued jobs directly and asks the owning runner (cluster event) to cancel
  and kill its containers (`iac_job_id` label); the pipeline task is actually cancelled now.
- `POST /maintenance/sync-repos` is a `repo_sync` job executed by a runner.
- Webhook auto-sync runs on exactly one instance (lease `iac:webhook_sync`).
- `health()` reports `degraded` (and the plugin status banner says so) while no runner is online.
- Settings `auto_apply` reads through to Vault instead of a per-process flag.

### Fixed
- Concurrent pipelines no longer overwrite each other's final status through the shared
  `last_deployment` flag; every job records its own result.
- Startup reconciliation touches only this runner's jobs (plus ownerless legacy rows), never a
  peer's running jobs.

## [1.1.2] - 2026-08-03

### Fixed
- **Rotated Git tokens now take effect on existing service checkouts** — the service-repo sync (`stages/git.py`, "Clone Service Repo") baked the token into the origin URL at clone time, so after rotating the credential an existing local checkout kept fetching with the *old* token and failed with `HTTP Basic: Access denied` (then refused to deploy stale state). The token is now passed per-invocation via an HTTP auth header through `GIT_CONFIG_*` (never persisted to `.git/config`, never in argv/logs, always current), and `origin` is reset to the clean token-less URL before each fetch to drop any previously-baked stale token. Mirrors core's `git_service` credential handling.

## [1.1.1] - 2026-08-03

### Fixed
- **Git credential aliases can now contain hyphens and uppercase** — the Git Credential Manager rejected any alias not matching `^[a-z0-9_]+$`, which made it impossible to save a credential named after the hyphenated GitLab namespace (e.g. `gitlab-int`). The alias charset is now `^[A-Za-z0-9_-]+$` (hyphens are valid Vault KV keys), unblocking saving a new token to fix a failed repo sync. The reserved-key / `iac_` guard is now case-insensitive so the widened charset can't be used to slip past it.

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
