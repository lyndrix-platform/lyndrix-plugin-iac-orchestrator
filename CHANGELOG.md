# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.8.0] - 2026-11-16
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

### Changed against the unreleased 1.7.0 draft of this feature (rebase onto the audit release 1.7.0)
- `target_hosts` and the provenance fields of a reconcile job (`stage`, `site`, `changed_keys`, `controller_sha`, `inventory_commit`, `source_job_id`, `approved_by`) are known to the job-payload validation. Every `target_hosts` entry must be a plain host name (no `:`, `&`, `!`, `*`, `,`): the list becomes the `--limit` pattern, so a pattern character could widen the deploy. `changed_keys` keeps key paths only. A raw GitLab push body never carries these fields.
- A host-limited job never absorbs a full deploy of the same service (or another host set) in the queue; the provenance fields do not make two triggers different.
- Reconcile jobs enter the queue through the same validation as every other trigger and pass the branch → target check on the runner. A service whose catalog prod branch is not `main` is refused there for stage prod.
- Reconcile database work runs on the plugin's own thread pool (`off_loop`), like the rest of 1.7.0.
- `bus` and `controller-diff` are both policy triggers.

### Fixed after the staging run of 2026-11-16
- A follow-up job for the same target was aborted ("Skipped: deployment for X already starting") when the queue claimed it while its predecessor was still in its post-run work: the in-memory dispatch key was released only after the terminal write plus drain, runner sync, log writer and cleanup. The key is now released right before the terminal write; until then the RUNNING row keeps the target busy. The deferred-reconcile drain of this release widened the window (2 of 2 follow-ups aborted on staging); the race exists in 1.7.0 too and is fixed there as 1.7.1. A job re-queued by the drain now lines up behind the job it waited for.
- `service_reconcile_exclude_services` saved empty was shown as `aac-hermes-agent` while nothing was excluded, and saving `aac-hermes-agent` back was skipped as "unchanged" — the protection stayed off. A stored empty value is now shown and compared as empty (`SettingField.empty_is_value`, this field only); a value that was never set still shows and applies the default.
- Docs: in mode `off` the detection summary is in the runner's container log, not in the job log. The exclude list acts at dispatch time — a reconcile job already queued when a service is added to the list still runs.

### Upgrade notes
- **Upgrade every instance together.** A 1.7.0 runner refuses a job that carries `target_hosts` ("unknown field", FAILED) instead of deploying to every host — safe, but the deploy does not happen.
- Default is still **off**; nothing changes until the rule and the setting are both enabled.
- The suite has 640 tests and passes on core 1.11.1 and 1.12.0. Staging run 2026-11-16 (modes off/notify/dry_run/deploy, thresholds, approval, deferred, exclude) against dummy services; not run against production GitLab or real services.

Based on 1.7.0.

## [1.7.1] - 2026-11-16
One fix on top of 1.7.0, found on staging while testing the next release. Python only — the React bundle is unchanged apart from the version number. No setting, schema or API change.

### Fixed
- A follow-up job for the same target (same service, same Terraform host, same rollout) could be handed back to the queue and, after three bounces, be **ABORTED** with "Skipped: deployment for X already starting". The job's in-memory dispatch key was released only after the terminal write plus the post-run work (runner sync, log writer, snapshot cleanup, publish); the queue claims the waiting follow-up the moment the row is final and its guard still found the key held. Staging 2026-11-16 on the 1.7.0 code path: 5 of 5 follow-ups succeeded, but 3 of them bounced two or three times (limit 3). The key is now released right before the terminal write (`_execute_dispatched`); until then the RUNNING row keeps the target busy (`claim_next` and the guard's RUNNING-row check), so there is no instant in which neither protects it. Keys are recorded per run, the release is idempotent, and the outer `finally` stays as the safety net (set-up errors, detached jobs) without ever freeing the key the next job reserved meanwhile.
- Tests: `tests/test_dispatch_key_release.py` (4 cases; the suite has 566 tests and passes on core 1.11.1 and 1.12.0).

## [1.7.0] - 2026-11-15
Security and cluster audit 2026-11, the findings of the independent review and the owner's decisions on OS-04, OS-05
and OS-06. The unreleased `feat/service-reconcile-1.7.0` became 1.8.0. Minimum core unchanged (core 1.11.1 /
API 1.13); newer core APIs are used only behind `getattr`. The React bundle must be rebuilt from this tree.
No credential has to be changed for the upgrade: the existing webhook token keeps working for every purpose.

### Upgrade notes
- **Runner images** outside `registry.gitlab.int.fam-feser.de/` and `ghcr.io/opentofu/opentofu` are refused at run
  time for every job. Compare the Ansible and Terraform image settings and the registry host with the allow-list
  before the rollout, or set `PLUGIN_IAC_ORCHESTRATOR_RUNNER_IMAGE_ALLOWLIST`.
- **Credential references** that point at a reserved key (`ansible_ssh_key`, `iac_*`) resolve to nothing; check
  `iac_gitlab_api_token_key` and every `repo_*_config.token_key`. A changed GitLab/base URL must be https and on an
  allowed host (`ALLOWED_GIT_HOSTS`, `ALLOW_INSECURE_URLS`).
- **New permissions:** `iac:settings_admin`, `iac:credentials_admin` (admin role), `iac:logs_read` (admin and operator
  roles). Plain plugin writers and scoped keys lose admin-tier settings, credentials, webhook-token generation,
  history deletion and job *log content* (status stays visible). Grant the roles first.
- **Upgrade every instance together and only while no job is RUNNING.** Nothing is resumed after a restart: an
  interrupted job ends FAILED "Interrupted by a runner restart … re-trigger".
- **Profile services follow the deploy policy** (traefik, cadvisor, wazuh-agent, …): on `prod` their CI deploys are
  parked for the window; without a window each deploy emits `maintenance:window_missing` and a maintenance override on
  every target host. Define prod windows or decide the behaviour for fleet services before the rollout.
- `POST /api/iac/infra/apply/gitlab` answers 404 unless `token_infra_apply_enabled` is set.
- Recommendation, the owner decides: treat the credentials that went through the monitoring sync as exposed (see
  Security, first item). The upgrade stops the exposure but does not invalidate them.
- **Runner containers get the repositories read-only** (`runner_readonly_mounts`, default on; off restores the mounts
  of 1.6.1). A playbook or role that writes into the checkout, or expects `.git` there, fails with "Read-only file
  system" — test on staging before the rollout. Staging confirmed the Ansible paths; the OpenTofu paths (run directory,
  provider cache) never ran with read-only mounts. **Switch `runner_readonly_mounts` off for the first production
  OpenTofu job (Check Env / plan / apply) and back on afterwards**, then run the next one with it on and watch it.
- **The Ansible runner image does not change.** The settings UI showed `…/iac-platform-assets/ansible-ci-image:latest`
  as default while the code ran `…/aac-template-engine:latest` (ansible-ci-image plus the template engine); the
  display now shows what runs (staging O-1). Nothing to do.
- **Branch → target is enforced.** Only the catalog's prod branch of a service deploys to all its hosts; `dev`,
  `*-dev` and `test` branches deploy to their stage only; any other branch is refused (HTTP 400 / refused job). A CI
  project whose `PROD_BRANCH` differs from the catalog is refused too.
- **A pending plan from before the upgrade cannot be approved** (HTTP 409) — run Check Env again.
- **Jobs have a maximum runtime** of 6 h (`job_max_runtime_seconds`, 0 = off). Raise it before a longer rollout.
- The NiceGUI page `/iac` needs `api:infra_apply`.
- New database objects, created idempotently under the bootstrap lock: job columns `claim_token`,
  `owner_incarnation`, `heartbeat_utc`, `exec_state`, `requested_by`; table `iac_orchestrator_locks`; state row
  `rules_applied`.

### Security
- **Outbound data is an allow-listed projection** (monitoring sync, bus events, repo snapshots): no service `config`
  block, no credentials, `terraform` reduced to placement facts. Before, hypervisor credentials and service secrets
  went to the monitoring plugin, which showed them to every reader.
- **Settings are validated per key and authorised per changed field** (tiers operator / settings admin / credentials
  admin / infra apply). Secrets are written only through `POST /settings/secrets/{key}`. Runner images, registry
  hosts, git hosts and credential references are checked again when they are used.
- Generating the webhook token, enabling `auto_apply`, `init_host` and the test-host deploy need `api:infra_apply`
  (a dry-run does not).
- Job payloads are validated when queued and again when claimed. `approve`, `force` and `trigger` are honoured only
  from entitled code paths; a trigger from the bus is untrusted and can never apply. A payload field this version
  does not know is refused, not dropped.
- Job output is masked before it is stored or streamed; `ansible --diff` runs only for dry-runs by default (setting
  `ansible_diff_mode`). Reading job logs needs `iac:logs_read`.
- Who triggered a job is recorded (`requested_by`); deleting job history needs `iac:settings_admin`.
- `GET /api/iac/health` returns only `{status, db_connected, runner_online}`; the token check is cached and off the
  event loop, and rejected calls are counted in the plugin health.
- Directly mounted public routes are removed on disable/unload.
- **One token per purpose, backward compatible (OS-04).** New secret slots `gitlab_hook_token`,
  `gitlab_service_ci_token`, `gitlab_controller_ci_token` (credentials tier). While a slot is empty the shared
  `gitlab_webhook_token` keeps authorising that purpose (deprecation line once per purpose and hour, hint
  `webhook_tokens` in the plugin health). With a slot set only that token works for its purpose; the service-CI token
  forces the prod branch and the hook token can no longer start a deployment with a `pipeline` body. Tokens are
  compared in constant time on bytes. After 20 rejected calls per source address in 60 s further rejected calls get
  429 with `Retry-After`; a valid token always passes. How to create and store the tokens:
  `documentation.md` → "Webhook-Tokens je Zweck".
- **Branch → target mapping (OS-05).** See the upgrade notes; payload flag `require_prod_branch`.
- **Approving a pending plan applies the plan that was reviewed, or nothing (OS-12).** The controller commit and a
  plan digest are stored with the pending plan; if the controller moved or the fresh plan differs, nothing is applied
  and a new pending plan is created. `GET /infra/pending-plan` gains `controller_sha` and `approvable`.
- **The generator and the render script no longer see the core's environment, and the bundled renderer runs in a
  Jinja2 sandbox (OS-06).** The plugin's git calls trust one repository per command and run no hooks, fsmonitor or
  credential helper; `safe.directory=*` is no longer written to the global git config (an existing entry stays).
- **Runner containers (OS-06):** Ansible gets `git_repos` and `services` read-only with `.git` masked, tofu gets only
  its per-job run dir read-write and the provider cache read-only (falls back to a run without the cache when a
  provider is missing), plus a scratch dir per job at `/data/scratch`.
- The NiceGUI page, its settings panel and its triggers check permissions; triggers carry the session user's
  entitlements and name, and the abort button uses the API stop path (OS-17).

### Changed
- **A dry-run is a property of the job:** Terraform only plans and Ansible runs with `--check`, whatever `approve` or
  `auto_apply` say.
- **A trigger for a busy target is no longer dropped.** It becomes a QUEUED follow-up, is merged into an identical job
  that has not started, or is refused and reported. Trigger responses carry `job_id`, `queued`, `outcome`
  (`queued | follow_up | coalesced | promoted | duplicate`), `waiting_for`, `duplicate_of`; HTTP stays 200. A run that
  carries `approve` is never queued behind another job.
- Claims are atomic and fenced (claim token + incarnation); a job closed elsewhere is never overwritten.
- Orphaned jobs are closed by a lease-based sweep on the control plane (`runner_lost: …`); nothing is re-run
  automatically. Settings `orphan_sweep_enabled`, `orphan_sweep_grace_seconds` (90),
  `orphan_sweep_container_grace_seconds` (1800).
- A plugin reload or a draining instance detaches running jobs instead of aborting them; the next runner start
  re-attaches to the containers.
- Terraform containers are named per job (`iac-tf-j<job>-<task>`), labelled and spawned through the docker CLI; run
  directories are per job. One lock per Terraform state (`tf:<site>_<stage>`): a second job waits
  (`tf_lock_timeout_seconds`, 1800), then fails "state in use by job #N". Abort stops tofu with `docker stop -t 30`.
- The rule stage evaluates everything published since the last completed rule stage; rule actions of a pipeline that
  failed after publishing are picked up by the next one. A dry-run leaves the changes pending; a failing evaluation is
  retried at most 3 times, then a "Rule actions skipped" notification lists the files.
- A due scheduled deploy whose window has closed waits for the next window; a deploy that cannot be parked fails
  instead of deploying outside the window. With several target hosts the strictest policy decides.
- A repository sync runs under the state lock; the NiceGUI "Resync Repositories" button queues a `repo_sync` job; a
  young `index.lock` is waited for, only a stale one (> 60 s) is removed.

### Fixed
- Runner success comes from the exit code and the Ansible recap, not from output substrings.
- "Global Adopt" / "Site Adopt" work again.
- A failed core repo sync writes the core's error text into the job log.
- Queue timestamps use UTC; the log writer is cancel-safe; a failed runtime-mount resolution is retried.
- `iac_core` tests run in the same invocation as the plugin tests.
- **Docker not answering is never "no containers" (review C1).** After a runner restart with an unreachable Docker
  daemon the adopted jobs stay RUNNING ("Runner restarted — container state unknown (Docker not reachable),
  retrying", bell entry "IaC runner — Docker not reachable") instead of being closed FAILED with their containers
  still running and the state lock free. The listing is retried at 5/10/20/40 s, then every 60 s, without an upper
  bound. Abort and fencing retry a failed container stop for about 30 minutes, then report "containers not stopped";
  the abort response gains `unstopped_jobs`, and such a job is not reported as aborted until its containers are gone.
- **A held lock counts as a container (review C2).** A job of a lost runner that holds a `tf:`/`rules` lock is swept
  only after `orphan_sweep_container_grace_seconds`, and the lock of a job that was closed without its runner is
  taken over only after the same grace — unless the runner itself confirmed that the containers are gone. A waiting
  job can therefore show "waiting … (job #N)" for up to 30 minutes.
- **A finished job's result survives a database blip (review C5).** The terminal write is retried on connection
  errors (0.5/1/2/4 s) and afterwards about every 20 s for up to 10 minutes; `mark_failed` is bound to the claim
  token, so a successful job is no longer flipped to FAILED by the recovery check.
- Teardown removes only the public routes of its own load, so a late teardown on core 1.11.1 cannot delete the routes
  of a version restored by the core's auto-revert (review R7).
- **No database, Vault or large-YAML work on the runner's event loop (review C3, OC-08).** Off-loop work runs in
  the plugin's own 12-thread pool, so hanging calls cannot starve the core's cluster heartbeat; progress writes are
  coalesced and a terminal write is never overtaken by a late progress write.
- **A job cannot stay RUNNING for ever (review C4).** After `job_max_runtime_seconds` the job is stopped and closed
  FAILED `max_runtime: …`; if the runner does not confirm within 120 s the job is closed with a note that containers
  may still be running and its locks stay held for the container grace. New notification `job_timed_out`.
- The rule-engine marker is a compare-and-set and only moves forward (review C6).
- Job-log lines survive a database error (buffer of 20 000 lines, a "[SYSTEM] [WARNING] N log line(s) are missing"
  marker when it overflowed); log age uses UTC; enqueue retries on 1020/1205/1213 (review C7).
- The abort endpoint keeps jobs whose containers could not be stopped attended and names them (`unstopped_jobs`, shown
  in the UI toast); a late teardown stops only its own service.
- **The test suite can no longer reach the host's Docker daemon (staging O-2).** One test ran the start-up
  reconciliation unmocked: on a runner host it listed the real runner containers and removed those whose job was not
  RUNNING in the test database. Tests now fail when they call `docker`, and a stub `docker` is first on `PATH` for
  the whole session. Never run the suite of an older tree on a runner host.
- A lease granted while the plugin is shutting down starts no new loop, so a cluster-wide disable no longer runs into
  the core's teardown timeout (staging O-3).
- A job closed as "System Restart (Aborted)" at runner start now removes its `.job_snapshots/job_<id>` directory
  (inventory snapshot and scratch) like every other terminal path (staging O-7).
- A `docker rm` that fails only because the container is already gone or being removed counts as stopped. Two stop
  paths racing for the same container no longer log "could not be stopped" and retry (staging O-8).
- The settings UI and schema show the Ansible runner image the code actually uses by default,
  `aac-template-engine:latest` (staging O-1). The code default is unchanged from 1.6.1.
- The suite has 562 tests (124 on 1.6.1) and passes on core 1.11.1 and on core 1.12.0.

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
