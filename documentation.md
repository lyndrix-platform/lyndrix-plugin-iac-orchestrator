# IaC Orchestrator — Documentation

## Overview

The IaC Orchestrator is a standalone GitOps controller for running Terraform and Ansible pipelines. It receives webhook triggers from GitLab CI, performs drift detection, starts parallel provisioning pipelines and manages the synchronization of the SSoT repository (Single Source of Truth). Changes to SSoT or iac-controller automatically trigger rule-driven infrastructure deployments.

The orchestrator has a complete React UI with i18n support and is ready to use directly after installation (`auto_enable_on_install=True`).

---

## Architecture

```
lyndrix-plugin-iac-orchestrator/
├── entrypoint.py                      # Manifest + lifecycle hooks
├── locales/
│   └── iac.<locale>.json              # i18n translations (namespace: iac)
└── app/
    ├── api.py                         # Authenticated plugin router (via ctx.register_routes)
    ├── api/
    │   └── stream_router.py           # SSE job stream (HMAC ticket auth)
    ├── controller/
    │   ├── service.py                 # IaCService: pipeline execution, drift detection, Git sync
    │   ├── api.py                     # Internal authenticated API router
    │   └── webhook_router.py          # Public GitLab webhook router (no core auth)
    ├── model/
    │   └── models.py                  # SQLAlchemy: pipeline runs, jobs, SSoT state
    └── ui/
        ├── react/                     # React frontend (Vite bundle)
        └── nicegui/
            ├── dashboard.py           # NiceGUI dashboard
            ├── settings.py            # NiceGUI settings
            └── widget.py              # Compact dashboard widget
```

---

## Cluster operation: control plane and runner (from 1.4, core API ≥ 1.7)

The plugin is split into two tiers that work together through the shared database.
The manifest declares `placement="any"`:

**Control plane — on every core instance.** REST API, React data, GitLab webhooks and
CI triggers (`/api/iac/*`), SSE stream and job logs. Every trigger (UI, webhook, CI, NiceGUI,
the rollout handover of the engine) still arrives as `iac:webhook_verified` on the local
bus and is **queued** there: a row in `iac_orchestrator_jobs` with status `QUEUED`,
`payload` (JSON) and `dedupe_key`. A second trigger for the same target (same service,
same host, running rollout, infra plan/apply) is rejected with "Pipeline Ignored"
as long as a job for it is `QUEUED` or `RUNNING`. `iac:job_queued` (`cluster=True`) wakes the
runners immediately.

**Execution plane — only instances with the role `runner`** (`LYNDRIX_INSTANCE_ROLES`; in
single mode the instance has all roles). The `JobWorker` (`app/controller/queue.py`) atomically claims
the oldest `QUEUED` job (`UPDATE … WHERE status='QUEUED'`, `owner_instance`,
`claimed_at`), runs the engine (up to `iac_max_parallel_jobs`, default 3), sends a
heartbeat every 10 s and marks orphaned jobs every minute (owner no longer alive or
heartbeat older than 3 min) as `FAILED` "Runner lost". On startup, a runner only takes care of its own
`RUNNING` jobs (resume or abort) and of orphaned `aac-runner-*` containers on the host.

**What all instances see from the database:**

- Job logs: the executing runner additionally writes every line to `iac_orchestrator_job_logs`
  (batches every 0.5 s). `GET /jobs/{id}/logs` reads from there first (`source: "db"`,
  `offset` = next sequence number); the file `logs/job_<id>.log` stays on the runner host.
- Runner status: `active_tasks` (status + last 50 lines per task) is stored as JSON in
  `iac_orchestrator_jobs.runners`; the SSE stream builds the same response from it as before.
- Repo snapshots: service catalog, assignments, Terraform hosts and inventory hosts are published by the
  runner after every run to `iac_orchestrator_state` (`snapshot:*`); instances without
  Git working trees answer from it.
- Stream tickets are signed with a key derived from `STORAGE_SECRET` and are therefore
  valid on every instance behind the load balancer.

**Abort:** `POST /abort` sets `QUEUED` directly to `ABORTED` and sends
`iac:job_abort_requested` (`cluster=True`); the owning runner aborts its pipeline task
and removes the containers with the label `iac_job_id=<id>`. The calling instance closes jobs of a dead
runner itself.

**Repo sync** (`POST /maintenance/sync-repos`) is a job of type `repo_sync` and therefore
also runs on a runner. The GitLab webhook registration runs exactly once in the cluster
(lease `iac:webhook_sync`). If no runner is online, the plugin reports `degraded` ("pipelines are
queued, not executed") and so does `health()` — the API remains usable, jobs wait.

**Dedicated runner container (deployment pattern, from 1.4.1).** The runner runs as its own
container on the Docker host: the same core image with `LYNDRIX_INSTANCE_ROLES=runner`, its own
instance ID, its own plugin directory, the Docker socket and the volumes `storage`
(`git_repos`, `services`, job logs) and `security` (SSH key). The API instance of the same host
no longer needs the socket for this. In the aac world this is the dependency `aac-lyndrix-runner`
in the SSoT `aac-lyndrix-core` (controller override `LYNDRIX_RUNNER_INSTANCE_ID`, core roles
`api`). A redeploy of the stack also restarts the runner; a job interrupted by this is
closed after startup from its `aac-runner-*` containers (still running → reattached,
finished → exit code and log tail), never blanket-marked as "Runner lost". Own jobs are
never orphan candidates; the first sweep waits 90 s.

Still open: a standalone `lyndrix-runner` image and a Kubernetes executor (jobs instead of
Docker socket), job logs and working trees moved away from the runner's local host.

---

## API reference

### Public routes (no core auth)

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/iac/webhook/gitlab` | Receive GitLab webhook (validates `X-Gitlab-Token`) |
| `POST` | `/api/iac/deploy/service/{svc}/gitlab` | Service deploy from the service CI (`trigger: gitlab-ci` → deploy policy applies, see "Maintenance windows") |
| `GET` | `/api/iac/deploy/service/{svc}/gitlab/status?since_epoch=&accept=` | Status of the newest job: `success` / `failed` / `running` (also `QUEUED`). A parked job (`SCHEDULED`) returns `scheduled` + `not_before` only with `accept=scheduled`; without `accept` the compatibility value `success` with `job_status: "SCHEDULED"` (old CI templates never turn red) |

This router is mounted directly on the FastAPI application (not via `ctx.register_routes`), so that external GitLab/CI callers can get through without a Lyndrix user token. Every handler validates the `X-Gitlab-Token` itself.

Further public token routes: `POST /api/iac/webhook/sync` (re-register hooks), `POST /api/iac/infra/plan/gitlab` (Check Env from the controller CI), `POST /api/iac/webhook/gitlab/test-host/{host}` and `POST /api/iac/infra/apply/gitlab` (**off by default**, setting `token_infra_apply_enabled`).

### Webhook tokens per purpose (audit OS-04)

Previously **one** token (`gitlab_webhook_token`) authorized every public action; it sits in every project hook and in two CI variables. Each purpose now has its own secret slot:

| Purpose | Secret slot (setting key) | Routes | Where the token lives on the GitLab side |
|---|---|---|---|
| Group/project hooks | `gitlab_hook_token` | `POST /api/iac/webhook/gitlab` | "Secret token" of the hooks — written automatically into all project hooks of the group by the webhook sync |
| Service CI | `gitlab_service_ci_token` | `POST /api/iac/deploy/service/{svc}/gitlab`, `GET …/gitlab/status` | CI/CD variable from which the service pipelines fill the header `X-Gitlab-Token` (group of the service repos) |
| Controller CI | `gitlab_controller_ci_token` | `POST /api/iac/infra/plan/gitlab`, `POST /api/iac/webhook/sync`, `…/test-host/{host}`, `…/infra/apply/gitlab` | CI/CD variable from which the pipeline of the project `iac-controller` fills the header `X-Gitlab-Token` |

Rules:

- **Slot empty** → for this purpose the existing `gitlab_webhook_token` continues to apply, exactly as before. The log shows at most one `DEPRECATED` warning per hour and purpose; the plugin health shows under `webhook_tokens` which purposes still run on the shared token.
- **Slot set** → for this purpose **only** this token is accepted (the shared one no longer), and it applies to no other purpose.
- The code never changes, moves or deletes the existing `gitlab_webhook_token`.
- With its own **hook token**, a `pipeline` body on the hook route no longer starts a deployment (response 200, `triggered: false`). Reason: the hooks registered by the plugin only send merge request events (`gitlab_webhooks.py`: `push_events`, `pipeline_events`, `job_events`, `tag_push_events` = `false`); deployments are triggered by the service CI. As long as the slot is empty, the route behaves as before.
- With its own **service CI token**, the prod branch of the service per catalog is always deployed (see below); the `branch` in the body is then not a choice of the caller.
- **Failed attempts:** after 20 rejected calls from one source address within 60 s, further *rejected* calls from that address receive `429` with `Retry-After`. A valid token is always accepted — the limit cannot lock out the CI (all GitLab runners share one outbound address; a project with an outdated variable must not stop the other pipelines). Counting is done per instance in memory (max. 2048 addresses).
- **Recommendation (not enforced):** set `lyndrix_base_url` (setting) and the orchestrator URL in the CI variables to an `https://` address — otherwise the tokens travel in plain text over the network.

#### Branch → target (audit OS-05)

A service deploy (`single_service`) depends on the branch: only the **prod branch per service catalog** (`service_catalog.remote_git_management.remote_git_default_branch_prod`, default `main`; optional per service) goes to all hosts of the service (`service_<svc>`). The dev branch (`dev`, `*-dev`) goes only to `stage_dev`, the test branch (`test`) only to `stage_test`. Every other branch is rejected: HTTP 400 on the API/token routes, 200 with `triggered: false` on the hook route, and the runner checks again after the repo sync (job FAILED "Refused: …").

#### Handover to operations (Hermes): switching tokens later

**Nothing needs to be done for the upgrade itself.** Without slots set, everything continues to run with the existing token. The switch can happen later, purpose by purpose and at any interval. No existing credentials are changed or rotated.

For each purpose the same order applies — **first prepare the other side, then set the slot**, because once the slot is set only the new token applies:

1. **Generate a new token** (random, at least 32 characters, e.g. with a password generator). Do not store it in tickets, chats or repos.
2. **Controller CI** (`gitlab_controller_ci_token`):
   1. In GitLab, in the project `iac-controller` under *Settings → CI/CD → Variables*, set the variable that the job uses for `X-Gitlab-Token` to the new token (masked, protected).
   2. Immediately afterwards in Lyndrix: *IaC Orchestrator → Settings → GitLab Webhooks → "Token: controller CI"* save the token in the secret slot (permission `iac:credentials_admin`; API: `POST /api/plugins/lyndrix.plugin.iac_orchestrator/settings/secrets/gitlab_controller_ci_token` with `{"value": "…"}`).
   3. Verify: run the controller pipeline again (Check Env is triggered, HTTP 200).
3. **Service CI** (`gitlab_service_ci_token`):
   1. Set the CI/CD variable of the service pipelines (group of the service repos) to the new token.
   2. Immediately afterwards set the secret slot "Token: service CI".
   3. Verify: push a service to the prod branch → trigger 200, job runs. Prerequisite: `PROD_BRANCH` of the pipeline matches the prod branch in the catalog (today `main`).
4. **Hooks** (`gitlab_hook_token`):
   1. Set the secret slot "Token: GitLab group hooks".
   2. Trigger the webhook sync immediately (Settings → "Register webhooks" or `POST /api/iac/webhook/sync`); it writes the new secret into all project hooks of the group. Until the sync is complete, hook calls with the old secret are rejected (affects only merge notifications, not deployments).
   3. Verify: health → `webhook_tokens.hook = "dedicated"`, in GitLab under *Webhooks → Recent events* response 200.
5. Between step *.1 and *.2 of a purpose, calls with the respective "wrong" token fail with 401 — so run the two steps directly one after the other, not during a running rollout.
6. **Back:** a slot cannot be emptied, only overwritten. To return to the old behavior, enter the slot's token again on the other side (or have an administrator remove the vault key of the slot).
7. Once all three purposes report `dedicated`, the shared `gitlab_webhook_token` is no longer accepted by any route. Whether and when it is removed is decided by operations (rotation list) — the plugin does not do it.

### SSE stream (HMAC ticket auth)

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/plugins/lyndrix.plugin.iac_orchestrator/stream/ticket` | Request an HMAC ticket for the SSE stream |
| `GET` | `/api/plugins/lyndrix.plugin.iac_orchestrator/stream?token=<ticket>` | Live job stream (EventSource) |

`EventSource` cannot send an `Authorization` header. Instead, the client requests a short-lived HMAC-signed ticket and passes it as the query parameter `?token=`.

**Ticket format:** `{permission}:{expiry}:{sig}`

**v0.9.1 fix:** the parser was changed from `rsplit(":", 2)` to `rsplit(":", 1)` + `split(":", 1)`. The previous code failed for permissions that themselves contain a `:` (e.g. `api:read`), because `rsplit(":", 2)` then split the ticket into three instead of two parts and assigned the signature incorrectly.

### Authenticated plugin routes

All routes under `/api/plugins/lyndrix.plugin.iac_orchestrator/` require valid authentication.

| Method | Path | Permission | Description |
|---|---|---|---|
| `GET` | `/pipelines` | `api:read` | Pipeline run history |
| `POST` | `/pipelines/trigger` | `api:write` | Start pipeline manually |
| `GET` | `/pipelines/{id}` | `api:read` | Single pipeline run |
| `GET` | `/jobs` | `api:read` | Job list |
| `GET` | `/drift` | `api:read` | Drift detection results |
| `GET` | `/settings` | `api:read` | Current orchestrator settings |
| `PUT` | `/settings` | `api:write` | Update settings |
| `GET` | `/git/status` | `api:read` | SSoT repository sync status |
| `POST` | `/git/sync` | `api:write` | Force SSoT sync |
| `GET` | `/schedules` | `api:read` | Parked deployments (`SCHEDULED`): `id`, `service`, `branch`, `trigger`, `not_before` (ISO UTC), `window_ci_ids`, `schedule_reason`, `start_time` |
| `POST` | `/schedules/{job_id}/run-now` | `api:write` | "Run now": `SCHEDULED → QUEUED` with `force`; the override is created at run time |
| `DELETE` | `/schedules/{job_id}` | `api:write` | Cancel a parked deployment (`ABORTED`) |
| `GET` | `/maintenance-windows?site=&stage=&kind=&q=` | `api:read` | Effective windows per hypervisor/host/service from the snapshot `maintenance_effective`, each item with `state` (`active`, `until`, `source`, `next_start`) via `core.api.maintenance.effective_state` |
| `GET` | `/maintenance-default` | `api:read` | Read-only: `maintenance_default` per site/stage (`enabled`, `windows`, `source_file`, `level`) from the controller snapshot |
| `GET` | `/reconcile/pending` | `api:read` | Service reconcile (from 1.8): pending (`pending`) and deferred (`deferred`) detections from a controller diff; only key names, never values; contains `mode` and `excluded_services[]` |
| `POST` | `/reconcile/pending/{id}/approve` | `api:write` | Approve the entry and queue it as a host-limited `single_service` job; body `{"dry_run": bool, "force": bool}` (`force` skips the maintenance window); excluded service → 409 |
| `POST` | `/reconcile/pending/approve-all` | `api:write` | Approve all `pending` entries (same body) |
| `POST` | `/reconcile/pending/{id}/dismiss` | `api:write` | Discard the entry without deploying |

**Note:** the settings endpoint is called `/settings` (not `/prefs`), because `/settings` is reserved at core level and would cause a routing conflict. The paths `/maintenance/*` are plugin maintenance (delete statistics, repo sync) — the maintenance windows are therefore under `/schedules`, `/maintenance-windows` and `/maintenance-default`.

---

## Events

| Direction | Topic | Description |
|---|---|---|
| subscribe | `vault:ready_for_data` | Vault ready — load settings |
| subscribe | `db:connected` | Database connection ready |
| subscribe | `iac:webhook_verified` | Verified webhook — start pipeline |
| subscribe | `git:status_update` | Git sync status update |
| subscribe | `socket:response` | Socket response received |
| subscribe | `entity_registry:identified` | Response of the Entity Registry to `entity_registry:identify` (request/response via `ctx.request`) |
| emit | `iac:pipeline_started` | Pipeline was started |
| emit | `iac:webhook_verified` | Webhook accepted and forwarded |
| emit | `git:sync` | Request Git sync |
| emit | `git:commit_push` | Request commit + push |
| emit | `system:notify` | Platform notification |
| emit | `user:notify` | User notification |
| emit | `monitoring:inventory_sync` | Inventory sync to monitoring plugin |
| emit | `iac:inventory_updated` | Generic host list `{hosts: [...], services: [...]}` for all interested plugins — per host `name`, `hostname`, `ip`, `groups`, `baseline_roles`, `terraform` (**without** credentials), `entity_type`, `site`, `stage`, `maintenance`, `incidents`, `ci_id`; per service instance `host`, `name`, `site`, `stage`, `ci_id`, `host_ci_id` |
| emit | `entity_registry:identify` | Batch request to the Entity Registry (chunks of 500): UUID per host via `iac.host`, `pve.vm` (`node/vmid`), `fqdn`; per service instance via `iac.service = "<host>/<service>"` (Registry ≥ 0.3.0) |
| emit | `entity_registry:relate` | `runs_on` service → host for each service instance with both UUIDs (idempotent) |
| emit | `server_manager:register_server` | One upsert per host in the Server Manager (`source=iac-orchestrator`, `external_id=<host key>`, `writeback=false` → read-only there, with `ci_id`) |
| emit | `maintenance:windows_published` | Effective maintenance windows of all hypervisors, hosts and service instances (`source: iac-orchestrator`, `replaces: true`) — after every inventory generation and at runner start, emitted **locally** |
| emit | `maintenance:override_started` / `maintenance:override_ended` | Every executed service deploy marks the affected service and host CIs as "in maintenance" (`override_id = iac-job-<id>`, `until = now + max_override_minutes`); `ended` exactly once on every exit path |
| emit | `maintenance:window_missing` | A `deploy_policy: window` deploy ran immediately because no window lies within the horizon (`event_id = window_missing:<ci_id>:<job_id>`) — the ITSM creates a task from it |
| emit | `socket:request` | Send socket request |
| emit | `messaging:outbound` | Outgoing message via Messaging Gateway |

### Inventory publication

After every inventory generation (and at startup) the orchestrator publishes the host list
from `inventory_state/global/ansible/inventory.yml`:

1. **Credentials are removed.** The merged `terraform` block of a host can
   contain Proxmox password/token and the SSH key; all keys named `password`, `secret`,
   `*_token` or `*_key` (as well as `token`, `ssh_key`, `api_token`, `private_key`) do not
   go onto the bus.
2. **CI UUIDs.** Via `ctx.request("entity_registry:identify", {items})` every host receives its
   Entity Registry UUID (`ci_id`). Hypervisors (`physical_infrastructure` or `type: hypervisor`)
   are identified as `hypervisor`, everything else as `host`. If the registry is missing (timeout),
   `ci_id` stays empty — the next run fetches it.
3. **Server Manager.** Every host is fed in via `server_manager:register_server`:
   `environment_id = <site>:<stage>` (from the groups `site_*`/`stage_*`), `server_type`
   `physical` (hypervisor) / `container` (guest with `vm_id`) / `vm`, `writeback=false`, tags = groups.
   The records are read-only there — changes belong in the IaC repository.

Prerequisite: core API ≥ 1.4 (`ctx.request`); without the registry or Server Manager plugin the
steps run into nothing, the orchestrator itself remains unaffected.

Since 1.6 the orchestrator additionally identifies **service instances** (`entity_type: service`,
scheme `iac.service = "<host>/<service>"`, display name `"<service> @ <host>"`) and sets the
relation `runs_on` service → host. The order in `NativeGenerateStage` and at runner start
is fixed: registry IDs (hosts + services) → monitoring inventory sync (with `ci_id`/`host_ci_id`)
→ `iac:inventory_updated` → `maintenance:windows_published`.

---

## Maintenance windows and scheduled deployments (from 1.6, core API ≥ 1.13)

The orchestrator is the **source** of the maintenance windows for hypervisors, hosts and services
and the only component that schedules deployments based on them. Vocabulary, time evaluation and the bus
contract come from `core.api.maintenance`; the orchestrator does not store windows of other plugins.

### Resolution (`app/controller/maintenance_resolver.py`)

Runs only on runners (Git working trees). Sources: the iac-controller
(`environments/sites/<site>/{site_vars,hardware}.yml`,
`stages/<stage>/{stage_vars,stage,hosts}.yml`, values literal) and the `service.yml`
files cloned on the runner (`services_dir/<service>/service.yml`: `maintenance`, `incidents`,
`stage_overrides[<stage>]`). The more specific one wins; lists are **replaced**:

- **Service:** `service.yml` (+ `stage_overrides`) → controller `hosts[].services[].maintenance`
  → host `maintenance` (unless `inherit: false`) → stage `maintenance_default` (if `enabled`)
  → site `maintenance_default` → none.
- **Host:** own `maintenance` → stage default → site default → none.
- **Hypervisor** (`hardware.yml`, also cluster nodes): own `maintenance` → site default → none.
- **Incident rule:** own block → `incident_defaults[<kind>]` of the stage → of the site → none
  (`core.api.maintenance.resolve_incident_rule`).
- **`deploy_policy`:** service → stage default (`prod` → `window`, otherwise `immediate`).
- If `tz` is missing, `site_vars.timezone` applies, otherwise UTC. An invalid block is logged and
  **skipped** (the chain continues) — the resolution never aborts a deployment.

After every inventory generation the runner emits `maintenance:windows_published`
(`replaces: true`, only items with `ci_id`) and stores the read model as the snapshot
`maintenance_effective` in `iac_orchestrator_state` (all items, also those without `ci_id`, plus the
default switches per site/stage) — `GET /maintenance-windows` and
`GET /maintenance-default` answer from it on every instance.

### Decision in the job: `MaintenancePolicyStage`

The decision "immediate or scheduled" is made **in the job on the runner**, directly after
`CloneServiceRepoStage` (fresh `service.yml`), not at queueing time:

1. Target hosts = controller hosts whose `services[]` contain the service (branch `dev`/`*-dev`/`test`
   restricts the stage). The **first host (sorted) decides**; deviating hosts are noted in the
   job log.
2. The policy applies only to triggers `gitlab-ci` / `webhook` (`trigger` in the payload). UI deploys
   (catalog button), `force` ("Run now") and a due `SCHEDULED` job (`not_before` reached) run without a check.
3. `deploy_policy: window` and no running window → the job is **parked**: status
   `SCHEDULED`, `not_before` = next window start (within `maintenance_schedule_horizon_days`),
   `window_ci_ids`, `schedule_reason`; claim released, no final status, bell entry "⏳ scheduled".
   `claim_next` takes due `SCHEDULED` jobs (`not_before <= now`) before the normal queue and clones
   again. **No window within the horizon** (or none defined at all) → warning in the job log, deploy
   **immediately** and `maintenance:window_missing` (decision H; only with service CI and setting
   `maintenance_report_missing_window`). Without ITSM the job log warning remains.
4. **Every executed deploy is an override** (`maintenance_deploy_marks_maintenance`):
   `maintenance:override_started` for service and host CIs of the target hosts
   (`override_id = iac-job-<id>`, `until = now + maintenance_max_override_minutes`,
   `linger_minutes`), `override_ended` exactly once at the end — pipeline end, abort, orphan sweep,
   container recovery, startup reconciliation (`DeploymentEngine.end_override_for`; the
   `override_id` is taken atomically from the job row).

Queue rules: a second trigger for a `SCHEDULED` job is **merged** (payload replaced,
`start_time = now`, so that the second CI finds the job via `since_epoch`); a UI deploy
**promotes** it (`QUEUED`, `force`). `QUEUED`/`RUNNING` follow "first wins" (`DuplicateJob`).
`POST /abort` leaves `SCHEDULED` in place, `DELETE /schedules/{id}` cancels it; "delete statistics"
keeps `SCHEDULED` and `RUNNING`. Times in the job table are, as before, naive in the process clock
(`to_queue_clock`/`from_queue_clock`).

### Default switches read-only (decision C)

The orchestrator remains an executor: it shows `maintenance_default` per site/stage (`enabled`,
windows, source file) from the snapshot, but **never** writes to the controller repo. On/off is
edited in
`environments/sites/<site>/stages/<stage>/stage_vars.yml` (block `maintenance_default` under
`stage_vars:`, or `site_vars.yml`); the next sync publishes again. The
webhook path ignores deploy triggers for commits with the reserved prefix `ci: maintenance`
(the future SSoT manager switch), the sync continues to run.

### Known limits

- A service on multiple hosts: the window of the first target host (sorted) decides.
- Services the runner has never cloned inherit the host/stage default (no `service.yml`).
- `window_missing` is emitted per deploy job; the deduplication (one open task per CI) lies
  in the ITSM.
- Service CIs need the Entity Registry ≥ 0.3.0 (scheme `iac.service`); before that, service items
  stay without `ci_id` (only host overrides, no `window_missing`).

---

## Notification endpoints

| Endpoint | Default | Description |
|---|---|---|
| `deployment_started` | active | Pipeline was queued or started |
| `deployment_succeeded` | active | Pipeline completed successfully |
| `deployment_failed` | active | Pipeline completed with errors |
| `webhook_verified` | **inactive** | Webhook accepted and forwarded (enable only if needed) |
| `drift_detected` | active | Drift detection found deviations |
| `service_reconcile` | active | An iac-controller diff changed rendered service entries (from 1.8): service, stage, host count, key names and result (queued, deferred, waiting for approval, notice only, excluded) |

---

## Configuration & settings

**`auto_enable_on_install=True`** — the orchestrator is active directly after installation, because it configures itself and requires no manual pre-configuration.

The settings are accessible via the React UI under `/iac/settings` or via the REST API (`GET/PUT /settings`).

Category **Maintenance** (from 1.6; env `PLUGIN_IAC_ORCHESTRATOR_MAINTENANCE_*` takes precedence over Vault):

| Key | Type | Default | Meaning |
|---|---|---|---|
| `maintenance_deploy_marks_maintenance` | bool | `true` | Every executed deploy reports a maintenance override for service and host CIs |
| `maintenance_max_override_minutes` | int | `240` | Upper limit of a deploy override (`until`); ends earlier with the job |
| `maintenance_linger_minutes` | int | `15` | Lingering time of the override after `override_ended` at the consumer |
| `maintenance_schedule_horizon_days` | int | `30` | Maximum lead time of a parked `window` deploy |
| `maintenance_report_missing_window` | bool | `true` | Emit `maintenance:window_missing` if a `window` deploy without a window runs immediately |

The default switch `maintenance_default.enabled` is **not** a setting — it lives in the iac-controller (see "Maintenance windows").

Category **Service Reconcile** (from 1.8; env `PLUGIN_IAC_ORCHESTRATOR_SERVICE_RECONCILE_*` takes precedence over Vault; details and operation: `docs/usage.md`, English):

| Key | Type | Default | Meaning |
|---|---|---|---|
| `service_reconcile_mode` | choice | `off` | `off` (detection only, summary in the runner's container log) / `notify` (bell + Discord) / `dry_run` (jobs with `--check`) / `deploy`. Double opt-in: additionally the rule `service_reconcile` in `config_engine/pipeline_rules.yml`. Independent of `auto_apply` |
| `service_reconcile_max_hosts` | int | `10` | More hosts in one job → entry waits for approval (`pending`) |
| `service_reconcile_max_services` | int | `3` | More services in one controller change → all wait for approval |
| `service_reconcile_exclude_services` | text | `aac-hermes-agent` | Comma-separated, case-insensitive: these services are never deployed by the service reconcile (not even by approval or from the deferred queue); saving empty = nothing excluded |

### Service reconcile (from 1.8)

If an iac-controller push changes the **rendered** entry of a service (profile variables, secrets,
host overrides), this previously triggered nothing. The rule action `service_reconcile`
(`stages/rules.py`, diff in `stages/service_diff.py`) compares every changed
`<site>/<stage>/ansible/inventory.yml` host by host after the publish commit and reports, per service and stage,
the affected hosts and **key paths** (never values). In `dry_run`/`deploy` a `single_service`
job with `trigger: controller-diff` and `target_hosts` is created per (service, stage) (→ `--limit h1:h2:&service_<name>`,
only the changed hosts). `controller-diff` is a policy trigger (`deploy_policy: window` parks prod
jobs). If a job for the same service is already running (`DuplicateJob`), the entry stays `deferred` and is
automatically re-queued when that job ends. Within a `rollout` pipeline only a notification is sent.
State: row `pending_service_reconcile` in `iac_orchestrator_state` (`app/controller/reconcile_store.py`).

**Upgrade:** update all instances together — a 1.7.x runner rejects jobs with `target_hosts`
(`unknown field`, FAILED) instead of deploying to all hosts. The default remains `off`.

---

### Runner container: mounts (audit OS-06)

Setting `runner_readonly_mounts` (category *Runner*, default **on**; Vault `iac_runner_readonly_mounts`, env `PLUGIN_IAC_ORCHESTRATOR_RUNNER_READONLY_MOUNTS`):

| Container | Path in container | on (default) | off (as in ≤ 1.6.1) |
|---|---|---|---|
| Ansible | `/data/storage/git_repos` | **read-only**, every `<repo>/.git` covered by an empty read-only tmpfs | read-write |
| Ansible | `/data/storage/services` | **read-only**, every `<service>/.git` covered | read-write |
| Ansible | `/data/scratch` (`IAC_SCRATCH_DIR`) | read-write, per job (`git_repos/.job_snapshots/job_<id>/scratch`), deleted with the job snapshot | — |
| tofu | `/data/storage/git_repos/<run dir of the job>` | **only this directory**, read-write | whole `git_repos` read-write |
| tofu | `/data/storage/terraform-providers` | **read-only** | read-write |

Background: the containers execute content from repositories (playbooks, roles, service repos, providers). With write access to the checkouts they could modify other repositories, the generated inventory or `.git/hooks`/`.git/config` — git in the core executes these later.

Notes for operation:

- The Ansible container still builds in its own file system (`/tmp/aac_builds`) and deploys via SSH; the SSH key is read from `services/.iac_keys/`. A playbook that **must write into the checkout** fails with "Read-only file system" → either write to `/data/scratch` or switch the setting off.
- If the read-only provider cache lacks a provider (new version), the run repeats `tofu init` without the cache and installs into its run dir (job log: "init with the read-only provider cache failed — retrying without the cache"). This works, but is slower and downloads anew on every run. **Fill the cache deliberately:** briefly switch the setting off, run a plan (Check Env), switch it on again.

## React routes

| Path | Visible in sidebar | Description |
|---|---|---|
| `/iac` | Yes | Main view: dashboard, pipeline history, job overview, tab "Maintenance" (scheduled deployments with "Run now"/"Cancel", effective windows with filters, default windows read-only) |
| `/iac/settings` | No | Orchestrator settings |

---

## Internationalization

The plugin registers the i18n namespace `iac`. Translation files under `locales/iac.<locale>.json` are automatically added to the Lyndrix catalog when loaded. The React client fetches them via `GET /api/i18n/{locale}?ns=iac`.

---

## Development & tests

```bash
# From the plugin directory (lyndrix-plugin-iac-orchestrator/)
pip install -r requirements-dev.txt

# Run tests
pytest

# Type checking
mypy .

# Linter
ruff check .

# Check formatter
black --check .
```

The controller and model layers are testable without a running core. `ModuleContext` can be mocked for lifecycle tests. The public webhook router can be tested in isolation with pytest-httpx or the FastAPI test client.
