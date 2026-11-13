# Usage

The IaC Orchestrator is the GitOps control plane of the platform: it renders the
iac-controller SSoT with the bundled `iac_core`, publishes the generated `inventory_state`,
plans/applies infrastructure with OpenTofu and deploys AAC services with Ansible. Every
operation is a **job** in the shared queue (`QUEUED → RUNNING → SUCCESS|FAILED|ABORTED`,
parked deploys are `SCHEDULED`). All endpoints below live under
`/api/plugins/lyndrix.plugin.iac_orchestrator/` and need `plugin:…:api:read` (reads) or
`plugin:…:api:write` (actions); fleet-wide Terraform applies additionally need
`plugin:…:api:infra_apply`.

For the platform context this plugin runs in, see the
[Lyndrix Core documentation](https://docs.lyndrix.eu).

## Screens

- **Overview** — KPIs, active pipelines, queue/scheduled counts, Terraform host states.
- **Jobs** — history with live log stream (`GET /jobs`, `GET /jobs/{id}/logs`, SSE `/stream/jobs`),
  abort (`POST /abort`).
- **Catalog / Deploy** — one button per service (`POST /deploy/service/{name}`, body
  `{"branch": "main", "dry_run": false}`), test-host deploys, host bootstrap.
- **Infrastructure** — assignments (which host runs which service), Terraform hosts,
  "Check Env" (`POST /infra/plan`), pending plan review (`GET /infra/pending-plan`,
  `POST /infra/pending-plan/apply|dismiss`), "Deploy Infra" (`POST /infra/apply`).
- **Maintenance** — scheduled deploys (`GET /schedules`, `POST /schedules/{id}/run-now`,
  `DELETE /schedules/{id}`), effective maintenance windows (`GET /maintenance-windows`),
  read-only default switch per site/stage (`GET /maintenance-default`).
- **Settings** (header gear) — schema-driven (`GET /settings/schema`, `GET/POST /settings/values`):
  pipeline switches, GitLab webhooks, Ansible/OpenTofu images and keys, repository roles,
  maintenance windows, service reconcile. Settings shadowed by an OS env var are shown as locked.

## Trigger model

Every trigger — UI button, GitLab CI/webhook (`/api/iac/…`, token-checked), NiceGUI, the
engine's own hand-offs — ends up as an `iac:webhook_verified` event that is **enqueued** on
any instance and executed by a runner-role instance. Pipeline types: `single_service`,
`rollout`, `host_provision`, `init_host`, `state_check`, `adopt_host`,
`bootstrap_compliance`, `compliance`, `infra_plan`, `infra_apply`, `connectivity`.

Every pipeline starts with the shared prefix *sync repos → generate → publish
inventory_state* (commit only on a real diff). Pipelines that own the rule engine
(`infra_plan`, `infra_apply`, `rollout`, `host_provision`, `compliance`, …) then run
`config_engine/pipeline_rules.yml` against exactly the commit they published:

| Change in the controller | Rule action | What runs |
|---|---|---|
| host entry in `terraform.tfvars.json` | `terraform` | plan (prod) / apply-eligible (dev, test; gated by `auto_apply`) for the changed hosts only |
| host gained a baseline role | `baseline_role` | that role on that host |
| shared `group_vars` | `baseline` | full baseline across the group |
| a service's **rendered entry** (profile vars, secrets, host overrides) | `service_reconcile` | see below |

Service **add/remove** per host is still owned by the `rollout` pipeline's drift check
(service names only).

## Service reconcile (plugin ≥ 1.7)

A profile edit such as `WAZUH_MANAGER` lands in every `<site>/<stage>/ansible/inventory.yml`
the generator writes. The `service_reconcile` action diffs those files host by host
(`all.hosts[*].services`), groups the result per **service and stage** and reports the
affected hosts plus the changed **key paths** (`config.vars.WAZUH_MANAGER`,
`config.secrets.AGENT_KEY`, `+service`). Values never appear in logs or notifications.

Double opt-in:

1. the rule `Service Reconcile (changed service entries)` in `pipeline_rules.yml`
   (iac-ansible-automation, paths `*/*/ansible/inventory.yml`), and
2. the setting `service_reconcile_mode` (env `PLUGIN_IAC_ORCHESTRATOR_SERVICE_RECONCILE_MODE`):

| Mode | Effect |
|---|---|
| `off` (default) | detection only, summary in the job log |
| `notify` | bell + Discord (endpoint `service_reconcile`): service, stage, host count, key names, UI path |
| `dry_run` | one `single_service` job per service and stage with `dry_run: true` (`--check`), limited to the changed hosts |
| `deploy` | the same jobs as real deploys |

Recommended rollout: `notify` for a week, then `dry_run`, then `deploy`. The mode is
independent of `auto_apply` (infrastructure only).

Job shape (`trigger: controller-diff`): `service_name`, `service_branch` (`main` for prod,
`dev`/`test` otherwise), `target_hosts` → Ansible `--limit h1:h2:&service_<name>`,
`changed_keys`, `inventory_commit`, `source_job_id`. Prod services with
`deploy_policy: window` are parked until their maintenance window like any CI deploy
("run now" under Maintenance promotes them). The rule never fails the infra job; inside a
`rollout` pipeline it only notifies (the rollout deploys inline).

### Blast radius, approval, deferred jobs

| Setting | Default | Effect |
|---|---|---|
| `service_reconcile_max_hosts` | 10 | more hosts in one job → **pending approval** |
| `service_reconcile_max_services` | 3 | more services in one controller change (e.g. an edit to `docker_standard`) → every one pending |
| `service_reconcile_exclude_services` | `aac-hermes-agent` | comma-separated, case-insensitive: **never dispatched** — not from the rule, not after an approval (409), not from the deferred drain; only logged and listed as `excluded` in the notification |

Excluded services exist because a `single_service` deploy tears the whole compose stack down
and recreates it (`compose down` + `docker rm -f <name>*`). For the AI agent on docker-gpu
that is an outage mid-session, so a controller diff only reports it and the operator deploys
it deliberately via the single-service rollout. The rule itself can add more names
(`exclude_services: [aac-x, aac-y]` or a comma string on the `service_reconcile` action);
excluded entries do not count against `max_services`. Save an empty setting to exclude
nothing. There is no other per-service protection in the deploy chain:
`deployment_strategy.<branch>.enabled` in the service repo (→ `deployment_enabled` gatekeeper
in `cd_rollout_single_service.yml`) is `true` for aac-hermes-agent on `main`.

Pending and deferred records live in the shared DB (every instance sees them):

- `GET /reconcile/pending` → `{items[], count, pending, deferred, mode, excluded_services[]}`; each item carries
  `id, status (pending|deferred), reason, service, stage, site, branch, hosts, changed_keys,
  dry_run, waiting_for_job, inventory_commit, source_job_id`.
- `POST /reconcile/pending/{id}/approve` body `{"dry_run": bool|null, "force": bool}` → queues
  the job (`force` skips the maintenance window); an active job for the same service turns the
  record into `deferred`.
- `POST /reconcile/pending/approve-all` → every *pending* record.
- `POST /reconcile/pending/{id}/dismiss` → drop without deploying.

A job the queue refuses as duplicate (the same service is already queued/running, e.g. a
CI deploy) is kept as **deferred** and re-queued automatically when that job ends — on
whichever instance finishes it. A failed dispatch keeps the record as pending with the
error in `reason`, because the inventory diff that produced it is already consumed by the
publish commit and the next pipeline would see `no_changes`.

Removed service entries are reported only; orphan cleanup is not implemented.

## Dry-runs

Any `single_service`/`rollout`/`pipeline` request accepts `"dry_run": true`: every Ansible
stage runs with `--check`, nothing is changed, no deploy state is persisted, and the job is
labelled `(Dry-Run)`. Terraform is unaffected (`infra_plan`/`state_check` are read-only
already). `--check` is unreliable for `deploy_type: ansible` services — treat a green dry-run
as a hint, not as proof.

## Cluster notes

Control plane (API, UI data, webhook ingest, queue inserts) runs on every instance; jobs
execute only on instances with the `runner` role. Do not restart the runner while a job
runs (the orphan sweep fails it after 180 s). Readiness for load balancers is
`GET /api/ready`.
