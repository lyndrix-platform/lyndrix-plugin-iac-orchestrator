# IaC Orchestrator — AI operating instructions

## Purpose
Lyndrix's GitOps control plane for infrastructure: provisions Proxmox/Hetzner infra with Terraform/OpenTofu and deploys containerized "AAC" services with Ansible, driven from a declarative SSoT (the iac-controller repo). You operate it to run infra plans/applies, adopt existing containers into TF state, roll out individual services, and inspect state — never to hand-edit live infra.

## Core concepts
- Sites × stages: every infra path is `<site=onprem|hetzner>/<stage=dev|test|prod>`; promotion dev→test→prod.
- SSoT = iac-controller: hosts, hardware, networks, profiles, service catalog, inline `terraform:` blocks (node_name, vm_id). The bundled iac_core engine renders Ansible inventory + Terraform tfvars from it. Change infra by editing the controller SSoT and re-rendering — not the artifacts, not the live box — or the next plan proposes destroy+recreate.
- Per-node TF modules: the rendered root splits containers per node as `module.lxc_<node>.proxmox_virtual_environment_container.ct["<host>"]`, filtered by each host's node_name. Changing node_name moves the resource to another module.
- Cluster & runner model: the control plane runs on EVERY instance (API/UI/webhooks/queue inserts), but jobs execute only on the runner-role instance (holds the Docker socket to spawn tofu/ansible CI containers). DB job queue (QUEUED→claim→heartbeat); an orphan sweep marks a RUNNING job FAILED if its owner heartbeat is stale >180 s.
- Leases coordinate cluster roles/work (e.g. `iac:webhook_sync`), TTL ~30 s, renewed ~10 s.

## API — how to drive it
- Base `$LYNDRIX_API_URL`, auth `Authorization: Bearer $LYNDRIX_API_KEY`.
- Discover exact current endpoints/payloads live (don't rely on a frozen list): plugin index `GET /api/ai/plugins` + platform OpenAPI (`/openapi.json` or `/api/docs`).
- Trigger model: pipelines are kicked by a verified event the plugin consumes as `iac:webhook_verified`, e.g. `{"pipeline_type":"adopt_host","host_name":"<host>"}` (one host) or `{"pipeline_type":"adopt_host","limit":"<site>"}` (whole site). Other pipeline_types: infra plan, infra apply, single-service rollout, repo sync, terraform state check — confirm names via discovery. In-UI buttons emit the same events in-process; external callers use the configured webhook token header.

## Main operations
1. Inspect state (read-only, safe): run the Terraform State Check pipeline for a host (`tofu state list`) to see which `lxc_<node>` module a CT is tracked under. Do this first when a node migration may have happened.
2. Adopt existing containers (node-migration safe): pipeline_type `adopt_host`, mode=import — runs `tofu state mv` when a CT is tracked under a DIFFERENT node's module (self-heal after a node_name change) and `tofu import` when tracked nowhere. Never applies. Expect "[adopt] moving module.lxc_<old>…ct[...] -> module.lxc_<new>…ct[...] (node changed)" then a plan that should be 0 to add / 0 to destroy (in-place diff OK).
3. Infra plan/apply: plan first, read the FULL plan. A -targeted plan can HIDE orphan destroys — a targeted "0 to destroy" is NOT proof; inspect the untargeted plan for "will be destroyed". Apply only after verifying 0 to destroy (or every change is an intended in-place update).
4. Single-service rollout: deploys one AAC service's container(s) to its host via Ansible. A CI/webhook-triggered rollout follows the service's maintenance `deploy_policy` (below); a UI/API-triggered one runs at once.

## Maintenance windows & scheduled deploys (plugin ≥ 1.6, core API 1.13)
- The orchestrator is the SOURCE of maintenance windows for hypervisors, hosts and services: it resolves them from the iac-controller (`maintenance`, `maintenance_default` under `stage_vars:`/`site_vars:`, `incident_defaults`) plus each service repo's `service.yml` (`maintenance.windows`, `maintenance.deploy_policy`, `incidents`, `stage_overrides`) and publishes them on the bus (`maintenance:windows_published`) after every inventory generation. Read them via `GET /maintenance-windows?site=&stage=&kind=&q=` (each item carries `state.active/until/next_start`).
- Scheduled deploys: a CI-triggered deploy of a `deploy_policy: window` service (default on stage `prod`) outside its window is PARKED as job status `SCHEDULED` with `not_before`; the runner re-clones and runs it when the window opens. List them with `GET /schedules`; `POST /schedules/{id}/run-now` runs one immediately (this is an operator decision — ask first, it deploys outside the window); `DELETE /schedules/{id}` cancels it. A newer CI trigger for the same service is folded into the existing scheduled job.
- No window within the horizon (setting `maintenance_schedule_horizon_days`, 30 d): the deploy runs at once with a job-log warning and emits `maintenance:window_missing` — the ITSM opens a task "define a maintenance window". Fix it by adding a window in `service.yml` or the controller, never by disabling the policy fleet-wide.
- Every executed deploy announces a maintenance override (`maintenance:override_started/ended`) for the service and host CIs, so monitoring shows "maintenance" instead of "down" while containers restart. Don't open incidents for a service whose deploy job is RUNNING.
- The default switch (`maintenance_default.enabled` per site/stage) is READ-ONLY here (`GET /maintenance-default`): change it by editing the iac-controller repo (`stage_vars.yml`), never through this plugin. Commits prefixed `ci: maintenance` sync without triggering a deploy.
- CI status contract: `GET /api/iac/deploy/service/{svc}/gitlab/status?accept=scheduled` → `{"status":"scheduled","not_before":…}` for a parked job; without `accept` it answers `status: "success"` + `job_status: "SCHEDULED"` (compatibility for old templates).

## How it works internally
Engine renders per env/host → `tofu init` → scoped `tofu plan|import|apply -target=module.lxc_<node>…ct["<host>"]` inside opentofu-ci-image, spawned via the Docker socket on the runner. Destroy-safety guards: `import_existing_cts.sh` (adopt live CTs before apply), renderer empty-container guard, CT import script.

## ⚠️ Guardrails — do NOT
- Don't restart the runner while an IaC job runs (no plugin toggle/upgrade, no aac-lyndrix-core SSoT push → Renovate redeploys the runner) — the orphan sweep FAILS the job (>180 s).
- Don't trust a targeted plan's "0 to destroy" — verify against a full plan.
- Don't apply an adopt — adopt is import/state-mv only.
- Don't hand-edit live Proxmox/containers or rendered tfvars — change the iac-controller SSoT and re-render.
- Don't disable this plugin to "restart" it — disable acts cluster-wide (every instance), and a running job is lost.
- Don't "run now" a SCHEDULED deploy without the operator's OK — it deploys outside the agreed maintenance window. Don't cancel scheduled deploys to "clean up": the next CI trigger would just re-park them.
- Don't try to toggle `maintenance_default` through this plugin — it is read-only here by design (decision C); edit the iac-controller repo.
