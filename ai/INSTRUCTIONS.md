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
4. Single-service rollout: deploys one AAC service's container(s) to its host via Ansible.

## How it works internally
Engine renders per env/host → `tofu init` → scoped `tofu plan|import|apply -target=module.lxc_<node>…ct["<host>"]` inside opentofu-ci-image, spawned via the Docker socket on the runner. Destroy-safety guards: `import_existing_cts.sh` (adopt live CTs before apply), renderer empty-container guard, CT import script.

## ⚠️ Guardrails — do NOT
- Don't restart the runner while an IaC job runs (no plugin toggle/upgrade, no aac-lyndrix-core SSoT push → Renovate redeploys the runner) — the orphan sweep FAILS the job (>180 s).
- Don't trust a targeted plan's "0 to destroy" — verify against a full plan.
- Don't apply an adopt — adopt is import/state-mv only.
- Don't hand-edit live Proxmox/containers or rendered tfvars — change the iac-controller SSoT and re-render.
- Don't disable this plugin to "restart" it — disable acts cluster-wide (every instance), and a running job is lost.
