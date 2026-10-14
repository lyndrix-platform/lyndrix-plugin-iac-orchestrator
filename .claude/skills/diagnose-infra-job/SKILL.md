---
name: diagnose-infra-job
description: Diagnose a failed IaC-Orchestrator infra job (Check Env / infra_plan / infra_apply) from the running lyndrix-core logs. Use when an "Infrastructure Plan (Check Env)" or Deploy-Infra job reports FATAL / "X ok, Y failed", when someone asks why an infra plan/apply failed, or when a Terraform-in-Docker env shows as failed. Covers the docker-wait/auto-remove false-failure race and the node-migration state-adoption fix. Read-only diagnosis against the fleet; no API key needed.
---

# Diagnose a failed IaC infra job

The orchestrator runs each Terraform environment's `tofu plan`/`apply` in a
throwaway Docker container (`iac-tf-Terraform-Plan--<env>` / `--Apply--`) spawned
by lyndrix-core, then aggregates per-env results into a summary. A FATAL job does
**not** always mean a real Terraform problem — one known wrapper bug produces
spurious failures. This skill finds the ground truth from the core logs.

Everything here is **read-only** and needs no Lyndrix API key — you read the
container logs directly over SSH. Commands verified 2026-10-12.

## Where the orchestrator runs

- lyndrix-core container = **`aac-lyndrix-core`** on **`10.1.130.71`** (`mngm.int.fam-feser.de`).
- SSH: `ssh -i ~/.ssh/ansible-agent-key -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ansible-agent@10.1.130.71`
- TF runs as Docker containers it spawns (image `opentofu-ci-image`), state on
  MinIO S3 `http://10.100.1.5:9000` (bucket `tfstate`, key `<env>/terraform.tfstate`).

## Pull a job's outcome (agent path)

Pipe a script via stdin to avoid the `bash -lc "..."` quoting traps (parens in
`echo`, Go-templates `{{.X}}`):

```bash
cat > /tmp/corelog.sh <<'EOF'
C=aac-lyndrix-core
echo "--- last plan/apply summaries ---"
sudo docker logs "$C" 2>&1 | grep -nE "Infra plan summary|Infra apply summary" | tail -3
echo "--- FATAL + the per-env except-line (engine.py:2659) ---"
sudo docker logs "$C" 2>&1 | grep -nE "\[FATAL\]|Infra .* failed for env" | tail -5
echo "--- the tell: auto-removed TF containers ---"
sudo docker logs "$C" 2>&1 | grep -cE "No such container: iac-tf-"
EOF
ssh -i ~/.ssh/ansible-agent-key -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
  ansible-agent@10.1.130.71 'sudo bash -lc "$(cat)"' < /tmp/corelog.sh
```

To read a specific job window, find the summary line number and
`sudo docker logs aac-lyndrix-core 2>&1 | sed -n '<start>,<end>p'`.

## Gotcha #1 — "Check Env" FATAL but every tofu plan was clean (docker-wait race)

If the summary says e.g. `3 ok, 1 failed` yet no env shows a TF `Error` and there's
**no** `Infra ... failed for env` except-line, it's the wrapper race, not a real
failure:

- Plan containers spawn with `remove: True` (auto-remove on exit). Fast plans
  (`No changes` ≈ 4 s) exit+remove **before** the engine's exit-code read
  (`execute_terraform_docker`, `docker wait` ≈ `engine.py:1902`).
- `docker wait` then errors `No such container: iac-tf-Terraform-Plan--<env>` and
  returns non-zero → `success = (exit_code == 0)` is False → `failed++`. No TF
  error, no exception.
- **Non-deterministic proof:** a clean Check Env flaps between `4 ok` (3×
  "No such container") and `3 ok, 1 failed` (4×). The failed env is whichever
  loses the race — not a specific env, and per-env exit codes aren't logged.

**Verdict:** treat the FATAL as spurious; the underlying plans are valid. **Fix**
(in `execute_terraform_docker`): drop `remove: True` → `docker wait` + read logs
→ explicit `docker rm`. See memory `iac-orchestrator-checkenv-false-fail`.

## Gotcha #2 — an env legitimately shows "+ create" after a CT node migration

If a plan shows `+ module.lxc_<newnode>...ct["<name>"]` (create) for a container
that already runs live, its `node_name` was changed (e.g. hydra→cerberus) and the
resource moved to a different per-node module. The import-guard only *adds*, never
removes the stale old-module row. Fix = `terraform state rm` the old-module
address (non-destructive) → "Adopt Host"/"Site Adopt" → re-plan. Full detail:
memory `iac-tf-node-migration-adoption`. tofu is **not** in the local `~/gitlab`
env — `state rm` runs in the orchestrator workclone.

## Real failure vs false failure — quick decision

| Signal | Meaning |
|---|---|
| `No such container: iac-tf-...` count == #envs, no TF `Error`, no except-line | false failure (Gotcha #1) — re-run or fix the wrapper |
| A `tofu plan`/`apply` printed `Error:` / the except-line `Infra ... failed for env` appears | real failure — read that env's rendered workdir under `/data/storage/git_repos/inventory_state/.terraform_runs/<env>__<plan|apply>` |
| `+ create` for a live CT | node-migration drift (Gotcha #2) |
