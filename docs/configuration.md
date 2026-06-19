# Configuration

> **TODO:** Document the IaC Orchestrator configuration surface — environment variables,
> Vault-backed secrets, and any operator-provided catalog/config files.

Secrets are stored in the plugin's own Vault namespace via `ctx.get_secret` /
`ctx.set_secret`; see [Plugin development → state and secrets](https://docs.lyndrix.eu/plugins/).
