# Lyndrix IaC Orchestrator

A GitOps controller for orchestrating Infrastructure-as-Code deployments with Terraform and Ansible, integrated with Lyndrix Core.

- **Repository:** [https://github.com/lyndrix-platform/lyndrix-plugin-iac-orchestrator](https://github.com/lyndrix-platform/lyndrix-plugin-iac-orchestrator)
- **Platform docs:** [Lyndrix Core](https://docs.lyndrix.eu) · [Plugin ecosystem](https://docs.lyndrix.eu/ecosystem/)

This plugin builds on the Lyndrix Core [sockets](https://docs.lyndrix.eu/core-components/sockets/) extension point.

## Features

- Terraform and Ansible pipeline orchestration
- Webhook triggers for CI/CD, git sync, and commit/push
- Drift detection and parallel provisioning
- Notification endpoints (started, succeeded, failed, drift_detected)

## Installation

Install **IaC Orchestrator** from the Lyndrix **Plugin Manager**, or declare it for
reconciliation on boot via `LYNDRIX_PLUGINS_DESIRED`:

```text
https://github.com/lyndrix-platform/lyndrix-plugin-iac-orchestrator
```

See the [Plugin Development Guide](https://docs.lyndrix.eu/plugins/) for the plugin model and
lifecycle, and [Usage](usage.md) / [Configuration](configuration.md) for details.
