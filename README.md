# IaC Orchestrator Plugin

A powerful, production-ready GitOps controller for orchestrating Infrastructure-as-Code (IaC) deployments using Terraform and Ansible. This plugin integrates seamlessly with Lyndrix Core to provide a unified platform for managing infrastructure configuration, service deployments, and infrastructure state.

---

## Overview

The IaC Orchestrator is a sophisticated GitOps automation engine designed for enterprise-scale infrastructure management. It coordinates:

- **Git-based Workflows**: Tracks infrastructure definitions in Git repositories
- **Service Catalogs**: Manages service definitions, profiles, and assignments
- **Multi-stage Pipelines**: Executes Terraform plans and Ansible playbooks in coordinated sequences
- **Deployment Orchestration**: Handles bulk rollouts across multiple hosts and environments
- **State Management**: Detects and reconciles infrastructure drift
- **Webhook Integration**: Triggers workflows via GitHub/GitLab webhooks
- **Real-Time Monitoring**: Dashboard displays active pipelines, logs, and execution status

---

## Features

### 🚀 Core Capabilities

- **GitOps-Driven**: All infrastructure state lives in Git, enabling version control and audit trails
- **Multi-Environment**: Manage dev, staging, and production environments from a single controller
- **Service Catalog**: Define services once, deploy to unlimited hosts via profile inheritance
- **Drift Detection**: Automatically detect configuration deviations and reconcile state
- **Concurrent Deployments**: Parallel worker pool for high-throughput bulk deployments
- **Docker Container Orchestration**: Launch isolated runner containers for each pipeline stage
- **Resume Capability**: Automatically resume interrupted pipelines after system restarts
- **Webhook Support**: Trigger deployments via GitHub/GitLab push/merge events

### 📊 Monitoring & Observability

- **Real-Time Dashboard**: Monitor active job queues, execution progress, and logs
- **Event Streaming**: Subscribe to system events (vault ready, deployments started)
- **Job History**: Track deployment outcomes, timings, and generated artifacts
- **Structured Logging**: Comprehensive audit logs for compliance and troubleshooting
- **Status Indicators**: Visual indicators for pipeline state on Lyndrix Core dashboard

### 🔐 Security

- **Secrets Management**: Credentials stored in HashiCorp Vault per plugin
- **Isolated Execution**: Each pipeline runs in isolated Docker containers
- **Audit Logging**: All operations logged for security and compliance
- **Role-Based Access**: Settings modal for permission management
- **Network Isolation**: Dedicated security directory for sensitive configurations

## Project Structure

As of **v0.3.0** the plugin follows the [Lyndrix Core plugin development standard](https://github.com/lyndrix-platform/lyndrix-core/blob/main/docs/plugins.md):

```
.
├── entrypoint.py          # Manifest + lifecycle hooks ONLY (pure wiring layer)
├── requirements.txt       # Runtime dependencies
├── requirements-dev.txt   # Dev toolchain (pytest, mypy, ruff, black)
├── CHANGELOG.md
├── README.md
├── examples/              # Sample operator-provided config files
├── app/                   # All plugin logic lives here
│   ├── model/             # SQLAlchemy ORM models and DB session helpers
│   │   ├── models.py
│   │   └── database.py
│   ├── controller/        # Business logic and services
│   │   ├── service.py     # IaCService — single shared service object
│   │   ├── engine.py      # DeploymentEngine (pipeline execution)
│   │   ├── config.py      # IaCConfig (env var / vault config)
│   │   ├── api.py         # FastAPI router for /api/iac/*
│   │   └── utils.py       # Shared utilities (StageResult, JobFileLogBridge)
│   └── ui/                # NiceGUI pages and widgets
│       ├── dashboard.py   # Main /iac dashboard page
│       ├── settings.py    # Settings injection UI
│       └── widget.py      # Mini status widget for the core dashboard
├── iac_core/              # Native artifact generation sub-process
├── stages/                # Reusable pipeline stage definitions
└── tests/                 # Unit and smoke tests
    └── test_service.py
```

---

## Installation

### Prerequisites

- Lyndrix Core v0.1.0+
- Docker Engine v24.0+
- Docker Compose v2.20+ (for dev environment)
- 2GB+ RAM
- Sufficient disk space for Git repositories and artifacts

### Installation Steps

#### Option 1: Clone as External Plugin (Development)

```bash
# Clone the plugin repository alongside lyndrix-core
cd /path/to/lyndrix-dev
git clone https://github.com/lyndrix-platform/lyndrix-plugin-iac-orchestrator.git

# The docker-compose.dev.yml automatically mounts this as /app/plugins/iac_orchestrator
docker compose -f lyndrix-core/docker/docker-compose.dev.yml up -d
```

#### Option 2: Install via Marketplace (Production)

```bash
# In Lyndrix Core UI:
1. Navigate to Plugins > Marketplace
2. Search for "IaC Orchestrator"
3. Click "Install"
4. System automatically downloads, extracts, and registers the plugin
5. Configure .env variables (see section below)
6. Restart Lyndrix Core
```

#### Option 3: Manual Installation

```bash
# Copy to plugins directory
mkdir -p /path/to/lyndrix-core/app/plugins/iac_orchestrator
cp -r /path/to/lyndrix-iac-orchestrator/* /path/to/lyndrix-core/app/plugins/iac_orchestrator/

# Update docker-compose.yml to mount the plugin directory
# Then restart
docker compose -f docker/docker-compose.dev.yml restart app
```

---

## Configuration

### Environment Variables

Add these variables to `docker/.env` or `docker/.env.prod` in your Lyndrix Core deployment:

#### Core Paths (Container Internal)

| Variable | Default | Description |
|----------|---------|-------------|
| `PLUGIN_IAC_ORCHESTRATOR_INTERNAL_STORAGE_DIR` | `/data/storage` | Base directory for all plugin data |
| `PLUGIN_IAC_ORCHESTRATOR_INTERNAL_GIT_REPOS_DIR` | `/data/storage/git_repos` | Git repositories storage |
| `PLUGIN_IAC_ORCHESTRATOR_INTERNAL_SERVICES_DIR` | `/data/storage/services` | Service artifacts and outputs |
| `PLUGIN_IAC_ORCHESTRATOR_INTERNAL_LOGS_DIR` | `/data/storage/logs` | Pipeline execution logs |
| `PLUGIN_IAC_ORCHESTRATOR_INTERNAL_SECURITY_DIR` | `/data/security` | SSH keys, certificates, credentials |

#### Host Paths (For Docker-in-Docker)

These paths help the plugin spawn sibling containers (for Ansible runners, etc.). Use the **host machine paths**, not container paths.

| Variable | Default | Description |
|----------|---------|-------------|
| `PLUGIN_IAC_ORCHESTRATOR_HOST_GIT_REPOS_DIR` | `/data/storage/git_repos` | Host path to Git repos (for docker-in-docker) |
| `PLUGIN_IAC_ORCHESTRATOR_HOST_SERVICES_DIR` | `/data/storage/services` | Host path to services directory |
| `PLUGIN_IAC_ORCHESTRATOR_HOST_SECURITY_DIR` | `/data/security` | Host path to security/secrets directory |

#### Execution Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `PLUGIN_IAC_ORCHESTRATOR_PARALLEL_WORKERS` | `20` | Number of concurrent deployment workers |
| `PLUGIN_IAC_ORCHESTRATOR_ANSIBLE_IMAGE` | `registry.gitlab.int.fam-feser.de/aac-application-definitions/aac-template-engine:latest` | Docker image for Ansible runner |
| `PLUGIN_IAC_ORCHESTRATOR_TIMEOUT_SECONDS` | `3600` | Pipeline execution timeout (seconds) |
| `PLUGIN_IAC_ORCHESTRATOR_MAX_RETRIES` | `3` | Failed stage retry attempts |

### Development Environment (.env.dev)

```env
# IaC Orchestrator Configuration
PLUGIN_IAC_ORCHESTRATOR_GIT_REPOS_DIR=/home/marvin/gitlab/lyndrix-dev/lyndrix-core/.dev/storage/git_repos
PLUGIN_IAC_ORCHESTRATOR_SECURITY_DIR=/home/marvin/gitlab/lyndrix-dev/lyndrix-core/.dev/secure_data
PLUGIN_IAC_ORCHESTRATOR_SERVICES_DIR=/home/marvin/gitlab/lyndrix-dev/lyndrix-core/.dev/storage/services
PLUGIN_IAC_ORCHESTRATOR_PARALLEL_WORKERS=20

# Host paths (for docker-in-docker support)
PLUGIN_IAC_ORCHESTRATOR_HOST_GIT_REPOS_DIR=/home/marvin/gitlab/lyndrix-dev/lyndrix-core/.dev/storage/git_repos
PLUGIN_IAC_ORCHESTRATOR_HOST_SERVICES_DIR=/home/marvin/gitlab/lyndrix-dev/lyndrix-core/.dev/storage/services
PLUGIN_IAC_ORCHESTRATOR_HOST_SECURITY_DIR=/home/marvin/gitlab/lyndrix-dev/lyndrix-core/.dev/secure_data
```

### Production Environment (.env.prod)

```env
# IaC Orchestrator Configuration
PLUGIN_IAC_ORCHESTRATOR_INTERNAL_STORAGE_DIR=/data/storage
PLUGIN_IAC_ORCHESTRATOR_INTERNAL_SECURITY_DIR=/data/security

# Host paths (must be absolute paths on host machine)
PLUGIN_IAC_ORCHESTRATOR_HOST_GIT_REPOS_DIR=/mnt/persistent/lyndrix/git_repos
PLUGIN_IAC_ORCHESTRATOR_HOST_SERVICES_DIR=/mnt/persistent/lyndrix/services
PLUGIN_IAC_ORCHESTRATOR_HOST_SECURITY_DIR=/mnt/persistent/lyndrix/security

# Execution
PLUGIN_IAC_ORCHESTRATOR_PARALLEL_WORKERS=50
PLUGIN_IAC_ORCHESTRATOR_ANSIBLE_IMAGE=myregistry.com/ansible-runner:latest
PLUGIN_IAC_ORCHESTRATOR_TIMEOUT_SECONDS=7200
```

---

## Volume Mounts

### Lyndrix Core Container Setup

For the IaC Orchestrator to function properly, configure your `docker-compose.yml` with the following volume mounts:

#### Development Setup (docker-compose.dev.yml)

```yaml
services:
  app:
    volumes:
      # Standard Lyndrix Core mounts
      - ../app:/app
      - ../.dev/storage:/data/storage
      - ../.dev/secure_data:/data/security
      
      # Plugin source (for hot-reloading)
      - ../../lyndrix-iac-orchestrator:/app/plugins/iac_orchestrator
      
      # Docker socket (required for plugin to spawn containers)
      - /var/run/docker.sock:/var/run/docker.sock
```

#### Production Setup (docker-compose.prod.yml)

```yaml
services:
  app:
    volumes:
      # Code volume (immutable in production)
      - lyndrix_code:/app
      
      # Persistent data
      - lyndrix_storage:/data/storage
      - lyndrix_security:/data/security
      
      # Docker socket (for IaC Orchestrator runners)
      - /var/run/docker.sock:/var/run/docker.sock
      
      # Application logs
      - lyndrix_logs:/app/logs

volumes:
  lyndrix_code:
    driver: local
  lyndrix_storage:
    driver: local
  lyndrix_security:
    driver: local
  lyndrix_logs:
    driver: local
```

#### Kubernetes Volume Mounts

```yaml
# clusterconfig/lyndrix-pvc.yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: lyndrix-storage
spec:
  accessModes:
    - ReadWriteMany  # Required for multi-pod deployments
  storageClassName: standard
  resources:
    requests:
      storage: 100Gi  # Adjust based on repository size

---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: lyndrix-security
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: standard
  resources:
    requests:
      storage: 10Gi
```

Mount in deployment:

```yaml
spec:
  containers:
  - name: lyndrix-core
    volumeMounts:
    - name: storage
      mountPath: /data/storage
    - name: security
      mountPath: /data/security
    - name: docker-sock
      mountPath: /var/run/docker.sock
  volumes:
  - name: storage
    persistentVolumeClaim:
      claimName: lyndrix-storage
  - name: security
    persistentVolumeClaim:
      claimName: lyndrix-security
  - name: docker-sock
    hostPath:
      path: /var/run/docker.sock
      type: Socket
```

---

## Usage

### Web Dashboard

Access the IaC Orchestrator dashboard at:

```
http://localhost:8081/iac
```

#### Main Features

**Job Queue**: View active, queued, and completed deployments
- Real-time progress tracking
- Log streaming
- Abort functionality

**Service Catalog**: Browse available services and their configurations
- Service dependencies
- Version history
- Configuration validation

**Deployment History**: Track all historical deployments
- Rollback capability
- Comparison view (before/after)
- Performance metrics

### Settings Panel

Click the settings icon on the IaC Orchestrator dashboard card to access:

- **Auto-Apply Toggle**: Automatically apply approved plans
- **Parallel Workers**: Adjust concurrent deployment count
- **Timeout Configuration**: Set pipeline execution timeout
- **Notification Settings**: Configure alerting (Slack, email, etc.)
- **Credentials Management**: Rotate SSH keys and API tokens

### Event Subscriptions

The plugin subscribes to system events:

```python
@ctx.subscribe('vault:ready_for_data')
async def on_vault_ready(payload):
    """Vault is unsealed and ready for secret access"""
    
@ctx.subscribe('db:connected')
async def on_db_ready(payload):
    """Database is connected, create tables"""
    
@ctx.subscribe('iac:webhook_verified')
async def on_webhook(payload):
    """Webhook received, execute pipeline"""
```

### Event Emissions

The plugin emits events for system coordination:

```python
bus.emit("iac:pipeline_started", {"job_id": "uuid", "timestamp": now})
bus.emit("iac:pipeline_completed", {"job_id": "uuid", "status": "success"})
bus.emit("git:sync", {"repo": "name", "branch": "main"})
bus.emit("git:commit_push", {"repo": "name", "sha": "hash", "message": "Auto-commit"})
```

---

## API Endpoints

The plugin exposes REST endpoints for programmatic interaction:

### Pipeline Management

**POST** `/api/iac/deploy`
- Trigger a deployment pipeline
- Parameters: `job_type`, `environment`, `services` (optional)
- Returns: `job_id`

**GET** `/api/iac/jobs/{job_id}`
- Get job status and logs
- Returns: Job info with current stage, progress, logs

**POST** `/api/iac/jobs/{job_id}/abort`
- Abort a running pipeline
- Returns: Status confirmation

### Service Catalog

**GET** `/api/iac/catalog`
- Fetch service catalog
- Returns: List of available services

**GET** `/api/iac/assignments`
- Get service-to-host assignments
- Returns: Deployment assignments

### Git Integration

**POST** `/api/iac/webhooks/github`
- GitHub webhook handler
- Payload: Standard GitHub push event
- Triggers: Automatic pipeline execution

**POST** `/api/iac/webhooks/gitlab`
- GitLab webhook handler
- Payload: Standard GitLab push event
- Triggers: Automatic pipeline execution

---

## Repository Structure

The plugin expects the following Git repository layout:

```
iac_controller/
├── environments/
│   ├── global/
│   │   ├── 01_service_definitions.yml
│   │   ├── 02_service_catalog.yml
│   │   └── 03_profiles.yml
│   └── sites/
│       ├── production/
│       │   ├── stages/
│       │   │   ├── phase-1/
│       │   │   │   └── hosts.yml
│       │   │   └── phase-2/
│       │   │       └── hosts.yml
│       │   └── common/
│       │       └── hosts.yml
│       └── staging/
│           └── common/
│               └── hosts.yml
├── terraform/
│   ├── modules/
│   │   ├── vpc/
│   │   ├── compute/
│   │   └── storage/
│   └── environments/
│       ├── prod.tf
│       └── staging.tf
└── ansible/
    ├── roles/
    │   ├── base/
    │   ├── monitoring/
    │   └── security/
    └── playbooks/
        ├── deploy.yml
        └── rollback.yml
```

---

## Workflow Examples

### Example 1: Simple Service Deployment

1. Commit service configuration to Git
2. GitHub webhook triggers IaC Orchestrator
3. Plugin detects changes and creates deployment job
4. Terraform plan is generated and displayed
5. Admin approves plan via dashboard
6. Ansible playbooks execute across target hosts
7. Dashboard shows real-time progress and logs

### Example 2: Multi-Stage Rollout

```yaml
# environments/global/03_profiles.yml
profiles:
  web_server:
    services:
      - name: nginx
        version: "1.25"
      - name: monitoring-agent
        version: "latest"
  database_server:
    services:
      - name: postgresql
        version: "15"
      - name: backup-agent
        version: "latest"
```

Deploy to multiple environments:

```yaml
# environments/sites/production/stages/phase-1/hosts.yml
hosts:
  web-1:
    profiles: [web_server]
  web-2:
    profiles: [web_server]
  db-primary:
    profiles: [database_server]
```

### Example 3: Drift Detection

The plugin automatically detects configuration drift:

1. Scheduled reconciliation runs every 30 minutes
2. Compares desired state (Git) with actual state (Infrastructure)
3. Highlights deviations in dashboard
4. Auto-remediation can be triggered manually or automatically

---

## Monitoring

### Dashboard Metrics

- **Active Jobs**: Currently running pipelines
- **Queue Depth**: Pending deployments
- **Success Rate**: Percentage of successful deployments
- **Average Duration**: Mean execution time
- **Last Deployment**: Timestamp of most recent deployment

### Health Checks

```bash
# Check plugin health
curl http://localhost:8081/api/iac/health

# Expected response:
{
  "status": "healthy",
  "version": "0.2.0",
  "db_connected": true,
  "vault_ready": true,
  "active_workers": 5
}
```

### Logging

Access logs via Lyndrix Core web UI or directly:

```bash
# Container logs
docker logs lyndrix-core-dev | grep -A 10 "IaC Orchestrator"

# File logs
tail -f .dev/storage/logs/iac_orchestrator.log
```

---

## Security Considerations

### Secrets Management

All sensitive data goes to HashiCorp Vault:

```python
# SSH keys for Git
ctx.vault.kv_v2_write(
    path="iac_orchestrator/ssh_keys/github",
    data={"private_key": "..."}
)

# Cloud provider credentials
ctx.vault.kv_v2_write(
    path="iac_orchestrator/cloud_credentials/aws",
    data={"access_key": "...", "secret_key": "..."}
)
```

### Network Security

- Restrict webhook receiver to trusted GitHub/GitLab IPs
- Use HTTPS for all API endpoints in production
- Implement rate limiting on webhook endpoints
- Enable audit logging for all deployments

### Access Control

Implement role-based access:

```python
# In settings UI
ROLES = {
    "viewer": ["list_jobs", "view_logs"],
    "deployer": ["list_jobs", "view_logs", "approve_plan"],
    "admin": ["*"]
}
```

---

## Troubleshooting

### Plugin Not Loading

```bash
# Check plugin registration
docker logs lyndrix-core-dev | grep -A 5 "IaC Orchestrator"

# Verify manifest
python -c "from app.plugins.iac_orchestrator.entrypoint import manifest; print(manifest)"
```

### Deployment Timeout

Increase timeout in `.env`:

```env
PLUGIN_IAC_ORCHESTRATOR_TIMEOUT_SECONDS=7200  # 2 hours
```

### Docker-in-Docker Issues

Verify socket mount:

```bash
# Inside container
docker ps  # Should list sibling containers

# Host machine
ls -la /var/run/docker.sock
```

### Git Authentication Failures

```bash
# Check stored SSH key
docker exec lyndrix-core-dev vault kv get iac_orchestrator/ssh_keys/github

# Verify key permissions
ssh-keygen -y -f /path/to/key  # Should not fail
```

### Database Connection Issues

```bash
# Verify database
docker exec lyndrix-db-dev mysql -u admin -p -e "SELECT 1"

# Check IaC tables
docker exec lyndrix-db-dev mysql -u admin -p lyndrix_db -e "SHOW TABLES LIKE 'iac_%'"
```

---

## Development

### Local Testing

```bash
# 1. Start development environment
cd lyndrix-core
docker compose -f docker/docker-compose.dev.yml up -d --build

# 2. Edit plugin code
nano ../lyndrix-iac-orchestrator/entrypoint.py

# 3. Automatic hot-reload happens
# 4. Refresh browser to see changes
```

### Plugin Dependencies

```bash
# Add to requirements.txt in plugin directory
echo "my-new-package==1.0.0" >> requirements.txt

# Dependencies are auto-installed during setup
```

### Testing API Endpoints

```bash
# Trigger deployment
curl -X POST http://localhost:8081/api/iac/deploy \
  -H "Content-Type: application/json" \
  -d '{"job_type": "deploy", "environment": "staging"}'

# Check job status
curl http://localhost:8081/api/iac/jobs/{job_id}
```

---

## Performance Tuning

### Parallel Workers

Adjust based on system capacity:

```env
PLUGIN_IAC_ORCHESTRATOR_PARALLEL_WORKERS=50  # For 32-core systems
```

### Memory Settings

For Ansible containers:

```env
# In Docker runner setup
--memory=2g
--memory-swap=2g
```

### Database Optimization

```sql
-- Create indexes for faster queries
CREATE INDEX idx_jobs_status ON iac_jobs(status);
CREATE INDEX idx_jobs_created_at ON iac_jobs(created_at);
```

---

## Migration from Legacy Systems

If migrating from separate Terraform/Ansible systems:

1. **Export** existing configurations
2. **Structure** into YAML format per repository layout
3. **Create** service catalog
4. **Test** in staging environment
5. **Plan** cutover with team
6. **Execute** first production deployment
7. **Monitor** for issues

---

## Support & Community

- **Documentation**: [Full docs](../docs/)
- **Issues**: GitHub Issues
- **Discussions**: GitHub Discussions
- **Plugin API**: [Lyndrix Plugin Development Guide](../docs/plugins.md)

---

## License

This plugin is part of Lyndrix Core and follows the same licensing terms.

---

**Built for enterprise-scale infrastructure automation.**