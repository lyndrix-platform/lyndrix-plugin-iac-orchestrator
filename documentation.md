# IaC Orchestrator — Dokumentation

## Übersicht

Der IaC Orchestrator ist ein eigenständiger GitOps-Controller für die Ausführung von Terraform- und Ansible-Pipelines. Er empfängt Webhook-Trigger von GitLab CI, führt Drift-Erkennung durch, startet parallele Provisionierungs-Pipelines und verwaltet die Synchronisation des SSoT-Repositories (Single Source of Truth). Änderungen an SSoT oder iac-controller lösen automatisch regelgesteuerte Infrastruktur-Deployments aus.

Der Orchestrator besitzt eine vollständige React-UI mit i18n-Unterstützung und ist direkt nach der Installation einsatzbereit (`auto_enable_on_install=True`).

---

## Architektur

```
lyndrix-plugin-iac-orchestrator/
├── entrypoint.py                      # Manifest + Lifecycle-Hooks
├── locales/
│   └── iac.<locale>.json              # i18n-Übersetzungen (Namespace: iac)
└── app/
    ├── api.py                         # Authentifizierter Plugin-Router (via ctx.register_routes)
    ├── api/
    │   └── stream_router.py           # SSE-Job-Stream (HMAC-Ticket-Auth)
    ├── controller/
    │   ├── service.py                 # IaCService: Pipeline-Ausführung, Drift-Erkennung, Git-Sync
    │   ├── api.py                     # Interner authentifizierter API-Router
    │   └── webhook_router.py          # Öffentlicher GitLab-Webhook-Router (kein Core-Auth)
    ├── model/
    │   └── models.py                  # SQLAlchemy: Pipeline-Runs, Jobs, SSoT-Zustand
    └── ui/
        ├── react/                     # React-Frontend (Vite-Bundle)
        └── nicegui/
            ├── dashboard.py           # NiceGUI-Dashboard
            ├── settings.py            # NiceGUI-Einstellungen
            └── widget.py              # Kompaktes Dashboard-Widget
```

---

## Cluster-Betrieb: Control Plane und Runner (ab 1.4, Core-API ≥ 1.7)

Das Plugin ist in zwei Ebenen geteilt, die über die gemeinsame Datenbank zusammenarbeiten.
Das Manifest deklariert `placement="any"`:

**Control Plane — auf jeder Core-Instanz.** REST-API, React-Daten, GitLab-Webhooks und
CI-Trigger (`/api/iac/*`), SSE-Stream und Job-Logs. Jeder Trigger (UI, Webhook, CI, NiceGUI,
die Rollout-Übergabe der Engine) landet weiterhin als `iac:webhook_verified` auf dem lokalen
Bus und wird dort **eingereiht**: Zeile in `iac_orchestrator_jobs` mit Status `QUEUED`,
`payload` (JSON) und `dedupe_key`. Ein zweiter Trigger für dasselbe Ziel (gleicher Service,
gleicher Host, laufender Rollout, Infra-Plan/Apply) wird mit „Pipeline Ignored“ abgewiesen,
solange ein Job dazu `QUEUED` oder `RUNNING` ist. `iac:job_queued` (`cluster=True`) weckt die
Runner sofort.

**Execution Plane — nur Instanzen mit Rolle `runner`** (`LYNDRIX_INSTANCE_ROLES`; im
Single-Modus hat die Instanz alle Rollen). Der `JobWorker` (`app/controller/queue.py`) claimt
den ältesten `QUEUED`-Job atomar (`UPDATE … WHERE status='QUEUED'`, `owner_instance`,
`claimed_at`), führt die Engine aus (bis zu `iac_max_parallel_jobs`, Default 3), sendet alle
10 s einen Heartbeat und markiert jede Minute verwaiste Jobs (Owner nicht mehr lebend oder
Heartbeat älter als 3 min) als `FAILED` „Runner lost“. Beim Start kümmert sich ein Runner nur um
eigene `RUNNING`-Jobs (Resume oder Abbruch) und um verwaiste `aac-runner-*`-Container des Hosts.

**Was alle Instanzen aus der Datenbank sehen:**

- Job-Logs: der ausführende Runner schreibt jede Zeile zusätzlich in `iac_orchestrator_job_logs`
  (Batches alle 0,5 s). `GET /jobs/{id}/logs` liest zuerst von dort (`source: "db"`,
  `offset` = nächste Sequenznummer), die Datei `logs/job_<id>.log` bleibt auf dem Runner-Host.
- Runner-Status: `active_tasks` (Status + letzte 50 Zeilen je Task) steht als JSON in
  `iac_orchestrator_jobs.runners`; der SSE-Stream baut daraus dieselbe Antwort wie bisher.
- Repo-Snapshots: Service-Katalog, Assignments, Terraform-Hosts und Inventar-Hosts werden vom
  Runner nach jedem Lauf in `iac_orchestrator_state` (`snapshot:*`) veröffentlicht; Instanzen ohne
  Git-Working-Trees antworten daraus.
- Stream-Tickets werden mit einem aus `STORAGE_SECRET` abgeleiteten Schlüssel signiert und gelten
  damit auf jeder Instanz hinter dem Load Balancer.

**Abbruch:** `POST /abort` setzt `QUEUED` direkt auf `ABORTED` und sendet
`iac:job_abort_requested` (`cluster=True`); der besitzende Runner bricht seinen Pipeline-Task ab
und entfernt die Container mit Label `iac_job_id=<id>`. Jobs eines toten Runners schließt die
aufrufende Instanz selbst.

**Repo-Sync** (`POST /maintenance/sync-repos`) ist ein Job vom Typ `repo_sync` und läuft damit
ebenfalls auf einem Runner. Die GitLab-Webhook-Registrierung läuft genau einmal im Cluster
(Lease `iac:webhook_sync`). Ist kein Runner online, meldet das Plugin `degraded` („pipelines are
queued, not executed“) und `health()` ebenso — die API bleibt nutzbar, Jobs warten.

Noch offen: eigenständiges `lyndrix-runner`-Image und ein Kubernetes-Executor (Jobs statt
Docker-Socket), Job-Logs und Working-Trees weg vom lokalen Host des Runners.

---

## API-Referenz

### Öffentliche Routen (kein Core-Auth)

| Methode | Pfad | Beschreibung |
|---|---|---|
| `POST` | `/api/iac/webhook/gitlab` | GitLab-Webhook empfangen (validiert `X-Gitlab-Token`) |

Dieser Router wird direkt auf der FastAPI-Applikation eingehängt (nicht über `ctx.register_routes`), damit externe GitLab/CI-Aufrufer ohne Lyndrix-User-Token durchkommen. Jeder Handler validiert den `X-Gitlab-Token` selbst.

### SSE-Stream (HMAC-Ticket-Auth)

| Methode | Pfad | Beschreibung |
|---|---|---|
| `POST` | `/api/plugins/lyndrix.plugin.iac_orchestrator/stream/ticket` | HMAC-Ticket für den SSE-Stream anfordern |
| `GET` | `/api/plugins/lyndrix.plugin.iac_orchestrator/stream?token=<ticket>` | Live-Job-Stream (EventSource) |

`EventSource` kann keinen `Authorization`-Header senden. Stattdessen fordert der Client ein kurzlebiges HMAC-signiertes Ticket an und übergibt es als Query-Parameter `?token=`.

**Format des Tickets:** `{permission}:{expiry}:{sig}`

**v0.9.1 Fix:** Der Parser wurde von `rsplit(":", 2)` auf `rsplit(":", 1)` + `split(":", 1)` umgestellt. Der vorherige Code scheiterte bei Berechtigungen, die selbst einen `:` enthalten (z. B. `api:read`), weil `rsplit(":", 2)` das Ticket dann in drei statt zwei Teile auftrennte und die Signatur falsch zuwies.

### Authentifizierte Plugin-Routen

Alle Routen unter `/api/plugins/lyndrix.plugin.iac_orchestrator/` erfordern eine gültige Authentifizierung.

| Methode | Pfad | Permission | Beschreibung |
|---|---|---|---|
| `GET` | `/pipelines` | `api:read` | Pipeline-Run-Historie |
| `POST` | `/pipelines/trigger` | `api:write` | Pipeline manuell starten |
| `GET` | `/pipelines/{id}` | `api:read` | Einzelner Pipeline-Run |
| `GET` | `/jobs` | `api:read` | Job-Liste |
| `GET` | `/drift` | `api:read` | Drift-Erkennungsergebnisse |
| `GET` | `/settings` | `api:read` | Aktuelle Orchestrator-Einstellungen |
| `PUT` | `/settings` | `api:write` | Einstellungen aktualisieren |
| `GET` | `/git/status` | `api:read` | SSoT-Repository-Sync-Status |
| `POST` | `/git/sync` | `api:write` | SSoT-Sync erzwingen |

**Hinweis:** Der Einstellungs-Endpunkt heißt `/settings` (nicht `/prefs`), da `/settings` auf Core-Ebene reserviert ist und zu einem Routing-Konflikt führen würde.

---

## Events

| Richtung | Topic | Beschreibung |
|---|---|---|
| subscribe | `vault:ready_for_data` | Vault bereit — Einstellungen laden |
| subscribe | `db:connected` | Datenbankverbindung bereit |
| subscribe | `iac:webhook_verified` | Verifizierter Webhook — Pipeline starten |
| subscribe | `git:status_update` | Git-Sync-Status-Update |
| subscribe | `socket:response` | Socket-Antwort empfangen |
| subscribe | `entity_registry:identified` | Antwort der Entity Registry auf `entity_registry:identify` (Request/Response via `ctx.request`) |
| emit | `iac:pipeline_started` | Pipeline wurde gestartet |
| emit | `iac:webhook_verified` | Webhook akzeptiert und weitergeleitet |
| emit | `git:sync` | Git-Sync anfordern |
| emit | `git:commit_push` | Commit + Push anfordern |
| emit | `system:notify` | Plattform-Benachrichtigung |
| emit | `user:notify` | Benutzer-Benachrichtigung |
| emit | `monitoring:inventory_sync` | Inventar-Sync an Monitoring-Plugin |
| emit | `iac:inventory_updated` | Generische Host-Liste `{hosts: [...]}` für alle interessierten Plugins — je Host `name`, `hostname`, `ip`, `groups`, `baseline_roles`, `terraform` (**ohne** Zugangsdaten), `entity_type`, `site`, `stage`, `ci_id` |
| emit | `entity_registry:identify` | Batch-Anfrage an die Entity Registry: UUID je Host über `iac.host`, `pve.vm` (`node/vmid`), `fqdn` |
| emit | `server_manager:register_server` | Je Host ein Upsert im Server-Manager (`source=iac-orchestrator`, `external_id=<Host-Key>`, `writeback=false` → dort schreibgeschützt, mit `ci_id`) |
| emit | `socket:request` | Socket-Anfrage senden |
| emit | `messaging:outbound` | Ausgehende Nachricht über Messaging Gateway |

### Inventar-Veröffentlichung

Nach jeder Inventar-Generierung (und beim Start) veröffentlicht der Orchestrator die Host-Liste
aus `inventory_state/global/ansible/inventory.yml`:

1. **Zugangsdaten werden entfernt.** Der zusammengeführte `terraform`-Block eines Hosts kann
   Proxmox-Passwort/-Token und den SSH-Key enthalten; alle Schlüssel, die `password`, `secret`,
   `*_token` oder `*_key` heißen (sowie `token`, `ssh_key`, `api_token`, `private_key`), gehen
   nicht auf den Bus.
2. **CI-UUIDs.** Über `ctx.request("entity_registry:identify", {items})` erhält jeder Host seine
   Entity-Registry-UUID (`ci_id`). Hypervisoren (`physical_infrastructure` bzw. `type: hypervisor`)
   werden als `hypervisor`, alles andere als `host` identifiziert. Fehlt die Registry (Timeout),
   bleibt `ci_id` leer — der nächste Lauf holt es nach.
3. **Server-Manager.** Jeder Host wird per `server_manager:register_server` eingespielt:
   `environment_id = <site>:<stage>` (aus den Gruppen `site_*`/`stage_*`), `server_type`
   `physical` (Hypervisor) / `container` (Gast mit `vm_id`) / `vm`, `writeback=false`, Tags = Gruppen.
   Die Datensätze sind dort schreibgeschützt — Änderungen gehören ins IaC-Repository.

Voraussetzung: Core-API ≥ 1.4 (`ctx.request`); ohne Registry- bzw. Server-Manager-Plugin laufen
die Schritte ins Leere, der Orchestrator selbst bleibt unbeeinträchtigt.

---

## Benachrichtigungsendpunkte

| Endpoint | Standard | Beschreibung |
|---|---|---|
| `deployment_started` | aktiv | Pipeline wurde in die Warteschlange gestellt oder gestartet |
| `deployment_succeeded` | aktiv | Pipeline erfolgreich abgeschlossen |
| `deployment_failed` | aktiv | Pipeline mit Fehlern abgeschlossen |
| `webhook_verified` | **inaktiv** | Webhook akzeptiert und weitergeleitet (nur bei Bedarf aktivieren) |
| `drift_detected` | aktiv | Drift-Erkennung hat Abweichungen gefunden |

---

## Konfiguration & Einstellungen

**`auto_enable_on_install=True`** — der Orchestrator ist direkt nach der Installation aktiv, da er sich selbst konfiguriert und keine manuelle Vorabkonfiguration benötigt.

Die Einstellungen sind über die React-UI unter `/iac/settings` oder über die REST-API (`GET/PUT /settings`) zugänglich.

---

## React-Routen

| Pfad | Sichtbar in Sidebar | Beschreibung |
|---|---|---|
| `/iac` | Ja | Hauptansicht: Dashboard, Pipeline-Historie, Job-Übersicht |
| `/iac/settings` | Nein | Orchestrator-Einstellungen |

---

## Internationaliserung

Das Plugin registriert den i18n-Namespace `iac`. Übersetzungsdateien unter `locales/iac.<locale>.json` werden automatisch beim Laden in den Lyndrix-Katalog aufgenommen. Der React-Client bezieht sie über `GET /api/i18n/{locale}?ns=iac`.

---

## Entwicklung & Tests

```bash
# Aus dem Plugin-Verzeichnis (lyndrix-plugin-iac-orchestrator/)
pip install -r requirements-dev.txt

# Tests ausführen
pytest

# Typprüfung
mypy .

# Linter
ruff check .

# Formatter prüfen
black --check .
```

Die Controller- und Model-Schicht sind ohne laufenden Core testbar. `ModuleContext` kann für Lifecycle-Tests gemockt werden. Der öffentliche Webhook-Router kann mit pytest-httpx oder dem FastAPI-Testclient isoliert getestet werden.
