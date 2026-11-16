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

**Dedizierter Runner-Container (Deployment-Muster, ab 1.4.1).** Der Runner läuft als eigener
Container auf dem Docker-Host: dasselbe Core-Image mit `LYNDRIX_INSTANCE_ROLES=runner`, eigener
Instanz-ID, eigenem Plugin-Verzeichnis, Docker-Socket und den Volumes `storage`
(`git_repos`, `services`, Job-Logs) und `security` (SSH-Key). Die API-Instanz desselben Hosts
braucht dafür keinen Socket mehr. In der aac-Welt ist das die Dependency `aac-lyndrix-runner`
in der SSoT `aac-lyndrix-core` (Controller-Override `LYNDRIX_RUNNER_INSTANCE_ID`, Core-Rollen
`api`). Ein Redeploy des Stacks startet auch den Runner neu; ein dabei unterbrochener Job wird
nach dem Start aus seinen `aac-runner-*`-Containern geschlossen (laufend → wieder angehängt,
beendet → Exit-Code und Log-Tail), nie pauschal als „Runner lost“ markiert. Eigene Jobs sind
nie Verwaisten-Kandidaten, der erste Sweep wartet 90 s.

Noch offen: eigenständiges `lyndrix-runner`-Image und ein Kubernetes-Executor (Jobs statt
Docker-Socket), Job-Logs und Working-Trees weg vom lokalen Host des Runners.

---

## API-Referenz

### Öffentliche Routen (kein Core-Auth)

| Methode | Pfad | Beschreibung |
|---|---|---|
| `POST` | `/api/iac/webhook/gitlab` | GitLab-Webhook empfangen (validiert `X-Gitlab-Token`) |
| `POST` | `/api/iac/deploy/service/{svc}/gitlab` | Service-Deploy aus der Service-CI (`trigger: gitlab-ci` → Deploy-Policy gilt, siehe „Wartungsfenster“) |
| `GET` | `/api/iac/deploy/service/{svc}/gitlab/status?since_epoch=&accept=` | Status des neuesten Jobs: `success` / `failed` / `running` (auch `QUEUED`). Ein geparkter Job (`SCHEDULED`) liefert nur mit `accept=scheduled` den Wert `scheduled` + `not_before`; ohne `accept` den Kompatibilitätswert `success` mit `job_status: "SCHEDULED"` (alte CI-Templates werden nie rot) |

Dieser Router wird direkt auf der FastAPI-Applikation eingehängt (nicht über `ctx.register_routes`), damit externe GitLab/CI-Aufrufer ohne Lyndrix-User-Token durchkommen. Jeder Handler validiert den `X-Gitlab-Token` selbst.

Weitere öffentliche Token-Routen: `POST /api/iac/webhook/sync` (Hooks neu registrieren), `POST /api/iac/infra/plan/gitlab` (Check Env aus der Controller-CI), `POST /api/iac/webhook/gitlab/test-host/{host}` und `POST /api/iac/infra/apply/gitlab` (**standardmäßig aus**, Setting `token_infra_apply_enabled`).

### Webhook-Tokens je Zweck (Audit OS-04)

Bisher autorisierte **ein** Token (`gitlab_webhook_token`) jede öffentliche Aktion; es steckt in jedem Projekt-Hook und in zwei CI-Variablen. Jeder Zweck hat jetzt einen eigenen Secret-Slot:

| Zweck | Secret-Slot (Setting-Key) | Routen | Wo das Token auf GitLab-Seite liegt |
|---|---|---|---|
| Gruppen-/Projekt-Hooks | `gitlab_hook_token` | `POST /api/iac/webhook/gitlab` | „Secret token“ der Hooks — wird vom Webhook-Sync automatisch in alle Projekt-Hooks der Gruppe geschrieben |
| Service-CI | `gitlab_service_ci_token` | `POST /api/iac/deploy/service/{svc}/gitlab`, `GET …/gitlab/status` | CI/CD-Variable, aus der die Service-Pipelines den Header `X-Gitlab-Token` befüllen (Gruppe der Service-Repos) |
| Controller-CI | `gitlab_controller_ci_token` | `POST /api/iac/infra/plan/gitlab`, `POST /api/iac/webhook/sync`, `…/test-host/{host}`, `…/infra/apply/gitlab` | CI/CD-Variable, aus der die Pipeline des Projekts `iac-controller` den Header `X-Gitlab-Token` befüllt |

Regeln:

- **Slot leer** → für diesen Zweck gilt weiter das bestehende `gitlab_webhook_token`, exakt wie bisher. Im Log steht dazu höchstens einmal pro Stunde und Zweck eine `DEPRECATED`-Warnung; die Plugin-Health zeigt unter `webhook_tokens`, welche Zwecke noch auf dem gemeinsamen Token laufen.
- **Slot gesetzt** → für diesen Zweck wird **nur noch** dieses Token akzeptiert (das gemeinsame nicht mehr), und es gilt für keinen anderen Zweck.
- Der Code ändert, verschiebt oder löscht das bestehende `gitlab_webhook_token` nie.
- Mit eigenem **Hook-Token** startet ein `pipeline`-Body auf der Hook-Route kein Deployment mehr (Antwort 200, `triggered: false`). Grundlage: Die vom Plugin registrierten Hooks senden nur Merge-Request-Events (`gitlab_webhooks.py`: `push_events`, `pipeline_events`, `job_events`, `tag_push_events` = `false`); Deployments löst die Service-CI aus. Solange der Slot leer ist, verhält sich die Route wie bisher.
- Mit eigenem **Service-CI-Token** wird immer der Prod-Branch des Service laut Katalog deployt (siehe unten); der `branch` im Body ist dann keine Wahl des Aufrufers.
- **Fehlversuche:** Nach 20 abgelehnten Aufrufen von einer Quelladresse innerhalb von 60 s bekommen weitere *abgelehnte* Aufrufe dieser Adresse `429` mit `Retry-After`. Ein gültiges Token wird immer akzeptiert — die Grenze kann die CI nicht aussperren (alle GitLab-Runner teilen sich eine Ausgangsadresse; ein Projekt mit veralteter Variable darf die anderen Pipelines nicht stoppen). Gezählt wird pro Instanz im Speicher (max. 2048 Adressen).
- **Empfehlung (nicht erzwungen):** `lyndrix_base_url` (Setting) und die Orchestrator-URL in den CI-Variablen auf eine `https://`-Adresse setzen — die Tokens laufen sonst im Klartext durchs Netz.

#### Branch → Ziel (Audit OS-05)

Ein Service-Deploy (`single_service`) richtet sich nach dem Branch: Nur der **Prod-Branch laut Service-Katalog** (`service_catalog.remote_git_management.remote_git_default_branch_prod`, Standard `main`; optional je Service) geht auf alle Hosts des Service (`service_<svc>`). Der Dev-Branch (`dev`, `*-dev`) geht nur auf `stage_dev`, der Test-Branch (`test`) nur auf `stage_test`. Jeder andere Branch wird abgelehnt: HTTP 400 auf den API-/Token-Routen, auf der Hook-Route 200 mit `triggered: false`, und der Runner prüft nach dem Repo-Sync erneut (Job FAILED „Refused: …“).

#### Übergabe an den Betrieb (Hermes): Tokens später umstellen

**Für das Upgrade selbst ist nichts zu tun.** Ohne gesetzte Slots läuft alles mit dem bestehenden Token weiter. Die Umstellung kann später, Zweck für Zweck und in beliebigem Abstand erfolgen. Es werden keine bestehenden Zugangsdaten geändert oder rotiert.

Je Zweck gilt dieselbe Reihenfolge — **erst die Gegenseite vorbereiten, dann den Slot setzen**, weil ab dem Setzen nur noch das neue Token gilt:

1. **Neues Token erzeugen** (zufällig, mindestens 32 Zeichen, z. B. mit einem Passwortgenerator). Nicht in Tickets, Chats oder Repos ablegen.
2. **Controller-CI** (`gitlab_controller_ci_token`):
   1. In GitLab im Projekt `iac-controller` unter *Settings → CI/CD → Variables* die Variable, die der Job für `X-Gitlab-Token` nutzt, auf das neue Token setzen (maskiert, geschützt).
   2. Direkt danach in Lyndrix: *IaC Orchestrator → Settings → GitLab Webhooks → „Token: controller CI“* das Token im Secret-Slot speichern (Recht `iac:credentials_admin`; API: `POST /api/plugins/lyndrix.plugin.iac_orchestrator/settings/secrets/gitlab_controller_ci_token` mit `{"value": "…"}`).
   3. Prüfen: Controller-Pipeline erneut laufen lassen (Check Env wird ausgelöst, HTTP 200).
3. **Service-CI** (`gitlab_service_ci_token`):
   1. Die CI/CD-Variable der Service-Pipelines (Gruppe der Service-Repos) auf das neue Token setzen.
   2. Direkt danach den Secret-Slot „Token: service CI“ setzen.
   3. Prüfen: einen Service auf dem Prod-Branch pushen → Trigger 200, Job läuft. Voraussetzung: `PROD_BRANCH` der Pipeline entspricht dem Prod-Branch im Katalog (heute `main`).
4. **Hooks** (`gitlab_hook_token`):
   1. Secret-Slot „Token: GitLab group hooks“ setzen.
   2. Sofort den Webhook-Sync auslösen (Settings → „Webhooks registrieren“ bzw. `POST /api/iac/webhook/sync`); er schreibt das neue Secret in alle Projekt-Hooks der Gruppe. Bis der Sync durch ist, werden Hook-Aufrufe mit dem alten Secret abgelehnt (betrifft nur Merge-Benachrichtigungen, keine Deployments).
   3. Prüfen: Health → `webhook_tokens.hook = "dedicated"`, in GitLab unter *Webhooks → Recent events* Antwort 200.
5. Zwischen Schritt *.1 und *.2 eines Zwecks schlagen Aufrufe mit dem jeweils „falschen“ Token mit 401 fehl — die beiden Schritte deshalb unmittelbar nacheinander ausführen, nicht während eines laufenden Rollouts.
6. **Zurück:** Einen Slot kann man nicht leeren, nur überschreiben. Um zum alten Verhalten zurückzukehren, das Token des Slots wieder auf der Gegenseite eintragen (oder den Vault-Key des Slots durch einen Administrator entfernen lassen).
7. Wenn alle drei Zwecke `dedicated` melden, wird das gemeinsame `gitlab_webhook_token` von keiner Route mehr akzeptiert. Ob und wann es entfernt wird, entscheidet der Betrieb (Rotationsliste) — das Plugin tut es nicht.

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
| `GET` | `/schedules` | `api:read` | Geparkte Deployments (`SCHEDULED`): `id`, `service`, `branch`, `trigger`, `not_before` (ISO UTC), `window_ci_ids`, `schedule_reason`, `start_time` |
| `POST` | `/schedules/{job_id}/run-now` | `api:write` | „Jetzt ausführen“: `SCHEDULED → QUEUED` mit `force`; das Override entsteht beim Lauf |
| `DELETE` | `/schedules/{job_id}` | `api:write` | Geparktes Deployment abbrechen (`ABORTED`) |
| `GET` | `/maintenance-windows?site=&stage=&kind=&q=` | `api:read` | Effektive Fenster je Hypervisor/Host/Service aus dem Snapshot `maintenance_effective`, je Item `state` (`active`, `until`, `source`, `next_start`) über `core.api.maintenance.effective_state` |
| `GET` | `/maintenance-default` | `api:read` | Nur lesend: `maintenance_default` je Site/Stage (`enabled`, `windows`, `source_file`, `level`) aus dem Controller-Snapshot |

**Hinweis:** Der Einstellungs-Endpunkt heißt `/settings` (nicht `/prefs`), da `/settings` auf Core-Ebene reserviert ist und zu einem Routing-Konflikt führen würde. Die Pfade `/maintenance/*` sind Plugin-Pflege (Statistik löschen, Repo-Sync) — die Wartungsfenster liegen daher unter `/schedules`, `/maintenance-windows` und `/maintenance-default`.

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
| emit | `iac:inventory_updated` | Generische Host-Liste `{hosts: [...], services: [...]}` für alle interessierten Plugins — je Host `name`, `hostname`, `ip`, `groups`, `baseline_roles`, `terraform` (**ohne** Zugangsdaten), `entity_type`, `site`, `stage`, `maintenance`, `incidents`, `ci_id`; je Service-Instanz `host`, `name`, `site`, `stage`, `ci_id`, `host_ci_id` |
| emit | `entity_registry:identify` | Batch-Anfrage an die Entity Registry (Chunks à 500): UUID je Host über `iac.host`, `pve.vm` (`node/vmid`), `fqdn`; je Service-Instanz über `iac.service = "<host>/<service>"` (Registry ≥ 0.3.0) |
| emit | `entity_registry:relate` | `runs_on` Service → Host für jede Service-Instanz mit beiden UUIDs (idempotent) |
| emit | `server_manager:register_server` | Je Host ein Upsert im Server-Manager (`source=iac-orchestrator`, `external_id=<Host-Key>`, `writeback=false` → dort schreibgeschützt, mit `ci_id`) |
| emit | `maintenance:windows_published` | Effektive Wartungsfenster aller Hypervisoren, Hosts und Service-Instanzen (`source: iac-orchestrator`, `replaces: true`) — nach jeder Inventar-Generierung und beim Runner-Start, **lokal** emittiert |
| emit | `maintenance:override_started` / `maintenance:override_ended` | Jeder ausgeführte Service-Deploy markiert die betroffenen Service- und Host-CIs als „in Wartung“ (`override_id = iac-job-<id>`, `until = now + max_override_minutes`); `ended` genau einmal auf jedem Endpfad |
| emit | `maintenance:window_missing` | Ein `deploy_policy: window`-Deploy lief sofort, weil im Horizont kein Fenster liegt (`event_id = window_missing:<ci_id>:<job_id>`) — das ITSM legt daraus einen Task an |
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

Seit 1.6 identifiziert der Orchestrator zusätzlich **Service-Instanzen** (`entity_type: service`,
Scheme `iac.service = "<host>/<service>"`, Anzeigename `"<service> @ <host>"`) und setzt die
Relation `runs_on` Service → Host. Die Reihenfolge in `NativeGenerateStage` und beim Runner-Start
ist fest: Registry-IDs (Hosts + Services) → Monitoring-Inventory-Sync (mit `ci_id`/`host_ci_id`)
→ `iac:inventory_updated` → `maintenance:windows_published`.

---

## Wartungsfenster und geplante Deployments (ab 1.6, Core-API ≥ 1.13)

Der Orchestrator ist die **Quelle** der Wartungsfenster für Hypervisoren, Hosts und Services
und der einzige, der Deployments danach plant. Vokabular, Zeitauswertung und Bus-Vertrag kommen
aus `core.api.maintenance`; der Orchestrator speichert keine Fenster anderer Plugins.

### Auflösung (`app/controller/maintenance_resolver.py`)

Läuft nur auf Runnern (Git-Working-Trees). Quellen: der iac-controller
(`environments/sites/<site>/{site_vars,hardware}.yml`,
`stages/<stage>/{stage_vars,stage,hosts}.yml`, Werte literal) und die auf dem Runner geklonten
`service.yml`-Dateien (`services_dir/<service>/service.yml`: `maintenance`, `incidents`,
`stage_overrides[<stage>]`). Spezifisch gewinnt, Listen werden **ersetzt**:

- **Service:** `service.yml` (+ `stage_overrides`) → Controller `hosts[].services[].maintenance`
  → Host `maintenance` (außer `inherit: false`) → Stage `maintenance_default` (wenn `enabled`)
  → Site `maintenance_default` → keins.
- **Host:** eigenes `maintenance` → Stage-Default → Site-Default → keins.
- **Hypervisor** (`hardware.yml`, auch Cluster-Nodes): eigenes `maintenance` → Site-Default → keins.
- **Incident-Regel:** eigener Block → `incident_defaults[<kind>]` der Stage → der Site → keine
  (`core.api.maintenance.resolve_incident_rule`).
- **`deploy_policy`:** Service → Stage-Default (`prod` → `window`, sonst `immediate`).
- Fehlt `tz`, gilt `site_vars.timezone`, sonst UTC. Ein ungültiger Block wird geloggt und
  **übersprungen** (die Kette läuft weiter) — die Auflösung bricht nie ein Deployment ab.

Nach jeder Inventar-Generierung emittiert der Runner `maintenance:windows_published`
(`replaces: true`, nur Items mit `ci_id`) und legt das Lesemodell als Snapshot
`maintenance_effective` in `iac_orchestrator_state` ab (alle Items, auch ohne `ci_id`, plus die
Default-Schalter je Site/Stage) — davon antworten `GET /maintenance-windows` und
`GET /maintenance-default` auf jeder Instanz.

### Entscheidung im Job: `MaintenancePolicyStage`

Die Entscheidung „sofort oder geplant“ fällt **im Job auf dem Runner**, direkt nach
`CloneServiceRepoStage` (frische `service.yml`), nicht beim Einreihen:

1. Zielhosts = Controller-Hosts, deren `services[]` den Service enthält (Branch `dev`/`*-dev`/`test`
   schränkt die Stage ein). Der **erste Host (sortiert) entscheidet**; abweichende Hosts stehen im
   Joblog.
2. Die Policy gilt nur für Trigger `gitlab-ci` / `webhook` (`trigger` im Payload). UI-Deploys
   (Katalog-Button), `force` („Jetzt ausführen“) und ein fälliger `SCHEDULED`-Job (`not_before`
   erreicht) laufen ohne Prüfung.
3. `deploy_policy: window` und kein laufendes Fenster → der Job wird **geparkt**: Status
   `SCHEDULED`, `not_before` = nächster Fensterbeginn (innerhalb `maintenance_schedule_horizon_days`),
   `window_ci_ids`, `schedule_reason`; Claim freigegeben, kein Endstatus, Bell-Eintrag „⏳ scheduled“.
   `claim_next` nimmt fällige `SCHEDULED`-Jobs (`not_before <= now`) vor der normalen Queue und klont
   erneut. **Kein Fenster im Horizont** (oder gar keines definiert) → Warnung im Joblog, Deploy
   **sofort** und `maintenance:window_missing` (Entscheidung H; nur mit Service-CI und Setting
   `maintenance_report_missing_window`). Ohne ITSM bleibt die Joblog-Warnung.
4. **Jeder ausgeführte Deploy ist ein Override** (`maintenance_deploy_marks_maintenance`):
   `maintenance:override_started` für Service- und Host-CIs der Zielhosts
   (`override_id = iac-job-<id>`, `until = now + maintenance_max_override_minutes`,
   `linger_minutes`), `override_ended` genau einmal am Ende — Pipeline-Ende, Abort, Orphan-Sweep,
   Container-Recovery, Startup-Reconciliation (`DeploymentEngine.end_override_for`, die
   `override_id` wird atomar aus der Jobzeile genommen).

Queue-Regeln: ein zweiter Trigger für einen `SCHEDULED`-Job wird **zusammengefasst** (Payload
ersetzt, `start_time = now`, damit die zweite CI den Job über `since_epoch` findet); ein UI-Deploy
**promotet** ihn (`QUEUED`, `force`). `QUEUED`/`RUNNING` bleiben „erster gewinnt“ (`DuplicateJob`).
`POST /abort` lässt `SCHEDULED` stehen, `DELETE /schedules/{id}` bricht ab; „Statistik löschen“
behält `SCHEDULED` und `RUNNING`. Zeiten in der Jobtabelle sind wie bisher naiv in der Prozessuhr
(`to_queue_clock`/`from_queue_clock`).

### Default-Schalter nur lesend (Entscheidung C)

Der Orchestrator bleibt Ausführung: er zeigt `maintenance_default` je Site/Stage (`enabled`,
Fenster, Quelldatei) aus dem Snapshot, schreibt aber **nie** ins Controller-Repo. Ein/Aus wird in
`environments/sites/<site>/stages/<stage>/stage_vars.yml` (Block `maintenance_default` unter
`stage_vars:`, bzw. `site_vars.yml`) editiert; der nächste Sync veröffentlicht neu. Der
Webhook-Pfad ignoriert Deploy-Trigger für Commits mit dem reservierten Präfix `ci: maintenance`
(der künftige SSoT-Manager-Schalter), der Sync läuft weiter.

### Bekannte Grenzen

- Ein Service auf mehreren Hosts: das Fenster des ersten Zielhosts (sortiert) entscheidet.
- Services, die der Runner noch nie geklont hat, erben Host/Stage-Default (kein `service.yml`).
- `window_missing` wird je Deploy-Job emittiert; die Deduplizierung (ein offener Task je CI) liegt
  im ITSM.
- Service-CIs brauchen die Entity Registry ≥ 0.3.0 (Scheme `iac.service`); davor bleiben Service-Items
  ohne `ci_id` (nur Host-Overrides, kein `window_missing`).

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

Kategorie **Maintenance** (ab 1.6; Env `PLUGIN_IAC_ORCHESTRATOR_MAINTENANCE_*` hat Vorrang vor Vault):

| Key | Typ | Default | Bedeutung |
|---|---|---|---|
| `maintenance_deploy_marks_maintenance` | bool | `true` | Jeder ausgeführte Deploy meldet ein Wartungs-Override für Service- und Host-CIs |
| `maintenance_max_override_minutes` | int | `240` | Obergrenze eines Deploy-Overrides (`until`); endet früher mit dem Job |
| `maintenance_linger_minutes` | int | `15` | Nachlauf des Overrides nach `override_ended` beim Konsumenten |
| `maintenance_schedule_horizon_days` | int | `30` | Maximaler Vorlauf eines geparkten `window`-Deploys |
| `maintenance_report_missing_window` | bool | `true` | `maintenance:window_missing` emittieren, wenn ein `window`-Deploy ohne Fenster sofort läuft |

Der Default-Schalter `maintenance_default.enabled` ist **kein** Setting — er liegt im iac-controller (siehe „Wartungsfenster“).

---

### Runner-Container: Mounts (Audit OS-06)

Setting `runner_readonly_mounts` (Kategorie *Runner*, Standard **an**; Vault `iac_runner_readonly_mounts`, Env `PLUGIN_IAC_ORCHESTRATOR_RUNNER_READONLY_MOUNTS`):

| Container | Pfad im Container | an (Standard) | aus (wie ≤ 1.6.1) |
|---|---|---|---|
| Ansible | `/data/storage/git_repos` | **read-only**, jedes `<repo>/.git` durch ein leeres read-only-tmpfs verdeckt | read-write |
| Ansible | `/data/storage/services` | **read-only**, jedes `<service>/.git` verdeckt | read-write |
| Ansible | `/data/scratch` (`IAC_SCRATCH_DIR`) | read-write, pro Job (`git_repos/.job_snapshots/job_<id>/scratch`), wird mit dem Job-Snapshot gelöscht | — |
| tofu | `/data/storage/git_repos/<Run-Dir des Jobs>` | **nur dieses Verzeichnis**, read-write | ganz `git_repos` read-write |
| tofu | `/data/storage/terraform-providers` | **read-only** | read-write |

Hintergrund: Die Container führen Inhalte aus Repositories aus (Playbooks, Rollen, Service-Repos, Provider). Mit Schreibrechten auf die Checkouts konnten sie andere Repositories, das generierte Inventar oder `.git/hooks`/`.git/config` verändern — git im Core führt das später aus.

Hinweise für den Betrieb:

- Der Ansible-Container baut weiterhin in seinem eigenen Dateisystem (`/tmp/aac_builds`) und deployt per SSH; der SSH-Key wird aus `services/.iac_keys/` gelesen. Ein Playbook, das **in den Checkout schreiben** muss, scheitert mit „Read-only file system“ → entweder nach `/data/scratch` schreiben oder das Setting ausschalten.
- Fehlt dem read-only Provider-Cache ein Provider (neue Version), wiederholt der Lauf `tofu init` ohne Cache und installiert in sein Run-Dir (Job-Log: „init with the read-only provider cache failed — retrying without the cache“). Das funktioniert, ist aber langsamer und lädt bei jedem Lauf neu. **Cache gezielt füllen:** das Setting kurz ausschalten, einen Plan (Check Env) laufen lassen, wieder einschalten.

## React-Routen

| Pfad | Sichtbar in Sidebar | Beschreibung |
|---|---|---|
| `/iac` | Ja | Hauptansicht: Dashboard, Pipeline-Historie, Job-Übersicht, Tab „Wartung“ (geplante Deployments mit „Jetzt ausführen“/„Abbrechen“, effektive Fenster mit Filtern, Default-Fenster nur lesend) |
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
