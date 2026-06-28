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
| emit | `iac:pipeline_started` | Pipeline wurde gestartet |
| emit | `iac:webhook_verified` | Webhook akzeptiert und weitergeleitet |
| emit | `git:sync` | Git-Sync anfordern |
| emit | `git:commit_push` | Commit + Push anfordern |
| emit | `system:notify` | Plattform-Benachrichtigung |
| emit | `user:notify` | Benutzer-Benachrichtigung |
| emit | `monitoring:inventory_sync` | Inventar-Sync an Monitoring-Plugin |
| emit | `socket:request` | Socket-Anfrage senden |
| emit | `messaging:outbound` | Ausgehende Nachricht über Messaging Gateway |

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
