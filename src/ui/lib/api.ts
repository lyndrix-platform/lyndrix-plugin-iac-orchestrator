const PLUGIN_ID = 'lyndrix.plugin.iac_orchestrator'
const TOKEN_KEY = 'lyndrix_token'

export function getToken(): string | null {
  return localStorage.getItem(TOKEN_KEY)
}

export function pluginPath(subpath: string): string {
  return `/api/plugins/${PLUGIN_ID}/${subpath}`
}

// Default per-request timeout. No request should ever hang forever: a stalled
// backend must surface an error so loading spinners resolve and polling loops
// don't accumulate pending promises.
const DEFAULT_TIMEOUT_MS = 20_000

/** Client-side (History API) redirect to the login shell — no hard reload. */
function redirectToLogin(): void {
  if (window.location.pathname === '/login') return
  window.history.pushState({}, '', '/login')
  window.dispatchEvent(new PopStateEvent('popstate'))
}

async function apiFetch<T>(path: string, init: RequestInit = {}): Promise<T> {
  const token = getToken()
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
    ...(init.headers as Record<string, string> | undefined),
  }

  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), DEFAULT_TIMEOUT_MS)

  let res: Response
  try {
    res = await fetch(path, { ...init, headers, signal: controller.signal })
  } catch (e) {
    if (e instanceof DOMException && e.name === 'AbortError') {
      throw new Error('Request timed out')
    }
    throw e
  } finally {
    clearTimeout(timer)
  }

  if (res.status === 401) {
    localStorage.removeItem(TOKEN_KEY)
    redirectToLogin()
    throw new Error('Nicht autorisiert')
  }

  if (!res.ok) {
    let msg = `HTTP ${res.status}`
    try {
      const body = (await res.json()) as { detail?: string }
      msg = body.detail ?? msg
    } catch { /* ignore */ }
    throw new Error(msg)
  }

  return res.json() as Promise<T>
}

export const pluginApi = {
  get: <T>(subpath: string) => apiFetch<T>(pluginPath(subpath)),
  post: <T>(subpath: string, body?: unknown) =>
    apiFetch<T>(pluginPath(subpath), {
      method: 'POST',
      body: body !== undefined ? JSON.stringify(body) : undefined,
    }),
  del: <T>(subpath: string) =>
    apiFetch<T>(pluginPath(subpath), { method: 'DELETE' }),
}

// ─── Domain types ───────────────────────────────────────────────────────────

export interface IaCJob {
  id: number
  pipeline_type: string
  status: string
  progress: number
  current_step?: string
  start_time: string
  end_time: string
}

export interface CatalogService {
  name?: string
  repository_name?: string
  branch?: string
  target_environment?: string
  host?: string
  deploy_type?: string
}

export interface RunnerTask {
  job_id?: number
  status?: string
  [k: string]: unknown
}

export interface JobStreamSnapshot {
  jobs: IaCJob[]
  active_tasks: Record<string, RunnerTask>
  is_running: boolean
  ts: number
}

export interface JobLogsResponse {
  job_id: number
  lines: string[]
  source: string
  tail: number
  grep: string | null
}

export interface StatsPhase {
  phase: string
  label: string
  icon: string
  color: string
  total: number
  success: number
  failed: number
  running: number
  success_rate: number
}

export interface StatsRecent {
  id: number
  pipeline_type: string
  type_label: string
  phase: string
  icon: string
  color: string
  status: string
  progress: number
  duration_s: number | null
  duration_human: string
  start_label: string
}

export interface OrchestratorStats {
  total: number
  success: number
  failed: number
  running: number
  finished: number
  success_rate: number
  avg_duration_s: number | null
  avg_duration_human: string
  last_deployment_status: string | null
  last_deployment_at: string | null
  by_status: Record<string, number>
  by_phase: StatsPhase[]
  recent: StatsRecent[]
}

export interface Assignment {
  site: string
  stage: string
  host: string
  services: string[]
}

export interface TerraformHost {
  site: string
  stage: string
  host: string
  ansible_host: string
  managed: boolean
  provider: string
  resource: string
  workspace: string
  state: string
}

export interface ServiceHistoryRow {
  id: number
  pipeline_type: string
  status: string
  progress: number
  start_time: string
}

export interface IaCSettings {
  auto_apply: boolean
  test_deploy_allowed_hosts: string
  gitlab_url: string
  group_id: string
  lyndrix_base_url: string
  gitlab_token_key: string
  autosync_enabled: boolean
  sync_interval: number
  webhook_endpoint: string
}

// Schema-driven settings (comprehensive surface: ansible / terraform / repo roles).
export interface SettingField {
  key: string
  label: string
  kind: 'str' | 'bool' | 'int' | 'select' | 'textarea' | 'password'
  category: string
  sensitive: boolean
  default: unknown
  description: string
  options: string[]
}

export interface SettingsSchemaResponse {
  schema: SettingField[]
}

export interface SettingsValuesResponse {
  values: Record<string, unknown>
}

export interface PipelinePayload {
  pipeline_type: 'bootstrap_compliance' | 'adopt_host' | 'rollout' | 'init_host' | 'compliance'
  limit?: string
  host_name?: string
}

export interface AcceptedResponse {
  status: string
  message?: string
  [k: string]: unknown
}

// ─── Typed endpoint helpers ─────────────────────────────────────────────────

export const iacApi = {
  jobs: (limit = 30) => pluginApi.get<IaCJob[]>(`jobs?limit=${limit}`),
  catalog: () => pluginApi.get<CatalogService[]>('catalog'),
  jobLogs: (id: number, tail = 400, grep?: string) =>
    pluginApi.get<JobLogsResponse>(
      `jobs/${id}/logs?tail=${tail}${grep ? `&grep=${encodeURIComponent(grep)}` : ''}`,
    ),
  jobRunners: (id: number) =>
    pluginApi.get<{ job_id: number; runners: Record<string, RunnerTask> }>(`jobs/${id}/runners`),
  deployService: (name: string, branch = 'main') =>
    pluginApi.post<{ status: string; message: string }>(`deploy/service/${name}`, { branch }),
  infraPlan: () => pluginApi.post<AcceptedResponse>('infra/plan'),
  infraApply: () => pluginApi.post<AcceptedResponse>('infra/apply'),

  // ── Parity additions ──────────────────────────────────────────────────────
  stats: () => pluginApi.get<OrchestratorStats>('stats'),
  assignments: () => pluginApi.get<Assignment[]>('infrastructure/assignments'),
  terraformHosts: () => pluginApi.get<TerraformHost[]>('infrastructure/terraform-hosts'),
  serviceHistory: (name: string) =>
    pluginApi.get<ServiceHistoryRow[]>(`service/${encodeURIComponent(name)}/history`),
  runPipeline: (payload: PipelinePayload) => pluginApi.post<AcceptedResponse>('pipeline', payload),
  abort: () => pluginApi.post<AcceptedResponse>('abort'),
  getSettings: () => pluginApi.get<IaCSettings>('settings/general'),
  saveSettings: (payload: Partial<IaCSettings>) => pluginApi.post<IaCSettings>('settings/general', payload),
  // Schema-driven comprehensive settings (ansible / terraform / repo roles).
  settingsSchema: () => pluginApi.get<SettingsSchemaResponse>('settings/schema'),
  settingsValues: () => pluginApi.get<SettingsValuesResponse>('settings/values'),
  saveSettingsValues: (values: Record<string, unknown>) =>
    pluginApi.post<{ status: string; saved: string[]; values: Record<string, unknown> }>(
      'settings/values', { values },
    ),
  listCredentials: () => pluginApi.get<{ credentials: string[] }>('settings/credentials'),
  addCredential: (alias: string, secret: string) =>
    pluginApi.post<{ status: string; alias: string; credentials: string[] }>(
      'settings/credentials', { alias, secret },
    ),
  deleteCredential: (alias: string) =>
    pluginApi.del<{ status: string; credentials: string[] }>(
      `settings/credentials/${encodeURIComponent(alias)}`,
    ),
  getWebhookToken: () =>
    pluginApi.get<{ configured: boolean; masked: string }>('settings/webhook-token'),
  generateWebhookToken: () =>
    pluginApi.post<{ status: string; token: string }>('settings/webhook-token/generate'),
  syncWebhooks: () =>
    pluginApi.post<AcceptedResponse & { projects_total?: number; created?: number; updated?: number; failed?: number }>(
      'settings/webhooks/sync',
    ),
  // Short-lived ticket so the SSE/raw-log URLs never carry the bearer token.
  streamTicket: () =>
    pluginApi.post<{ ticket: string; expires_in: number }>('stream/ticket'),

  // Maintenance actions (ported from the NiceGUI settings page).
  clearStats: () =>
    pluginApi.post<{ status: string; deleted: number }>('maintenance/clear-stats'),
  syncRepos: () =>
    pluginApi.post<AcceptedResponse>('maintenance/sync-repos'),
}
