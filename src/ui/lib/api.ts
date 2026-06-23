const PLUGIN_ID = 'lyndrix.plugin.iac_orchestrator'
const TOKEN_KEY = 'lyndrix_token'

export function getToken(): string | null {
  return localStorage.getItem(TOKEN_KEY)
}

export function pluginPath(subpath: string): string {
  return `/api/plugins/${PLUGIN_ID}/${subpath}`
}

async function apiFetch<T>(path: string, init: RequestInit = {}): Promise<T> {
  const token = getToken()
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
    ...(init.headers as Record<string, string> | undefined),
  }

  const res = await fetch(path, { ...init, headers })

  if (res.status === 401) {
    localStorage.removeItem(TOKEN_KEY)
    window.location.href = '/login'
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
  infraPlan: () => pluginApi.post<{ status: string; message: string }>('infra/plan'),
  infraApply: () => pluginApi.post<{ status: string; message: string }>('infra/apply'),
}
