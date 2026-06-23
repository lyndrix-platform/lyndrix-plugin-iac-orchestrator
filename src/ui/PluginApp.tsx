import React, { useState, useEffect, useCallback, useMemo, useRef } from 'react'
import { iacApi, type CatalogService, type IaCJob, type RunnerTask } from './lib/api'
import { useJobsSSE } from './lib/hooks'

// ─── Helpers ─────────────────────────────────────────────────────────────────

const RUNNING_STATES = new Set(['RUNNING', 'PENDING'])
const FAIL_STATES = new Set(['FAILED', 'ERROR', 'ABORTED'])

function statusColor(status: string): string {
  const s = (status || '').toUpperCase()
  if (s === 'SUCCESS') return 'var(--lx-state-up)'
  if (FAIL_STATES.has(s)) return 'var(--lx-state-down)'
  if (RUNNING_STATES.has(s)) return 'var(--lx-accent)'
  return 'var(--lx-state-unknown)'
}

function describeType(t: string): string {
  return (t || 'unknown').replace(/[:_]/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase())
}

// ─── Shared atoms ────────────────────────────────────────────────────────────

function StatusBadge({ status }: { status: string }) {
  const color = statusColor(status)
  return (
    <span style={{
      display: 'inline-flex', alignItems: 'center', gap: 4,
      fontSize: '0.65rem', fontWeight: 700, color,
      background: `color-mix(in srgb, ${color} 12%, transparent)`,
      border: `1px solid color-mix(in srgb, ${color} 30%, transparent)`,
      borderRadius: 'var(--lx-radius-sm)', padding: '2px 7px',
      letterSpacing: '0.04em', textTransform: 'uppercase',
    }}>
      <span style={{ width: 5, height: 5, borderRadius: '50%', background: color }} />
      {status || 'UNKNOWN'}
    </span>
  )
}

function ProgressBar({ value, color }: { value: number; color?: string }) {
  const c = color ?? 'var(--lx-accent)'
  return (
    <div style={{
      width: '100%', height: 6, borderRadius: 999,
      background: 'var(--lx-border-soft)', overflow: 'hidden',
    }}>
      <div style={{
        width: `${Math.max(0, Math.min(100, value))}%`, height: '100%',
        background: c, transition: 'width 0.4s ease',
      }} />
    </div>
  )
}

function Card({ children, accent }: { children: React.ReactNode; accent?: string }) {
  return (
    <div style={{
      background: 'var(--lx-surface)',
      border: '1px solid var(--lx-border-soft)',
      borderRadius: 'var(--lx-radius-md)',
      overflow: 'hidden',
      ...(accent ? { borderTop: `2px solid ${accent}` } : {}),
    }}>
      {children}
    </div>
  )
}

function KpiCard({ label, value, color }: { label: string; value: React.ReactNode; color?: string }) {
  return (
    <Card>
      <div style={{ padding: '1rem 1.1rem' }}>
        <div style={{ fontSize: '1.6rem', fontWeight: 800, color: color ?? 'var(--lx-text)', lineHeight: 1.1 }}>
          {value}
        </div>
        <div style={{ fontSize: '0.7rem', color: 'var(--lx-text-muted)', textTransform: 'uppercase', letterSpacing: '0.06em', marginTop: 4 }}>
          {label}
        </div>
      </div>
    </Card>
  )
}

function ErrorBox({ msg }: { msg: string }) {
  return (
    <div style={{
      padding: '0.6rem 1rem', borderRadius: 'var(--lx-radius-md)',
      background: 'color-mix(in srgb, var(--lx-state-down) 10%, transparent)',
      border: '1px solid color-mix(in srgb, var(--lx-state-down) 25%, transparent)',
      color: 'var(--lx-state-down)', fontSize: '0.8rem', marginBottom: '1rem',
    }}>{msg}</div>
  )
}

function Button({ label, onClick, variant = 'default', disabled, icon }: {
  label: string; onClick: () => void; variant?: 'default' | 'primary' | 'danger'; disabled?: boolean; icon?: string
}) {
  const accent = variant === 'danger' ? 'var(--lx-state-down)' : 'var(--lx-accent)'
  return (
    <button onClick={onClick} disabled={disabled} style={{
      padding: '4px 12px', fontSize: '0.72rem', fontWeight: 600,
      border: `1px solid color-mix(in srgb, ${accent} 40%, transparent)`,
      borderRadius: 'var(--lx-radius-sm)',
      background: variant === 'primary' ? `color-mix(in srgb, ${accent} 20%, transparent)` : `color-mix(in srgb, ${accent} 8%, transparent)`,
      color: disabled ? 'var(--lx-text-muted)' : accent,
      cursor: disabled ? 'not-allowed' : 'pointer', opacity: disabled ? 0.5 : 1,
      display: 'inline-flex', alignItems: 'center', gap: 5,
    }}>
      
      {label}
    </button>
  )
}

// ─── Live log viewer ─────────────────────────────────────────────────────────

function LogViewer({ jobId, onClose }: { jobId: number; onClose: () => void }) {
  const [lines, setLines] = useState<string[]>([])
  const [grep, setGrep] = useState('')
  const [err, setErr] = useState<string | null>(null)
  const scrollRef = useRef<HTMLDivElement | null>(null)

  const load = useCallback(async () => {
    try {
      const res = await iacApi.jobLogs(jobId, 600, grep || undefined)
      setLines(res.lines)
      setErr(null)
    } catch (e) {
      setErr(e instanceof Error ? e.message : 'Log konnte nicht geladen werden')
    }
  }, [jobId, grep])

  useEffect(() => {
    void load()
    const t = setInterval(() => void load(), 2000)
    return () => clearInterval(t)
  }, [load])

  useEffect(() => {
    if (scrollRef.current) scrollRef.current.scrollTop = scrollRef.current.scrollHeight
  }, [lines])

  return (
    <div onClick={onClose} style={{
      position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.6)', zIndex: 1000,
      display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '2rem',
    }}>
      <div onClick={(e) => e.stopPropagation()} style={{
        width: '100%', maxWidth: 1000, height: '80vh', display: 'flex', flexDirection: 'column',
        background: 'var(--lx-elevated)', border: '1px solid var(--lx-border)',
        borderRadius: 'var(--lx-radius-lg)', overflow: 'hidden',
      }}>
        <div style={{
          display: 'flex', alignItems: 'center', gap: '0.75rem', padding: '0.75rem 1rem',
          borderBottom: '1px solid var(--lx-border-soft)', background: 'var(--lx-surface)',
        }}>
          <span style={{ fontWeight: 700, color: 'var(--lx-accent)' }}>Live Logs · Job #{jobId}</span>
          <input value={grep} onChange={(e) => setGrep(e.target.value)} placeholder="grep…" style={{
            marginLeft: 'auto', padding: '0.3rem 0.6rem', fontSize: '0.75rem',
            borderRadius: 'var(--lx-radius-sm)', border: '1px solid var(--lx-border-soft)',
            background: 'var(--lx-elevated)', color: 'var(--lx-text)', outline: 'none',
          }} />
          <button onClick={onClose} style={{ background: 'none', border: 'none', color: 'var(--lx-text-muted)', cursor: 'pointer', fontSize: '1.1rem' }}>✕</button>
        </div>
        <div ref={scrollRef} style={{
          flex: 1, overflow: 'auto', background: '#000', padding: '0.75rem 1rem',
          fontFamily: 'monospace', fontSize: '0.7rem', color: '#4ade80', whiteSpace: 'pre-wrap', wordBreak: 'break-word',
        }}>
          {err ? <span style={{ color: 'var(--lx-state-down)' }}>{err}</span>
            : lines.length ? lines.join('\n') : 'Keine Logs gefunden.'}
        </div>
      </div>
    </div>
  )
}

// ─── Active pipelines (live) ─────────────────────────────────────────────────

function ActivePipelines({ jobs, runnersByJob, onLogs }: {
  jobs: IaCJob[]
  runnersByJob: Record<number, [string, RunnerTask][]>
  onLogs: (id: number) => void
}) {
  const running = jobs.filter((j) => RUNNING_STATES.has((j.status || '').toUpperCase()))
  if (!running.length) {
    return (
      <div style={{ textAlign: 'center', padding: '3rem 0', color: 'var(--lx-text-muted)' }}>
        <div style={{ fontSize: 40, opacity: 0.4 }}>✓</div>
        <div style={{ marginTop: 8, fontWeight: 600 }}>Infrastructure is stable. No active jobs.</div>
      </div>
    )
  }
  return (
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(380px, 1fr))', gap: '1rem' }}>
      {running.map((job) => {
        const runners = runnersByJob[job.id] || []
        return (
          <Card key={job.id} accent="var(--lx-accent)">
            <div style={{ padding: '1rem' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                <div>
                  <div style={{ fontWeight: 800, color: 'var(--lx-accent)', fontSize: '1.05rem' }}>Pipeline #{job.id}</div>
                  <div style={{ fontSize: '0.6rem', textTransform: 'uppercase', letterSpacing: '0.1em', color: 'var(--lx-text-muted)', fontWeight: 700 }}>{job.pipeline_type}</div>
                </div>
                <StatusBadge status={job.status} />
              </div>
              <div style={{ marginTop: '0.85rem', display: 'flex', alignItems: 'center', gap: 8 }}>
                <div style={{ flex: 1 }}><ProgressBar value={job.progress} /></div>
                <span style={{ fontSize: '0.7rem', fontWeight: 700, color: 'var(--lx-text)' }}>{job.progress}%</span>
              </div>
              <div style={{ marginTop: 6, fontSize: '0.7rem', fontFamily: 'monospace', color: 'var(--lx-text-muted)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                {job.current_step || '…'}
              </div>

              <div style={{ fontSize: '0.6rem', textTransform: 'uppercase', letterSpacing: '0.08em', color: 'var(--lx-text-muted)', fontWeight: 700, marginTop: '0.9rem', marginBottom: 4 }}>Active Runners</div>
              <div style={{ background: 'rgba(0,0,0,0.25)', border: '1px solid var(--lx-border-soft)', borderRadius: 'var(--lx-radius-sm)', padding: '0.4rem 0.6rem', minHeight: 28 }}>
                {runners.length ? runners.map(([name, data]) => (
                  <div key={name} style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: '0.68rem', color: 'var(--lx-text)' }}>
                    <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{name}</span>
                    <span style={{ marginLeft: 'auto', fontSize: '0.6rem', color: 'var(--lx-text-muted)' }}>{String(data.status ?? '')}</span>
                  </div>
                )) : <span style={{ fontSize: '0.65rem', color: 'var(--lx-text-muted)', fontStyle: 'italic' }}>Waiting for pool…</span>}
              </div>

              <div style={{ marginTop: '0.85rem' }}>
                <Button label="Live Logs" icon="terminal" onClick={() => onLogs(job.id)} />
              </div>
            </div>
          </Card>
        )
      })}
    </div>
  )
}

// ─── History ─────────────────────────────────────────────────────────────────

function History({ jobs, onLogs }: { jobs: IaCJob[]; onLogs: (id: number) => void }) {
  const [filter, setFilter] = useState('')
  const term = filter.toLowerCase()
  const rows = term
    ? jobs.filter((j) => String(j.id).includes(term) || j.pipeline_type.toLowerCase().includes(term) || j.status.toLowerCase().includes(term))
    : jobs

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
        <h2 style={{ margin: 0, fontSize: '1rem', fontWeight: 700, color: 'var(--lx-text)' }}>Deployment History</h2>
        <input value={filter} onChange={(e) => setFilter(e.target.value)} placeholder="Filter ID / Type / Status…" style={{
          padding: '0.35rem 0.7rem', fontSize: '0.78rem', width: 240,
          borderRadius: 'var(--lx-radius-sm)', border: '1px solid var(--lx-border-soft)',
          background: 'var(--lx-elevated)', color: 'var(--lx-text)', outline: 'none',
        }} />
      </div>
      <Card>
        {rows.length === 0 && (
          <div style={{ padding: '2rem', textAlign: 'center', color: 'var(--lx-text-muted)', fontSize: '0.85rem' }}>Keine Deployments gefunden.</div>
        )}
        {rows.map((job, i) => (
          <div key={job.id} style={{
            display: 'flex', alignItems: 'center', gap: '0.75rem', padding: '0.6rem 1rem',
            borderTop: i === 0 ? 'none' : '1px solid var(--lx-border-soft)',
          }}>
            <span style={{ width: 4, height: 30, borderRadius: 2, background: statusColor(job.status), flexShrink: 0 }} />
            <div style={{ flex: 1, minWidth: 0 }}>
              <div style={{ fontSize: '0.8rem', fontWeight: 600, color: 'var(--lx-text)' }}>
                #{job.id} · {describeType(job.pipeline_type)}
              </div>
              <div style={{ fontSize: '0.66rem', color: 'var(--lx-text-muted)', fontFamily: 'monospace' }}>
                {job.start_time} → {job.end_time}
              </div>
            </div>
            <div style={{ width: 90 }}><ProgressBar value={job.progress} color={statusColor(job.status)} /></div>
            <StatusBadge status={job.status} />
            <button onClick={() => onLogs(job.id)} title="Logs" style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--lx-text-muted)' }}>
              </button>
          </div>
        ))}
      </Card>
    </div>
  )
}

// ─── Service catalog ─────────────────────────────────────────────────────────

function ServiceCatalog() {
  const [services, setServices] = useState<CatalogService[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [notice, setNotice] = useState<string | null>(null)
  const [search, setSearch] = useState('')

  useEffect(() => {
    iacApi.catalog()
      .then(setServices)
      .catch((e) => setError(e instanceof Error ? e.message : 'Katalog konnte nicht geladen werden'))
      .finally(() => setLoading(false))
  }, [])

  async function deploy(name: string, branch?: string) {
    setNotice(null)
    try {
      await iacApi.deployService(name, branch || 'main')
      setNotice(`Deployment für "${name}" eingereiht.`)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Deploy fehlgeschlagen')
    }
  }

  const term = search.toLowerCase()
  const rows = services.filter((s) => !term || (s.name || '').toLowerCase().includes(term) || (s.repository_name || '').toLowerCase().includes(term))

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
        <h2 style={{ margin: 0, fontSize: '1rem', fontWeight: 700, color: 'var(--lx-text)' }}>Service Catalog</h2>
        <input value={search} onChange={(e) => setSearch(e.target.value)} placeholder="Service suchen…" style={{
          padding: '0.35rem 0.7rem', fontSize: '0.78rem', width: 240,
          borderRadius: 'var(--lx-radius-sm)', border: '1px solid var(--lx-border-soft)',
          background: 'var(--lx-elevated)', color: 'var(--lx-text)', outline: 'none',
        }} />
      </div>
      {error && <ErrorBox msg={error} />}
      {notice && (
        <div style={{ padding: '0.6rem 1rem', borderRadius: 'var(--lx-radius-md)', background: 'color-mix(in srgb, var(--lx-state-up) 10%, transparent)', border: '1px solid color-mix(in srgb, var(--lx-state-up) 25%, transparent)', color: 'var(--lx-state-up)', fontSize: '0.8rem', marginBottom: '1rem' }}>{notice}</div>
      )}
      {loading && <div style={{ color: 'var(--lx-text-muted)', padding: '2rem', textAlign: 'center' }}>Lade Katalog…</div>}
      {!loading && rows.length === 0 && (
        <Card><div style={{ padding: '2rem', textAlign: 'center', color: 'var(--lx-text-muted)', fontSize: '0.85rem' }}>Keine Services gefunden. Stelle sicher, dass "iac_controller" synchronisiert ist.</div></Card>
      )}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(300px, 1fr))', gap: '1rem' }}>
        {rows.map((svc) => {
          const name = svc.name || 'Unknown'
          const branch = svc.branch || 'main'
          const target = svc.target_environment || svc.host || 'Auto-Assigned'
          return (
            <Card key={name} accent="var(--lx-accent-2)">
              <div style={{ padding: '1rem' }}>
                <div style={{ fontWeight: 700, color: 'var(--lx-text)', fontSize: '0.9rem' }}>{name}</div>
                <div style={{ fontSize: '0.65rem', color: 'var(--lx-text-muted)' }}>Repo: {svc.repository_name || name}</div>
                <div style={{ display: 'flex', gap: '1rem', marginTop: '0.7rem', fontSize: '0.68rem', color: 'var(--lx-text-muted)', fontFamily: 'monospace' }}>
                  <span>{target}</span>
                  <span>{branch}</span>
                </div>
                <div style={{ marginTop: '0.85rem', display: 'flex', justifyContent: 'flex-end' }}>
                  <Button label="Deploy" icon="rocket" variant="primary" onClick={() => void deploy(name, branch)} />
                </div>
              </div>
            </Card>
          )
        })}
      </div>
    </div>
  )
}

// ─── Overview ────────────────────────────────────────────────────────────────

function Overview({ jobs, isRunning }: { jobs: IaCJob[]; isRunning: boolean }) {
  const total = jobs.length
  const success = jobs.filter((j) => (j.status || '').toUpperCase() === 'SUCCESS').length
  const failed = jobs.filter((j) => FAIL_STATES.has((j.status || '').toUpperCase())).length
  const running = jobs.filter((j) => RUNNING_STATES.has((j.status || '').toUpperCase())).length
  const rate = total ? Math.round((success / total) * 100) : 0

  return (
    <div>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))', gap: '1rem', marginBottom: '1.5rem' }}>
        <KpiCard label="Deployments" value={total} />
        <KpiCard label="Success Rate" value={`${rate}%`} color="var(--lx-state-up)" />
        <KpiCard label="Failed" value={failed} color={failed ? 'var(--lx-state-down)' : undefined} />
        <KpiCard label="Active" value={running} color={running ? 'var(--lx-accent)' : undefined} />
        <KpiCard label="Engine" value={isRunning ? 'Busy' : 'Idle'} color={isRunning ? 'var(--lx-accent)' : 'var(--lx-state-up)'} />
      </div>
      <Card>
        <div style={{ padding: '0.75rem 1rem', borderBottom: '1px solid var(--lx-border-soft)', fontSize: '0.78rem', fontWeight: 700, color: 'var(--lx-text)', background: 'var(--lx-elevated)' }}>
          Recent Activity
        </div>
        {jobs.slice(0, 8).map((job, i) => (
          <div key={job.id} style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', padding: '0.55rem 1rem', borderTop: i === 0 ? 'none' : '1px solid var(--lx-border-soft)' }}>
            <span style={{ width: 4, height: 24, borderRadius: 2, background: statusColor(job.status) }} />
            <span style={{ flex: 1, fontSize: '0.78rem', color: 'var(--lx-text)' }}>#{job.id} · {describeType(job.pipeline_type)}</span>
            <span style={{ fontSize: '0.66rem', color: 'var(--lx-text-muted)', fontFamily: 'monospace' }}>{job.start_time}</span>
            <StatusBadge status={job.status} />
          </div>
        ))}
        {jobs.length === 0 && <div style={{ padding: '2rem', textAlign: 'center', color: 'var(--lx-text-muted)', fontSize: '0.85rem' }}>Noch keine Jobs.</div>}
      </Card>
    </div>
  )
}

// ─── Root ────────────────────────────────────────────────────────────────────

type TabId = 'overview' | 'active' | 'catalog' | 'history'

const TABS: { id: TabId; label: string; icon: string }[] = [
  { id: 'overview', label: 'Overview', icon: 'dashboard' },
  { id: 'active', label: 'Active Pipelines', icon: 'bolt' },
  { id: 'catalog', label: 'Service Catalog', icon: 'apps' },
  { id: 'history', label: 'History', icon: 'history' },
]

export default function PluginApp() {
  const { snapshot, connected, error } = useJobsSSE()
  const [tab, setTab] = useState<TabId>('overview')
  const [logJob, setLogJob] = useState<number | null>(null)

  const jobs = snapshot?.jobs ?? []
  const isRunning = snapshot?.is_running ?? false

  const runnersByJob = useMemo(() => {
    const map: Record<number, [string, RunnerTask][]> = {}
    const active = snapshot?.active_tasks ?? {}
    for (const [name, data] of Object.entries(active)) {
      const jid = typeof data?.job_id === 'number' ? data.job_id : undefined
      if (jid === undefined) continue
      ;(map[jid] ||= []).push([name, data])
    }
    return map
  }, [snapshot])

  const runningCount = jobs.filter((j) => RUNNING_STATES.has((j.status || '').toUpperCase())).length

  return (
    <div style={{ maxWidth: 1100, margin: '0 auto', padding: '1.5rem 1.5rem 3rem' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '1.25rem' }}>
        <h1 style={{ margin: 0, fontSize: '1.2rem', fontWeight: 800, color: 'var(--lx-text)' }}>IaC Orchestrator</h1>
        <span title={connected ? 'Live verbunden' : 'Getrennt'} style={{
          marginLeft: 'auto', display: 'inline-flex', alignItems: 'center', gap: 5,
          fontSize: '0.68rem', color: connected ? 'var(--lx-state-up)' : 'var(--lx-state-down)',
        }}>
          <span style={{ width: 7, height: 7, borderRadius: '50%', background: connected ? 'var(--lx-state-up)' : 'var(--lx-state-down)' }} />
          {connected ? 'LIVE' : 'OFFLINE'}
        </span>
      </div>

      {error && <ErrorBox msg={error} />}

      <div style={{ display: 'flex', gap: 4, marginBottom: '1.5rem', borderBottom: '1px solid var(--lx-border-soft)' }}>
        {TABS.map((t) => {
          const activeTab = t.id === tab
          const badge = t.id === 'active' && runningCount > 0 ? runningCount : null
          return (
            <button key={t.id} onClick={() => setTab(t.id)} style={{
              display: 'inline-flex', alignItems: 'center', gap: 6, padding: '0.55rem 0.9rem',
              background: 'none', border: 'none', cursor: 'pointer',
              fontSize: '0.8rem', fontWeight: activeTab ? 700 : 500,
              color: activeTab ? 'var(--lx-accent)' : 'var(--lx-text-muted)',
              borderBottom: activeTab ? '2px solid var(--lx-accent)' : '2px solid transparent',
              marginBottom: -1,
            }}>
              {t.label}
              {badge !== null && (
                <span style={{ background: 'var(--lx-accent)', color: '#000', borderRadius: 999, fontSize: '0.6rem', fontWeight: 800, padding: '1px 6px' }}>{badge}</span>
              )}
            </button>
          )
        })}
      </div>

      {tab === 'overview' && <Overview jobs={jobs} isRunning={isRunning} />}
      {tab === 'active' && <ActivePipelines jobs={jobs} runnersByJob={runnersByJob} onLogs={setLogJob} />}
      {tab === 'catalog' && <ServiceCatalog />}
      {tab === 'history' && <History jobs={jobs} onLogs={setLogJob} />}

      {logJob !== null && <LogViewer jobId={logJob} onClose={() => setLogJob(null)} />}
    </div>
  )
}
