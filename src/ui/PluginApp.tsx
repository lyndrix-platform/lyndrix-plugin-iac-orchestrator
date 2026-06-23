import React, { useState, useEffect, useCallback, useMemo, useRef } from 'react'
import {
  iacApi,
  type CatalogService,
  type IaCJob,
  type RunnerTask,
  type OrchestratorStats,
  type Assignment,
  type TerraformHost,
  type ServiceHistoryRow,
  type IaCSettings,
  type PipelinePayload,
} from './lib/api'
import { useJobsSSE } from './lib/hooks'

// ─── Helpers ─────────────────────────────────────────────────────────────────

const RUNNING_STATES = new Set(['RUNNING', 'PENDING'])
const FAIL_STATES = new Set(['FAILED', 'ERROR', 'ABORTED'])

const STEM_COLORS: Record<string, string> = {
  violet: '#8b5cf6', sky: '#0ea5e9', emerald: '#10b981', amber: '#f59e0b',
  rose: '#f43f5e', zinc: '#71717a', indigo: '#6366f1', teal: '#14b8a6',
}
function stemColor(s?: string): string {
  return STEM_COLORS[s || ''] || 'var(--lx-accent)'
}

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

function KpiCard({ label, value, color, sub }: {
  label: string; value: React.ReactNode; color?: string; sub?: string
}) {
  return (
    <Card>
      <div style={{ padding: '1rem 1.1rem' }}>
        <div style={{ fontSize: '1.6rem', fontWeight: 800, color: color ?? 'var(--lx-text)', lineHeight: 1.1 }}>
          {value}
        </div>
        <div style={{ fontSize: '0.7rem', color: 'var(--lx-text-muted)', textTransform: 'uppercase', letterSpacing: '0.06em', marginTop: 4 }}>
          {label}
        </div>
        {sub && <div style={{ fontSize: '0.66rem', color: 'var(--lx-text-muted)', marginTop: 4 }}>{sub}</div>}
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

function Button({ label, onClick, variant = 'default', disabled, title }: {
  label: string; onClick: () => void
  variant?: 'default' | 'primary' | 'danger' | 'warn'; disabled?: boolean; title?: string
}) {
  const accent =
    variant === 'danger' ? 'var(--lx-state-down)'
      : variant === 'warn' ? '#f59e0b'
        : 'var(--lx-accent)'
  return (
    <button onClick={onClick} disabled={disabled} title={title} style={{
      padding: '4px 12px', fontSize: '0.72rem', fontWeight: 600,
      border: `1px solid color-mix(in srgb, ${accent} 40%, transparent)`,
      borderRadius: 'var(--lx-radius-sm)',
      background: variant === 'primary' ? `color-mix(in srgb, ${accent} 20%, transparent)` : `color-mix(in srgb, ${accent} 8%, transparent)`,
      color: disabled ? 'var(--lx-text-muted)' : accent,
      cursor: disabled ? 'not-allowed' : 'pointer', opacity: disabled ? 0.5 : 1,
      display: 'inline-flex', alignItems: 'center', gap: 5, whiteSpace: 'nowrap',
    }}>
      {label}
    </button>
  )
}

function Modal({ title, onClose, children, width = 720 }: {
  title: string; onClose: () => void; children: React.ReactNode; width?: number
}) {
  return (
    <div onClick={onClose} style={{
      position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.6)', zIndex: 1000,
      display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '2rem',
    }}>
      <div onClick={(e) => e.stopPropagation()} style={{
        width: '100%', maxWidth: width, maxHeight: '85vh', display: 'flex', flexDirection: 'column',
        background: 'var(--lx-elevated)', border: '1px solid var(--lx-border)',
        borderRadius: 'var(--lx-radius-lg)', overflow: 'hidden',
      }}>
        <div style={{
          display: 'flex', alignItems: 'center', gap: '0.75rem', padding: '0.75rem 1rem',
          borderBottom: '1px solid var(--lx-border-soft)', background: 'var(--lx-surface)',
        }}>
          <span style={{ fontWeight: 700, color: 'var(--lx-text)' }}>{title}</span>
          <button onClick={onClose} style={{ marginLeft: 'auto', background: 'none', border: 'none', color: 'var(--lx-text-muted)', cursor: 'pointer', fontSize: '1.1rem' }}>✕</button>
        </div>
        <div style={{ overflow: 'auto', padding: '1rem' }}>{children}</div>
      </div>
    </div>
  )
}

// ─── Confirm dialog (shared, mandatory for every destructive action) ─────────

interface ConfirmOpts {
  title: string
  body: string
  confirmLabel?: string
  onConfirm: () => void
}

function ConfirmDialog({ opts, onClose }: { opts: ConfirmOpts; onClose: () => void }) {
  return (
    <div onClick={onClose} style={{
      position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.6)', zIndex: 1100,
      display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '2rem',
    }}>
      <div onClick={(e) => e.stopPropagation()} style={{
        width: '100%', maxWidth: 460,
        background: 'var(--lx-elevated)', border: '1px solid color-mix(in srgb, var(--lx-state-down) 40%, transparent)',
        borderRadius: 'var(--lx-radius-lg)', padding: '1.25rem',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
          <span style={{ fontSize: '1.1rem' }}>⚠️</span>
          <span style={{ fontWeight: 700, color: 'var(--lx-text)', fontSize: '0.95rem' }}>{opts.title}</span>
        </div>
        <div style={{ fontSize: '0.8rem', color: 'var(--lx-text-muted)', lineHeight: 1.5, marginBottom: '1.1rem' }}>
          {opts.body}
        </div>
        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
          <Button label="Cancel" onClick={onClose} />
          <Button
            label={opts.confirmLabel ?? 'Confirm'}
            variant="danger"
            onClick={() => { opts.onConfirm(); onClose() }}
          />
        </div>
      </div>
    </div>
  )
}

type ConfirmFn = (opts: ConfirmOpts) => void
type ToastFn = (msg: string, kind?: 'ok' | 'err') => void

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
                <Button label="Live Logs" onClick={() => onLogs(job.id)} />
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
            <button onClick={() => onLogs(job.id)} title="Logs" style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--lx-accent)', fontSize: '0.72rem', fontWeight: 600 }}>
              Logs
            </button>
          </div>
        ))}
      </Card>
    </div>
  )
}

// ─── Service history modal ───────────────────────────────────────────────────

function ServiceHistoryModal({ service, onClose, onLogs }: {
  service: string; onClose: () => void; onLogs: (id: number) => void
}) {
  const [rows, setRows] = useState<ServiceHistoryRow[] | null>(null)
  const [err, setErr] = useState<string | null>(null)

  useEffect(() => {
    iacApi.serviceHistory(service)
      .then(setRows)
      .catch((e) => setErr(e instanceof Error ? e.message : 'Verlauf konnte nicht geladen werden'))
  }, [service])

  return (
    <Modal title={`Deployment History: ${service}`} onClose={onClose}>
      {err && <ErrorBox msg={err} />}
      {!rows && !err && <div style={{ color: 'var(--lx-text-muted)' }}>Lade…</div>}
      {rows && rows.length === 0 && (
        <div style={{ color: 'var(--lx-text-muted)', fontStyle: 'italic' }}>Keine Einträge gefunden.</div>
      )}
      {rows && rows.map((r, i) => (
        <div key={r.id} style={{
          display: 'flex', alignItems: 'center', gap: 12, padding: '0.5rem 0',
          borderTop: i === 0 ? 'none' : '1px solid var(--lx-border-soft)',
        }}>
          <span style={{ fontFamily: 'monospace', fontSize: '0.72rem', color: 'var(--lx-text-muted)', width: 50 }}>#{r.id}</span>
          <span style={{ flex: 1, fontSize: '0.72rem', color: 'var(--lx-text)' }}>{r.start_time}</span>
          <StatusBadge status={r.status} />
          <button onClick={() => { onClose(); onLogs(r.id) }} style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--lx-accent)', fontSize: '0.72rem', fontWeight: 600 }}>Logs</button>
        </div>
      ))}
    </Modal>
  )
}

// ─── Service catalog ─────────────────────────────────────────────────────────

function ServiceCatalog({ confirm, toast, onLogs }: { confirm: ConfirmFn; toast: ToastFn; onLogs: (id: number) => void }) {
  const [services, setServices] = useState<CatalogService[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [search, setSearch] = useState('')
  const [historyFor, setHistoryFor] = useState<string | null>(null)

  useEffect(() => {
    iacApi.catalog()
      .then(setServices)
      .catch((e) => setError(e instanceof Error ? e.message : 'Katalog konnte nicht geladen werden'))
      .finally(() => setLoading(false))
  }, [])

  function deploy(name: string, branch?: string) {
    confirm({
      title: `Deploy service "${name}"?`,
      body: `This triggers a single-service deployment of "${name}" on branch ${branch || 'main'}. Real services will be updated.`,
      confirmLabel: 'Deploy',
      onConfirm: () => {
        iacApi.deployService(name, branch || 'main')
          .then(() => toast(`Deployment für "${name}" eingereiht.`))
          .catch((e) => toast(e instanceof Error ? e.message : 'Deploy fehlgeschlagen', 'err'))
      },
    })
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
                <div style={{ marginTop: '0.85rem', display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
                  <Button label="History" onClick={() => setHistoryFor(name)} />
                  <Button label="Deploy" variant="primary" onClick={() => deploy(name, branch)} />
                </div>
              </div>
            </Card>
          )
        })}
      </div>
      {historyFor && <ServiceHistoryModal service={historyFor} onClose={() => setHistoryFor(null)} onLogs={onLogs} />}
    </div>
  )
}

// ─── Overview (stats) ────────────────────────────────────────────────────────

function Overview({ statsTick, isRunning }: { statsTick: number; isRunning: boolean }) {
  const [stats, setStats] = useState<OrchestratorStats | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    iacApi.stats()
      .then((s) => { if (!cancelled) { setStats(s); setError(null) } })
      .catch((e) => { if (!cancelled) setError(e instanceof Error ? e.message : 'Statistik konnte nicht geladen werden') })
    return () => { cancelled = true }
  }, [statsTick])

  if (error) return <ErrorBox msg={error} />
  if (!stats) return <div style={{ color: 'var(--lx-text-muted)', padding: '2rem', textAlign: 'center' }}>Lade Statistiken…</div>

  const rateColor = stats.success_rate >= 80 ? 'var(--lx-state-up)' : stats.success_rate >= 50 ? '#f59e0b' : 'var(--lx-state-down)'
  const lastColor = stats.last_deployment_status === 'SUCCESS' ? 'var(--lx-state-up)'
    : stats.last_deployment_status === 'RUNNING' ? '#f59e0b'
      : FAIL_STATES.has(stats.last_deployment_status || '') ? 'var(--lx-state-down)' : undefined
  const byStatus = Object.entries(stats.by_status).sort((a, b) => b[1] - a[1])

  return (
    <div>
      {/* KPI row */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: '1rem', marginBottom: '1.25rem' }}>
        <KpiCard label="Total Deployments" value={stats.total} sub={`${stats.finished} finished · ${stats.running} active`} />
        <KpiCard label="Success Rate" value={`${Math.round(stats.success_rate)}%`} color={rateColor} sub={`${stats.success} ok · ${stats.failed} failed`} />
        <KpiCard label="Avg Duration" value={stats.avg_duration_human} color="var(--lx-accent-2)" sub="successful runs" />
        <KpiCard label="Last Deployment" value={(stats.last_deployment_status || '—')} color={lastColor} sub={stats.last_deployment_at ? new Date(stats.last_deployment_at).toLocaleString() : 'No runs yet'} />
        <KpiCard label="Engine" value={isRunning ? 'Busy' : 'Idle'} color={isRunning ? 'var(--lx-accent)' : 'var(--lx-state-up)'} />
      </div>

      {/* Host lifecycle (3 phases) */}
      <Card>
        <div style={{ padding: '0.75rem 1rem', borderBottom: '1px solid var(--lx-border-soft)', background: 'var(--lx-elevated)', fontSize: '0.78rem', fontWeight: 700, color: 'var(--lx-text)' }}>
          Host Lifecycle · Provision → Configure → Deploy
        </div>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.75rem', padding: '1rem', alignItems: 'stretch' }}>
          {stats.by_phase.map((p, idx) => {
            const c = stemColor(p.color)
            return (
              <React.Fragment key={p.phase}>
                <div style={{
                  flex: '1 1 180px', minWidth: 160, border: `1px solid var(--lx-border-soft)`,
                  borderTop: `2px solid ${c}`, borderRadius: 'var(--lx-radius-sm)', padding: '0.75rem',
                  background: 'var(--lx-surface)',
                }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <span style={{ fontSize: '0.8rem', fontWeight: 700, color: 'var(--lx-text)' }}>{p.label}</span>
                    <span style={{ fontSize: '1.1rem', fontWeight: 800, fontFamily: 'monospace', color: c }}>{p.total}</span>
                  </div>
                  {p.total > 0 ? (
                    <div style={{ marginTop: 8 }}>
                      <ProgressBar value={p.success_rate} color={c} />
                      <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: 4, fontSize: '0.62rem', color: 'var(--lx-text-muted)' }}>
                        <span>{p.success} ok · {p.failed} fail</span>
                        <span>{Math.round(p.success_rate)}%</span>
                      </div>
                    </div>
                  ) : (
                    <div style={{ marginTop: 8, fontSize: '0.66rem', fontStyle: 'italic', color: c }}>No runs yet</div>
                  )}
                </div>
                {idx < stats.by_phase.length - 1 && (
                  <div style={{ alignSelf: 'center', color: 'var(--lx-text-muted)', fontSize: '1.1rem' }}>→</div>
                )}
              </React.Fragment>
            )
          })}
        </div>
      </Card>

      {/* Status breakdown + recent feed */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))', gap: '1rem', marginTop: '1rem' }}>
        <Card>
          <div style={{ padding: '0.75rem 1rem', borderBottom: '1px solid var(--lx-border-soft)', background: 'var(--lx-elevated)', fontSize: '0.78rem', fontWeight: 700, color: 'var(--lx-text)' }}>
            Status Breakdown
          </div>
          <div style={{ padding: '1rem' }}>
            {byStatus.length === 0 && <div style={{ color: 'var(--lx-text-muted)', fontStyle: 'italic' }}>No deployments recorded yet.</div>}
            {byStatus.map(([status, count]) => {
              const pct = stats.total ? (count / stats.total) * 100 : 0
              return (
                <div key={status} style={{ marginBottom: '0.6rem' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 3 }}>
                    <span style={{ fontSize: '0.72rem', fontWeight: 600, color: 'var(--lx-text)' }}>{status}</span>
                    <span style={{ fontSize: '0.66rem', fontFamily: 'monospace', color: 'var(--lx-text-muted)' }}>{count} · {Math.round(pct)}%</span>
                  </div>
                  <ProgressBar value={pct} color={statusColor(status)} />
                </div>
              )
            })}
          </div>
        </Card>

        <Card>
          <div style={{ padding: '0.75rem 1rem', borderBottom: '1px solid var(--lx-border-soft)', background: 'var(--lx-elevated)', fontSize: '0.78rem', fontWeight: 700, color: 'var(--lx-text)' }}>
            Recent Deployments
          </div>
          <div style={{ padding: '0.25rem 0', maxHeight: 280, overflow: 'auto' }}>
            {stats.recent.length === 0 && <div style={{ padding: '1rem', color: 'var(--lx-text-muted)', fontStyle: 'italic' }}>Nothing here yet.</div>}
            {stats.recent.map((j) => (
              <div key={j.id} style={{ display: 'flex', alignItems: 'center', gap: 10, padding: '0.4rem 1rem' }}>
                <span style={{ width: 4, height: 22, borderRadius: 2, background: stemColor(j.color), flexShrink: 0 }} />
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{ fontSize: '0.74rem', color: 'var(--lx-text)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>#{j.id} {j.type_label}</div>
                  <div style={{ fontSize: '0.62rem', color: 'var(--lx-text-muted)' }}>{j.start_label} · {j.duration_human}</div>
                </div>
                <StatusBadge status={j.status} />
              </div>
            ))}
          </div>
        </Card>
      </div>
    </div>
  )
}

// ─── Provision (Terraform) ───────────────────────────────────────────────────

function Provision({ confirm, toast, isRunning, statsTick }: {
  confirm: ConfirmFn; toast: ToastFn; isRunning: boolean; statsTick: number
}) {
  const [hosts, setHosts] = useState<TerraformHost[] | null>(null)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(() => {
    iacApi.terraformHosts()
      .then((h) => { setHosts(h); setError(null) })
      .catch((e) => setError(e instanceof Error ? e.message : 'Terraform-Hosts konnten nicht geladen werden'))
  }, [])

  useEffect(() => { load() }, [load, statsTick])

  function checkEnv() {
    iacApi.infraPlan()
      .then(() => toast('Infrastructure plan (Check Env) queued.'))
      .catch((e) => toast(e instanceof Error ? e.message : 'Plan fehlgeschlagen', 'err'))
  }
  function deployInfra() {
    confirm({
      title: 'Deploy entire infrastructure?',
      body: 'This runs `tofu apply` across every Terraform environment and will create, change or destroy real infrastructure to match the desired plan. Run Check Env first to review the plan.',
      confirmLabel: 'Deploy Infra',
      onConfirm: () => {
        iacApi.infraApply()
          .then(() => toast('Infrastructure deploy queued.'))
          .catch((e) => toast(e instanceof Error ? e.message : 'Deploy fehlgeschlagen', 'err'))
      },
    })
  }

  const managed = (hosts || []).filter((h) => h.managed)
  const unmanaged = (hosts || []).filter((h) => !h.managed)

  // Group by site → stage
  const grouped = useMemo(() => {
    const m: Record<string, Record<string, TerraformHost[]>> = {}
    for (const h of hosts || []) {
      ;(m[h.site] ||= {})
      ;(m[h.site][h.stage] ||= []).push(h)
    }
    return m
  }, [hosts])

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem', gap: 8, flexWrap: 'wrap' }}>
        <h2 style={{ margin: 0, fontSize: '1rem', fontWeight: 700, color: 'var(--lx-text)' }}>Provisioning (Terraform)</h2>
        <div style={{ display: 'flex', gap: 8 }}>
          <Button label="Check Env" onClick={checkEnv} disabled={isRunning} title="Read-only Terraform plan across all environments" />
          <Button label="Deploy Infra" variant="danger" onClick={deployInfra} disabled={isRunning} title="Apply Terraform across the entire infrastructure" />
          <Button label="Refresh" onClick={load} />
        </div>
      </div>

      {error && <ErrorBox msg={error} />}
      {!hosts && !error && <div style={{ color: 'var(--lx-text-muted)', padding: '2rem', textAlign: 'center' }}>Lade Hosts…</div>}

      {hosts && (
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(170px, 1fr))', gap: '1rem', marginBottom: '1.25rem' }}>
          <KpiCard label="Total Hosts" value={hosts.length} />
          <KpiCard label="Terraform-Managed" value={managed.length} color="#8b5cf6" sub="have a terraform block" />
          <KpiCard label="Unmanaged" value={unmanaged.length} color="#f59e0b" sub="Ansible-only / manual" />
        </div>
      )}

      {hosts && hosts.length === 0 && (
        <Card><div style={{ padding: '2rem', textAlign: 'center', color: 'var(--lx-text-muted)' }}>No hosts found. Ensure 'iac_controller/environments' is synced.</div></Card>
      )}

      {Object.entries(grouped).sort().map(([site, stages]) => (
        <div key={site} style={{ marginBottom: '1.25rem' }}>
          <div style={{ fontSize: '0.95rem', fontWeight: 800, letterSpacing: '0.08em', color: 'var(--lx-text)', borderBottom: '1px solid var(--lx-border-soft)', paddingBottom: 6, marginBottom: 10 }}>
            {site.toUpperCase()}
          </div>
          {Object.entries(stages).sort().map(([stage, items]) => (
            <div key={stage} style={{ marginBottom: 12 }}>
              <div style={{ fontSize: '0.72rem', fontWeight: 700, color: 'var(--lx-accent-3, var(--lx-accent))', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 8 }}>{stage}</div>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(300px, 1fr))', gap: '0.75rem' }}>
                {items.map((h) => {
                  const c = h.managed ? '#8b5cf6' : 'var(--lx-state-unknown)'
                  return (
                    <Card key={h.host} accent={c}>
                      <div style={{ padding: '0.85rem' }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                          <div style={{ minWidth: 0 }}>
                            <div style={{ fontWeight: 700, color: 'var(--lx-text)', fontSize: '0.85rem', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{h.host}</div>
                            <div style={{ fontSize: '0.62rem', color: 'var(--lx-text-muted)' }}>{h.site} / {h.stage}</div>
                          </div>
                          <StatusBadge status={h.state === 'unknown' ? 'UNKNOWN' : h.state.toUpperCase()} />
                        </div>
                        <div style={{ marginTop: 8, fontSize: '0.66rem', fontFamily: 'monospace', color: 'var(--lx-text-muted)', display: 'grid', gap: 2 }}>
                          <div>addr: {h.ansible_host}</div>
                          <div style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>provider: {h.provider}</div>
                          <div>workspace: {h.workspace}</div>
                        </div>
                        {!h.managed && (
                          <div style={{ marginTop: 8, fontSize: '0.62rem', fontStyle: 'italic', color: 'var(--lx-text-muted)' }}>No terraform block</div>
                        )}
                      </div>
                    </Card>
                  )
                })}
              </div>
            </div>
          ))}
        </div>
      ))}
    </div>
  )
}

// ─── Assignments / Topography ────────────────────────────────────────────────

function Assignments({ confirm, toast, isRunning }: { confirm: ConfirmFn; toast: ToastFn; isRunning: boolean }) {
  const [items, setItems] = useState<Assignment[] | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [search, setSearch] = useState('')

  const load = useCallback(() => {
    iacApi.assignments()
      .then((a) => { setItems(a); setError(null) })
      .catch((e) => setError(e instanceof Error ? e.message : 'Assignments konnten nicht geladen werden'))
  }, [])
  useEffect(() => { load() }, [load])

  function run(payload: PipelinePayload, confirmTitle: string, body: string) {
    confirm({
      title: confirmTitle,
      body,
      confirmLabel: 'Run',
      onConfirm: () => {
        iacApi.runPipeline(payload)
          .then((r) => toast(r.message || 'Pipeline queued.'))
          .catch((e) => toast(e instanceof Error ? e.message : 'Trigger fehlgeschlagen', 'err'))
      },
    })
  }

  const term = search.toLowerCase()
  const filtered = (items || []).filter((it) =>
    !term || it.host.toLowerCase().includes(term) || it.site.toLowerCase().includes(term)
    || it.stage.toLowerCase().includes(term) || it.services.some((s) => s.toLowerCase().includes(term)),
  )

  const grouped = useMemo(() => {
    const m: Record<string, Record<string, Assignment[]>> = {}
    for (const it of filtered) {
      ;(m[it.site] ||= {})
      ;(m[it.site][it.stage] ||= []).push(it)
    }
    return m
  }, [filtered])

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem', gap: 8, flexWrap: 'wrap' }}>
        <h2 style={{ margin: 0, fontSize: '1rem', fontWeight: 700, color: 'var(--lx-text)' }}>Infrastructure Topography</h2>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
          <input value={search} onChange={(e) => setSearch(e.target.value)} placeholder="Host / Service…" style={{
            padding: '0.35rem 0.7rem', fontSize: '0.78rem', width: 180,
            borderRadius: 'var(--lx-radius-sm)', border: '1px solid var(--lx-border-soft)',
            background: 'var(--lx-elevated)', color: 'var(--lx-text)', outline: 'none',
          }} />
          <Button label="Global Bootstrap" variant="warn" disabled={isRunning} onClick={() => run({ pipeline_type: 'bootstrap_compliance', limit: 'all' }, 'Global compliance bootstrap?', 'Runs the compliance/baseline playbook (as root) across ALL hosts.')} />
          <Button label="Global Adopt" variant="warn" disabled={isRunning} onClick={() => run({ pipeline_type: 'adopt_host', limit: 'all' }, 'Global adopt?', 'Imports every managed container (all sites) into Terraform state.')} />
          <Button label="Global Rollout" variant="danger" disabled={isRunning} onClick={() => run({ pipeline_type: 'rollout', limit: 'all' }, 'Global rollout?', 'Triggers a full infrastructure rollout across ALL hosts.')} />
        </div>
      </div>

      {error && <ErrorBox msg={error} />}
      {!items && !error && <div style={{ color: 'var(--lx-text-muted)', padding: '2rem', textAlign: 'center' }}>Lade Assignments…</div>}
      {items && items.length === 0 && (
        <Card><div style={{ padding: '2rem', textAlign: 'center', color: 'var(--lx-text-muted)' }}>No assignments found. Ensure 'iac_controller/environments' is populated.</div></Card>
      )}

      {Object.entries(grouped).sort().map(([site, stages]) => (
        <div key={site} style={{ marginBottom: '1.25rem' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, borderBottom: '1px solid var(--lx-border-soft)', paddingBottom: 6, marginBottom: 10, flexWrap: 'wrap' }}>
            <span style={{ fontSize: '0.95rem', fontWeight: 800, letterSpacing: '0.08em', color: 'var(--lx-text)' }}>{site.toUpperCase()}</span>
            <span style={{ display: 'flex', gap: 6, marginLeft: 'auto' }}>
              <Button label="Site Bootstrap" variant="warn" disabled={isRunning} onClick={() => run({ pipeline_type: 'bootstrap_compliance', limit: site }, `Bootstrap site ${site}?`, `Runs compliance/baseline (as root) across all ${site.toUpperCase()} hosts.`)} />
              <Button label="Site Adopt" variant="warn" disabled={isRunning} onClick={() => run({ pipeline_type: 'adopt_host', limit: site }, `Adopt site ${site}?`, `Imports all managed ${site.toUpperCase()} containers into Terraform state.`)} />
              <Button label="Site Rollout" variant="danger" disabled={isRunning} onClick={() => run({ pipeline_type: 'rollout', limit: site }, `Rollout site ${site}?`, `Rolls out all hosts in ${site.toUpperCase()}.`)} />
            </span>
          </div>
          {Object.entries(stages).sort().map(([stage, hosts]) => (
            <div key={stage} style={{ marginBottom: 12, paddingLeft: 12, borderLeft: '2px solid var(--lx-border-soft)' }}>
              <div style={{ fontSize: '0.72rem', fontWeight: 700, color: 'var(--lx-state-up)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 8 }}>{stage}</div>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(320px, 1fr))', gap: '0.75rem' }}>
                {hosts.map((it) => (
                  <Card key={it.host} accent="var(--lx-state-up)">
                    <div style={{ padding: '0.85rem' }}>
                      <div style={{ fontWeight: 700, color: 'var(--lx-text)', fontSize: '0.85rem', marginBottom: 8, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{it.host}</div>
                      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                        <Button label="Adopt" variant="warn" disabled={isRunning} onClick={() => run({ pipeline_type: 'adopt_host', host_name: it.host }, `Adopt host ${it.host}?`, `Imports the existing container for ${it.host} into Terraform state (import + plan, no apply).`)} />
                        <Button label="Init" disabled={isRunning} onClick={() => run({ pipeline_type: 'init_host', host_name: it.host }, `Init host ${it.host}?`, `Provisions the container for ${it.host} via Terraform only (no Ansible, no services). Real infrastructure will be created.`)} />
                        <Button label="Bootstrap" variant="warn" disabled={isRunning} onClick={() => run({ pipeline_type: 'bootstrap_compliance', host_name: it.host }, `Bootstrap host ${it.host}?`, `Runs the initial compliance/baseline playbook as root on ${it.host}.`)} />
                        <Button label="Compliance" disabled={isRunning} onClick={() => run({ pipeline_type: 'compliance', host_name: it.host }, `Run compliance on ${it.host}?`, `Re-runs the compliance baseline as the svc user (ansible-agent) on ${it.host} — no service deployment.`)} />
                        <Button label="Deploy Services" variant="danger" disabled={isRunning} onClick={() => run({ pipeline_type: 'rollout', limit: it.host }, `Deploy services to ${it.host}?`, `Deploys this host's services to ${it.host}.`)} />
                      </div>
                      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 5, marginTop: 10 }}>
                        {it.services.map((s) => (
                          <span key={s} style={{ fontSize: '0.62rem', color: 'var(--lx-text-muted)', background: 'var(--lx-elevated)', border: '1px solid var(--lx-border-soft)', borderRadius: 'var(--lx-radius-sm)', padding: '1px 7px' }}>{s}</span>
                        ))}
                      </div>
                    </div>
                  </Card>
                ))}
              </div>
            </div>
          ))}
        </div>
      ))}
    </div>
  )
}

// ─── Settings page (own route: /iac/settings) ────────────────────────────────

function Field({ label, children, hint }: { label: string; children: React.ReactNode; hint?: string }) {
  return (
    <div style={{ marginBottom: '0.9rem' }}>
      <label style={{ display: 'block', fontSize: '0.72rem', fontWeight: 600, color: 'var(--lx-text)', marginBottom: 4 }}>{label}</label>
      {children}
      {hint && <div style={{ fontSize: '0.64rem', color: 'var(--lx-text-muted)', marginTop: 3 }}>{hint}</div>}
    </div>
  )
}

const inputStyle: React.CSSProperties = {
  width: '100%', padding: '0.4rem 0.6rem', fontSize: '0.78rem', boxSizing: 'border-box',
  borderRadius: 'var(--lx-radius-sm)', border: '1px solid var(--lx-border-soft)',
  background: 'var(--lx-elevated)', color: 'var(--lx-text)', outline: 'none',
}

function SectionCard({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <Card>
      <div style={{ padding: '0.75rem 1rem', borderBottom: '1px solid var(--lx-border-soft)', background: 'var(--lx-elevated)', fontSize: '0.78rem', fontWeight: 700, color: 'var(--lx-text)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
        {title}
      </div>
      <div style={{ padding: '1rem' }}>{children}</div>
    </Card>
  )
}

function SettingsPage({ confirm, toast }: { confirm: ConfirmFn; toast: ToastFn }) {
  const [cfg, setCfg] = useState<IaCSettings | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [tokenInfo, setTokenInfo] = useState<{ configured: boolean; masked: string } | null>(null)
  const [tokenReveal, setTokenReveal] = useState<string | null>(null)

  useEffect(() => {
    iacApi.getSettings().then(setCfg).catch((e) => setError(e instanceof Error ? e.message : 'Einstellungen konnten nicht geladen werden'))
    iacApi.getWebhookToken().then(setTokenInfo).catch(() => { /* non-fatal */ })
  }, [])

  function patch(p: Partial<IaCSettings>) {
    setCfg((c) => (c ? { ...c, ...p } : c))
  }

  function save() {
    if (!cfg) return
    iacApi.saveSettings(cfg)
      .then((s) => { setCfg(s); toast('Einstellungen gespeichert.') })
      .catch((e) => toast(e instanceof Error ? e.message : 'Speichern fehlgeschlagen', 'err'))
  }

  function generateToken() {
    confirm({
      title: 'Generate a new webhook token?',
      body: 'This replaces the current GitLab webhook token in Vault. Existing GitLab webhooks must be re-synced with the new token afterwards.',
      confirmLabel: 'Generate',
      onConfirm: () => {
        iacApi.generateWebhookToken()
          .then((r) => { setTokenReveal(r.token); setTokenInfo({ configured: true, masked: '•'.repeat(32) }); toast('Neuer Webhook-Token erzeugt und in Vault gespeichert.') })
          .catch((e) => toast(e instanceof Error ? e.message : 'Token-Erzeugung fehlgeschlagen', 'err'))
      },
    })
  }

  function syncWebhooks() {
    confirm({
      title: 'Sync GitLab webhooks?',
      body: 'Upserts merge-request webhooks for all projects in the configured GitLab group to point at the Lyndrix orchestrator endpoint.',
      confirmLabel: 'Sync',
      onConfirm: () => {
        iacApi.syncWebhooks()
          .then((r) => toast(`Webhook sync ok — projects=${r.projects_total ?? '?'}, created=${r.created ?? 0}, updated=${r.updated ?? 0}, failed=${r.failed ?? 0}.`))
          .catch((e) => toast(e instanceof Error ? e.message : 'Sync fehlgeschlagen', 'err'))
      },
    })
  }

  return (
    <div style={{ maxWidth: 760, margin: '0 auto', padding: '1.5rem 1.5rem 3rem' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: '1.25rem' }}>
        <button onClick={() => window.history.back()} style={{ background: 'none', border: '1px solid var(--lx-border-soft)', borderRadius: 'var(--lx-radius-sm)', color: 'var(--lx-text-muted)', cursor: 'pointer', padding: '3px 10px', fontSize: '0.72rem' }}>← Back</button>
        <h1 style={{ margin: 0, fontSize: '1.15rem', fontWeight: 800, color: 'var(--lx-text)' }}>IaC Orchestrator · Settings</h1>
      </div>

      {error && <ErrorBox msg={error} />}
      {!cfg && !error && <div style={{ color: 'var(--lx-text-muted)' }}>Lade…</div>}

      {cfg && (
        <div style={{ display: 'grid', gap: '1rem' }}>
          <SectionCard title="Pipeline Configuration">
            <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: '0.6rem' }}>
              <input type="checkbox" checked={cfg.auto_apply} onChange={(e) => patch({ auto_apply: e.target.checked })} id="auto_apply" />
              <label htmlFor="auto_apply" style={{ fontSize: '0.8rem', color: 'var(--lx-text)' }}>Enable Auto-Apply</label>
            </div>
            <div style={{ fontSize: '0.66rem', color: '#f59e0b', fontStyle: 'italic', marginBottom: '0.8rem' }}>
              Warning: Auto-Apply executes infrastructure changes immediately on webhook receipt.
            </div>
            <Field label="Test Deploy Allowed Hosts (comma-separated)" hint="Used by /api/iac/deploy/test-host/{host}; blocks rollout to non-allowlisted hosts.">
              <input style={inputStyle} value={cfg.test_deploy_allowed_hosts} onChange={(e) => patch({ test_deploy_allowed_hosts: e.target.value })} placeholder="e.g. pve-test-01" />
            </Field>
          </SectionCard>

          <SectionCard title="GitLab Webhooks">
            <Field label="GitLab Base URL">
              <input style={inputStyle} value={cfg.gitlab_url} onChange={(e) => patch({ gitlab_url: e.target.value })} />
            </Field>
            <Field label="GitLab Group ID">
              <input style={inputStyle} value={cfg.group_id} onChange={(e) => patch({ group_id: e.target.value })} />
            </Field>
            <Field label="Lyndrix Base URL">
              <input style={inputStyle} value={cfg.lyndrix_base_url} onChange={(e) => patch({ lyndrix_base_url: e.target.value })} />
            </Field>
            <Field label="GitLab API Credential (Vault key)">
              <input style={inputStyle} value={cfg.gitlab_token_key} onChange={(e) => patch({ gitlab_token_key: e.target.value })} />
            </Field>
            <Field label="Webhook Endpoint Preview">
              <input style={{ ...inputStyle, color: 'var(--lx-text-muted)' }} value={cfg.webhook_endpoint} readOnly />
            </Field>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
              <label style={{ fontSize: '0.78rem', color: 'var(--lx-text)', display: 'flex', alignItems: 'center', gap: 6 }}>
                <input type="checkbox" checked={cfg.autosync_enabled} onChange={(e) => patch({ autosync_enabled: e.target.checked })} />
                Auto-sync new repos
              </label>
              <label style={{ fontSize: '0.72rem', color: 'var(--lx-text-muted)', display: 'flex', alignItems: 'center', gap: 6 }}>
                Interval (s)
                <input type="number" min={300} step={60} style={{ ...inputStyle, width: 110 }} value={cfg.sync_interval} onChange={(e) => patch({ sync_interval: Number(e.target.value) })} />
              </label>
            </div>
            <div style={{ display: 'flex', gap: 8, marginTop: 12 }}>
              <Button label="Sync Webhooks Now" variant="primary" onClick={syncWebhooks} />
            </div>
          </SectionCard>

          <SectionCard title="Security · Webhook Token">
            <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap' }}>
              <input style={{ ...inputStyle, flex: 1, minWidth: 220, fontFamily: 'monospace' }} readOnly value={tokenReveal ?? (tokenInfo?.configured ? tokenInfo.masked : '(not set)')} />
              <Button label="Generate Token" variant="warn" onClick={generateToken} />
            </div>
            {tokenReveal && (
              <div style={{ fontSize: '0.64rem', color: '#f59e0b', marginTop: 6 }}>Copy this token now — it will not be shown again.</div>
            )}
          </SectionCard>

          <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
            <Button label="Save Settings" variant="primary" onClick={save} />
          </div>
        </div>
      )}
    </div>
  )
}

// ─── Root ────────────────────────────────────────────────────────────────────

type TabId = 'overview' | 'active' | 'provision' | 'catalog' | 'assignments' | 'history'

const TABS: { id: TabId; label: string }[] = [
  { id: 'overview', label: 'Overview' },
  { id: 'active', label: 'Active Pipelines' },
  { id: 'provision', label: 'Provision' },
  { id: 'catalog', label: 'Service Catalog' },
  { id: 'assignments', label: 'Assignments' },
  { id: 'history', label: 'History' },
]

function Dashboard() {
  const { snapshot, connected, error } = useJobsSSE()
  const [tab, setTab] = useState<TabId>('overview')
  const [logJob, setLogJob] = useState<number | null>(null)
  const [confirmOpts, setConfirmOpts] = useState<ConfirmOpts | null>(null)
  const [toastMsg, setToastMsg] = useState<{ msg: string; kind: 'ok' | 'err' } | null>(null)

  const confirm: ConfirmFn = useCallback((opts) => setConfirmOpts(opts), [])
  const toast: ToastFn = useCallback((msg, kind = 'ok') => {
    setToastMsg({ msg, kind })
    setTimeout(() => setToastMsg(null), 4000)
  }, [])

  const jobs = snapshot?.jobs ?? []
  const isRunning = snapshot?.is_running ?? false

  // Drive periodic stats/provision refresh from SSE snapshot changes + a timer.
  const [statsTick, setStatsTick] = useState(0)
  useEffect(() => {
    const t = setInterval(() => setStatsTick((n) => n + 1), 8000)
    return () => clearInterval(t)
  }, [])
  useEffect(() => { setStatsTick((n) => n + 1) }, [snapshot?.ts])

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

  function abort() {
    confirm({
      title: 'Abort running execution?',
      body: 'This kills the runner containers and marks all RUNNING jobs as ABORTED. In-flight infrastructure changes may be left partially applied.',
      confirmLabel: 'Abort',
      onConfirm: () => {
        iacApi.abort()
          .then((r) => toast(`Execution aborted (${(r.aborted_jobs as number[] | undefined)?.length ?? 0} job(s)).`))
          .catch((e) => toast(e instanceof Error ? e.message : 'Abort fehlgeschlagen', 'err'))
      },
    })
  }

  function goSettings() {
    const p = window.location.pathname.replace(/\/+$/, '')
    const target = p.endsWith('/iac') ? `${p}/settings` : `${p}/iac/settings`
    window.location.assign(target)
  }

  return (
    <div style={{ maxWidth: 1100, margin: '0 auto', padding: '1.5rem 1.5rem 3rem' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '1.25rem', flexWrap: 'wrap' }}>
        <h1 style={{ margin: 0, fontSize: '1.2rem', fontWeight: 800, color: 'var(--lx-text)' }}>IaC Orchestrator</h1>
        <span title={connected ? 'Live verbunden' : 'Getrennt'} style={{
          marginLeft: 'auto', display: 'inline-flex', alignItems: 'center', gap: 5,
          fontSize: '0.68rem', color: connected ? 'var(--lx-state-up)' : 'var(--lx-state-down)',
        }}>
          <span style={{ width: 7, height: 7, borderRadius: '50%', background: connected ? 'var(--lx-state-up)' : 'var(--lx-state-down)' }} />
          {connected ? 'LIVE' : 'OFFLINE'}
        </span>
        <Button label="Abort" variant="danger" disabled={!isRunning} onClick={abort} title={isRunning ? 'Abort the running execution' : 'No active job'} />
        <Button label="Settings" onClick={goSettings} />
      </div>

      {error && <ErrorBox msg={error} />}

      <div style={{ display: 'flex', gap: 4, marginBottom: '1.5rem', borderBottom: '1px solid var(--lx-border-soft)', flexWrap: 'wrap' }}>
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

      {tab === 'overview' && <Overview statsTick={statsTick} isRunning={isRunning} />}
      {tab === 'active' && <ActivePipelines jobs={jobs} runnersByJob={runnersByJob} onLogs={setLogJob} />}
      {tab === 'provision' && <Provision confirm={confirm} toast={toast} isRunning={isRunning} statsTick={statsTick} />}
      {tab === 'catalog' && <ServiceCatalog confirm={confirm} toast={toast} onLogs={setLogJob} />}
      {tab === 'assignments' && <Assignments confirm={confirm} toast={toast} isRunning={isRunning} />}
      {tab === 'history' && <History jobs={jobs} onLogs={setLogJob} />}

      {logJob !== null && <LogViewer jobId={logJob} onClose={() => setLogJob(null)} />}
      {confirmOpts && <ConfirmDialog opts={confirmOpts} onClose={() => setConfirmOpts(null)} />}
      {toastMsg && (
        <div style={{
          position: 'fixed', bottom: 24, left: '50%', transform: 'translateX(-50%)', zIndex: 1200,
          padding: '0.6rem 1.1rem', borderRadius: 'var(--lx-radius-md)', fontSize: '0.8rem', fontWeight: 600,
          color: toastMsg.kind === 'err' ? 'var(--lx-state-down)' : 'var(--lx-state-up)',
          background: 'var(--lx-elevated)',
          border: `1px solid color-mix(in srgb, ${toastMsg.kind === 'err' ? 'var(--lx-state-down)' : 'var(--lx-state-up)'} 40%, transparent)`,
          boxShadow: 'var(--lx-glow)',
        }}>{toastMsg.msg}</div>
      )}
    </div>
  )
}

// Settings page needs its own confirm/toast surface (it is a separate route mount).
function SettingsRoot() {
  const [confirmOpts, setConfirmOpts] = useState<ConfirmOpts | null>(null)
  const [toastMsg, setToastMsg] = useState<{ msg: string; kind: 'ok' | 'err' } | null>(null)
  const confirm: ConfirmFn = useCallback((opts) => setConfirmOpts(opts), [])
  const toast: ToastFn = useCallback((msg, kind = 'ok') => {
    setToastMsg({ msg, kind })
    setTimeout(() => setToastMsg(null), 4000)
  }, [])
  return (
    <>
      <SettingsPage confirm={confirm} toast={toast} />
      {confirmOpts && <ConfirmDialog opts={confirmOpts} onClose={() => setConfirmOpts(null)} />}
      {toastMsg && (
        <div style={{
          position: 'fixed', bottom: 24, left: '50%', transform: 'translateX(-50%)', zIndex: 1200,
          padding: '0.6rem 1.1rem', borderRadius: 'var(--lx-radius-md)', fontSize: '0.8rem', fontWeight: 600,
          color: toastMsg.kind === 'err' ? 'var(--lx-state-down)' : 'var(--lx-state-up)',
          background: 'var(--lx-elevated)',
          border: `1px solid color-mix(in srgb, ${toastMsg.kind === 'err' ? 'var(--lx-state-down)' : 'var(--lx-state-up)'} 40%, transparent)`,
          boxShadow: 'var(--lx-glow)',
        }}>{toastMsg.msg}</div>
      )}
    </>
  )
}

export default function PluginApp() {
  const isSettings = window.location.pathname.endsWith('/settings')
  return isSettings ? <SettingsRoot /> : <Dashboard />
}
