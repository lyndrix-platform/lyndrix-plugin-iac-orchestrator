import { useEffect, useRef, useState } from 'react'
import { getToken, pluginPath, iacApi, type JobStreamSnapshot } from './api'

export interface JobsSSEState {
  snapshot: JobStreamSnapshot | null
  connected: boolean
  error: string | null
}

/**
 * Subscribe to the live orchestrator job stream via Server-Sent Events.
 *
 * EventSource cannot set an Authorization header, so before each connection we
 * mint a short-lived, single-purpose stream ticket (via an authenticated POST)
 * and pass it as a `?ticket=` query parameter. This keeps the long-lived bearer
 * token out of URLs, reverse-proxy access logs and browser history. Returns the
 * latest snapshot plus connection state. Auto-reconnects on transient errors.
 */
export function useJobsSSE(): JobsSSEState {
  const [snapshot, setSnapshot] = useState<JobStreamSnapshot | null>(null)
  const [connected, setConnected] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const esRef = useRef<EventSource | null>(null)
  const retryRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  useEffect(() => {
    let closed = false

    async function connect() {
      if (closed) return
      const token = getToken()
      if (!token) {
        setError('Kein Token vorhanden — bitte neu anmelden.')
        return
      }
      let ticket: string
      try {
        ticket = (await iacApi.streamTicket()).ticket
      } catch {
        if (closed) return
        setConnected(false)
        // Could not mint a ticket (transient) — back off and retry.
        retryRef.current = setTimeout(() => void connect(), 3000)
        return
      }
      if (closed) return
      const url = `${pluginPath('stream/jobs')}?ticket=${encodeURIComponent(ticket)}`
      const es = new EventSource(url)
      esRef.current = es

      es.onopen = () => {
        if (closed) return
        setConnected(true)
        setError(null)
      }

      es.onmessage = (ev) => {
        if (closed || !ev.data) return
        try {
          setSnapshot(JSON.parse(ev.data) as JobStreamSnapshot)
        } catch { /* ignore malformed frame */ }
      }

      es.onerror = () => {
        if (closed) return
        setConnected(false)
        es.close()
        esRef.current = null
        // Backoff then reconnect (mints a fresh ticket) — keeps the dashboard
        // live across redeploys.
        retryRef.current = setTimeout(() => void connect(), 3000)
      }
    }

    void connect()

    return () => {
      closed = true
      if (retryRef.current) clearTimeout(retryRef.current)
      esRef.current?.close()
      esRef.current = null
    }
  }, [])

  return { snapshot, connected, error }
}
