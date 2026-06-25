import { useEffect, useRef, useState } from 'react'
import { getToken, pluginPath, type JobStreamSnapshot } from './api'

export interface JobsSSEState {
  snapshot: JobStreamSnapshot | null
  connected: boolean
  error: string | null
}

/**
 * Subscribe to the live orchestrator job stream via Server-Sent Events.
 *
 * EventSource cannot set an Authorization header, so the bearer token (stored in
 * localStorage as `lyndrix_token`) is passed as a `?token=` query parameter, which
 * the backend validates in-handler. Returns the latest snapshot plus connection
 * state. Auto-reconnects on transient errors.
 */
export function useJobsSSE(): JobsSSEState {
  const [snapshot, setSnapshot] = useState<JobStreamSnapshot | null>(null)
  const [connected, setConnected] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const esRef = useRef<EventSource | null>(null)
  const retryRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  useEffect(() => {
    let closed = false

    function connect() {
      const token = getToken()
      if (!token) {
        setError('Kein Token vorhanden — bitte neu anmelden.')
        return
      }
      const url = `${pluginPath('stream/jobs')}?token=${encodeURIComponent(token)}`
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
        // Backoff then reconnect — keeps the dashboard live across redeploys.
        retryRef.current = setTimeout(connect, 3000)
      }
    }

    connect()

    return () => {
      closed = true
      if (retryRef.current) clearTimeout(retryRef.current)
      esRef.current?.close()
      esRef.current = null
    }
  }, [])

  return { snapshot, connected, error }
}
