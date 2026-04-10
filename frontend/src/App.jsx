import { useState, useEffect, useRef } from 'react'
import {
  fetchItems,
  fetchLatest,
  fetchRecentTelemetry,
  fetchTelemetryHistory,
  fetchAnomalies,
  simulateAnomaly,
  createSubscription,
} from './api/client'
import ParameterSelector from './components/ParameterSelector'
import TelemetryChart from './components/TelemetryChart'
import AnomalyLog from './components/AnomalyLog'
import SimulationPanel from './components/SimulationPanel'
import SubscriptionPanel from './components/SubscriptionPanel'

const MAX_POINTS = 120  // ~4 minutes at 2s polling
const POLL_INTERVAL_MS = 2000
const SIMULATION_VISIBLE_MS = 1500
const RANGE_OPTIONS = [
  { value: 'recent', label: 'Live' },
  { value: '1h', label: '1H' },
  { value: '6h', label: '6H' },
  { value: '24h', label: '24H' },
]

function toChartPoint(point) {
  return {
    t: new Date(point.timestamp_utc).getTime(),
    value: point.value,
    timestamp_utc: point.timestamp_utc,
    source: point.source,
  }
}

function historyWindow(range) {
  const now = new Date()
  const hours = range === '1h' ? 1 : range === '6h' ? 6 : 24
  const from = new Date(now.getTime() - hours * 60 * 60 * 1000)
  return {
    from: from.toISOString(),
    to: now.toISOString(),
  }
}

export default function App() {
  const [items, setItems] = useState([])
  const [selectedItem, setSelectedItem] = useState(null)
  const [timeRange, setTimeRange] = useState('recent')
  const [buffer, setBuffer] = useState([])
  const [anomalies, setAnomalies] = useState([])
  const [simStatus, setSimStatus] = useState(null)
  const [subscriptionStatus, setSubscriptionStatus] = useState(null)
  const [error, setError] = useState(null)
  const telemetryIntervalRef = useRef(null)
  const anomalyIntervalRef = useRef(null)
  const simulationResetRef = useRef(null)

  // Load item metadata once on mount
  useEffect(() => {
    fetchItems()
      .then((data) => {
        setItems(data)
        if (data.length > 0) setSelectedItem(data[0].item)
      })
      .catch((e) => setError(e.message))
  }, [])

  useEffect(() => {
    return () => clearTimeout(simulationResetRef.current)
  }, [])

  // Load recent/history data, then keep live polling only for the recent view.
  useEffect(() => {
    if (!selectedItem) return

    setBuffer([])
    clearTimeout(simulationResetRef.current)
    clearInterval(telemetryIntervalRef.current)

    const loadInitialBuffer = async () => {
      try {
        if (timeRange === 'recent') {
          const points = await fetchRecentTelemetry(selectedItem, MAX_POINTS)
          setBuffer(points.map(toChartPoint))
          setError(null)
          return
        }

        const window = historyWindow(timeRange)
        const points = await fetchTelemetryHistory(selectedItem, {
          from: window.from,
          to: window.to,
          limit: 1000,
        })
        setBuffer(points.map(toChartPoint))
        setError(null)
      } catch (e) {
        setError(e.message)
      }
    }

    const poll = async () => {
      try {
        const point = await fetchLatest(selectedItem)
        setBuffer((prev) => {
          if (prev[prev.length - 1]?.timestamp_utc === point.timestamp_utc) {
            return prev
          }

          const next = [...prev, toChartPoint(point)]
          return next.slice(-MAX_POINTS)
        })
        setError(null)
      } catch (e) {
        setError(e.message)
      }
    }

    loadInitialBuffer()

    if (timeRange === 'recent') {
      poll()
      telemetryIntervalRef.current = setInterval(poll, POLL_INTERVAL_MS)
    }

    return () => clearInterval(telemetryIntervalRef.current)
  }, [selectedItem, timeRange])

  // Anomaly polling — 5s
  useEffect(() => {
    if (!selectedItem) return

    setAnomalies([])
    clearInterval(anomalyIntervalRef.current)

    const pollAnomalies = async () => {
      try {
        const data = await fetchAnomalies(selectedItem)
        setAnomalies(data)
      } catch {
        // silent — anomaly fetch failure shouldn't block the main chart
      }
    }

    pollAnomalies()
    anomalyIntervalRef.current = setInterval(pollAnomalies, 5000)

    return () => clearInterval(anomalyIntervalRef.current)
  }, [selectedItem])

  async function handleSimulate(mode) {
    setSimStatus(null)
    try {
      const response = await simulateAnomaly({ item: selectedItem, mode })
      const event = response.event
      const simulatedPoint = {
        t: event.received_unix_ms ?? Date.now(),
        value: event.value_numeric,
      }
      const baselinePoint = latestPoint ? { ...latestPoint } : null

      clearTimeout(simulationResetRef.current)
      setBuffer((prev) => [...prev, simulatedPoint].slice(-MAX_POINTS))

      // Keep the injected sample visible briefly, then restore the most
      // recent real value instead of waiting for the next collector sample.
      if (baselinePoint) {
        simulationResetRef.current = setTimeout(() => {
          setBuffer((prev) => {
            const recoveryPoint = { t: Date.now(), value: baselinePoint.value }
            return [...prev, recoveryPoint].slice(-MAX_POINTS)
          })
        }, SIMULATION_VISIBLE_MS)
      }

      fetchAnomalies(selectedItem)
        .then((data) => setAnomalies(data))
        .catch(() => {})

      setSimStatus({ ok: true, message: 'Anomaly injected' })
    } catch (e) {
      setSimStatus({ ok: false, message: e.message })
    }
  }

  async function handleSubscribe(payload) {
    setSubscriptionStatus(null)
    try {
      const response = await createSubscription(payload)
      setSubscriptionStatus({ ok: true, message: response.message })
    } catch (e) {
      setSubscriptionStatus({ ok: false, message: e.message })
    }
  }

  const selectedMeta = items.find((i) => i.item === selectedItem)
  const latestPoint = buffer[buffer.length - 1]

  return (
    <div className="app">
      <header className="header">
        <span className="header-title">ISS Telemetry</span>
        {selectedMeta && (
          <span className="header-category">{selectedMeta.category}</span>
        )}
      </header>

      <main className="main">
        <div className="controls-row">
          <ParameterSelector
            items={items}
            selectedItem={selectedItem}
            onChange={setSelectedItem}
          />
          <div className="range-pills-wrapper">
            <span className="selector-label">Time Range</span>
            <div className="range-pills">
              {RANGE_OPTIONS.map((option) => (
                <button
                  key={option.value}
                  className={`range-pill ${timeRange === option.value ? 'range-pill--active' : ''}`}
                  onClick={() => setTimeRange(option.value)}
                >
                  {option.label}
                </button>
              ))}
            </div>
          </div>
          {selectedMeta && (
            <div className="item-meta">
              <span className="meta-label">{selectedMeta.label}</span>
              <span className="meta-desc">{selectedMeta.description}</span>
            </div>
          )}
          {latestPoint && (
            <div className="latest-kpi">
              <span className="latest-label">Latest</span>
              <div className="latest-kpi-value">
                <span className="latest-number">{latestPoint.value?.toFixed(4) ?? '—'}</span>
                <span className="latest-unit">{selectedMeta?.unit}</span>
              </div>
              <span className="latest-time">
                {new Date(latestPoint.t).toLocaleTimeString('en-US', { hour12: false })}
              </span>
            </div>
          )}
        </div>

        {error && (
          <div className="error-banner">Backend unreachable — {error}</div>
        )}

        <div className="chart-container">
          <TelemetryChart
            buffer={buffer}
            unit={selectedMeta?.unit ?? ''}
            anomalies={anomalies}
            hasError={!!error}
            showBrush={timeRange !== 'recent'}
          />
        </div>

        <div className="bottom-row">
          <AnomalyLog anomalies={anomalies} />
          <div className="side-panels">
            <SimulationPanel
              selectedItem={selectedItem}
              onSimulate={handleSimulate}
              status={simStatus}
            />
            <SubscriptionPanel
              selectedItem={selectedItem}
              onSubscribe={handleSubscribe}
              status={subscriptionStatus}
            />
          </div>
        </div>
      </main>
    </div>
  )
}
