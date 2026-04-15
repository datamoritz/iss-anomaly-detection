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
const HISTORY_LIMITS = {
  '1h': 5000,
  '6h': 30000,
  '24h': 100000,
}
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

function historyHours(range) {
  return range === '1h' ? 1 : range === '6h' ? 6 : 24
}

function historyWindow(range) {
  const now = new Date()
  const from = new Date(now.getTime() - historyHours(range) * 60 * 60 * 1000)
  return { from: from.toISOString(), to: now.toISOString() }
}

function getXDomain(range) {
  if (range === 'recent') return null
  const now = Date.now()
  return [now - historyHours(range) * 60 * 60 * 1000, now]
}

export default function App() {
  const [items, setItems] = useState([])
  const [selectedItem, setSelectedItem] = useState(null)
  const [timeRange, setTimeRange] = useState('recent')
  const [buffer, setBuffer] = useState([])
  const [latestTelemetry, setLatestTelemetry] = useState(null)
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

  // Load recent/history data for the selected range.
  useEffect(() => {
    if (!selectedItem) return

    setBuffer([])
    clearTimeout(simulationResetRef.current)

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
          limit: HISTORY_LIMITS[timeRange],
        })
        setBuffer(points.map(toChartPoint))
        setError(null)
      } catch (e) {
        setError(e.message)
      }
    }

    loadInitialBuffer()
  }, [selectedItem, timeRange])

  // Always poll the canonical latest endpoint so the KPI never depends on
  // whichever historical slice is currently rendered in the chart.
  useEffect(() => {
    if (!selectedItem) return

    clearInterval(telemetryIntervalRef.current)

    const pollLatest = async () => {
      try {
        const point = await fetchLatest(selectedItem)
        const chartPoint = toChartPoint(point)
        setLatestTelemetry(chartPoint)

        if (timeRange !== 'recent') {
          setError(null)
          return
        }

        setBuffer((prev) => {
          if (prev[prev.length - 1]?.timestamp_utc === point.timestamp_utc) {
            return prev
          }

          const next = [...prev, chartPoint]
          return next.slice(-MAX_POINTS)
        })
        setError(null)
      } catch (e) {
        setError(e.message)
      }
    }

    pollLatest()
    telemetryIntervalRef.current = setInterval(pollLatest, POLL_INTERVAL_MS)

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
      const baselinePoint = latestTelemetry ? { ...latestTelemetry } : null

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
  const latestPoint = latestTelemetry ?? buffer[buffer.length - 1]

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
            xDomain={getXDomain(timeRange)}
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
