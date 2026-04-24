import { useState, useEffect, useRef } from 'react'
import {
  fetchItems,
  fetchLatest,
  fetchLatestContinuousAngle,
  fetchRecentTelemetry,
  fetchRecentContinuousAngle,
  fetchTelemetryHistory,
  fetchContinuousAngleHistory,
  fetchAnomalies,
  fetchTelemetryFeatures,
  simulateAnomaly,
  postInjection,
  createSubscription,
  createTelemetryWebSocket,
} from './api/client'
import ParameterSelector from './components/ParameterSelector'
import TelemetryChart from './components/TelemetryChart'
import AnomalyLog from './components/AnomalyLog'
import SimulationPanel from './components/SimulationPanel'
import SubscriptionPanel from './components/SubscriptionPanel'

const MAX_POINTS = 120  // ~4 minutes at 2s polling
const SIMULATION_VISIBLE_MS = 1500
const SOLAR_JOINT_ITEM = 'S0000004'
const CABIN_TEMP_ITEM  = 'USLAB000059'
const SOLAR_JOINT_CONTINUOUS_ITEM = 'S0000004__continuous'
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

function isContinuousSolarView(itemId) {
  return itemId === SOLAR_JOINT_CONTINUOUS_ITEM
}

function getBackendItemId(itemId) {
  return isContinuousSolarView(itemId) ? SOLAR_JOINT_ITEM : itemId
}

function addContinuousSolarOption(items) {
  const solarIndex = items.findIndex((item) => item.item === SOLAR_JOINT_ITEM)
  if (solarIndex === -1) return items

  const continuousItem = {
    ...items[solarIndex],
    item: SOLAR_JOINT_CONTINUOUS_ITEM,
    label: 'Solar Joint Angle (continuous)',
    displayLabel: 'Solar Joint Angle (continuous)',
    unit: 'sin/cos',
    description: 'Continuous frontend-only visualization of the Solar Alpha Rotary Joint using sin(angle) and cos(angle).',
  }

  return [
    ...items.slice(0, solarIndex + 1),
    continuousItem,
    ...items.slice(solarIndex + 1),
  ]
}

function toChartPoint(point, itemId) {
  if (isContinuousSolarView(itemId)) {
    return {
      t: new Date(point.timestamp_utc).getTime(),
      value: point.angle_deg,
      sinValue: point.angle_sin,
      cosValue: point.angle_cos,
      timestamp_utc: point.timestamp_utc,
      source: point.source,
    }
  }

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

function getWindowMs(range) {
  if (range === 'recent') return null
  return historyHours(range) * 60 * 60 * 1000
}

function appendPointForRange(prev, point, range) {
  const lastPoint = prev[prev.length - 1]
  if (lastPoint?.timestamp_utc === point.timestamp_utc) return prev
  if (lastPoint?.t != null && point.t <= lastPoint.t) return prev

  if (range === 'recent') {
    return [...prev, point].slice(-MAX_POINTS)
  }

  const windowMs = getWindowMs(range)
  const cutoff = point.t - windowMs
  return [...prev.filter((entry) => entry.t >= cutoff), point]
}

export default function App() {
  const [items, setItems] = useState([])
  const [selectedItem, setSelectedItem] = useState(null)
  const [timeRange, setTimeRange] = useState('recent')
  const [buffer, setBuffer] = useState([])
  const [latestTelemetry, setLatestTelemetry] = useState(null)
  const [anomalies, setAnomalies] = useState([])
  const [features, setFeatures] = useState(null)
  const [simStatus, setSimStatus] = useState(null)
  const [subscriptionStatus, setSubscriptionStatus] = useState(null)
  const [error, setError] = useState(null)
  const anomalyIntervalRef = useRef(null)
  const simulationResetRef = useRef(null)
  const telemetrySocketRef = useRef(null)
  const reconnectTimeoutRef = useRef(null)
  const selectedItemRef = useRef(null)
  const timeRangeRef = useRef('recent')

  // Load item metadata once on mount
  useEffect(() => {
    fetchItems()
      .then((data) => {
        const nextItems = addContinuousSolarOption(data)
        setItems(nextItems)
        if (nextItems.length > 0) setSelectedItem(nextItems[0].item)
      })
      .catch((e) => setError(e.message))
  }, [])

  useEffect(() => {
    return () => clearTimeout(simulationResetRef.current)
  }, [])

  useEffect(() => {
    selectedItemRef.current = selectedItem
  }, [selectedItem])

  useEffect(() => {
    timeRangeRef.current = timeRange
  }, [timeRange])

  // Load recent/history data for the selected range.
  useEffect(() => {
    if (!selectedItem) return

    const backendItemId = getBackendItemId(selectedItem)
    setBuffer([])
    clearTimeout(simulationResetRef.current)

    const loadInitialBuffer = async () => {
      try {
        if (timeRange === 'recent') {
          const points = isContinuousSolarView(selectedItem)
            ? await fetchRecentContinuousAngle(MAX_POINTS)
            : await fetchRecentTelemetry(backendItemId, MAX_POINTS)
          setBuffer(points.map((point) => toChartPoint(point, selectedItem)))
          setError(null)
          return
        }

        const window = historyWindow(timeRange)
        const points = isContinuousSolarView(selectedItem)
          ? await fetchContinuousAngleHistory({
              from: window.from,
              to: window.to,
              limit: HISTORY_LIMITS[timeRange],
            })
          : await fetchTelemetryHistory(backendItemId, {
              from: window.from,
              to: window.to,
              limit: HISTORY_LIMITS[timeRange],
            })
        setBuffer(points.map((point) => toChartPoint(point, selectedItem)))
        setError(null)
      } catch (e) {
        setError(e.message)
      }
    }

    loadInitialBuffer()
  }, [selectedItem, timeRange])

  // Load the canonical latest point on parameter change so the KPI is correct
  // even before the live WebSocket has delivered the next update.
  useEffect(() => {
    if (!selectedItem) return

    const backendItemId = getBackendItemId(selectedItem)
    setLatestTelemetry(null)
    const loadLatest = async () => {
      try {
        const point = isContinuousSolarView(selectedItem)
          ? await fetchLatestContinuousAngle()
          : await fetchLatest(backendItemId)
        setLatestTelemetry(toChartPoint(point, selectedItem))
        setError(null)
      } catch (e) {
        setError(e.message)
      }
    }

    loadLatest()
  }, [selectedItem])

  // Live telemetry stream: bootstrap via HTTP, then keep every view moving
  // forward with WebSocket updates from the backend.
  useEffect(() => {
    let cancelled = false

    const connect = () => {
      if (cancelled) return

      const socket = createTelemetryWebSocket()
      telemetrySocketRef.current = socket

      socket.onmessage = (event) => {
        try {
          const payload = JSON.parse(event.data)
          const activeItem = selectedItemRef.current
          const activeRange = timeRangeRef.current

          if (!activeItem) return

          const backendItemId = getBackendItemId(activeItem)
          if (payload.item !== backendItemId) return

          const chartPoint = toChartPoint(
            isContinuousSolarView(activeItem)
              ? {
                  angle_deg: payload.value_numeric,
                  angle_rad: payload.value_numeric != null ? (payload.value_numeric * Math.PI) / 180 : null,
                  angle_sin: payload.value_numeric != null ? Math.sin((payload.value_numeric * Math.PI) / 180) : null,
                  angle_cos: payload.value_numeric != null ? Math.cos((payload.value_numeric * Math.PI) / 180) : null,
                  timestamp_utc: payload.received_at_utc,
                  source: payload.source,
                }
              : {
                  value: payload.value_numeric,
                  timestamp_utc: payload.received_at_utc,
                  source: payload.source,
                },
            activeItem,
          )

          setLatestTelemetry(chartPoint)
          setBuffer((prev) => appendPointForRange(prev, chartPoint, activeRange))
        } catch {
          // ignore malformed live messages
        }
      }

      socket.onclose = () => {
        if (telemetrySocketRef.current === socket) {
          telemetrySocketRef.current = null
        }
        if (!cancelled) {
          reconnectTimeoutRef.current = setTimeout(connect, 2000)
        }
      }
    }

    connect()

    return () => {
      cancelled = true
      clearTimeout(reconnectTimeoutRef.current)
      if (telemetrySocketRef.current) {
        telemetrySocketRef.current.close()
        telemetrySocketRef.current = null
      }
    }
  }, [])

  // Features fetch — once per parameter selection
  useEffect(() => {
    if (!selectedItem) return
    const backendItemId = getBackendItemId(selectedItem)
    setFeatures(null)
    fetchTelemetryFeatures(backendItemId)
      .then(setFeatures)
      .catch(() => setFeatures(null))
  }, [selectedItem])

  // Anomaly polling — 5s
  useEffect(() => {
    if (!selectedItem) return

    const backendItemId = getBackendItemId(selectedItem)
    setAnomalies([])
    clearInterval(anomalyIntervalRef.current)

    const pollAnomalies = async () => {
      try {
        const data = await fetchAnomalies(backendItemId)
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
      const backendItemId = getBackendItemId(selectedItem)
      const response = await simulateAnomaly({ item: backendItemId, mode })
      const event = response.event
      const simulatedPoint = {
        t: event.received_unix_ms ?? Date.now(),
        value: event.value_numeric,
        ...(isContinuousSolarView(selectedItem) && event.value_numeric != null
          ? {
              sinValue: Math.sin((event.value_numeric * Math.PI) / 180),
              cosValue: Math.cos((event.value_numeric * Math.PI) / 180),
            }
          : {}),
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

      fetchAnomalies(backendItemId)
        .then((data) => setAnomalies(data))
        .catch(() => {})

      setSimStatus({ ok: true, message: 'Anomaly injected' })
    } catch (e) {
      setSimStatus({ ok: false, message: e.message })
    }
  }

  async function handleInject(payload) {
    setSimStatus(null)
    try {
      await postInjection(payload)
      const backendItemId = getBackendItemId(selectedItem)
      fetchAnomalies(backendItemId)
        .then((data) => setAnomalies(data))
        .catch(() => {})
      setSimStatus({ ok: true, message: `Prototype injected` })
    } catch (e) {
      setSimStatus({ ok: false, message: e.message })
    }
  }

  async function handleSubscribe(payload) {
    setSubscriptionStatus(null)
    try {
      const response = await createSubscription({
        ...payload,
        item_id: payload.item_id ? getBackendItemId(payload.item_id) : null,
      })
      setSubscriptionStatus({ ok: true, message: response.message })
    } catch (e) {
      setSubscriptionStatus({ ok: false, message: e.message })
    }
  }

  const selectedMeta = items.find((i) => i.item === selectedItem)
  const latestPoint = latestTelemetry ?? buffer[buffer.length - 1]
  const chartSeries = isContinuousSolarView(selectedItem)
    ? [
        { key: 'sinValue', label: 'sin(angle)', color: '#22d3ee' },
        { key: 'cosValue', label: 'cos(angle)', color: '#f59e0b' },
      ]
    : [
        { key: 'value', label: selectedMeta?.label ?? 'value', color: '#22d3ee' },
      ]

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
          <div className="right-kpi-group">
            {features && (
              <div className="features-stats">
                <span className="selector-label">Baseline (n={features.value_count})</span>
                <div className="features-row">
                  <span className="features-item">
                    <span className="features-label">Mean</span>
                    <span className="features-value">{features.baseline_mean.toFixed(3)}</span>
                  </span>
                  <span className="features-sep">·</span>
                  <span className="features-item">
                    <span className="features-label">Std</span>
                    <span className="features-value">{features.baseline_std.toFixed(3)}</span>
                  </span>
                  <span className="features-sep">·</span>
                  <span className="features-item">
                    <span className="features-label">dt</span>
                    <span className="features-value">{features.median_delta_t_seconds.toFixed(1)}s</span>
                  </span>
                </div>
              </div>
            )}
            {latestPoint && (
              <div className="latest-kpi">
                <span className="latest-label">Latest</span>
                {isContinuousSolarView(selectedItem) ? (
                  <>
                    <div className="latest-kpi-value">
                      <span className="latest-number">{latestPoint.sinValue?.toFixed(4) ?? '—'}</span>
                      <span className="latest-unit">sin</span>
                    </div>
                    <div className="latest-kpi-value">
                      <span className="latest-number">{latestPoint.cosValue?.toFixed(4) ?? '—'}</span>
                      <span className="latest-unit">cos</span>
                    </div>
                  </>
                ) : (
                  <div className="latest-kpi-value">
                    <span className="latest-number">{latestPoint.value?.toFixed(4) ?? '—'}</span>
                    <span className="latest-unit">{selectedMeta?.unit}</span>
                  </div>
                )}
                <span className="latest-time">
                  {new Date(latestPoint.t).toLocaleTimeString('en-US', { hour12: false })}
                </span>
              </div>
            )}
          </div>
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
            series={chartSeries}
            showAnomalyDots={!isContinuousSolarView(selectedItem)}
            jumpBreakThreshold={selectedItem === SOLAR_JOINT_ITEM ? 180 : null}
            showSmoothedOption={selectedItem === CABIN_TEMP_ITEM}
            timeRange={timeRange}
          />
        </div>

        <div className="bottom-row">
          <AnomalyLog anomalies={anomalies} />
          <div className="side-panels">
            <SimulationPanel
              selectedItem={selectedItem}
              onSimulate={handleSimulate}
              onInject={handleInject}
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
