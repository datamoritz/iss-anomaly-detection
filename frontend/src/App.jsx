import { useState, useEffect, useRef } from 'react'
import { fetchItems, fetchLatest, fetchAnomalies, simulateAnomaly } from './api/client'
import ParameterSelector from './components/ParameterSelector'
import TelemetryChart from './components/TelemetryChart'
import AnomalyLog from './components/AnomalyLog'
import SimulationPanel from './components/SimulationPanel'

const MAX_POINTS = 120  // ~4 minutes at 2s polling
const POLL_INTERVAL_MS = 2000
const SIMULATION_VISIBLE_MS = 1500

export default function App() {
  const [items, setItems] = useState([])
  const [selectedItem, setSelectedItem] = useState(null)
  const [buffer, setBuffer] = useState([])
  const [anomalies, setAnomalies] = useState([])
  const [simStatus, setSimStatus] = useState(null)
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

  // Telemetry polling — 2s
  useEffect(() => {
    if (!selectedItem) return

    setBuffer([])
    clearTimeout(simulationResetRef.current)
    clearInterval(telemetryIntervalRef.current)

    const poll = async () => {
      try {
        const point = await fetchLatest(selectedItem)
        setBuffer((prev) => {
          const next = [...prev, { t: Date.now(), value: point.value }]
          return next.slice(-MAX_POINTS)
        })
        setError(null)
      } catch (e) {
        setError(e.message)
      }
    }

    poll()
    telemetryIntervalRef.current = setInterval(poll, POLL_INTERVAL_MS)

    return () => clearInterval(telemetryIntervalRef.current)
  }, [selectedItem])

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
          {selectedMeta && (
            <div className="item-meta">
              <span className="meta-label">{selectedMeta.label}</span>
              <span className="meta-desc">{selectedMeta.description}</span>
            </div>
          )}
        </div>

        {error && (
          <div className="error-banner">Backend unreachable — {error}</div>
        )}

        <div className="chart-container">
          <TelemetryChart buffer={buffer} unit={selectedMeta?.unit ?? ''} anomalies={anomalies} hasError={!!error} />
        </div>

        {latestPoint && (
          <div className="latest-row">
            <span className="latest-label">Latest</span>
            <span className="latest-number">
              {latestPoint.value?.toFixed(4) ?? '—'}
            </span>
            <span className="latest-unit">{selectedMeta?.unit}</span>
            <span className="latest-time">
              {new Date(latestPoint.t).toLocaleTimeString('en-US', { hour12: false })}
            </span>
          </div>
        )}

        <div className="bottom-row">
          <div className="log-section">
            <span className="section-label">Anomaly Log</span>
            <AnomalyLog anomalies={anomalies} />
          </div>
          <div className="sim-section">
            <SimulationPanel
              selectedItem={selectedItem}
              onSimulate={handleSimulate}
              status={simStatus}
            />
          </div>
        </div>
      </main>
    </div>
  )
}
