import { useState, useRef, useEffect } from 'react'
import { anomalyColor } from '../utils/anomaly'
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ReferenceLine,
  ReferenceDot,
  ResponsiveContainer,
  Brush,
} from 'recharts'

function formatTime(ms) {
  return new Date(ms).toLocaleTimeString('en-US', { hour12: false })
}

function CustomTooltip({ active, payload, label, unit }) {
  if (!active || !payload?.length) return null
  return (
    <div className="tooltip">
      <div className="tooltip-time">{formatTime(label)}</div>
      <div className="tooltip-value">
        {payload[0].value?.toFixed(4)} {unit}
      </div>
    </div>
  )
}

export default function TelemetryChart({ buffer, unit, anomalies = [], hasError = false, showBrush = false }) {
  const [yDomain, setYDomain] = useState(null)
  const [brushStart, setBrushStart] = useState(0)
  const [brushEnd, setBrushEnd] = useState(Math.max(0, buffer.length - 1))
  const pinnedToEnd = useRef(true)
  const prevLengthRef = useRef(buffer.length)
  const containerRef = useRef(null)
  const yDomainRef = useRef(yDomain)
  const bufferRef = useRef(buffer)

  useEffect(() => { yDomainRef.current = yDomain }, [yDomain])
  useEffect(() => { bufferRef.current = buffer }, [buffer])

  // Manage brush indices as buffer grows or resets
  useEffect(() => {
    const len = buffer.length
    const prevLen = prevLengthRef.current

    if (len === 0) {
      pinnedToEnd.current = true
    } else if (prevLen === 0) {
      setBrushStart(0)
      setBrushEnd(len - 1)
    } else if (pinnedToEnd.current && len > prevLen) {
      setBrushEnd(len - 1)
    }

    prevLengthRef.current = len
  }, [buffer.length])

  // Reset Y domain when buffer is cleared (param/range switch)
  useEffect(() => {
    if (buffer.length === 0) setYDomain(null)
  }, [buffer.length])

  // Wheel handler for vertical zoom — registered with passive:false so preventDefault works
  useEffect(() => {
    const el = containerRef.current
    if (!el) return

    const handler = (e) => {
      e.preventDefault()
      const buf = bufferRef.current
      if (!buf.length) return

      const values = buf.map((p) => p.value).filter((v) => v != null)
      const dataMin = Math.min(...values)
      const dataMax = Math.max(...values)
      const dom = yDomainRef.current
      const currentMin = dom ? dom[0] : dataMin
      const currentMax = dom ? dom[1] : dataMax
      const range = currentMax - currentMin || 1
      const center = (currentMin + currentMax) / 2
      const factor = e.deltaY > 0 ? 1.2 : 0.83
      const newRange = range * factor
      setYDomain([center - newRange / 2, center + newRange / 2])
    }

    el.addEventListener('wheel', handler, { passive: false })
    return () => el.removeEventListener('wheel', handler)
  }, [])

  if (buffer.length === 0) {
    const msg = hasError
      ? 'No data — item not yet in pipeline state'
      : 'Waiting for data…'
    return <div className="chart-empty">{msg}</div>
  }

  const tMin = buffer[0]?.t ?? 0
  const tMax = buffer[buffer.length - 1]?.t ?? 0
  const visibleAnomalies = anomalies
    .map((a) => ({ ...a, t: new Date(a.detected_at_utc).getTime() }))
    .filter((a) => a.t >= tMin && a.t <= tMax)

  const chartMargin = showBrush
    ? { top: 10, right: 24, bottom: 4, left: 10 }
    : { top: 10, right: 24, bottom: 10, left: 10 }

  return (
    <div className="chart-inner" ref={containerRef}>
      {yDomain && (
        <button className="chart-reset-y" onClick={() => setYDomain(null)}>
          Reset Y
        </button>
      )}
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={buffer} margin={chartMargin}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1a2030" vertical={false} />
          <XAxis
            dataKey="t"
            type="number"
            domain={['dataMin', 'dataMax']}
            tickFormatter={formatTime}
            tick={{ fill: '#4a5568', fontSize: 11 }}
            axisLine={{ stroke: '#1a2030' }}
            tickLine={false}
            minTickGap={80}
          />
          <YAxis
            tick={{ fill: '#4a5568', fontSize: 11 }}
            axisLine={{ stroke: '#1a2030' }}
            tickLine={false}
            width={72}
            tickFormatter={(v) => v?.toFixed(2)}
            domain={yDomain ?? ['auto', 'auto']}
          />
          <Tooltip content={<CustomTooltip unit={unit} />} />
          <Line
            type="monotone"
            dataKey="value"
            stroke="#22d3ee"
            strokeWidth={2}
            dot={false}
            isAnimationActive={false}
          />
          {visibleAnomalies.map((a, i) => (
            <ReferenceLine
              key={`vline-${i}`}
              x={a.t}
              stroke={anomalyColor(a)}
              strokeWidth={1}
              strokeDasharray="4 3"
            />
          ))}
          {visibleAnomalies.map((a, i) => (
            <ReferenceDot
              key={`dot-${i}`}
              x={a.t}
              y={a.value_numeric}
              r={5}
              fill={anomalyColor(a)}
              stroke="#080b10"
              strokeWidth={1.5}
            />
          ))}
          {showBrush && (
            <Brush
              dataKey="t"
              startIndex={brushStart}
              endIndex={Math.min(brushEnd, buffer.length - 1)}
              height={28}
              stroke="#1a2030"
              fill="#080b10"
              travellerWidth={6}
              tickFormatter={formatTime}
              onChange={({ startIndex, endIndex }) => {
                setBrushStart(startIndex)
                setBrushEnd(endIndex)
                pinnedToEnd.current = endIndex >= buffer.length - 1
              }}
            />
          )}
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}
