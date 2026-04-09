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

export default function TelemetryChart({ buffer, unit, anomalies = [], hasError = false }) {
  if (buffer.length === 0) {
    const msg = hasError
      ? 'No data — item not yet in pipeline state'
      : 'Waiting for data…'
    return <div className="chart-empty">{msg}</div>
  }

  const tMin = buffer[0]?.t ?? 0
  const tMax = buffer[buffer.length - 1]?.t ?? 0

  // Only show anomaly markers that fall within the current chart window
  const visibleAnomalies = anomalies
    .map((a) => ({ ...a, t: new Date(a.detected_at_utc).getTime() }))
    .filter((a) => a.t >= tMin && a.t <= tMax)

  return (
    <ResponsiveContainer width="100%" height="100%">
      <LineChart data={buffer} margin={{ top: 10, right: 24, bottom: 10, left: 10 }}>
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
      </LineChart>
    </ResponsiveContainer>
  )
}
