import { isSimulated } from '../utils/anomaly'

function formatTime(isoStr) {
  return new Date(isoStr).toLocaleTimeString('en-US', { hour12: false })
}

function formatType(type) {
  return type.replace(/_/g, ' ')
}

export default function AnomalyLog({ anomalies }) {
  if (anomalies.length === 0) {
    return (
      <div className="log-empty">No anomalies recorded for this parameter.</div>
    )
  }

  return (
    <div className="log-wrapper">
      <table className="log-table">
        <thead>
          <tr>
            <th>Time</th>
            <th>Source</th>
            <th>Type</th>
            <th>Value</th>
            <th>Previous</th>
            <th>Threshold</th>
          </tr>
        </thead>
        <tbody>
          {anomalies.map((a, i) => {
            const simulated = isSimulated(a)
            return (
              <tr key={i}>
                <td className="log-time">{formatTime(a.detected_at_utc)}</td>
                <td className={simulated ? 'log-kind--injected' : 'log-kind--real'}>
                  {simulated ? 'Injected' : 'Real'}
                </td>
                <td className={simulated ? 'log-kind--injected' : 'log-kind--real'}>
                  {formatType(a.anomaly_type)}
                </td>
                <td>{a.value_numeric?.toFixed(4) ?? '—'}</td>
                <td>{a.previous_value_numeric?.toFixed(4) ?? '—'}</td>
                <td>{a.threshold_value?.toFixed(4) ?? '—'}</td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}
