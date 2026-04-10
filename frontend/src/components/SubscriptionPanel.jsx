import { useEffect, useState } from 'react'

const ANOMALY_OPTIONS = [
  { value: '', label: 'All anomaly types' },
  { value: 'threshold_breach_high', label: 'Threshold Breach High' },
  { value: 'threshold_breach_low', label: 'Threshold Breach Low' },
  { value: 'sudden_jump', label: 'Sudden Jump' },
]

export default function SubscriptionPanel({ items, selectedItem, onSubscribe, status }) {
  const [email, setEmail] = useState('')
  const [itemId, setItemId] = useState(selectedItem ?? '')
  const [anomalyType, setAnomalyType] = useState('')

  useEffect(() => {
    setItemId(selectedItem ?? '')
  }, [selectedItem])

  async function handleSubmit(e) {
    e.preventDefault()
    await onSubscribe({
      email,
      item_id: itemId || null,
      anomaly_type: anomalyType || null,
    })
  }

  return (
    <div className="subscription-panel">
      <span className="sim-title">Email Alerts</span>
      <form className="subscription-form" onSubmit={handleSubmit}>
        <input
          className="subscription-input"
          type="email"
          placeholder="you@example.com"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
          required
        />
        <select
          className="selector sim-select"
          value={itemId}
          onChange={(e) => setItemId(e.target.value)}
        >
          <option value="">All parameters</option>
          {items.map((item) => (
            <option key={item.item} value={item.item}>
              {item.label}
            </option>
          ))}
        </select>
        <select
          className="selector sim-select"
          value={anomalyType}
          onChange={(e) => setAnomalyType(e.target.value)}
        >
          {ANOMALY_OPTIONS.map((option) => (
            <option key={option.value || 'all'} value={option.value}>
              {option.label}
            </option>
          ))}
        </select>
        <button type="submit" className="sim-btn">
          Subscribe
        </button>
      </form>
      {status && (
        <span className={`sim-status ${status.ok ? 'sim-status--ok' : 'sim-status--err'}`}>
          {status.message}
        </span>
      )}
    </div>
  )
}
