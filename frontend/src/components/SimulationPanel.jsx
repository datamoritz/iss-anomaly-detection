const MODES = [
  { value: 'threshold_breach_high', label: 'Threshold Breach High' },
  { value: 'threshold_breach_low',  label: 'Threshold Breach Low' },
  { value: 'sudden_jump',           label: 'Sudden Jump' },
]

export default function SimulationPanel({ selectedItem, onSimulate, status }) {
  function handleSubmit(e) {
    e.preventDefault()
    const mode = e.target.mode.value
    onSimulate(mode)
  }

  return (
    <div className="sim-panel">
      <span className="sim-title">Simulate Anomaly</span>
      <form className="sim-form" onSubmit={handleSubmit}>
        <select name="mode" className="selector sim-select">
          {MODES.map((m) => (
            <option key={m.value} value={m.value}>{m.label}</option>
          ))}
        </select>
        <button type="submit" className="sim-btn" disabled={!selectedItem}>
          Inject
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
