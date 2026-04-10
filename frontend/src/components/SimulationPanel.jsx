import { useState } from 'react'

const MODES = [
  { value: 'threshold_breach_high', label: 'Thr High' },
  { value: 'threshold_breach_low',  label: 'Thr Low' },
  { value: 'sudden_jump',           label: 'Sudden Jump' },
]

export default function SimulationPanel({ selectedItem, onSimulate, status }) {
  const [mode, setMode] = useState(MODES[0].value)

  function handleInject() {
    onSimulate(mode)
  }

  return (
    <div className="panel-card">
      <span className="panel-card-title">Simulate Anomaly</span>
      <div className="sim-mode-pills">
        {MODES.map((m) => (
          <button
            key={m.value}
            className={`sim-mode-pill ${mode === m.value ? 'sim-mode-pill--active' : ''}`}
            onClick={() => setMode(m.value)}
          >
            {m.label}
          </button>
        ))}
      </div>
      <button
        className="sim-btn sim-btn--full"
        onClick={handleInject}
        disabled={!selectedItem}
      >
        Inject
      </button>
      {status && (
        <span className={`sim-status ${status.ok ? 'sim-status--ok' : 'sim-status--err'}`}>
          {status.message}
        </span>
      )}
    </div>
  )
}
