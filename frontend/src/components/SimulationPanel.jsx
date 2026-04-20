import { useState } from 'react'
import InjectionModal from './InjectionModal'

export default function SimulationPanel({ selectedItem, onSimulate, onInject, status }) {
  const [modalOpen, setModalOpen] = useState(false)

  return (
    <div className="panel-card">
      <span className="panel-card-title">Simulate Anomaly</span>
      <button
        className="sim-btn sim-btn--full"
        disabled={!selectedItem}
        onClick={() => setModalOpen(true)}
      >
        Inject Anomaly…
      </button>
      {status && (
        <span className={`sim-status ${status.ok ? 'sim-status--ok' : 'sim-status--err'}`}>
          {status.message}
        </span>
      )}
      {modalOpen && (
        <InjectionModal
          selectedItem={selectedItem}
          onClose={() => setModalOpen(false)}
          onInject={async (payload) => {
            if (payload.prototype_id) {
              await onInject(payload)
            } else {
              await onSimulate(payload.mode)
            }
            setModalOpen(false)
          }}
        />
      )}
    </div>
  )
}
