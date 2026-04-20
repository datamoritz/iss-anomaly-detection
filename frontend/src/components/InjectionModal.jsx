import { useState } from 'react'
import { PROTOTYPES, MAX_INJECTION_TICKS } from '../data/prototypes'

const CLASS_COLORS = {
  collective: '#f59e0b',
  contextual: '#a78bfa',
  point: '#ef4444',
}

function Sparkline({ z }) {
  const W = 120
  const H = 40
  const min = Math.min(...z)
  const max = Math.max(...z)
  const range = max - min || 1

  const pts = z.map((v, i) => {
    const x = (i / (z.length - 1)) * W
    const y = H - ((v - min) / range) * H
    return `${x.toFixed(1)},${y.toFixed(1)}`
  })

  return (
    <svg width={W} height={H} viewBox={`0 0 ${W} ${H}`} className="sparkline">
      <polyline
        points={pts.join(' ')}
        fill="none"
        stroke="#22d3ee"
        strokeWidth="1.5"
        strokeLinejoin="round"
        strokeLinecap="round"
      />
    </svg>
  )
}

export default function InjectionModal({ selectedItem, onClose, onInject }) {
  const [injecting, setInjecting] = useState(null)
  const [status, setStatus] = useState(null)

  async function handlePrototype(proto) {
    setInjecting(proto.id)
    setStatus(null)
    try {
      await onInject({
        prototype_id: proto.id,
        item_id: selectedItem,
        severity: 1.0,
        time_scale: proto.duration > MAX_INJECTION_TICKS
          ? MAX_INJECTION_TICKS / proto.duration
          : 1.0,
        recenter: true,
      })
      setStatus({ ok: true, message: `Injected ${proto.label}` })
    } catch (e) {
      setStatus({ ok: false, message: e.message })
    } finally {
      setInjecting(null)
    }
  }

  async function handleThreshold(mode) {
    setInjecting(mode)
    setStatus(null)
    try {
      await onInject({ item_id: selectedItem, mode })
      setStatus({ ok: true, message: `Threshold breach injected` })
    } catch (e) {
      setStatus({ ok: false, message: e.message })
    } finally {
      setInjecting(null)
    }
  }

  return (
    <div className="modal-overlay" onClick={(e) => e.target === e.currentTarget && onClose()}>
      <div className="modal">
        <div className="modal-header">
          <span className="modal-title">Inject Anomaly</span>
          <button className="modal-close" onClick={onClose}>✕</button>
        </div>

        <div className="modal-section-label">Threshold Breach</div>
        <div className="modal-threshold-row">
          <button
            className="sim-btn modal-thresh-btn"
            disabled={!!injecting}
            onClick={() => handleThreshold('threshold_breach_high')}
          >
            {injecting === 'threshold_breach_high' ? 'Injecting…' : 'Breach High'}
          </button>
          <button
            className="sim-btn modal-thresh-btn"
            disabled={!!injecting}
            onClick={() => handleThreshold('threshold_breach_low')}
          >
            {injecting === 'threshold_breach_low' ? 'Injecting…' : 'Breach Low'}
          </button>
        </div>

        <div className="modal-section-label">SMAP Prototypes</div>
        <div className="proto-grid">
          {PROTOTYPES.map((proto) => {
            const scaled = proto.duration > MAX_INJECTION_TICKS
            const classColor = CLASS_COLORS[proto.proposalClass] ?? '#4a5568'
            const busy = injecting === proto.id
            return (
              <button
                key={proto.id}
                className={`proto-card ${busy ? 'proto-card--busy' : ''}`}
                disabled={!!injecting}
                onClick={() => handlePrototype(proto)}
              >
                <Sparkline z={proto.z} />
                <div className="proto-label">{proto.label}</div>
                <div className="proto-meta">
                  <span className="proto-class" style={{ color: classColor }}>{proto.proposalClass}</span>
                  <span className="proto-shape">{proto.shapeSubtype}</span>
                </div>
                <div className="proto-ticks">
                  {proto.duration} ticks
                  {scaled && <span className="proto-scaled">→ {MAX_INJECTION_TICKS}</span>}
                </div>
                {busy && <div className="proto-busy-overlay">Injecting…</div>}
              </button>
            )
          })}
        </div>

        {status && (
          <div className={`modal-status ${status.ok ? 'modal-status--ok' : 'modal-status--err'}`}>
            {status.message}
          </div>
        )}
      </div>
    </div>
  )
}
