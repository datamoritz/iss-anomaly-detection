const API_BASE = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000'

export async function fetchItems() {
  const res = await fetch(`${API_BASE}/api/v1/items`)
  if (!res.ok) throw new Error(`Failed to fetch items (${res.status})`)
  return res.json()
}

export async function fetchLatest(itemId) {
  const res = await fetch(`${API_BASE}/api/v1/telemetry/latest/${itemId}`)
  if (!res.ok) throw new Error(`Failed to fetch latest telemetry (${res.status})`)
  return res.json()
}

export async function fetchLatestContinuousAngle() {
  const res = await fetch(`${API_BASE}/api/v1/telemetry/latest/angle_cont`)
  if (!res.ok) throw new Error(`Failed to fetch latest continuous angle telemetry (${res.status})`)
  return res.json()
}

export async function fetchRecentTelemetry(itemId, limit = 100) {
  const res = await fetch(`${API_BASE}/api/v1/telemetry/recent/${itemId}?limit=${limit}`)
  if (!res.ok) throw new Error(`Failed to fetch recent telemetry (${res.status})`)
  return res.json()
}

export async function fetchRecentContinuousAngle(limit = 100) {
  const res = await fetch(`${API_BASE}/api/v1/telemetry/recent/angle_cont?limit=${limit}`)
  if (!res.ok) throw new Error(`Failed to fetch recent continuous angle telemetry (${res.status})`)
  return res.json()
}

export async function fetchTelemetryHistory(itemId, { from, to, limit = 1000 }) {
  const params = new URLSearchParams({
    from,
    to,
    limit: String(limit),
  })
  const res = await fetch(`${API_BASE}/api/v1/telemetry/history/${itemId}?${params.toString()}`)
  if (!res.ok) throw new Error(`Failed to fetch telemetry history (${res.status})`)
  return res.json()
}

export async function fetchContinuousAngleHistory({ from, to, limit = 1000 }) {
  const params = new URLSearchParams({
    from,
    to,
    limit: String(limit),
  })
  const res = await fetch(`${API_BASE}/api/v1/telemetry/history/angle_cont?${params.toString()}`)
  if (!res.ok) throw new Error(`Failed to fetch continuous angle history (${res.status})`)
  return res.json()
}

export async function fetchAnomalies(itemId) {
  const res = await fetch(`${API_BASE}/api/v1/anomalies/recent/${itemId}?limit=50`)
  if (!res.ok) throw new Error(`Failed to fetch anomalies (${res.status})`)
  return res.json()
}

export async function simulateAnomaly(payload) {
  const res = await fetch(`${API_BASE}/api/v1/simulate-anomaly`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
  if (!res.ok) throw new Error(`Failed to simulate anomaly (${res.status})`)
  return res.json()
}

export async function createSubscription(payload) {
  const res = await fetch(`${API_BASE}/api/v1/subscriptions`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  })
  if (!res.ok) throw new Error(`Failed to create subscription (${res.status})`)
  return res.json()
}
