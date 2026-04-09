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