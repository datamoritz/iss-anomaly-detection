export async function fetchItems() {
  const res = await fetch('/api/v1/items')
  if (!res.ok) throw new Error(`Failed to fetch items (${res.status})`)
  return res.json()
}

export async function fetchLatest(itemId) {
  const res = await fetch(`/api/v1/telemetry/latest/${itemId}`)
  if (!res.ok) throw new Error(`Failed to fetch telemetry (${res.status})`)
  return res.json()
}

export async function fetchAnomalies(itemId) {
  const res = await fetch(`/api/v1/anomalies/recent/${itemId}?limit=50`)
  if (!res.ok) throw new Error(`Failed to fetch anomalies (${res.status})`)
  return res.json()
}

export async function simulateAnomaly(item, mode) {
  const res = await fetch('/api/v1/simulate-anomaly', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ item, mode }),
  })
  const data = await res.json()
  if (!res.ok) throw new Error(data.detail ?? `Simulation failed (${res.status})`)
  return data
}
