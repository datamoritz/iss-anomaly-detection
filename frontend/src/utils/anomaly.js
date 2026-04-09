const ONE_HOUR_MS = 60 * 60 * 1000

// is_simulated may be null on legacy records — fall back to time-based heuristic
export function isSimulated(anomaly) {
  if (anomaly.is_simulated === true) return true
  const age = Date.now() - new Date(anomaly.detected_at_utc).getTime()
  return age < ONE_HOUR_MS
}

export function anomalyColor(anomaly) {
  return isSimulated(anomaly) ? '#f59e0b' : '#ef4444'
}
