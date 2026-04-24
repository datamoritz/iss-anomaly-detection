export function isSimulated(anomaly) {
  return anomaly.is_simulated === true
}

export function anomalyColor(anomaly) {
  return isSimulated(anomaly) ? '#f59e0b' : '#ef4444'
}
