const SMOOTH_CONFIG = {
  '1h':  { stepMs: 5_000,  maxGapMs: 90_000,  smoother: 'ema',    alpha: 0.3 },
  '6h':  { stepMs: 30_000, maxGapMs: 120_000, smoother: 'median', window: 3  },
  '24h': { stepMs: 60_000, maxGapMs: 300_000, smoother: 'median', window: 3  },
}

function resample(rawBuffer, stepMs, maxGapMs) {
  if (rawBuffer.length < 2) return rawBuffer

  const result = []
  const tStart = rawBuffer[0].t
  const tEnd   = rawBuffer[rawBuffer.length - 1].t
  let j = -1

  for (let t = tStart; t <= tEnd; t += stepMs) {
    while (j + 1 < rawBuffer.length && rawBuffer[j + 1].t <= t) j++

    const left  = j >= 0 ? rawBuffer[j] : null
    const right = j + 1 < rawBuffer.length ? rawBuffer[j + 1] : null

    if (!left && !right)                           { result.push({ t, value: null }); continue }
    if (!left)                                     { result.push({ t, value: right.value }); continue }
    if (!right)                                    { result.push({ t, value: left.value }); continue }
    if (left.value == null || right.value == null) { result.push({ t, value: null }); continue }

    const gap = right.t - left.t
    if (gap > maxGapMs)                            { result.push({ t, value: null }); continue }

    const frac = (t - left.t) / gap
    result.push({ t, value: left.value + frac * (right.value - left.value) })
  }

  return result
}

function applyEMA(points, alpha) {
  let ema = null
  return points.map((pt) => {
    if (pt.value == null) { ema = null; return pt }
    ema = ema == null ? pt.value : alpha * pt.value + (1 - alpha) * ema
    return { ...pt, value: ema }
  })
}

function applyRollingMedian(points, winSize) {
  const half = Math.floor(winSize / 2)
  return points.map((pt, i) => {
    if (pt.value == null) return pt
    const vals = []
    for (let k = i - half; k <= i + half; k++) {
      if (k >= 0 && k < points.length && points[k].value != null) vals.push(points[k].value)
    }
    if (!vals.length) return pt
    const sorted = [...vals].sort((a, b) => a - b)
    return { ...pt, value: sorted[Math.floor(sorted.length / 2)] }
  })
}

export function buildSmoothedBuffer(rawBuffer, timeRange) {
  if (rawBuffer.length < 2 || timeRange === 'recent') return rawBuffer

  const cfg = SMOOTH_CONFIG[timeRange]
  if (!cfg) return rawBuffer

  const resampled = resample(rawBuffer, cfg.stepMs, cfg.maxGapMs)
  return cfg.smoother === 'ema'
    ? applyEMA(resampled, cfg.alpha)
    : applyRollingMedian(resampled, cfg.window)
}
