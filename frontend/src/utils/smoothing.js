// Pass 1: median kills quantization spikes. Pass 2: EMA rounds off residual noise.
const SMOOTH_CONFIG = {
  '1h':  { stepMs: 5_000,  maxGapMs: 90_000,  medianWindow: 5,  emaAlpha: 0.15 },
  '6h':  { stepMs: 30_000, maxGapMs: 120_000, medianWindow: 7,  emaAlpha: 0.12 },
  '24h': { stepMs: 60_000, maxGapMs: 300_000, medianWindow: 11, emaAlpha: 0.10 },
}

// Linear interpolation onto a regular grid; real gaps (> maxGapMs) become null breaks.
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

// Two-pass: median first, then EMA.
export function buildSmoothedBuffer(rawBuffer, timeRange) {
  if (rawBuffer.length < 2 || timeRange === 'recent') return rawBuffer

  const cfg = SMOOTH_CONFIG[timeRange]
  if (!cfg) return rawBuffer

  const resampled    = resample(rawBuffer, cfg.stepMs, cfg.maxGapMs)
  const afterMedian  = applyRollingMedian(resampled, cfg.medianWindow)
  return applyEMA(afterMedian, cfg.emaAlpha)
}

// Zero-order hold bridge: holds the last known value flat across each gap,
// then steps vertically to the next real value at the very last moment.
// A synthetic hold point is injected 1 ms before each right boundary so
// Recharts renders a true horizontal→vertical step rather than a diagonal.
export function buildConnectedData(buffer) {
  const intermediate = buffer.map((pt) => ({ ...pt, bridgeValue: null }))

  let i = 0
  while (i < intermediate.length) {
    if (intermediate[i].value !== null) { i++; continue }

    const gapStart = i
    while (i < intermediate.length && intermediate[i].value === null) i++
    const gapEnd = i

    const leftIdx  = gapStart - 1
    const rightIdx = gapEnd

    if (leftIdx < 0 || rightIdx >= intermediate.length) continue

    const leftPt  = intermediate[leftIdx]
    const rightPt = intermediate[rightIdx]

    // Left boundary and all null gap points hold the left value flat
    intermediate[leftIdx].bridgeValue = leftPt.value
    for (let k = gapStart; k < gapEnd; k++) {
      intermediate[k].bridgeValue = leftPt.value
    }
    // Right boundary takes the new value — the step happens here
    intermediate[rightIdx].bridgeValue = rightPt.value
  }

  // Second pass: inject a hold point 1 ms before each right-boundary step
  // so Recharts draws horizontal then vertical rather than a diagonal
  const result = []
  for (let j = 0; j < intermediate.length; j++) {
    const pt   = intermediate[j]
    const next = intermediate[j + 1]
    result.push(pt)

    if (
      pt.value === null &&
      pt.bridgeValue != null &&
      next?.value != null &&
      next.bridgeValue != null &&
      next.t - pt.t > 1
    ) {
      result.push({ t: next.t - 1, value: null, bridgeValue: pt.bridgeValue })
    }
  }

  return result
}
