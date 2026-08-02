/** Sentinel from the engine: the quantile fell above the largest bucket. */
export const LATENCY_OVERFLOW = -1;

/**
 * Renders a latency percentile from the backend, which is a histogram bucket
 * *bound* rather than a measurement — a p99 of 100000 means "somewhere between
 * 10ms and 100ms". Printing the raw number reads as an exact figure and is off
 * by up to a full bucket width, so the qualifier is not decoration.
 */
export function formatLatencyBound(us: number | undefined): string {
  if (us === undefined || us === 0) return "—";
  if (us === LATENCY_OVERFLOW) return "off scale";
  return `≤${us.toLocaleString()}µs`;
}
