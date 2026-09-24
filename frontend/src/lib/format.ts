/** Sentinel from the engine: the quantile fell above the largest bucket. */
export const LATENCY_OVERFLOW = -1;

/**
 * Renders a latency percentile, which is a histogram bucket bound, not a
 * measurement: 100000 means "between 10ms and 100ms", so it prints as "≤".
 */
export function formatLatencyBound(us: number | undefined): string {
  if (us === undefined || us === 0) return "—";
  if (us === LATENCY_OVERFLOW) return "off scale";
  return `≤${us.toLocaleString()}µs`;
}
