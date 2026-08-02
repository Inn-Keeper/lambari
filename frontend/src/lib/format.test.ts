import { describe, expect, it } from "vitest";
import { formatLatencyBound, LATENCY_OVERFLOW } from "./format";

describe("formatLatencyBound", () => {
  // The backend sends bucket bounds. Rendering 100000 as "100,000µs" claims a
  // precision the histogram does not have — the real value is anywhere in
  // (10ms, 100ms].
  it("marks values as bounds, not measurements", () => {
    expect(formatLatencyBound(25)).toBe("≤25µs");
    expect(formatLatencyBound(100_000)).toBe("≤100,000µs");
  });

  // Rendering the overflow sentinel verbatim would print "-1µs"; rendering the
  // largest bucket instead would print a degraded p99 as an exact 1,000,000µs.
  it("says off scale rather than inventing a number", () => {
    expect(formatLatencyBound(LATENCY_OVERFLOW)).toBe("off scale");
  });

  it("shows nothing rather than zero before anything is scored", () => {
    expect(formatLatencyBound(0)).toBe("—");
    expect(formatLatencyBound(undefined)).toBe("—");
  });
});
