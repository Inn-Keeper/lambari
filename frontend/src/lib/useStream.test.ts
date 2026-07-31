import { describe, expect, it } from "vitest";
import {
  initialStreamState,
  streamReducer,
  type StreamMessage,
} from "./useStream";

function msg(rate: number): StreamMessage {
  return {
    stats: {
      processed: 1,
      approved: 1,
      reviewed: 0,
      declined: 0,
      rate_per_sec: rate,
      p50_us: 10,
      p99_us: 20,
      queue_depth: 0,
      queue_cap: 16384,
      uptime_sec: 1,
      rule_fires: {},
      flagged_rate: 0,
    },
    recent: null,
    sim: { running: false, rate: 0 },
    cases: { open: 0, confirmed_fraud: 0, false_positive: 0 },
  };
}

describe("streamReducer", () => {
  it("open/error toggle connected", () => {
    const opened = streamReducer(initialStreamState, { type: "open" });
    expect(opened.connected).toBe(true);
    expect(streamReducer(opened, { type: "error" }).connected).toBe(false);
  });

  it("message applies payload and null recent becomes []", () => {
    const s = streamReducer(initialStreamState, { type: "message", data: msg(42) });
    expect(s.connected).toBe(true);
    expect(s.stats?.rate_per_sec).toBe(42);
    expect(s.recent).toEqual([]);
    expect(s.rateHistory).toEqual([42]);
  });

  it("rate history is capped at 90, oldest dropped", () => {
    let s = initialStreamState;
    for (let i = 0; i < 95; i++) {
      s = streamReducer(s, { type: "message", data: msg(i) });
    }
    expect(s.rateHistory).toHaveLength(90);
    expect(s.rateHistory[0]).toBe(5);
    expect(s.rateHistory[89]).toBe(94);
  });
});
