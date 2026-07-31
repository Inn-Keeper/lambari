import { describe, expect, it } from "vitest";
import type { Case } from "./api";
import { casesReducer, initialQueueState } from "./casesReducer";

const mk = (id: string): Case => ({
  id,
  verdict: {
    tx_id: id,
    card_bin: "520082",
    amount: 100,
    currency: "SEK",
    country: "SE",
    score: 50,
    decision: "review",
    latency_us: 10,
    at: 0,
  },
  status: "open",
  opened_at: 0,
});

const loaded = (ids: string[]) =>
  casesReducer(initialQueueState, { type: "loaded", cases: ids.map(mk) });

describe("casesReducer", () => {
  it("loaded replaces cases and clears error", () => {
    const errored = casesReducer(initialQueueState, { type: "loadFailed" });
    expect(errored.error).toMatch(/refresh/);
    const s = casesReducer(errored, { type: "loaded", cases: [mk("a")] });
    expect(s.cases.map((c) => c.id)).toEqual(["a"]);
    expect(s.error).toBeNull();
  });

  it("resolveStart removes optimistically; resolveOk commits", () => {
    let s = loaded(["a", "b", "c"]);
    s = casesReducer(s, { type: "resolveStart", id: "b" });
    expect(s.cases.map((c) => c.id)).toEqual(["a", "c"]);
    expect(s.busy).toBe("b");
    s = casesReducer(s, { type: "resolveOk" });
    expect(s.busy).toBeNull();
    expect(s.pending).toBeNull();
  });

  it("resolveFail reinstates the case at its original index with an error", () => {
    let s = loaded(["a", "b", "c"]);
    s = casesReducer(s, { type: "resolveStart", id: "b" });
    s = casesReducer(s, { type: "resolveFail" });
    expect(s.cases.map((c) => c.id)).toEqual(["a", "b", "c"]);
    expect(s.busy).toBeNull();
    expect(s.error).toContain("b");
  });

  it("resolveGone drops the case without rollback and says why", () => {
    let s = loaded(["a", "b"]);
    s = casesReducer(s, { type: "resolveStart", id: "a" });
    s = casesReducer(s, { type: "resolveGone" });
    expect(s.cases.map((c) => c.id)).toEqual(["b"]);
    expect(s.busy).toBeNull();
    expect(s.pending).toBeNull();
    expect(s.error).toContain("a");
  });

  it("while paused, fresh loads are deferred; resume applies the freshest", () => {
    let s = loaded(["a", "b"]);
    s = casesReducer(s, { type: "pause" });
    s = casesReducer(s, { type: "loaded", cases: [mk("c"), mk("d")] });
    expect(s.cases.map((c) => c.id)).toEqual(["a", "b"]); // list holds still
    expect(s.deferred?.map((c) => c.id)).toEqual(["c", "d"]);
    s = casesReducer(s, { type: "resume" });
    expect(s.cases.map((c) => c.id)).toEqual(["c", "d"]);
    expect(s.paused).toBe(false);
    expect(s.deferred).toBeNull();
  });

  it("resume without deferred data just unpauses", () => {
    let s = loaded(["a"]);
    s = casesReducer(s, { type: "pause" });
    s = casesReducer(s, { type: "resume" });
    expect(s.cases.map((c) => c.id)).toEqual(["a"]);
    expect(s.paused).toBe(false);
  });

  it("resume still filters the case with a resolve in flight", () => {
    let s = loaded(["a", "b"]);
    s = casesReducer(s, { type: "pause" });
    s = casesReducer(s, { type: "resolveStart", id: "a" });
    s = casesReducer(s, { type: "loaded", cases: [mk("a"), mk("b")] });
    s = casesReducer(s, { type: "resume" });
    expect(s.cases.map((c) => c.id)).toEqual(["b"]);
  });

  it("loaded during an in-flight resolve keeps the optimistic removal", () => {
    let s = loaded(["a", "b"]);
    s = casesReducer(s, { type: "resolveStart", id: "a" });
    // the refetch raced the resolve: server still lists "a"
    s = casesReducer(s, { type: "loaded", cases: [mk("a"), mk("b")] });
    expect(s.cases.map((c) => c.id)).toEqual(["b"]);
  });
});
