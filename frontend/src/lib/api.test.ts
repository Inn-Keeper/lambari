import { afterEach, describe, expect, it, vi } from "vitest";
import { fetchCases, resolveCase, setSimulation } from "./api";

const ok = (body: unknown) => new Response(JSON.stringify(body), { status: 200 });
const fail = () => new Response("boom", { status: 500 });

afterEach(() => vi.unstubAllGlobals());

describe("api layer", () => {
  it("fetchCases returns cases and tolerates null", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => ok({ cases: null })));
    expect(await fetchCases()).toEqual([]);
  });

  it("every call throws on non-2xx instead of swallowing", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => fail()));
    await expect(fetchCases()).rejects.toThrow("HTTP 500");
    await expect(resolveCase("tx_1", "confirmed_fraud")).rejects.toThrow("HTTP 500");
    await expect(setSimulation(1000)).rejects.toThrow("HTTP 500");
  });

  it("resolveCase posts the resolution to the right route", async () => {
    const f = vi.fn(async () => ok({}));
    vi.stubGlobal("fetch", f);
    await resolveCase("tx_9", "false_positive");
    expect(f).toHaveBeenCalledWith(
      "/api/cases/tx_9/resolve",
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({ resolution: "false_positive" }),
      }),
    );
  });
});
