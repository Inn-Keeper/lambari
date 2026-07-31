# React Triage Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the dashboard's live-data paths honest and testable: reducer-based stream state, push-driven queue (no polling), optimistic resolve with rollback, SimControl revert-on-failure, and a vitest test suite.

**Architecture:** Pure exported reducers (`streamReducer` in `useStream.ts`, `casesReducer` in a new `lib/casesReducer.ts`) carry all state transitions so they're unit-testable without DOM. A new `lib/api.ts` is the only place `fetch` happens, and it throws on `!res.ok`. Components dispatch and render; tests stub `EventSource` and `fetch` globals.

**Tech Stack:** React 19, TypeScript strict, Vite 6, vitest + jsdom + @testing-library/react + user-event + jest-dom (dev-only). No runtime deps added.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-07-31-react-triage-hardening-design.md`.
- No new runtime dependencies; test stack is devDependencies only. No msw — `vi.fn` fetch stubs.
- No visual redesign; keep existing markup/classes except the new error banners.
- Backend untouched.
- Explicit vitest imports (`import { describe, it, expect, vi } from "vitest"`), no globals mode — keeps `tsc -b` clean without tsconfig type shims.
- Commits attributed to the user only — no Claude/Anthropic trailers.
- `useStream()`'s return shape must not change (App.tsx and components depend on it).

---

### Task 1: Test foundation + streamReducer

**Files:**
- Modify: `frontend/package.json` (devDeps via pnpm, `"test": "vitest run"` script)
- Modify: `frontend/vite.config.ts` (vitest `test` block)
- Create: `frontend/src/test/setup.ts`
- Modify: `frontend/src/lib/useStream.ts` (extract reducer, useReducer, drop ref; `setSimulation` moves out in Task 2)
- Create: `frontend/src/lib/useStream.test.ts`
- Modify: `Makefile` (`test-web` target, `.PHONY`)

**Interfaces:**
- Produces: `streamReducer(s: StreamState, a: StreamAction): StreamState`, `initialStreamState: StreamState`, `type StreamAction = {type:"open"} | {type:"error"} | {type:"message"; data: StreamMessage}`, `interface StreamMessage { stats: Stats; recent: Verdict[] | null; sim: SimState; cases: CaseCounts }` — all exported from `useStream.ts`. Existing exported types (`Stats`, `Verdict`, `SimState`, `CaseCounts`, `StreamState`, `Decision`) unchanged.

- [ ] **Step 1: Install the test stack**

```bash
pnpm --filter lambari-dashboard add -D vitest jsdom @testing-library/react @testing-library/user-event @testing-library/jest-dom
```

- [ ] **Step 2: Wire vitest**

`frontend/src/test/setup.ts`:

```ts
import "@testing-library/jest-dom/vitest";
```

`frontend/vite.config.ts` — add at top `/// <reference types="vitest/config" />` and inside `defineConfig({...})`:

```ts
  test: {
    environment: "jsdom",
    setupFiles: "./src/test/setup.ts",
  },
```

`frontend/package.json` scripts: add `"test": "vitest run"`.

`Makefile`: `.PHONY` gains ` test-web`; after `test:`

```makefile
test-web:         ## run frontend (vitest) tests
	pnpm --filter lambari-dashboard test
```

- [ ] **Step 3: Write the failing reducer tests** — `frontend/src/lib/useStream.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import {
  initialStreamState,
  streamReducer,
  type StreamMessage,
} from "./useStream";

function msg(rate: number): StreamMessage {
  return {
    stats: {
      processed: 1, approved: 1, reviewed: 0, declined: 0,
      rate_per_sec: rate, p50_us: 10, p99_us: 20,
      queue_depth: 0, queue_cap: 16384, uptime_sec: 1,
      rule_fires: {}, flagged_rate: 0,
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
```

- [ ] **Step 4: Run to verify failure**

Run: `pnpm --filter lambari-dashboard test`
Expected: FAIL — `streamReducer` is not exported.

- [ ] **Step 5: Refactor `useStream.ts`** — replace the `useState` + ref internals (keep all existing type exports and the imports section; `setSimulation` stays put until Task 2):

```ts
export interface StreamMessage {
  stats: Stats;
  recent: Verdict[] | null;
  sim: SimState;
  cases: CaseCounts;
}

export type StreamAction =
  | { type: "open" }
  | { type: "error" }
  | { type: "message"; data: StreamMessage };

export const initialStreamState: StreamState = {
  connected: false,
  stats: null,
  recent: [],
  sim: { running: false, rate: 0 },
  cases: { open: 0, confirmed_fraud: 0, false_positive: 0 },
  rateHistory: [],
};

const MAX_HISTORY = 90;

export function streamReducer(s: StreamState, a: StreamAction): StreamState {
  switch (a.type) {
    case "open":
      return { ...s, connected: true };
    case "error":
      return { ...s, connected: false };
    case "message":
      return {
        connected: true,
        stats: a.data.stats,
        recent: a.data.recent ?? [],
        sim: a.data.sim,
        cases: a.data.cases,
        rateHistory: [...s.rateHistory, a.data.stats.rate_per_sec].slice(-MAX_HISTORY),
      };
  }
}

export function useStream(): StreamState {
  const [state, dispatch] = useReducer(streamReducer, initialStreamState);

  useEffect(() => {
    const es = new EventSource("/api/stream");
    es.onopen = () => dispatch({ type: "open" });
    es.onerror = () => dispatch({ type: "error" });
    es.onmessage = (ev) =>
      dispatch({ type: "message", data: JSON.parse(ev.data) as StreamMessage });
    return () => es.close();
  }, []);

  return state;
}
```

(Import becomes `import { useEffect, useReducer } from "react";`.)

- [ ] **Step 6: Run tests + typecheck**

Run: `pnpm --filter lambari-dashboard test && pnpm --filter lambari-dashboard build`
Expected: 3 tests PASS; build clean.

---

### Task 2: Data layer `lib/api.ts`

**Files:**
- Create: `frontend/src/lib/api.ts`
- Create: `frontend/src/lib/api.test.ts`
- Modify: `frontend/src/lib/useStream.ts` (delete `setSimulation`)
- Modify: `frontend/src/components/SimControl.tsx` (import `setSimulation` from `../lib/api` — behavior change comes in Task 5)
- Modify: `frontend/src/components/ReviewQueue.tsx` (import `Case` type from `../lib/api`, delete local interface — behavior change comes in Task 4)

**Interfaces:**
- Consumes: `Verdict` from `./useStream`.
- Produces (all from `lib/api.ts`): `interface Case { id: string; verdict: Verdict; status: "open" | "resolved"; opened_at: number }`, `type Resolution = "confirmed_fraud" | "false_positive"`, `fetchCases(signal?: AbortSignal): Promise<Case[]>`, `resolveCase(id: string, resolution: Resolution): Promise<void>`, `setSimulation(rate: number): Promise<void>`. All throw `Error` on `!res.ok`.

- [ ] **Step 1: Write the failing tests** — `frontend/src/lib/api.test.ts`:

```ts
import { afterEach, describe, expect, it, vi } from "vitest";
import { fetchCases, resolveCase, setSimulation } from "./api";

const ok = (body: unknown) =>
  new Response(JSON.stringify(body), { status: 200 });
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
```

- [ ] **Step 2: Run to verify failure**

Run: `pnpm --filter lambari-dashboard test`
Expected: FAIL — `./api` does not exist.

- [ ] **Step 3: Implement `frontend/src/lib/api.ts`**

```ts
// The only file that talks HTTP. Every function throws on a non-2xx
// response — callers decide how to surface it, nothing gets swallowed.
import type { Verdict } from "./useStream";

export interface Case {
  id: string;
  verdict: Verdict;
  status: "open" | "resolved";
  opened_at: number;
}

export type Resolution = "confirmed_fraud" | "false_positive";

function ensureOk(res: Response, what: string): Response {
  if (!res.ok) throw new Error(`${what}: HTTP ${res.status}`);
  return res;
}

const post = (url: string, body: unknown, what: string) =>
  fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  }).then((res) => void ensureOk(res, what));

export async function fetchCases(signal?: AbortSignal): Promise<Case[]> {
  const res = ensureOk(await fetch("/api/cases", { signal }), "load cases");
  const data = (await res.json()) as { cases: Case[] | null };
  return data.cases ?? [];
}

export const resolveCase = (id: string, resolution: Resolution) =>
  post(`/api/cases/${id}/resolve`, { resolution }, `resolve ${id}`);

export const setSimulation = (rate: number) =>
  post("/api/simulate", { rate }, "set simulation");
```

- [ ] **Step 4: Move imports** — delete `setSimulation` from `useStream.ts`; in `SimControl.tsx` change to `import { setSimulation } from "../lib/api"; import type { SimState } from "../lib/useStream";`; in `ReviewQueue.tsx` delete the local `Case` interface and add `import { fetchCases, resolveCase, type Case } from "../lib/api";` (the fetch-call rewiring itself is Task 4 — for now just keep it compiling by leaving existing fetch calls in place and the unused imports out until used; simplest: only move the `Case` type now).

- [ ] **Step 5: Run tests + typecheck**

Run: `pnpm --filter lambari-dashboard test && pnpm --filter lambari-dashboard build`
Expected: all PASS, build clean.

---

### Task 3: `casesReducer`

**Files:**
- Create: `frontend/src/lib/casesReducer.ts`
- Create: `frontend/src/lib/casesReducer.test.ts`

**Interfaces:**
- Consumes: `Case` from `./api`.
- Produces: `interface QueueState { cases: Case[]; busy: string | null; pending: { c: Case; index: number } | null; error: string | null }`, `type QueueAction = {type:"loaded"; cases: Case[]} | {type:"loadFailed"} | {type:"resolveStart"; id: string} | {type:"resolveOk"} | {type:"resolveFail"}`, `initialQueueState: QueueState`, `casesReducer(s, a): QueueState`.

- [ ] **Step 1: Write the failing tests** — `frontend/src/lib/casesReducer.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import type { Case } from "./api";
import { casesReducer, initialQueueState } from "./casesReducer";

const mk = (id: string): Case => ({
  id,
  verdict: {
    tx_id: id, card_bin: "520082", amount: 100, currency: "SEK",
    country: "SE", score: 50, decision: "review", latency_us: 10, at: 0,
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

  it("loaded during an in-flight resolve keeps the optimistic removal", () => {
    let s = loaded(["a", "b"]);
    s = casesReducer(s, { type: "resolveStart", id: "a" });
    // the refetch raced the resolve: server still lists "a"
    s = casesReducer(s, { type: "loaded", cases: [mk("a"), mk("b")] });
    expect(s.cases.map((c) => c.id)).toEqual(["b"]);
  });
});
```

- [ ] **Step 2: Run to verify failure**

Run: `pnpm --filter lambari-dashboard test`
Expected: FAIL — `./casesReducer` does not exist.

- [ ] **Step 3: Implement `frontend/src/lib/casesReducer.ts`**

```ts
// Pure state machine for the review queue: optimistic resolve with rollback.
// Kept free of fetch/React so it can be tested as data-in data-out.
import type { Case } from "./api";

export interface QueueState {
  cases: Case[];
  /** case id with a resolve in flight (one at a time) */
  busy: string | null;
  /** the optimistically removed case, kept for rollback */
  pending: { c: Case; index: number } | null;
  error: string | null;
}

export type QueueAction =
  | { type: "loaded"; cases: Case[] }
  | { type: "loadFailed" }
  | { type: "resolveStart"; id: string }
  | { type: "resolveOk" }
  | { type: "resolveFail" };

export const initialQueueState: QueueState = {
  cases: [],
  busy: null,
  pending: null,
  error: null,
};

// ponytail: single in-flight resolve; make pending a map if analysts need burst triage
export function casesReducer(s: QueueState, a: QueueAction): QueueState {
  switch (a.type) {
    case "loaded": {
      // a refetch can race an in-flight resolve and still list the case we
      // optimistically removed — keep it removed until resolveOk/Fail decides
      const cases = s.busy ? a.cases.filter((c) => c.id !== s.busy) : a.cases;
      return { ...s, cases, error: null };
    }
    case "loadFailed":
      return { ...s, error: "Couldn't refresh the queue — retrying on next update" };
    case "resolveStart": {
      const index = s.cases.findIndex((c) => c.id === a.id);
      if (index < 0 || s.busy) return s;
      return {
        cases: s.cases.filter((c) => c.id !== a.id),
        busy: a.id,
        pending: { c: s.cases[index], index },
        error: null,
      };
    }
    case "resolveOk":
      return { ...s, busy: null, pending: null };
    case "resolveFail": {
      if (!s.pending) return { ...s, busy: null };
      const cases = [...s.cases];
      cases.splice(Math.min(s.pending.index, cases.length), 0, s.pending.c);
      return {
        cases,
        busy: null,
        pending: null,
        error: `Couldn't resolve ${s.pending.c.id} — try again`,
      };
    }
  }
}
```

- [ ] **Step 4: Run tests**

Run: `pnpm --filter lambari-dashboard test`
Expected: all PASS.

---

### Task 4: ReviewQueue rework + interaction tests

**Files:**
- Modify: `frontend/src/components/ReviewQueue.tsx`
- Create: `frontend/src/components/ReviewQueue.test.tsx`

**Interfaces:**
- Consumes: `fetchCases`, `resolveCase`, `Case`, `Resolution` from `../lib/api`; `casesReducer`, `initialQueueState` from `../lib/casesReducer`; `CaseCounts` from `../lib/useStream`.
- Produces: same component signature `ReviewQueue({ counts }: { counts: CaseCounts })`.

- [ ] **Step 1: Write the failing interaction tests** — `frontend/src/components/ReviewQueue.test.tsx`:

```tsx
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ReviewQueue } from "./ReviewQueue";

const counts = { open: 1, confirmed_fraud: 0, false_positive: 0 };
const caseBody = {
  cases: [
    {
      id: "tx_1",
      verdict: {
        tx_id: "tx_1", card_bin: "520082", amount: 123.45, currency: "SEK",
        country: "SE", score: 55, flags: ["amount_high"], decision: "review",
        latency_us: 10, at: 0,
      },
      status: "open",
      opened_at: 0,
    },
  ],
};

const ok = (body: unknown) => new Response(JSON.stringify(body), { status: 200 });

function stubFetch(resolveStatus: number) {
  const f = vi.fn(async (input: RequestInfo | URL) => {
    const url = String(input);
    if (url.endsWith("/resolve")) return new Response("{}", { status: resolveStatus });
    return ok(caseBody);
  });
  vi.stubGlobal("fetch", f);
  return f;
}

afterEach(() => vi.unstubAllGlobals());

describe("ReviewQueue", () => {
  it("loads via the push signal and resolves optimistically", async () => {
    stubFetch(200);
    render(<ReviewQueue counts={counts} />);
    await screen.findByText("123.45 SEK");

    await userEvent.click(screen.getByRole("button", { name: /confirm fraud/i }));
    await waitFor(() =>
      expect(screen.queryByText("123.45 SEK")).not.toBeInTheDocument(),
    );
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });

  it("rolls back and shows an error when the resolve fails", async () => {
    stubFetch(500);
    render(<ReviewQueue counts={counts} />);
    await screen.findByText("123.45 SEK");

    await userEvent.click(screen.getByRole("button", { name: /clear/i }));
    await screen.findByRole("alert");
    expect(screen.getByText("123.45 SEK")).toBeInTheDocument();
    expect(screen.getByRole("alert").textContent).toContain("tx_1");
  });
});
```

- [ ] **Step 2: Run to verify failure**

Run: `pnpm --filter lambari-dashboard test`
Expected: the rollback test FAILS (current component fakes success), and the optimistic test may pass incidentally — that asymmetry is the bug being fixed.

- [ ] **Step 3: Rework the component** — replace `ReviewQueue.tsx`'s state and data logic (keep the JSX row markup as-is):

```tsx
import { useEffect, useReducer } from "react";
import { AnimatePresence, motion } from "motion/react";
import { fetchCases, resolveCase, type Resolution } from "../lib/api";
import { casesReducer, initialQueueState } from "../lib/casesReducer";
import type { CaseCounts } from "../lib/useStream";

const VISIBLE = 8;

export function ReviewQueue({ counts }: { counts: CaseCounts }) {
  const [state, dispatch] = useReducer(casesReducer, initialQueueState);

  // Push-driven: the SSE stream updates `counts` every second; a change in
  // the counts is the refetch signal. No polling. AbortController keeps a
  // stale response from clobbering a newer one.
  useEffect(() => {
    const ctl = new AbortController();
    fetchCases(ctl.signal)
      .then((cases) => dispatch({ type: "loaded", cases: cases.slice(0, VISIBLE) }))
      .catch((err) => {
        if (ctl.signal.aborted) return;
        console.error("case refresh failed", err);
        dispatch({ type: "loadFailed" });
      });
    return () => ctl.abort();
  }, [counts.open, counts.confirmed_fraud, counts.false_positive]);

  const resolve = async (id: string, resolution: Resolution) => {
    dispatch({ type: "resolveStart", id });
    try {
      await resolveCase(id, resolution);
      dispatch({ type: "resolveOk" });
    } catch (err) {
      console.error("resolve failed", err);
      dispatch({ type: "resolveFail" });
    }
  };
  // ...
```

In the JSX: `cases.map` becomes `state.cases.map`; both buttons get `disabled={state.busy !== null}`; empty-state condition becomes `state.cases.length === 0`; and directly under the header row add:

```tsx
      {state.error && (
        <p
          role="alert"
          className="mt-3 rounded border border-decline/40 bg-decline/10 px-3 py-2 text-xs text-decline"
        >
          {state.error}
        </p>
      )}
```

- [ ] **Step 4: Run tests + typecheck**

Run: `pnpm --filter lambari-dashboard test && pnpm --filter lambari-dashboard build`
Expected: all PASS, build clean.

---

### Task 5: SimControl revert-on-failure + test

**Files:**
- Modify: `frontend/src/components/SimControl.tsx`
- Create: `frontend/src/components/SimControl.test.tsx`

**Interfaces:**
- Consumes: `setSimulation` from `../lib/api`; `SimState` from `../lib/useStream`. Component signature unchanged.

- [ ] **Step 1: Write the failing test** — `frontend/src/components/SimControl.test.tsx`:

```tsx
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SimControl } from "./SimControl";

afterEach(() => vi.unstubAllGlobals());

describe("SimControl", () => {
  it("reverts the switch and shows an error when the POST fails", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => new Response("boom", { status: 500 })));
    render(<SimControl sim={{ running: false, rate: 0 }} />);

    const sw = screen.getByRole("switch", { name: /simulator/i });
    await userEvent.click(sw);

    await screen.findByRole("alert");
    expect(sw).toHaveAttribute("aria-checked", "false");
  });

  it("keeps the switch on when the POST succeeds", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => new Response("{}", { status: 200 })));
    render(<SimControl sim={{ running: false, rate: 0 }} />);

    const sw = screen.getByRole("switch", { name: /simulator/i });
    await userEvent.click(sw);
    expect(sw).toHaveAttribute("aria-checked", "true");
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });
});
```

- [ ] **Step 2: Run to verify failure**

Run: `pnpm --filter lambari-dashboard test`
Expected: revert test FAILS (switch stays checked, no alert).

- [ ] **Step 3: Implement** — in `SimControl.tsx` add `const [error, setError] = useState(false);` and replace the two handlers:

```tsx
  const toggle = async (on: boolean) => {
    setRunning(on);
    setError(false);
    try {
      await setSimulation(on ? rate : 0);
    } catch (err) {
      console.error("simulator update failed", err);
      setRunning(sim.running); // revert to server-reported truth
      setError(true);
    }
  };

  const changeRate = async (v: number) => {
    setRate(v);
    if (!running) return;
    setError(false);
    try {
      await setSimulation(v);
    } catch (err) {
      console.error("simulator update failed", err);
      if (sim.running && sim.rate > 0) setRate(sim.rate);
      setError(true);
    }
  };
```

And next to the switch label:

```tsx
      {error && (
        <span role="alert" className="text-xs text-decline">
          sim update failed
        </span>
      )}
```

- [ ] **Step 4: Run tests + typecheck**

Run: `pnpm --filter lambari-dashboard test && pnpm --filter lambari-dashboard build`
Expected: all PASS, build clean.

---

### Task 6: Verify whole suite, smoke the app, ship

**Files:**
- Commit: everything from Tasks 1-5 + this plan.

- [ ] **Step 1: Full verification**

Run: `pnpm --filter lambari-dashboard test && pnpm --filter lambari-dashboard build && cd backend && go test ./...`
Expected: all frontend tests + build + backend tests green.

- [ ] **Step 2: Live smoke** — `make run-api` + `make run-web`, open :5173, start the simulator, resolve a case; then stop the api and confirm the queue shows the refresh error instead of silently freezing, and the sim toggle reverts with its error.

- [ ] **Step 3: One feature commit** (user attribution only):

```bash
git add frontend/ Makefile docs/superpowers/plans/2026-07-31-react-triage-hardening.md
git commit -m "Harden dashboard live-data paths; add React test suite

- Stream state via exported pure streamReducer (useReducer, ref dropped)
- Review queue is push-driven off SSE case counts — polling removed
- Optimistic resolve with rollback + inline error via pure casesReducer;
  failed resolves no longer fake success
- All fetches move to lib/api.ts and throw on non-2xx
- SimControl reverts to server state and surfaces failed sim updates
- vitest + testing-library foundation: reducer, api, and interaction tests
  (make test-web)"
```

- [ ] **Step 4: Memory update** — `lambari-open-gaps.md`: React live-data hardening done; remaining: metrics/observability (next), virtualized explorer (optional), K8s.
