# React case-triage hardening + test foundation — design

Date: 2026-07-31
Status: approved

## Goal

Make the dashboard's live-data paths honest and testable — the senior-React
interview topics in one iteration: reducer-based state modeling, push-driven
effects (no polling next to a push channel), optimistic mutations with
rollback, and a real test suite. No visual redesign, no new runtime
dependencies.

## Current warts this removes

1. Zero frontend tests (the Go side has 11; React has none).
2. `ReviewQueue` polls `/api/cases` every 2.5 s while an SSE channel is
   already pushing case counts every second.
3. `ReviewQueue.resolve` fakes success: it removes the case even when the
   POST failed (no `res.ok` check) — an analyst's action lost silently.
4. `SimControl.toggle` flips the switch optimistically and `setSimulation`
   never checks the response; a failed POST leaves the UI lying (until SSE
   happens to correct it; forever if the network is down) with no feedback.
5. Stream state is a single `useState` blob plus a mutable ref.

## Design

### 1. Stream state as a reducer

`useStream`'s state moves to a pure, exported
`streamReducer(state: StreamState, action: StreamAction)` with actions
`{type:"open"} | {type:"error"} | {type:"message", data}`. The rate-history
ring (cap 90) becomes reducer state; the `useRef` goes away. The hook's
return shape and every consuming component are unchanged.

### 2. Push-driven queue

`ReviewQueue` drops its `setInterval`. The push signal is the SSE frame
itself — App passes `stats.processed` (monotonic, bumps once per frame) as a
`tick` prop, and the queue refetches `/api/cases` on each tick, with an
`AbortController` cancelling the in-flight request so stale responses can't
clobber newer ones. The polling path is deleted as an orphan of this change.

*Revised during smoke testing:* the original signal (the `cases` counts) has
a blind spot — with the open count pinned at the server's 200-case eviction
cap and nothing resolving, counts never change while the queue's contents
keep churning, so the list froze and resolves 404'd against evicted cases.
The frame tick has no such blind spot and still goes quiet when the stream
is down.

### 3. Honest mutations: optimistic with rollback

New data layer `frontend/src/lib/api.ts` — every mutation/fetch the app
makes, all throwing on `!res.ok`:

- `fetchCases(signal): Promise<Case[]>`
- `resolveCase(id, resolution): Promise<void>`
- `setSimulation(rate): Promise<void>` (moves out of `useStream.ts`)
- The `Case` type moves here from `ReviewQueue.tsx`.

A pure, exported `casesReducer` models the queue with actions
`loaded | resolveStart | resolveOk | resolveFail`:

- `resolveStart` removes the case optimistically (keeping it in a pending
  slot) and marks it busy.
- `resolveOk` discards the pending case.
- `resolveFail` reinstates it at its original index and sets an error
  (`"Couldn't resolve <id> — try again"`) rendered as an inline banner in
  the panel; the banner clears on the next successful action or refetch.
- `resolveGone` (added during smoke testing): a 404 means the case no longer
  exists server-side — evicted or resolved elsewhere. Rolling back would
  resurrect a row nobody can act on, so the row stays gone and the banner
  says `"Case <id> was already resolved or evicted"`. `lib/api.ts` throws a
  typed `HttpError` carrying `status` so the component can tell 404 apart.

### 4. SimControl: revert on failure

`setSimulation` now lives in `lib/api.ts` and throws on failure.
`SimControl.toggle`/`changeRate` catch, revert local state to
the server-reported `sim` prop, and show a brief inline error next to the
switch. No retry machinery — the SSE stream keeps re-syncing state anyway.

### 5. Test foundation

Dev-only additions: `vitest`, `jsdom`, `@testing-library/react`,
`@testing-library/user-event`, `@testing-library/jest-dom` — the standard
Vite-native stack, zero runtime dependencies. Fetch is stubbed with
`vi.fn`; no msw.

Tests:

- `streamReducer`: open/error/message transitions; history capped at 90.
- `casesReducer`: optimistic removal, rollback restores original order,
  error set on fail and cleared on next load.
- `useStream` with a mocked `EventSource`: connects, applies a message,
  flips `connected` on error.
- `ReviewQueue` interactions: rows render; resolve click removes the row;
  failing POST brings the row back and shows the error banner.
- `SimControl`: toggle with failing fetch reverts the switch and shows the
  error.

`package.json` gains a `test` script (`vitest run`); `Makefile` gains
`test-web`. Backend `make test` untouched.

## Out of scope

- List virtualization / verdict explorer (possible follow-up spec).
- Visual redesign of any component.
- Backend/API changes — the Go side is untouched.
