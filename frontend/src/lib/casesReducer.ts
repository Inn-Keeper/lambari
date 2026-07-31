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
  | { type: "resolveFail" }
  /** the case vanished server-side (evicted or resolved elsewhere): don't
   *  roll back — reinstating it would show a ghost nobody can act on */
  | { type: "resolveGone" };

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
    case "resolveGone":
      return {
        ...s,
        busy: null,
        pending: null,
        error: s.pending
          ? `Case ${s.pending.c.id} was already resolved or evicted`
          : null,
      };
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
