import { useEffect, useReducer } from "react";
import { AnimatePresence, motion } from "motion/react";
import { fetchCases, HttpError, resolveCase, type Resolution } from "../lib/api";
import { casesReducer, initialQueueState } from "../lib/casesReducer";
import type { CaseCounts } from "../lib/useStream";

const VISIBLE = 8;

/**
 * The analyst's half of the pipeline: flagged transactions land here as
 * cases, sorted worst-score-first. Each resolution is stored as a label —
 * the raw material for a future ML rule.
 */
export function ReviewQueue({ counts, tick }: { counts: CaseCounts; tick: number }) {
  const [state, dispatch] = useReducer(casesReducer, initialQueueState);

  // Push-driven: `tick` (the stream's monotonic processed counter) bumps
  // once per SSE frame, so the queue refetches exactly when the engine
  // reports fresh work and goes quiet when the stream is down. Counts alone
  // are not enough — pinned at the eviction cap they stop changing while
  // the queue's contents keep churning. AbortController keeps a stale
  // response from clobbering a newer one.
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
  }, [tick]);

  const resolve = async (id: string, resolution: Resolution) => {
    dispatch({ type: "resolveStart", id });
    try {
      await resolveCase(id, resolution);
      dispatch({ type: "resolveOk" });
    } catch (err) {
      console.error("resolve failed", err);
      // 404 means the case no longer exists server-side — rolling back
      // would resurrect a row nobody can act on.
      if (err instanceof HttpError && err.status === 404) {
        dispatch({ type: "resolveGone" });
      } else {
        dispatch({ type: "resolveFail" });
      }
    }
  };

  return (
    <div className="rounded-lg border border-line bg-panel p-4">
      <div className="flex items-baseline justify-between">
        <div className="text-[11px] font-medium uppercase tracking-[0.14em] text-muted">
          Review queue
        </div>
        <div className="font-mono text-[11px] tabular-nums text-faint">
          {counts.open} open · {counts.confirmed_fraud} fraud ·{" "}
          {counts.false_positive} cleared
        </div>
      </div>

      {state.error && (
        <p
          role="alert"
          className="mt-3 rounded border border-decline/40 bg-decline/10 px-3 py-2 text-xs text-decline"
        >
          {state.error}
        </p>
      )}

      {state.cases.length === 0 ? (
        <p className="mt-4 text-sm text-faint">
          Queue is clear. New cases open automatically when the engine flags a
          transaction.
        </p>
      ) : (
        <div className="mt-3 space-y-2">
          <AnimatePresence initial={false}>
            {state.cases.map((c) => {
              const v = c.verdict;
              const isDecline = v.decision === "decline";
              return (
                <motion.div
                  key={c.id}
                  layout
                  initial={{ opacity: 0, y: -6 }}
                  animate={{ opacity: 1, y: 0 }}
                  exit={{ opacity: 0, x: 24 }}
                  transition={{ duration: 0.2 }}
                  className="flex flex-wrap items-center gap-x-3 gap-y-2 rounded-md border border-line bg-panel-2 px-3 py-2"
                >
                  <span
                    className={`font-mono text-sm font-semibold tabular-nums ${
                      isDecline ? "text-decline" : "text-review"
                    }`}
                  >
                    {v.score}
                  </span>
                  <span className="font-mono text-sm tabular-nums">
                    {v.amount.toFixed(2)} {v.currency}
                  </span>
                  <span className="font-mono text-xs text-muted">
                    {v.card_bin}·· {v.country}
                  </span>
                  <span className="min-w-0 flex-1 truncate text-xs text-faint">
                    {(v.flags ?? []).join(", ")}
                  </span>
                  <div className="flex gap-1.5">
                    <button
                      onClick={() => resolve(c.id, "confirmed_fraud")}
                      disabled={state.busy !== null}
                      className="rounded border border-decline/40 px-2 py-1 text-xs font-medium text-decline transition-colors hover:bg-decline/10 focus-visible:ring-2 focus-visible:ring-decline focus:outline-none disabled:opacity-50"
                    >
                      Confirm fraud
                    </button>
                    <button
                      onClick={() => resolve(c.id, "false_positive")}
                      disabled={state.busy !== null}
                      className="rounded border border-approve/40 px-2 py-1 text-xs font-medium text-approve transition-colors hover:bg-approve/10 focus-visible:ring-2 focus-visible:ring-approve focus:outline-none disabled:opacity-50"
                    >
                      Clear
                    </button>
                  </div>
                </motion.div>
              );
            })}
          </AnimatePresence>
        </div>
      )}
    </div>
  );
}
