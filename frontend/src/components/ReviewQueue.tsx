import { useCallback, useEffect, useState } from "react";
import { AnimatePresence, motion } from "motion/react";
import type { CaseCounts, Verdict } from "../lib/useStream";

interface Case {
  id: string;
  verdict: Verdict;
  status: "open" | "resolved";
  opened_at: number;
}

/**
 * The analyst's half of the pipeline: flagged transactions land here as
 * cases, sorted worst-score-first. Each resolution is stored as a label —
 * the raw material for a future ML rule.
 */
export function ReviewQueue({ counts }: { counts: CaseCounts }) {
  const [cases, setCases] = useState<Case[]>([]);
  const [busy, setBusy] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    const res = await fetch("/api/cases");
    const data = (await res.json()) as { cases: Case[] };
    setCases(data.cases.slice(0, 8));
  }, []);

  useEffect(() => {
    refresh();
    const t = setInterval(refresh, 2500);
    return () => clearInterval(t);
  }, [refresh]);

  const resolve = async (id: string, resolution: "confirmed_fraud" | "false_positive") => {
    setBusy(id);
    await fetch(`/api/cases/${id}/resolve`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ resolution }),
    });
    setCases((cs) => cs.filter((c) => c.id !== id));
    setBusy(null);
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

      {cases.length === 0 ? (
        <p className="mt-4 text-sm text-faint">
          Queue is clear. New cases open automatically when the engine flags a
          transaction.
        </p>
      ) : (
        <div className="mt-3 space-y-2">
          <AnimatePresence initial={false}>
            {cases.map((c) => {
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
                      disabled={busy === c.id}
                      className="rounded border border-decline/40 px-2 py-1 text-xs font-medium text-decline transition-colors hover:bg-decline/10 focus-visible:ring-2 focus-visible:ring-decline focus:outline-none disabled:opacity-50"
                    >
                      Confirm fraud
                    </button>
                    <button
                      onClick={() => resolve(c.id, "false_positive")}
                      disabled={busy === c.id}
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
