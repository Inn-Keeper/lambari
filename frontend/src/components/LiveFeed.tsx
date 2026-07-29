import { AnimatePresence, motion } from "motion/react";
import type { Verdict } from "../lib/useStream";

const DECISION_STYLE: Record<string, { dot: string; text: string; label: string }> = {
  approve: { dot: "bg-approve", text: "text-approve", label: "APPROVE" },
  review: { dot: "bg-review", text: "text-review", label: "REVIEW" },
  decline: { dot: "bg-decline", text: "text-decline", label: "DECLINE" },
};

/**
 * A signal wire runs down the left edge; each verdict is a node
 * on the wire. Flagged transactions light up amber or red as they land.
 */
export function LiveFeed({ verdicts }: { verdicts: Verdict[] }) {
  const shown = verdicts.slice(0, 14);

  return (
    <div className="rounded-lg border border-line bg-panel p-4">
      <div className="flex items-baseline justify-between">
        <div className="text-[11px] font-medium uppercase tracking-[0.14em] text-muted">
          Live verdicts
        </div>
        <div className="text-[11px] text-faint">
          flagged transactions + sampled approvals
        </div>
      </div>

      {shown.length === 0 ? (
        <p className="mt-4 text-sm text-faint">
          Waiting for traffic. Start the simulator or run the load generator.
        </p>
      ) : (
        <div className="relative mt-3 pl-5">
          {/* the wire */}
          <div className="absolute bottom-1 left-[5px] top-1 w-px bg-line" aria-hidden />
          <AnimatePresence initial={false}>
            {shown.map((v) => {
              const s = DECISION_STYLE[v.decision];
              return (
                <motion.div
                  key={v.tx_id}
                  layout
                  initial={{ opacity: 0, x: -8 }}
                  animate={{ opacity: 1, x: 0 }}
                  exit={{ opacity: 0 }}
                  transition={{ duration: 0.25 }}
                  className="relative flex items-center gap-3 py-1.5 font-mono text-[13px]"
                >
                  {/* node on the wire */}
                  <span
                    className={`absolute -left-5 top-1/2 h-[9px] w-[9px] -translate-y-1/2 rounded-full ring-4 ring-panel ${s.dot}`}
                    aria-hidden
                  />
                  <span className={`w-[72px] font-semibold ${s.text}`}>{s.label}</span>
                  <span className="w-[64px] tabular-nums text-faint">
                    score {v.score}
                  </span>
                  <span className="w-[110px] tabular-nums">
                    {v.amount.toFixed(2)} {v.currency}
                  </span>
                  <span className="w-[64px] text-muted">
                    {v.card_bin}·· {v.country}
                  </span>
                  <span className="hidden flex-1 truncate text-faint sm:block">
                    {(v.flags ?? []).join(", ") || "clean"}
                  </span>
                  <span className="hidden w-[70px] text-right tabular-nums text-faint md:block">
                    {v.latency_us}µs
                  </span>
                </motion.div>
              );
            })}
          </AnimatePresence>
        </div>
      )}
    </div>
  );
}
