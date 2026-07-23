import { motion } from "motion/react";
import type { Stats } from "../lib/useStream";

const RULE_LABELS: Record<string, string> = {
  amount_extreme: "Extreme amount",
  amount_high: "High amount",
  card_velocity_extreme: "Card velocity (extreme)",
  card_velocity: "Card velocity",
  ip_fanout_extreme: "IP fan-out (extreme)",
  ip_fanout: "IP fan-out",
  geo_mismatch: "Geo mismatch",
  high_risk_mcc: "High-risk merchant",
};

export function DecisionSplit({ stats }: { stats: Stats }) {
  const total = Math.max(stats.processed, 1);
  const segments = [
    { label: "Approved", n: stats.approved, color: "var(--color-approve)" },
    { label: "Review", n: stats.reviewed, color: "var(--color-review)" },
    { label: "Declined", n: stats.declined, color: "var(--color-decline)" },
  ];

  return (
    <div className="rounded-lg border border-line bg-panel p-4">
      <div className="text-[11px] font-medium uppercase tracking-[0.14em] text-muted">
        Decisions
      </div>
      <div className="mt-3 flex h-2.5 w-full overflow-hidden rounded-full bg-panel-2">
        {segments.map((s) => (
          <motion.div
            key={s.label}
            animate={{ width: `${(s.n / total) * 100}%` }}
            transition={{ duration: 0.4 }}
            style={{ background: s.color }}
          />
        ))}
      </div>
      <div className="mt-3 space-y-1.5">
        {segments.map((s) => (
          <div key={s.label} className="flex items-center justify-between text-sm">
            <span className="flex items-center gap-2 text-muted">
              <i
                className="inline-block h-2 w-2 rounded-full"
                style={{ background: s.color }}
              />
              {s.label}
            </span>
            <span className="font-mono tabular-nums">
              {s.n.toLocaleString()}
              <span className="ml-2 text-faint">
                {((s.n / total) * 100).toFixed(1)}%
              </span>
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

export function RuleBreakdown({ stats }: { stats: Stats }) {
  const entries = Object.entries(stats.rule_fires ?? {}).sort((a, b) => b[1] - a[1]);
  const max = Math.max(1, ...entries.map(([, n]) => n));

  return (
    <div className="rounded-lg border border-line bg-panel p-4">
      <div className="text-[11px] font-medium uppercase tracking-[0.14em] text-muted">
        Rule fires
      </div>
      {entries.length === 0 ? (
        <p className="mt-3 text-sm text-faint">
          No rules have fired yet. Start the simulator to send traffic through the engine.
        </p>
      ) : (
        <div className="mt-3 space-y-2">
          {entries.map(([rule, n]) => (
            <div key={rule}>
              <div className="flex justify-between text-xs">
                <span className="text-muted">{RULE_LABELS[rule] ?? rule}</span>
                <span className="font-mono tabular-nums text-ink">
                  {n.toLocaleString()}
                </span>
              </div>
              <div className="mt-1 h-1 w-full rounded-full bg-panel-2">
                <motion.div
                  animate={{ width: `${(n / max) * 100}%` }}
                  transition={{ duration: 0.4 }}
                  className="h-1 rounded-full bg-review"
                />
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
