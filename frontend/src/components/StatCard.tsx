import { motion } from "motion/react";

interface Props {
  label: string;
  value: string;
  hint?: string;
  tone?: "default" | "warn";
}

export function StatCard({ label, value, hint, tone = "default" }: Props) {
  return (
    <div className="rounded-lg border border-line bg-panel px-4 py-3">
      <div className="text-[11px] font-medium uppercase tracking-[0.14em] text-muted">
        {label}
      </div>
      <motion.div
        key={value}
        initial={{ opacity: 0.4 }}
        animate={{ opacity: 1 }}
        transition={{ duration: 0.25 }}
        className={`mt-1 font-mono text-2xl font-semibold tabular-nums ${
          tone === "warn" ? "text-review" : "text-ink"
        }`}
      >
        {value}
      </motion.div>
      {hint && <div className="mt-0.5 text-xs text-faint">{hint}</div>}
    </div>
  );
}
