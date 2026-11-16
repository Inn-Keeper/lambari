import { useEffect, useState } from "react";
import { useStream } from "./lib/useStream";
import { formatLatencyBound, formatShare } from "./lib/format";
import { StatCard } from "./components/StatCard";
import { ThroughputChart } from "./components/ThroughputChart";
import { DecisionSplit, RuleBreakdown } from "./components/Breakdown";
import { LiveFeed } from "./components/LiveFeed";
import { SimControl } from "./components/SimControl";
import { ReviewQueue } from "./components/ReviewQueue";
import { FraudInfo } from "./components/FraudInfo";

function fmtUptime(sec: number): string {
  const m = Math.floor(sec / 60);
  const s = sec % 60;
  return m > 0 ? `${m}m ${s}s` : `${s}s`;
}

// How long a cold engine may take to answer before we call it down.
const STARTING_MS = 20_000;

const STATUS = {
  live: { text: "text-approve", dot: "h-1.5 w-1.5 animate-pulse bg-approve", label: "engine live" },
  starting: {
    text: "text-review",
    dot: "h-2.5 w-2.5 animate-spin border-2 border-review border-t-transparent",
    label: "engine starting…",
  },
  down: { text: "text-decline", dot: "h-1.5 w-1.5 bg-decline", label: "disconnected" },
};

// EventSource retries on its own, so "not connected" starts out meaning
// "connecting": a cold engine takes a few seconds to answer. Past
// STARTING_MS we stop pretending and show it as down; a later connect
// still flips it to live.
function EngineStatus({ connected }: { connected: boolean }) {
  const [stalled, setStalled] = useState(false);
  useEffect(() => {
    if (connected) return setStalled(false);
    const t = setTimeout(() => setStalled(true), STARTING_MS);
    return () => clearTimeout(t);
  }, [connected]);

  const s = STATUS[connected ? "live" : stalled ? "down" : "starting"];
  return (
    <span
      role="status"
      className={`flex items-center gap-1.5 rounded-full border border-line px-2.5 py-0.5 text-xs ${s.text}`}
    >
      <i className={`rounded-full ${s.dot}`} />
      {s.label}
    </span>
  );
}

export default function App() {
  const { connected, stats, recent, sim, cases, queue, rateHistory } = useStream();

  return (
    <div className="mx-auto max-w-6xl px-4 py-6">
      <header className="mb-6 flex flex-wrap items-center justify-between gap-4">
        <div className="flex items-center gap-3">
          <img src="/lambari-mark.svg" alt="" className="h-8 w-8" />
          <h1 className="font-display text-xl font-bold tracking-tight">
            LAMBARI
          </h1>
          <EngineStatus connected={connected} />
          {stats && (
            <span className="hidden text-xs text-faint sm:block">
              up {fmtUptime(stats.uptime_sec)}
            </span>
          )}
        </div>
        <SimControl sim={sim} />
      </header>

      <div className="mb-3">
        <FraudInfo />
      </div>

      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <StatCard
          label="Throughput"
          value={`${(stats?.rate_per_sec ?? 0).toLocaleString()}/s`}
          hint={
            stats?.rejected
              ? `${stats.rejected.toLocaleString()} shed at ingest`
              : "transactions scored per second"
          }
          tone={stats?.rejected ? "warn" : "default"}
        />
        {/* Recall on the simulator's own labels; real traffic has none. */}
        <StatCard
          label="Fraud caught"
          value={formatShare(stats?.fraud_caught ?? 0, stats?.labeled_fraud ?? 0)}
          hint={`of simulated fraud · precision ${formatShare(
            stats?.fraud_caught ?? 0,
            (stats?.fraud_caught ?? 0) + (stats?.legit_flagged ?? 0),
          )}`}
        />
        <StatCard
          label="Flagged"
          value={`${((stats?.flagged_rate ?? 0) * 100).toFixed(1)}%`}
          hint="review + decline"
          tone={(stats?.flagged_rate ?? 0) > 0.15 ? "warn" : "default"}
        />
        <StatCard
          label="p99 latency"
          value={formatLatencyBound(stats?.p99_us)}
          hint={`queue ${stats?.queue_depth ?? 0}/${stats?.queue_cap ?? 0}`}
        />
      </div>

      <div className="mt-3 grid gap-3 lg:grid-cols-3">
        <div className="min-w-0 lg:col-span-2">
          <ThroughputChart history={rateHistory} current={stats?.rate_per_sec ?? 0} />
        </div>
        <div className="space-y-3">
          {stats && <DecisionSplit stats={stats} />}
        </div>
      </div>

      <div className="mt-3">
        <ReviewQueue counts={cases} queue={queue} />
      </div>

      <div className="mt-3 grid gap-3 lg:grid-cols-3">
        <div className="min-w-0 lg:col-span-2">
          <LiveFeed verdicts={recent} />
        </div>
        <div>{stats && <RuleBreakdown stats={stats} />}</div>
      </div>

      <footer className="mt-6 text-center text-xs text-faint">
        Lambari · Go scoring engine · Kafka-ready pipeline · proof of concept
      </footer>
    </div>
  );
}
