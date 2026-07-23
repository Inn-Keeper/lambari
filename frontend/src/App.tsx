import { useStream } from "./lib/useStream";
import { StatCard } from "./components/StatCard";
import { ThroughputChart } from "./components/ThroughputChart";
import { DecisionSplit, RuleBreakdown } from "./components/Breakdown";
import { LiveFeed } from "./components/LiveFeed";
import { SimControl } from "./components/SimControl";
import { ReviewQueue } from "./components/ReviewQueue";

function fmtUptime(sec: number): string {
  const m = Math.floor(sec / 60);
  const s = sec % 60;
  return m > 0 ? `${m}m ${s}s` : `${s}s`;
}

export default function App() {
  const { connected, stats, recent, sim, cases, rateHistory } = useStream();

  return (
    <div className="mx-auto max-w-6xl px-4 py-6">
      <header className="mb-6 flex flex-wrap items-center justify-between gap-4">
        <div className="flex items-center gap-3">
          <h1 className="font-display text-xl font-bold tracking-tight">
            TRIPWIRE
          </h1>
          <span
            className={`flex items-center gap-1.5 rounded-full border border-line px-2.5 py-0.5 text-xs ${
              connected ? "text-approve" : "text-decline"
            }`}
          >
            <i
              className={`h-1.5 w-1.5 rounded-full ${
                connected ? "animate-pulse bg-approve" : "bg-decline"
              }`}
            />
            {connected ? "engine live" : "disconnected"}
          </span>
          {stats && (
            <span className="hidden text-xs text-faint sm:block">
              up {fmtUptime(stats.uptime_sec)}
            </span>
          )}
        </div>
        <SimControl sim={sim} />
      </header>

      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <StatCard
          label="Throughput"
          value={`${(stats?.rate_per_sec ?? 0).toLocaleString()}/s`}
          hint="transactions scored per second"
        />
        <StatCard
          label="Processed"
          value={(stats?.processed ?? 0).toLocaleString()}
          hint="since engine start"
        />
        <StatCard
          label="Flagged"
          value={`${((stats?.flagged_rate ?? 0) * 100).toFixed(1)}%`}
          hint="review + decline"
          tone={(stats?.flagged_rate ?? 0) > 0.15 ? "warn" : "default"}
        />
        <StatCard
          label="p99 latency"
          value={`${(stats?.p99_us ?? 0).toLocaleString()}µs`}
          hint={`queue ${stats?.queue_depth ?? 0}/${stats?.queue_cap ?? 0}`}
        />
      </div>

      <div className="mt-3 grid gap-3 lg:grid-cols-3">
        <div className="lg:col-span-2">
          <ThroughputChart history={rateHistory} current={stats?.rate_per_sec ?? 0} />
        </div>
        <div className="space-y-3">
          {stats && <DecisionSplit stats={stats} />}
        </div>
      </div>

      <div className="mt-3">
        <ReviewQueue counts={cases} />
      </div>

      <div className="mt-3 grid gap-3 lg:grid-cols-3">
        <div className="lg:col-span-2">
          <LiveFeed verdicts={recent} />
        </div>
        <div>{stats && <RuleBreakdown stats={stats} />}</div>
      </div>

      <footer className="mt-6 text-center text-xs text-faint">
        Tripwire · Go scoring engine · Kafka-ready pipeline · proof of concept
      </footer>
    </div>
  );
}
