import { useEffect, useRef, useState } from "react";

export type Decision = "approve" | "review" | "decline";

export interface Stats {
  processed: number;
  approved: number;
  reviewed: number;
  declined: number;
  rate_per_sec: number;
  p50_us: number;
  p99_us: number;
  queue_depth: number;
  queue_cap: number;
  uptime_sec: number;
  rule_fires: Record<string, number>;
  flagged_rate: number;
}

export interface Verdict {
  tx_id: string;
  card_bin: string;
  amount: number;
  currency: string;
  country: string;
  score: number;
  flags?: string[];
  decision: Decision;
  latency_us: number;
  at: number;
}

export interface SimState {
  running: boolean;
  rate: number;
}

export interface CaseCounts {
  open: number;
  confirmed_fraud: number;
  false_positive: number;
}

export interface StreamState {
  connected: boolean;
  stats: Stats | null;
  recent: Verdict[];
  sim: SimState;
  cases: CaseCounts;
  /** last ~90 rate samples for the throughput chart */
  rateHistory: number[];
}

const MAX_HISTORY = 90;

export function useStream(): StreamState {
  const [state, setState] = useState<StreamState>({
    connected: false,
    stats: null,
    recent: [],
    sim: { running: false, rate: 0 },
    cases: { open: 0, confirmed_fraud: 0, false_positive: 0 },
    rateHistory: [],
  });
  const history = useRef<number[]>([]);

  useEffect(() => {
    const es = new EventSource("/api/stream");

    es.onopen = () => setState((s) => ({ ...s, connected: true }));
    es.onerror = () => setState((s) => ({ ...s, connected: false }));
    es.onmessage = (ev) => {
      const data = JSON.parse(ev.data) as {
        stats: Stats;
        recent: Verdict[] | null;
        sim: SimState;
        cases: CaseCounts;
      };
      const h = history.current;
      h.push(data.stats.rate_per_sec);
      if (h.length > MAX_HISTORY) h.shift();
      setState({
        connected: true,
        stats: data.stats,
        recent: data.recent ?? [],
        sim: data.sim,
        cases: data.cases,
        rateHistory: [...h],
      });
    };
    return () => es.close();
  }, []);

  return state;
}

export async function setSimulation(rate: number): Promise<void> {
  await fetch("/api/simulate", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ rate }),
  });
}
