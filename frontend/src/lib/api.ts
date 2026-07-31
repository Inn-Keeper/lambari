// The only file that talks HTTP. Every function throws on a non-2xx
// response — callers decide how to surface it, nothing gets swallowed.
import type { Verdict } from "./useStream";

export interface Case {
  id: string;
  verdict: Verdict;
  status: "open" | "resolved";
  opened_at: number;
}

export type Resolution = "confirmed_fraud" | "false_positive";

export class HttpError extends Error {
  constructor(
    what: string,
    public readonly status: number,
  ) {
    super(`${what}: HTTP ${status}`);
  }
}

function ensureOk(res: Response, what: string): Response {
  if (!res.ok) throw new HttpError(what, res.status);
  return res;
}

const post = (url: string, body: unknown, what: string) =>
  fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  }).then((res) => void ensureOk(res, what));

export async function fetchCases(signal?: AbortSignal): Promise<Case[]> {
  const res = ensureOk(await fetch("/api/cases", { signal }), "load cases");
  const data = (await res.json()) as { cases: Case[] | null };
  return data.cases ?? [];
}

export const resolveCase = (id: string, resolution: Resolution): Promise<void> =>
  post(`/api/cases/${id}/resolve`, { resolution }, `resolve ${id}`);

export const setSimulation = (rate: number): Promise<void> =>
  post("/api/simulate", { rate }, "set simulation");
