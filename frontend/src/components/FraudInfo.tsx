// Explains what the engine treats as fraud. Values mirror
// backend/internal/engine/rules.go.
// ponytail: hand-copied, not served by the API; expose rules via /stats if they start changing often.
const RULES = [
  { flag: "amount_high / amount_extreme", when: "Amount ≥ 1,500 / ≥ 5,000", pts: "+20 / +45" },
  { flag: "card_velocity / _extreme", when: "Same card used ≥ 4 / ≥ 8 times in 60s", pts: "+25 / +50" },
  { flag: "ip_fanout / _extreme", when: "≥ 12 / ≥ 30 transactions from one IP in 5 min", pts: "+20 / +40" },
  { flag: "geo_mismatch", when: "Card's issuing country ≠ transaction country", pts: "+25" },
  { flag: "high_risk_mcc", when: "Merchant is gambling, crypto, direct marketing or wire transfer", pts: "+15" },
];

export function FraudInfo() {
  return (
    <details className="rounded-lg border border-line bg-panel px-4 py-3 text-sm">
      <summary className="cursor-pointer text-[11px] font-medium uppercase tracking-[0.14em] text-muted">
        What counts as fraud?
      </summary>
      <p className="mt-3 text-faint">
        Card fraud is a payment made without the cardholder's consent: stolen
        cards, card testing (many small tries to find valid numbers) or
        account takeover. Lambari can't know intent, so it scores signals that
        commonly accompany fraud. Each rule that fires adds points.
      </p>
      <div className="mt-3 overflow-x-auto">
        <table className="w-full text-left text-xs">
          <thead className="text-muted">
            <tr>
              <th className="py-1 pr-3 font-medium">Flag</th>
              <th className="py-1 pr-3 font-medium">Fires when</th>
              <th className="py-1 font-medium">Points</th>
            </tr>
          </thead>
          <tbody>
            {RULES.map((r) => (
              <tr key={r.flag} className="border-t border-line">
                <td className="py-1 pr-3 font-mono">{r.flag}</td>
                <td className="py-1 pr-3">{r.when}</td>
                <td className="py-1 font-mono tabular-nums">{r.pts}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="mt-3 text-xs">
        Total score: <span className="text-approve">0–39 approve</span> ·{" "}
        <span className="text-review">40–69 review</span> ·{" "}
        <span className="text-decline">70+ decline</span>
      </p>
    </details>
  );
}
