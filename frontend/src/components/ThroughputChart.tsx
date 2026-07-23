const W = 720;
const H = 180;
const PAD = 8;

interface Props {
  history: number[];
  current: number;
}

/** Live throughput as a filled area — samples arrive every 400ms. */
export function ThroughputChart({ history, current }: Props) {
  const max = Math.max(1000, ...history) * 1.15;
  const n = Math.max(history.length, 2);
  const stepX = (W - PAD * 2) / (n - 1);

  const points = history.map((v, i) => {
    const x = PAD + i * stepX;
    const y = H - PAD - (v / max) * (H - PAD * 2);
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  });

  const line = points.length >= 2 ? `M ${points.join(" L ")}` : "";
  const area =
    points.length >= 2
      ? `${line} L ${(PAD + (n - 1) * stepX).toFixed(1)},${H - PAD} L ${PAD},${H - PAD} Z`
      : "";

  const gridLines = [0.25, 0.5, 0.75].map((f) => H - PAD - f * (H - PAD * 2));

  return (
    <div className="rounded-lg border border-line bg-panel p-4">
      <div className="mb-2 flex items-baseline justify-between">
        <div className="text-[11px] font-medium uppercase tracking-[0.14em] text-muted">
          Throughput
        </div>
        <div className="font-mono text-sm tabular-nums text-accent">
          {current.toLocaleString()} tx/s
        </div>
      </div>
      <svg
        viewBox={`0 0 ${W} ${H}`}
        className="h-40 w-full"
        role="img"
        aria-label={`Throughput chart, currently ${current} transactions per second`}
      >
        <defs>
          <linearGradient id="tpFill" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="var(--color-accent)" stopOpacity="0.35" />
            <stop offset="100%" stopColor="var(--color-accent)" stopOpacity="0.02" />
          </linearGradient>
        </defs>
        {gridLines.map((y) => (
          <line
            key={y}
            x1={PAD}
            x2={W - PAD}
            y1={y}
            y2={y}
            stroke="var(--color-line)"
            strokeWidth="1"
            strokeDasharray="2 6"
          />
        ))}
        {area && <path d={area} fill="url(#tpFill)" />}
        {line && (
          <path
            d={line}
            fill="none"
            stroke="var(--color-accent)"
            strokeWidth="2"
            strokeLinejoin="round"
          />
        )}
        {points.length > 0 && (
          <circle
            cx={points[points.length - 1].split(",")[0]}
            cy={points[points.length - 1].split(",")[1]}
            r="3.5"
            fill="var(--color-accent)"
          />
        )}
      </svg>
      <div className="mt-1 flex justify-between text-[10px] text-faint">
        <span>−36s</span>
        <span>now</span>
      </div>
    </div>
  );
}
