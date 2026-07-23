-- Production shape for the case store (internal/cases.Store).
-- The in-memory PoC store mirrors this exactly; swap in a pgx-backed
-- implementation without touching the HTTP handlers.

CREATE TYPE case_status AS ENUM ('open', 'resolved');
CREATE TYPE case_resolution AS ENUM ('confirmed_fraud', 'false_positive');

CREATE TABLE cases (
    id          text PRIMARY KEY,               -- = tx_id
    verdict     jsonb NOT NULL,                 -- full scored verdict
    score       int  GENERATED ALWAYS AS ((verdict->>'score')::int) STORED,
    status      case_status NOT NULL DEFAULT 'open',
    resolution  case_resolution,
    opened_at   timestamptz NOT NULL DEFAULT now(),
    resolved_at timestamptz,
    resolved_by text                            -- analyst id, once auth exists
);

-- review queue: open cases, worst first
CREATE INDEX idx_cases_queue ON cases (status, score DESC, opened_at DESC);

-- labeled training data export: resolved cases with their features
CREATE INDEX idx_cases_labels ON cases (resolution) WHERE status = 'resolved';
