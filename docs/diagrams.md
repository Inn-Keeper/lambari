# Tripwire — UML Diagrams

Companion to [knowledge-base.md](knowledge-base.md). GitHub renders these
Mermaid blocks natively — no separate viewer needed.

## Component diagram

How a transaction enters, gets scored, and reaches the dashboard.

```mermaid
flowchart LR
    subgraph Ingest
        LG[loadgen]
        SIM[built-in simulator]
    end

    LG -->|HTTP POST /api/transactions| API
    LG -->|Kafka: transactions topic| KC
    SIM --> API

    subgraph API[API :8080]
        HTTP[HTTP handlers]
        KC[franz-go consumer]
        ENG[Engine\nchan 16384 -> 2xNumCPU workers]
        RULES[Rule chain\namount, velocity, ip fan-out, geo, mcc]
        HOOK[OnFlagged hook]
        SSE[GET /api/stream - SSE 400ms]

        HTTP --> ENG
        KC --> ENG
        ENG --> RULES
        RULES --> HOOK
        ENG --> SSE
    end

    HOOK --> CASES[(cases.Store\nMemStore, Postgres-shaped)]
    HOOK -->|kafka mode| VT[Kafka: verdicts topic]

    SSE --> DASH[React dashboard :5173]
    CASES -->|GET /api/cases| DASH
```

## Sequence diagram — one transaction, HTTP path

```mermaid
sequenceDiagram
    participant C as Client (loadgen/sim)
    participant H as HTTP handler
    participant E as Engine
    participant W as Worker + Rules
    participant S as cases.Store
    participant D as Dashboard (SSE)

    C->>H: POST /api/transactions (batch)
    H->>E: Submit(tx) per transaction
    E->>E: enqueue on bounded channel (16384)
    Note over E: full channel = backpressure,<br/>request rejected, never blocks OOM
    E->>W: dispatch to worker pool
    W->>W: run rule chain, sum score
    W->>W: Decide(score) -> approve/review/decline
    alt review or decline
        W->>S: OnFlagged(verdict) -> Open(case)
    end
    W-->>E: Verdict recorded (atomics, reservoir sample)
    loop every 400ms
        E->>D: stats + recent verdicts + case counts
    end
```

## Class diagram — core types

```mermaid
classDiagram
    class Transaction {
        +string ID
        +string CardBIN
        +string CardHash
        +float64 Amount
        +string Currency
        +string Country
        +string IP
        +string MerchantID
        +string MCC
        +time.Time Timestamp
    }

    class Decision {
        <<enumeration>>
        Approve
        Review
        Decline
    }

    class Verdict {
        +string TxID
        +string CardBIN
        +float64 Amount
        +string Currency
        +string Country
        +int Score
        +[]string Flags
        +Decision Decision
        +int64 LatencyUS
        +int64 At
    }

    class Engine {
        -chan Transaction queue
        +New() Engine
        +OnFlagged(fn)
        +Start()
        +Stop()
        +Submit(tx)
        +TrySubmit(tx) bool
        +Snapshot() Stats
        +Recent() []Verdict
        -worker()
        -score(tx)
    }

    class Stats {
        +int64 Processed
        +int64 QueueDepth
        +... rate/percentile fields
    }

    class Rule {
        <<function type>>
        +(tx, State) (points int, flag string)
    }

    class State {
        -[256]shard cardShards
        -[256]shard ipShards
        +NewState() State
        +StartSweeper(done)
    }

    class Status {
        <<enumeration>>
        Open
        Resolved
    }

    class Resolution {
        <<enumeration>>
        ConfirmedFraud
        FalsePositive
    }

    class Case {
        +string ID
        +Verdict Verdict
        +Status Status
        +Resolution Resolution
        +int64 OpenedAt
        +int64 ResolvedAt
    }

    class Store {
        <<interface>>
        +Open(v Verdict)
        +List(status, limit) []Case
        +Resolve(id, r) (Case, error)
        +Counts() (open, confirmed, falsePos int64)
    }

    class MemStore {
        -map~string,Case~ open
        -[]string openIDs
        +NewMemStore(maxOpen) MemStore
        +Open(v Verdict)
        +List(status, limit) []Case
        +Resolve(id, r) (Case, error)
        +Counts() (int64, int64, int64)
    }

    Engine --> Rule : runs chain of
    Engine ..> State : shares across workers
    Rule ..> State : reads/writes velocity windows
    Engine ..> Verdict : produces
    Verdict --> Decision
    Case --> Verdict : wraps
    Case --> Status
    Case --> Resolution
    Store <|.. MemStore : implements
    Engine ..> Store : OnFlagged hook writes to
```

## State diagram — case lifecycle

```mermaid
stateDiagram-v2
    [*] --> Open : verdict = review or decline\n(OnFlagged hook)
    Open --> Resolved_ConfirmedFraud : POST /resolve\n{confirmed_fraud}
    Open --> Resolved_FalsePositive : POST /resolve\n{false_positive}
    Open --> Evicted : queue > maxOpen\n(oldest unreviewed dropped)
    Resolved_ConfirmedFraud --> [*] : capped history\n(bounded memory)
    Resolved_FalsePositive --> [*] : capped history\n(bounded memory)
    Evicted --> [*]

    note right of Resolved_ConfirmedFraud
        Labels feed the future
        ML rule (see roadmap §11)
    end note
```
