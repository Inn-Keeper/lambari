# AGENTS.md

Guidelines for Lambari, a real-time fraud-scoring proof of concept. The README
is the source of truth for architecture, measurements and the delivery
contract ([Delivery semantics](README.md#delivery-semantics)); don't restate
it elsewhere — link to it.

## Layout

| Folder | What it is | Stack |
| --- | --- | --- |
| `backend/` | Scoring engine, HTTP/SSE API, Kafka consumer, load generator | Go 1.22, stdlib `net/http`, franz-go |
| `frontend/` | Live dashboard and review queue | React 19, TypeScript, Vite, Tailwind v4, Vitest |
| `docs/` | Architecture pages, diagrams, design specs and plans | Markdown / standalone HTML |

`schema.sql` is the Postgres shape for `cases.Store`; nothing applies it yet.

## Working Principles

- YAGNI over SOLID.
- No overengineering, tech theater, or overkill solutions.
- Mind cyclomatic complexity.
- Keep components and functions small.
- Write comments that explain what and why.
- Keep descriptions concise.
- Keep changes small, focused, and reviewable. Avoid unrelated refactors.
- Prefer existing project patterns over introducing new ones.
- Backend keeps its single dependency (franz-go). Don't add Go or production
  npm dependencies without explaining why.
- Numbers in the README are measured. Don't change one without re-running the
  measurement that produced it.
- Preserve accessibility, responsive behavior, and type safety.
- Never print, log, or commit secrets.

## Before Making Changes

- Inspect the relevant files first.
- Follow existing conventions for naming, testing and folder structure.
- Ask when requirements are unclear.

## After Making Changes

| Area | Checks |
| --- | --- |
| `backend/` | `make test`, `cd backend && go vet ./...` |
| `frontend/` | `make test-web`, `pnpm build` (runs `tsc -b`) |

Broker-backed tests (`make e2e`, `make rebalance`) need Redpanda on
`localhost:19092`; run them when touching `internal/kafka/` or the delivery
path. If a check can't run, say so and why.

CI (`.github/workflows/ci.yml`) runs all of the above, broker tests included,
on every push to `main` and every pull request.

## Changelog

`CHANGELOG.md` records notable changes, newest first, grouped into Fixed /
Changed / Added. Add to today's dated entry, or start one; leave earlier
entries untouched.

## Commits

Commit only when asked. Commits and pull requests are attributed to the
repository owner only: no AI co-author trailers or "generated with" footers.

## Review Expectations

When summarizing work, include: files changed, commands run, skipped checks,
and any manual follow-up.
