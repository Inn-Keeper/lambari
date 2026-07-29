# Lambari Rename Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the former project identity with Lambari in the repository, GitHub, and the local checkout.

**Architecture:** Apply one mechanical, atomic identity change without compatibility aliases. Verify the code and documentation before changing the external repository and checkout names.

**Tech Stack:** Go 1.22, React 19, TypeScript 5.7, Vite 6, pnpm 10, Git, GitHub CLI

## Global Constraints

- Use `Lambari` for visible product names.
- Use `lambari` for machine-readable names and identifiers.
- Replace the former uppercase environment-variable prefix with
  `LAMBARI_ADDR` and `LAMBARI_KAFKA_BROKERS`.
- Leave no compatibility aliases or active former-identity references.
- Preserve all unrelated behavior and dependencies.

---

### Task 1: Rename machine-readable identifiers

**Files:**
- Modify: `package.json`
- Modify: `frontend/package.json`
- Modify: `backend/go.mod`
- Modify: `backend/cmd/api/main.go`
- Modify: `backend/cmd/loadgen/main.go`
- Modify: `backend/internal/api/server.go`
- Modify: `backend/internal/cases/cases.go`
- Modify: `backend/internal/cases/cases_test.go`
- Modify: `backend/internal/engine/engine.go`
- Modify: `backend/internal/engine/engine_test.go`
- Modify: `backend/internal/engine/rules.go`
- Modify: `backend/internal/kafka/kafka.go`
- Modify: `Makefile`

**Interfaces:**
- Produces: Go module `lambari`, frontend package `lambari-dashboard`,
  environment variables `LAMBARI_ADDR` and `LAMBARI_KAFKA_BROKERS`, Kafka
  consumer group `lambari-scoring`.

- [ ] **Step 1: Confirm the old identifiers exist**

Run:

```bash
rg -n 'trip''wire|TRIP''WIRE_' package.json frontend/package.json backend Makefile
```

Expected: matches in package names, Go imports, environment variables, Kafka
configuration, and log text.

- [ ] **Step 2: Apply the minimal identifier replacements**

Rename the root and dashboard packages, Go module and internal imports, Kafka
consumer group, environment-variable prefix, and API log label to their
`lambari` / `LAMBARI` forms.


- [ ] **Step 3: Verify the renamed code**

Run:

```bash
cd backend && go test ./...
pnpm build
```

Expected: all Go tests pass and the frontend production build completes.

### Task 2: Rename visible identity and documentation

**Files:**
- Modify: `README.md`
- Modify: `docs/diagrams.md`
- Modify: `docs/diagrams.html`
- Modify: `docs/knowledge-base.md`
- Modify: `docs/superpowers/specs/2026-07-29-lambari-rename-design.md`
- Modify: `frontend/index.html`
- Modify: `frontend/src/App.tsx`
- Modify: `frontend/src/components/LiveFeed.tsx`

**Interfaces:**
- Consumes: the Lambari machine identifiers from Task 1.
- Produces: visible `Lambari` branding and links to
  `https://github.com/Inn-Keeper/lambari`.

- [ ] **Step 1: Replace product-name references**

Use `Lambari` in headings, titles, application chrome, footer copy, and prose.
Use `lambari` in architecture labels, paths, and the GitHub repository URL.
Rewrite the former product-name visual metaphor as a neutral “signal wire” so
no old product identity remains. Normalize the approved design spec to refer to
the “former name” so the completed tree contains no stale identity string.

- [ ] **Step 2: Assert that the old identity is gone**

Run:

```bash
rg -n -i --hidden --glob '!.git/**' --glob '!node_modules/**' \
  --glob '!vendor/**' 'trip''wire'
```

Expected: no matches.

- [ ] **Step 3: Run repository verification**

Run:

```bash
make test
pnpm build
git diff --check
git status --short
```

Expected: tests and build pass, the diff has no whitespace errors, and only the
planned rename files are modified.

- [ ] **Step 4: Commit the repository rename**

Run:

```bash
git add Makefile README.md backend docs frontend package.json
git commit -m "Rename project to Lambari"
git push origin main
```

Expected: the rename commit and the preceding design commit are on
`origin/main`.

### Task 3: Rename GitHub repository and local checkout

**Files:**
- External rename: the current `Inn-Keeper` repository to
  `Inn-Keeper/lambari`
- Local rename: the current checkout directory to
  `/Volumes/T7/inn-kepper-overnight-machinery/lambari`

**Interfaces:**
- Consumes: the pushed repository rename from Task 2.
- Produces: GitHub repository `Inn-Keeper/lambari`, matching `origin`, and a
  checkout directory named `lambari`.

- [ ] **Step 1: Rename the GitHub repository**

Run:

```bash
gh repo rename lambari --repo Inn-Keeper/$(printf 'trip%s' wire) --yes
git remote set-url origin https://github.com/Inn-Keeper/lambari.git
git remote -v
```

Expected: fetch and push URLs are
`https://github.com/Inn-Keeper/lambari.git`.

- [ ] **Step 2: Verify the renamed GitHub repository**

Run:

```bash
gh repo view Inn-Keeper/lambari --json nameWithOwner,url
git ls-remote origin HEAD
```

Expected: GitHub reports `Inn-Keeper/lambari` and the remote resolves.

- [ ] **Step 3: Rename the local checkout**

Run from the parent directory:

```bash
mv /Volumes/T7/inn-kepper-overnight-machinery/$(printf 'trip%s' wire) \
  /Volumes/T7/inn-kepper-overnight-machinery/lambari
```

- [ ] **Step 4: Run final verification from the new path**

Run:

```bash
pwd
git status --short --branch
git remote -v
rg -n -i --hidden --glob '!.git/**' --glob '!node_modules/**' \
  --glob '!vendor/**' 'trip''wire'
```

Expected: the path ends in `/lambari`, `main` matches `origin/main`, the
worktree is clean, the remote is `Inn-Keeper/lambari`, and the search returns
no matches.
