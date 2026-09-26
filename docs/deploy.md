# Deploy

The public demo is two pieces: the Go API as one Docker service on Render's
free plan, and the dashboard as a static Vite build on Vercel that calls the
API directly (CORS). There is no Kafka and no database in this deploy: the API
runs in inline mode and keeps everything in memory.

```
browser ──▶ Vercel (static dashboard) ──fetch/SSE──▶ Render (lambari-api, :$PORT)
```

## Files involved

| File | Role |
| --- | --- |
| [`render.yaml`](../render.yaml) | Render Blueprint: service, plan, health check, env vars |
| [`backend/Dockerfile`](../backend/Dockerfile) | Static Go binary on distroless, non-root |
| [`backend/.dockerignore`](../backend/.dockerignore) | Keeps local binaries and tests out of the build context |
| [`frontend/src/lib/api.ts`](../frontend/src/lib/api.ts) | Reads `VITE_API_BASE`; empty means same-origin (dev proxy) |

## First deploy

### 1. API on Render

1. Render dashboard → **New → Blueprint** → pick this repo.
2. Render reads `render.yaml` and prompts for `LAMBARI_ALLOWED_ORIGINS`.
   Enter the dashboard's URL, e.g. `https://lambari-frontend.vercel.app`.
   Comma-separate several; exact match, no trailing slash, scheme included.
   If the Vercel URL isn't known yet, put a placeholder and fix it in step 3.
3. Wait for the build and for `/api/health` to go green. Note the service URL,
   e.g. `https://lambari-api.onrender.com`.

### 2. Dashboard on Vercel

1. **Add New → Project** → import this repo.
2. Root directory: `frontend`. Framework preset: Vite. Output: `dist`.
   Vercel detects pnpm from the lockfile.
3. Environment Variables → `VITE_API_BASE` = the Render URL from step 1,
   no trailing slash.
4. Deploy.

### 3. Close the loop

If the Vercel URL differs from what you gave Render, update
`LAMBARI_ALLOWED_ORIGINS` in Render → service → Environment. Render restarts
the service on save.

## Verify

```bash
API=https://lambari-api.onrender.com
WEB=https://lambari-frontend.vercel.app

curl -s $API/api/health                                   # 200
curl -si -H "Origin: $WEB" $API/api/stats | grep -i access-control-allow-origin
curl -s -X POST $API/api/transactions -H 'Content-Type: application/json' -d '[]'   # 403: ingest off
```

Then open `$WEB`: the header should read *engine live*. Start the simulator
from the dashboard and watch verdicts stream.

## Updating

- **API**: push to `main`. `autoDeploy: true` rebuilds on Render.
  CI runs on the same push; Render doesn't wait for it.
- **Dashboard**: push to `main`; Vercel rebuilds. Preview deploys run for
  PRs, but their URLs aren't in `LAMBARI_ALLOWED_ORIGINS`, so they can't
  reach the API unless you add them.
- **Changing `VITE_API_BASE`** requires a Vercel redeploy: Vite bakes it in
  at build time.
- **Rollback**: Render → service → Deploys → pick an earlier one →
  *Rollback*. Vercel → Deployments → *Promote to Production*.

## Configuration

API environment variables (read in [`backend/cmd/api/main.go`](../backend/cmd/api/main.go)):

| Variable | Default | Render value | Purpose |
| --- | --- | --- | --- |
| `PORT` / `LAMBARI_ADDR` | `:8080` | set by Render | Listen address; `LAMBARI_ADDR` wins |
| `LAMBARI_ALLOWED_ORIGINS` | none | your Vercel URL(s) | Origins that get CORS headers |
| `LAMBARI_SIM_MAX_RATE` | `100000` | `1000` | Simulator ceiling, tx/s |
| `LAMBARI_SIM_MAX_DURATION` | none | `10m` | Simulator stops itself after this |
| `LAMBARI_DISABLE_INGEST` | `false` | `true` | `POST /api/transactions` answers 403 |
| `LAMBARI_KAFKA_BROKERS` | none | unset | Set it to switch to Kafka mode (not used in this deploy) |

An invalid `LAMBARI_SIM_MAX_RATE` or `LAMBARI_SIM_MAX_DURATION` makes the API
exit at startup; Render's logs show `config` with the reason.

Dashboard build variable: `VITE_API_BASE` (Vercel).

## What the free plan means

- **Sleeps** after 15 minutes without traffic; waking takes about a minute.
  An open dashboard keeps it awake.
- **State is memory only.** Sleep, restart or redeploy resets every case and
  velocity window.
- **One instance only.** A second would split the in-memory state; don't
  scale it out.
- **512 MB.** The demo limits above keep the simulator inside it.
- **No auth.** Anyone with the URL can start the simulator (within the caps)
  and resolve cases. Ingest is off because it would bypass the simulator cap.

## Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| Dashboard stuck, console shows CORS error | Origin missing or mistyped in `LAMBARI_ALLOWED_ORIGINS` (trailing slash, `http` vs `https`, preview URL) |
| Dashboard calls `/api/...` on the Vercel domain (404) | `VITE_API_BASE` unset at build time; set it and redeploy |
| First request takes ~1 min | Service was asleep |
| Cases vanished | Service slept or redeployed; expected |
| `rate must be between 0 and 1000` | `LAMBARI_SIM_MAX_RATE` cap working as intended |
| Render deploy fails health check | Check logs for a `config` error or a port mismatch; the binary must listen on `$PORT` |

## Local image check

Build and run the same image Render builds:

```bash
docker build -t lambari-api backend
docker run --rm -p 8080:8080 \
  -e LAMBARI_SIM_MAX_RATE=1000 -e LAMBARI_SIM_MAX_DURATION=10m \
  -e LAMBARI_DISABLE_INGEST=true -e LAMBARI_ALLOWED_ORIGINS=http://localhost:5173 \
  lambari-api
curl -s localhost:8080/api/health
```

## Out of scope

Kafka mode, Postgres (`schema.sql` is not applied anywhere), auth and
multi-instance deploys. See the [README](../README.md) for Kafka mode and the
[delivery contract](../README.md#delivery-semantics).
