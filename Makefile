.PHONY: setup build run-api run-web sim bench test test-web loadgen kafka-up kafka-run broker-check e2e rebalance

setup:            ## install backend + frontend dependencies
	cd backend && go mod tidy
	pnpm install

build:            ## compile backend binaries (backend/api, backend/loadgen)
	cd backend && go build -o api ./cmd/api && go build -o loadgen ./cmd/loadgen

run-api:          ## start the scoring engine + API (inline mode, no Kafka needed)
	cd backend && go run ./cmd/api

run-web:          ## start the dashboard dev server on :5173
	pnpm dev

sim:              ## start the built-in simulator at 5000 tx/s
	curl -s -X POST localhost:8080/api/simulate -d '{"rate":5000}'

test:             ## run backend tests
	cd backend && go test ./...

test-web:         ## run frontend (vitest) tests
	pnpm --filter lambari-dashboard test

bench:            ## engine throughput benchmark
	cd backend && go test -bench=BenchmarkEngineThroughput -benchtime=3s -run XXX ./internal/engine/

loadgen:          ## flood the HTTP ingest path at 5000 tx/s for 30s
	cd backend && go run ./cmd/loadgen -rate 5000 -duration 30s

kafka-up:         ## start Redpanda + console (localhost:8081)
	docker compose up -d

kafka-run:        ## run the API consuming from Kafka
	cd backend && LAMBARI_KAFKA_BROKERS=localhost:19092 go run ./cmd/api

kafka-loadgen:    ## produce 5000 tx/s into the transactions topic
	cd backend && go run ./cmd/loadgen -rate 5000 -duration 30s -kafka localhost:19092

# Starting the broker and running an experiment are separate steps on purpose:
# `make kafka-up` needs the Compose plugin, which not every Docker install has
# (colima ships without it — see the README for the one-line `docker run`).
# The experiments only need a reachable broker, however it got there.
broker-check:
	@nc -z localhost 19092 || { \
	  echo "no broker on localhost:19092 — run 'make kafka-up', or start Redpanda"; \
	  echo "directly if your Docker has no compose plugin (see README, Kafka mode)."; \
	  exit 1; }

e2e: broker-check   ## crash-replay at-least-once test against Redpanda (SIGKILLs a real consumer)
	cd backend && LAMBARI_KAFKA_BROKERS=localhost:19092 go test -run TestCrashReplayAtLeastOnce -v -timeout 5m -count=1 ./internal/kafka/

rebalance: broker-check ## show velocity state dying when a partition moves between consumers
	cd backend && LAMBARI_KAFKA_BROKERS=localhost:19092 go test -run TestRebalanceLosesVelocityState -v -timeout 5m -count=1 ./internal/kafka/
