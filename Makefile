.PHONY: setup build run-api run-web sim bench test loadgen kafka-up kafka-run

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
