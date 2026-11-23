.PHONY: up down api worker frontend check bench
up:
	docker compose up -d --build
down:
	docker compose down
api:
	go run ./cmd/api
worker:
	go run ./cmd/worker
frontend:
	cd frontend && npm ci && npm run check
check: frontend
	test -z "$$(gofmt -l cmd internal)"
	go vet ./...
	go test -race ./...
	docker compose config -q
bench:
	go run ./cmd/bench -json docs/benchmarks-raw.json
