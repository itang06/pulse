# Pulse: build, code generation, and local stack targets.
# `make help` lists everything.

COMPOSE := docker compose -f deploy/docker-compose.yml

.PHONY: help proto build test lint up down logs migrate clean verify-ingress verify-processor replay-processor verify-sink

help: ## List targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

proto: ## Lint schemas and regenerate Go code (Rust regenerates in build.rs)
	buf lint
	buf generate

build: ## Build the Go gateway and the Rust workspace
	cd gateway && go build ./...
	cargo build --workspace

test: ## Run all tests
	./scripts/tests/ingress-common-test.sh
	./scripts/tests/processor-harness-test.sh
	./scripts/tests/sink-harness-test.sh
	cd gateway && go test ./...
	cargo test --workspace

lint: ## buf lint, go vet, clippy
	buf lint
	cd gateway && go vet ./...
	cargo clippy --workspace -- -D warnings

up: ## Start Kafka, TimescaleDB, Prometheus, Grafana (creates deploy/.env on first run)
	@test -f deploy/.env || { cp deploy/.env.example deploy/.env; \
		echo ">>> created deploy/.env from example; edit the passwords <<<"; }
	$(COMPOSE) up -d --wait

down: ## Stop the stack (data volumes are kept; use clean to drop them)
	$(COMPOSE) down

logs: ## Tail stack logs
	$(COMPOSE) logs -f

migrate: ## Apply deploy/migrations/*.sql in name order
	@for f in $$(ls deploy/migrations/*.sql | sort); do \
		echo "applying $$f"; \
		$(COMPOSE) exec -T timescaledb \
			sh -c 'psql -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"' < $$f \
			|| exit 1; \
	done

clean: ## Stop the stack AND delete its data volumes
	$(COMPOSE) down -v

verify-ingress: ## Run a short deterministic SDK-to-Kafka ingress check
	./scripts/verify-ingress.sh

verify-processor: ## Run a small acknowledged gateway-to-processor Kafka integration check
	./scripts/verify-processor.sh

replay-processor: ## Run deterministic anomaly replay and archive its JSON report
	./scripts/replay-processor.sh

verify-sink: ## Verify sink database/offset crash recovery (defaults: 50000 events, 3 crashes)
	./scripts/verify-sink.sh
