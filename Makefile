# Mini Exchange Portfolio System — developer tasks.
# Run services locally against the docker-compose infrastructure.
# Connection settings are read from .env automatically (via dotenvy).

.DEFAULT_GOAL := help
RUST_LOG ?= info
export RUST_LOG

# --- Infrastructure --------------------------------------------------------

.PHONY: infra-up
infra-up: ## Start infra (Postgres + Kafka + topic/db init + UIs) and wait until healthy
	docker compose up -d postgres db-init kafka kafka-init kafka-ui adminer
	@printf "waiting for postgres + kafka to be healthy"
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' exchange-postgres 2>/dev/null)" = healthy ] && \
	       [ "$$(docker inspect -f '{{.State.Health.Status}}' exchange-kafka 2>/dev/null)" = healthy ]; do \
	  printf "."; sleep 1; done; echo " ok"
	@echo "Kafka UI: http://localhost:$${KAFKA_UI_PORT:-8080}   Adminer: http://localhost:$${ADMINER_PORT:-8085}"

.PHONY: infra-down
infra-down: ## Stop infra (keeps data volumes)
	docker compose down

.PHONY: infra-reset
infra-reset: ## Stop infra and DELETE all data (Postgres + Kafka volumes)
	docker compose down -v

.PHONY: ps
ps: ## Show compose container status
	docker compose ps

.PHONY: infra-logs
infra-logs: ## Tail infra logs
	docker compose logs -f

# --- Services (run each in its own terminal) -------------------------------

.PHONY: market
market: ## Run Market Service (:8081)
	cargo run -p market-service

.PHONY: market-fail
market-fail: ## Run Market Service with DOGE forced "market unavailable" (failure test)
	MARKET_FAIL_SYMBOLS=DOGE cargo run -p market-service

.PHONY: portfolio
portfolio: ## Run Portfolio Service (:8082) — auto-migrates + seeds the demo user
	cargo run -p portfolio-service

.PHONY: audit
audit: ## Run Audit Service (:8083)
	cargo run -p audit-service

# --- Build / quality / tests ----------------------------------------------

.PHONY: build
build: ## Build the whole workspace
	cargo build

.PHONY: fmt
fmt: ## Format the code
	cargo fmt

.PHONY: clippy
clippy: ## Lint (warnings = errors)
	cargo clippy --workspace --all-targets -- -D warnings

.PHONY: test
test: ## Unit tests (no infrastructure needed)
	cargo test --workspace

.PHONY: test-it
test-it: ## Portfolio saga integration tests (needs infra; starts postgres+db-init)
	docker compose up -d postgres db-init >/dev/null
	cargo test -p portfolio-service -- --ignored --test-threads=1

.PHONY: e2e
e2e: ## Full end-to-end choreography test (boots infra + all 3 services)
	./scripts/e2e.sh

# --- DB inspection ---------------------------------------------------------

.PHONY: psql-portfolio
psql-portfolio: ## Open psql on the portfolio database
	docker exec -it exchange-postgres psql -U exchange -d portfolio

.PHONY: psql-audit
psql-audit: ## Open psql on the audit database
	docker exec -it exchange-postgres psql -U exchange -d audit

# --- Help ------------------------------------------------------------------

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z0-9_-]+:.*## ' $(MAKEFILE_LIST) \
	  | sort | awk 'BEGIN {FS = ":.*## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'
