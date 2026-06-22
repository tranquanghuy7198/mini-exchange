# Mini Exchange Portfolio System

A simplified trading-platform backend in **Rust**, built as **three independent
microservices** that communicate **only over Kafka** using the **SAGA
choreography** pattern (no central orchestrator). Orders are placed over REST and
resolved asynchronously through a chain of events.

> Built with the help of an AI coding agent — see [AI_USAGE.md](AI_USAGE.md).
> Original assignment brief: [requirements.md](requirements.md).
> Step-by-step run/test walkthrough with copy-paste `curl`s: [GUIDELINE.md](GUIDELINE.md).

---

## Services

| Service              | Port | Responsibility                                                                          | Owns DB       |
| -------------------- | ---- | --------------------------------------------------------------------------------------- | ------------- |
| **Market**           | 8081 | `GET /symbols`, `/prices`, `/prices/{symbol}` (mocked); prices orders via Kafka         | — (stateless) |
| **Portfolio**        | 8082 | `GET /portfolio/{userId}`, `GET /orders/{orderId}`, `POST /orders`; runs the order saga | `portfolio`   |
| **Audit** (optional) | 8083 | records every saga event; `GET /audit/orders/{orderId}`                                 | `audit`       |

Each service is an independent binary with its own Docker image potential and its
own database — a true microservice boundary. They never call each other over HTTP
or share tables; **all** inter-service communication is Kafka events.

---

## Architecture

### Component view

```
                 ┌──────────────┐   REST    ┌──────────────────────────┐
   client  ─────▶│  Portfolio   │◀──────────│  GET /portfolio /orders  │
   POST /orders  │  Service     │           └──────────────────────────┘
                 └─────┬────▲───┘
                       │    │  Kafka (SAGA choreography)
        order-created  │    │  price-quoted / order-rejected
                       ▼    │
                 ┌──────────┴───┐
                 │   Market     │   (mocked prices; rejects unknown/unavailable)
                 │   Service    │
                 └──────────────┘
                       │
        all saga events│ (order-created, price-quoted, order-executed, order-rejected)
                       ▼
                 ┌──────────────┐
                 │    Audit     │   (append-only event log)
                 │   Service    │
                 └──────────────┘

   Postgres: `portfolio` DB (users, portfolios, holdings, orders, processed_events)
             `audit` DB     (audit_events)
```

### The order saga (choreography)

A market order moves through the system as a chain of events, each service
reacting to the previous step and emitting the next — there is no orchestrator.

```
POST /orders
   │  persist order = CREATED
   ▼
[order-created] ───▶ Market Service
                        │  look up price
                        ├─ listed     ─▶ [price-quoted] ───▶ Portfolio Service
                        └─ unknown /                              │  apply BUY/SELL atomically:
                           unavailable ─▶ [order-rejected] ──┐    │   BUY  → check+deduct cash, add asset
                                                             │    │   SELL → check+deduct asset, add cash
                                                             │    ├─ success      ─▶ status EXECUTED, [order-executed]
                                                             │    └─ insufficient ─▶ status REJECTED, [order-rejected]
                                                             ▼
                                            Portfolio marks the order REJECTED
                                            (compensation; nothing was reserved)

   Audit Service consumes ALL of the above and records each event.
```

### Topics & event contracts

Events are JSON envelopes (`event_id`, `order_id` correlation key, `version`,
`occurred_at`, `payload`), keyed by `order_id` so a saga's events stay ordered on
one partition. Defined once in the `shared` crate so producer/consumer contracts
can't drift.

| Topic            | Payload         | Produced by           | Consumed by      |
| ---------------- | --------------- | --------------------- | ---------------- |
| `order-created`  | `OrderCreated`  | Portfolio             | Market, Audit    |
| `price-quoted`   | `PriceQuoted`   | Market                | Portfolio, Audit |
| `order-executed` | `OrderExecuted` | Portfolio             | Audit            |
| `order-rejected` | `OrderRejected` | Market _or_ Portfolio | Portfolio, Audit |

### Order lifecycle

`CREATED` → (`price-quoted`) → `EXECUTED` **or** `REJECTED`.
Rejection reasons: `UNKNOWN_SYMBOL`, `MARKET_UNAVAILABLE` (from Market);
`INSUFFICIENT_BALANCE`, `INSUFFICIENT_ASSET` (from Portfolio).

---

## Tech stack

- **Language:** Rust (edition 2021, toolchain pinned in `rust-toolchain.toml`)
- **HTTP:** `axum` + `tokio`
- **Messaging:** Apache Kafka (KRaft, single node) via `rdkafka`
- **Storage:** PostgreSQL 16 via `sqlx` (database-per-service)
- **Money/ids/time:** `rust_decimal` (exact decimals — no floats), `uuid`, `chrono`
- **Observability:** `tracing`
- **Workspace:** one Cargo workspace, a crate per service plus a `shared` library

```
.
├── Cargo.toml                # workspace
├── shared/                   # domain models, Kafka event schemas, infra helpers
├── market-service/           # Market Service (bin)
├── portfolio-service/        # Portfolio Service (lib + bin) + migrations
├── audit-service/            # Audit Service (bin) + migrations
├── docker-compose.yml        # Postgres + Kafka + topic/db init + UIs
├── Makefile                  # dev tasks (see `make help`)
├── scripts/e2e.sh            # end-to-end choreography test
├── GUIDELINE.md              # detailed run & curl walkthrough
└── AI_USAGE.md               # AI coding agent usage notes
```

---

## Key design decisions

- **SAGA choreography, not orchestration** — services react to events and emit the
  next event; the order's lifecycle is advanced collaboratively.
- **Database-per-service** — `portfolio` and `audit` are separate databases (not
  just separate tables), so schemas and migrations never collide.
- **Exact decimal money** — `rust_decimal` everywhere (`NUMERIC` in Postgres); no
  floating-point for cash/quantities.
- **Idempotent, transactional consumers** — each saga state change runs in one DB
  transaction, deduped by `event_id` (a `processed_events` ledger) plus an
  order-status guard, so redelivered events are safe (at-least-once Kafka).
- **No intake cash reservation** — market orders have no price at intake, so
  consistency is enforced with row-level locks (`SELECT … FOR UPDATE`) at
  execution time, which prevents double-spend across concurrent in-flight orders.
- **Mocked market** — prices are static bases with small bounded-random jitter; a
  `MARKET_FAIL_SYMBOLS` hook deterministically simulates market unavailability for
  testing the failure path.

A known limitation: emitting the outcome event happens just after the DB commit
(no transactional outbox), so a crash in that gap could drop an event. Acceptable
at this scope; the production fix is an outbox pattern.

---

## Getting started

### Prerequisites

- Docker (with `docker compose`)
- Rust toolchain (1.95; auto-selected via `rust-toolchain.toml`)
- `curl` (optional `jq`)

### Quick start

```bash
cp .env.example .env        # only if .env is missing
make infra-up               # Postgres + Kafka + topic/db init + UIs, waits until healthy
make build                  # compiles the workspace (first build also builds librdkafka)

# run each service in its own terminal:
make market                 # :8081
make portfolio              # :8082  (auto-migrates + seeds a demo user)
make audit                  # :8083
```

A demo user is seeded automatically on Portfolio startup:
`user_id = 11111111-1111-1111-1111-111111111111`, `100000.00` cash, `1.0 BTC`.
Listed symbols: **BTC, ETH, SOL, ADA, DOGE**.

Inspection UIs: **Kafka UI** http://localhost:8080, **Adminer** http://localhost:8085.

Run `make help` for all developer tasks. For a full walkthrough with copy-paste
`curl` commands for every scenario, see **[GUIDELINE.md](GUIDELINE.md)**.

### API summary

```
# Market
GET  /symbols
GET  /prices
GET  /prices/{symbol}          # 404 unknown, 503 unavailable

# Portfolio
GET  /portfolio/{userId}       # cash + holdings (404 if no such user)
GET  /orders/{orderId}         # order + current saga status
POST /orders                   # {user_id, symbol, side: BUY|SELL, quantity} -> 202 {order_id, status}

# Audit
GET  /audit/orders/{orderId}   # recorded lifecycle events for an order

# All services: GET /health (liveness); DB-backed services: GET /ready
```

Example — place a BUY and check the result (orders resolve asynchronously):

```bash
DEMO=11111111-1111-1111-1111-111111111111
curl -s -X POST localhost:8082/orders -H 'content-type: application/json' \
     -d "{\"user_id\":\"$DEMO\",\"symbol\":\"ETH\",\"side\":\"BUY\",\"quantity\":\"2\"}"
# {"order_id":"...","status":"CREATED"}
curl -s localhost:8082/orders/<order_id>     # poll until EXECUTED / REJECTED
curl -s localhost:8082/portfolio/$DEMO       # cash debited, ETH credited
```

---

## Testing

Covers the required scenarios: successful BUY/SELL, insufficient balance, market
service failure handling, and correct portfolio updates after trades.

```bash
# 1. Unit tests — no infrastructure needed
make test
#    (shared domain/serde/validation; market price engine)

# 2. Saga integration tests — deterministic, against a test Postgres
make test-it
#    (BUY/SELL execute + portfolio updates, insufficient balance/asset,
#     market-failure handling, idempotent replay)

# 3. End-to-end — full Kafka choreography across all three services
make e2e
#    (boots infra + services; asserts every scenario incl. market-unavailable,
#     plus the audit trail)
```

The integration tests are marked `#[ignore]` so plain `cargo test` stays green
without infrastructure; `make test-it` starts Postgres and runs them with
`--ignored`. See [GUIDELINE.md](GUIDELINE.md) §4 for the manual `curl` versions of
each test scenario.

---

## Teardown

```bash
make infra-down     # stop containers, keep data
make infra-reset    # stop and DELETE all data (fresh demo state next run)
```

---

## Deliverables

| Requirement                      | Where                                                                |
| -------------------------------- | -------------------------------------------------------------------- |
| Source code (Rust)               | `shared/`, `market-service/`, `portfolio-service/`, `audit-service/` |
| README with setup & architecture | this file                                                            |
| AI_USAGE.md                      | [AI_USAGE.md](AI_USAGE.md)                                           |
| Docker Compose (or equivalent)   | `docker-compose.yml` (+ `Makefile`)                                  |
| Test instructions                | this file (Testing) + [GUIDELINE.md](GUIDELINE.md)                   |
