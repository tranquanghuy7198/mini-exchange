# TODO — Mini Exchange Portfolio System

Architecture: each service is an independent microservice. Services communicate
**only** via **Kafka** following the **SAGA choreography** pattern — there is no
central orchestrator. Each service reacts to events and emits new events; the
order lifecycle is advanced step by step by the services themselves, with
compensating actions on failure.

**Repository layout (DECIDED — do not revisit):** a single **Cargo workspace**
with one **independent crate per service** plus a `shared` library crate. NOT one
big binary, and NOT separate disconnected Rust projects. Each service crate is its
own binary → its own Docker image → independently deployable/scalable, preserving
the microservice boundary at runtime. The `shared` crate is the single source of
truth for domain models + Kafka event schemas (so producer/consumer contracts
can't drift) and for shared infra helpers (Kafka producer/consumer, sqlx pool,
error types). Layout:

```
coinmy-test/
├── Cargo.toml            # [workspace] members = [...]
├── shared/               # domain models + Kafka event envelope/schemas + infra helpers
├── market-service/       # own binary, own Dockerfile
├── portfolio-service/    # own binary, own Dockerfile
├── audit-service/        # own binary, own Dockerfile (optional)
└── docker-compose.yml    # infrastructure (Task 1), extended with services (Task 12)
```

Tasks are ordered so that dependencies only flow forward: a lower-numbered task
is never blocked by a higher-numbered one. Implement them top to bottom.

Each task has four sections:

- **Goal** — what the task should achieve.
- **Solution** — the planned approach.
- **Progress** — one of `TODO` / `INPROGRESS` / `DONE`.
- **Blocker** — anything obstructing the task during implementation (else `None`).

---

## 1. Bring up the local environment with Docker Compose

- **Goal:** A single `docker compose up` that stands up all backing infrastructure
  — Postgres and Kafka (+ Zookeeper or KRaft) — so the rest of development can run
  against a real environment from day one.
- **Solution:** Write a `docker-compose.yml` defining: a `postgres` service (with
  a persisted volume, default user/password/db, exposed port), a `kafka` broker
  (+ `zookeeper` if not using KRaft) with listeners configured for both in-cluster
  and host access, and optionally a topic-init container and a UI (e.g.
  `kafka-ui`/`adminer`) for inspection. Add `healthcheck`s for Postgres and Kafka.
  Put connection settings in an `.env` file. (Application service containers are
  added later in Task 12 — this task is infrastructure only.) Verify both Postgres
  and Kafka are reachable from the host.
- **Progress:** DONE
- **Blocker:** None
- **Outcome:** `docker-compose.yml` brings up `postgres` (16-alpine), `kafka`
  (`apache/kafka:3.7.0`, KRaft single-node, INTERNAL `kafka:9092` / EXTERNAL
  `localhost:9094`), a `kafka-init` one-shot that creates topics
  (`order-created`, `price-quoted`, `order-executed`, `order-rejected`), plus
  `kafka-ui` (:8080) and `adminer` (:8081). Settings live in `.env`
  (`.env.example` committed). Verified: Postgres healthy + accepts connections,
  Kafka healthy + produce→consume round-trip succeeds, topics created.
  Note: switched off Bitnami images (deprecated/removed from Docker Hub) to the
  official `apache/kafka` image.

## 2. Initialize codebase & workspace

- **Goal:** Set up a Rust workspace hosting the independent microservices
  (Market, Portfolio, optional Audit) with shared code.
- **Solution:** Create a Cargo workspace with member crates: `market-service`,
  `portfolio-service`, `audit-service` (optional), and a `shared`/`common`
  library crate for types, errors, and Kafka helpers. Add `.gitignore`, pin a
  Rust toolchain (`rust-toolchain.toml`), and confirm `cargo build` succeeds on
  an empty skeleton.
- **Progress:** TODO
- **Blocker:** None

## 3. Choose the web/HTTP stack & shared scaffolding

- **Goal:** Establish a consistent HTTP framework and conventions across services
  (HTTP is for client-facing REST endpoints only — inter-service comms use Kafka).
- **Solution:** Adopt `axum` (with `tokio`) for HTTP, `serde` for (de)serialization,
  `tracing` for logging, and `thiserror`/`anyhow` for errors. Add a shared error
  type that maps to HTTP responses. Wire a minimal `GET /health` endpoint into
  each service binary to validate the stack end to end.
- **Progress:** TODO
- **Blocker:** None

## 4. Define shared domain models & event schemas

- **Goal:** Centralize core data types AND the Kafka event contracts that drive
  the saga.
- **Solution:** In the `shared` crate define domain types (`Symbol`, `Price`,
  `Order` with `side: BUY|SELL` and `OrderStatus: CREATED|PRICED|EXECUTED|REJECTED`,
  `Portfolio`, `Holding`) and the saga **event envelope** + event variants, e.g.
  `OrderCreated`, `PriceQuoted`, `OrderExecuted`, `OrderRejected` (and any
  compensation events). Each event carries an `order_id`/correlation id, version,
  and timestamp. Derive `serde` and decide serialization (JSON to start). Document
  which service produces/consumes each event and on which topic.
- **Progress:** TODO
- **Blocker:** None

## 5. Connect to Postgres

- **Goal:** Provide per-service database connectivity and schema for persistent
  state (each service owns its own tables — no shared DB access across services).
- **Solution:** Use `sqlx` with a Postgres connection pool configured from
  `DATABASE_URL` (pointing at the Compose Postgres from Task 1). Add SQL migrations:
  Portfolio owns `portfolios`/`users` (incl. `cash_balance`), `holdings`, `orders`;
  Audit owns `audit_events`. Provide a reusable pool initializer in `shared` and a
  healthcheck query.
- **Progress:** TODO
- **Blocker:** None

## 6. Kafka shared client

- **Goal:** A reusable producer/consumer layer for all services, against the Kafka
  broker from Task 1.
- **Solution:** Choose a Rust client (`rdkafka`). In the `shared` crate build
  helpers: a typed producer that serializes the event envelope (Task 4) and a
  consumer wrapper with consumer groups, manual offset commit, and graceful
  shutdown. Define the topic names and partitioning key (key by `order_id` so a
  saga's events stay ordered per order). Provide a small admin step to create
  topics on startup (or via the topic-init container from Task 1).
- **Progress:** TODO
- **Blocker:** None

## 7. Implement the Market Service

- **Goal:** Serve symbols/prices over REST and participate in the saga by pricing
  orders via Kafka.
- **Solution:** Implement `GET /symbols`, `GET /prices`, `GET /prices/{symbol}`
  with mocked (static or bounded-random) prices; 404 on unknown symbol. Add a
  Kafka consumer for `OrderCreated`: look up the price and emit `PriceQuoted`,
  or emit `OrderRejected` (reason: unknown symbol / market unavailable) so the
  saga can terminate cleanly. (Enables the "market service failure handling" test.)
- **Progress:** TODO
- **Blocker:** None

## 8. Implement the Portfolio Service — read endpoints

- **Goal:** Expose portfolio and order state over REST.
- **Solution:** Implement `GET /portfolio/{userId}` (cash balance + holdings)
  and `GET /orders/{orderId}` (order with current saga status). Back them with
  the Postgres schema from Task 5. Seed at least one demo user with starting cash.
- **Progress:** TODO
- **Blocker:** None

## 9. Implement order intake & the choreographed saga (Portfolio Service)

- **Goal:** Process market BUY/SELL orders as a SAGA choreography over Kafka,
  keeping portfolio state consistent with compensations.
- **Solution:**
  - `POST /orders`: validate, persist order as `CREATED`, emit `OrderCreated`,
    return `202 Accepted` with the `order_id` (result resolved asynchronously).
  - On **BUY** intake, optionally **reserve** cash so it can't be double-spent
    while the saga is in flight.
  - Consume `PriceQuoted`: compute cost/proceeds at the quoted price.
    - **BUY:** confirm reserved cash covers cost → deduct cash → add asset →
      mark `EXECUTED` and emit `OrderExecuted`; if insufficient → release
      reservation, mark `REJECTED`, emit `OrderRejected`.
    - **SELL:** check asset holding → deduct asset → add cash → `EXECUTED`/emit;
      else `REJECTED`/emit.
  - Consume `OrderRejected` (incl. ones emitted by Market): run compensation
    (release any reservation) and mark the order `REJECTED`.
  - Make consumers **idempotent** (dedupe by event id) and wrap each local state
    change in a single DB transaction.
- **Progress:** TODO
- **Blocker:** None

## 10. Optional: Audit Service

- **Goal:** Capture the order lifecycle by subscribing to saga events.
- **Solution:** Implement an Audit Service that consumes the saga topics and
  records `ORDER_CREATED`, `ORDER_EXECUTED`, `ORDER_REJECTED` (and price/compensation
  events) into `audit_events`. Expose a read endpoint to list events for an order.
  Pure consumer — emits no domain events.
- **Progress:** TODO
- **Blocker:** None

## 11. Testing

- **Goal:** Cover the required scenarios for an event-driven system.
- **Solution:** Unit-test order/saga logic. Integration tests covering: successful
  BUY / SELL (assert the saga reaches `EXECUTED` and the emitted events),
  insufficient balance (→ `REJECTED` + compensation), market service failure
  handling (Market emits `OrderRejected` / is unavailable), and correct portfolio
  updates after trades. Use a test Postgres and a real or embedded Kafka
  (e.g. testcontainers) and assert eventual state, since execution is asynchronous.
- **Progress:** TODO
- **Blocker:** None

## 12. Containerize the services & full end-to-end Compose

- **Goal:** Run the whole system — services plus the Task 1 infrastructure — with
  one command.
- **Solution:** Write `Dockerfile`s for each service and extend the `docker-compose.yml`
  from Task 1 to also build/run all application services, with env vars,
  healthchecks, and a migration runner. Ensure correct startup ordering
  (Kafka/Postgres healthy before services). Verify `docker compose up` brings up a
  working, end-to-end system.
- **Progress:** TODO
- **Blocker:** None

## 13. Documentation & deliverables

- **Goal:** Ship the required docs.
- **Solution:** Update `README.md` with setup, an architecture description of the
  SAGA choreography (event/topic flow diagram), and run/test instructions. Write
  `AI_USAGE.md` covering tools used, key prompts, tasks delegated to AI, accepted
  vs. modified output, and at least one example of incorrect AI output and how it
  was handled.
- **Progress:** TODO
- **Blocker:** None
