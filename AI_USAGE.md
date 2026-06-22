# AI Usage

This project was built with the assistance of an AI coding agent. This document
records how the agent was used, as required by the assignment.

## Tools used

- **AI coding agent:** Claude Code (Anthropic's official CLI), running the
  Claude Opus model. Used interactively from the terminal/IDE to plan, write, and
  verify the code.
- **Supporting toolchain (driven by the agent):** `cargo` (build/test),
  `clippy` (lint, run with `-D warnings`), `rustfmt`, and `docker compose`
  (Postgres + Kafka). The agent ran these to build, test, and validate every step
  against live infrastructure.

## Key prompts

The work was driven top-down: first turn the assignment into a task plan, then
implement and verify one task at a time. Representative prompts:

- _"Based on the README, break it down into smaller tasks (init codebase, connect
  to Postgres, etc.) and write them into a `TODO.md`. Each task has four sections:
  goal, solution, progress, and blocker. Order them so a lower-numbered task is
  never blocked by a higher-numbered one."_
- _"Update the TODO — each section is a microservice; they must follow the SAGA
  choreography pattern and use Kafka to communicate."_

## Tasks delegated to AI

Essentially the full implementation, broken into the ordered tasks in
[TODO.md](TODO.md):

1. **Local environment** — `docker-compose.yml` (Postgres + Kafka in KRaft, topic
   & per-service DB init, inspection UIs) and `.env`.
2. **Workspace** — Cargo workspace with `shared`, `market-service`,
   `portfolio-service`, `audit-service`.
3. **HTTP/error scaffolding** — `axum` stack, shared `AppError`/`AppResult`,
   `tracing`, `/health`.
4. **Domain & event schemas** — `shared` domain types and the Kafka event
   envelope/contracts (single source of truth for the saga).
5. **Postgres** — `sqlx` pool + per-service migrations (database-per-service).
6. **Kafka client** — reusable idempotent producer + consumer-group wrapper.
7. **Market Service** — mocked prices over REST + pricing orders via Kafka.
8. **Portfolio read endpoints** — `GET /portfolio`, `GET /orders`, demo seeding.
9. **Order saga** — `POST /orders` + the choreographed BUY/SELL state machine with
   transactional, idempotent consumers and compensation.
10. **Audit Service** — consumes all saga topics into an append-only event log.
11. **Tests** — unit tests, deterministic DB integration tests, and an end-to-end
    choreography script.
12. **Local-run tooling** — `Makefile` and `GUIDELINE.md`.
13. **Documentation** — this file and `README.md`.

The agent also verified each task against running infrastructure (REST calls,
producing/consuming Kafka events, inspecting Postgres) rather than only compiling.

## What was accepted vs. modified

The large majority of the AI's output was **accepted as written** — the workspace
and service layout, the domain/Kafka event schemas, the order saga state machine,
the Kafka producer/consumer wrapper, the SQL migrations, and the tests all went in
essentially unchanged. This was possible because every task was verified against
live infrastructure (real REST calls, real Kafka produce/consume, inspecting
Postgres) before moving on, so issues were caught early.

The **modifications** were targeted corrections surfaced during that verification
and during manual review — not rewrites:

- **`AppError` → HTTP mapping (modified).** The first version masked the body of
  _every_ 5xx response as `"internal server error"`, which also hid the useful
  detail on `503 Service Unavailable`. Narrowed it to mask only the `Internal`/500
  variant. (Detailed below.)
- **Adminer host port (modified).** Compose first mapped Adminer to `8081`, which
  collides with the Market Service. Caught when `make infra-up` + `make market`
  made `GET /symbols` return Adminer's HTML. Moved Adminer to `8085`.
- **Kafka image (modified).** `docker-compose.yml` initially used `bitnami/kafka`,
  which has been removed from Docker Hub; switched to the official `apache/kafka`
  (KRaft) image.
- **GUIDELINE SELL scenario (modified, user-requested).** The walkthrough assumed
  the pristine demo balance, but the database persists across runs, so a
  documented `SELL 0.5 BTC` could fail once earlier trades had reduced the holding.
  Added a "reset to pristine state" note and a "sell within your holdings" caveat.

## Example of incorrect AI output and how it was handled

**Where:** the shared HTTP error type (`shared/src/error.rs`).

**Symptom.** While verifying the Market Service, requesting a symbol whose pricing
was forced unavailable returned the right status but the wrong body — the real
reason had been swallowed:

```
GET /prices/ETH   (with MARKET_FAIL_SYMBOLS=ETH)
HTTP/1.1 503 Service Unavailable
{"error":"internal server error"}
```

**Root cause.** The AI's first `IntoResponse` implementation masked the message for
_any_ server-error status:

```rust
if status.is_server_error() {
    return (status, Json(json!({ "error": "internal server error" }))).into_response();
}
```

`AppError::Unavailable` maps to `503`, which is a 5xx, so its safe and useful
detail (`"market unavailable for ETH"`) was hidden along with genuine `500`s.

**How it was handled.** I caught it by actually exercising the endpoint (not just
compiling), then narrowed the masking to only the `Internal` variant — which may
wrap sensitive detail and is logged server-side — and let every other variant
(including `503`) surface its message:

```rust
let message = match &self {
    AppError::Internal(e) => {
        tracing::error!(error = %e, "request failed");
        "internal server error".to_string()
    }
    other => other.to_string(),
};
(self.status(), Json(json!({ "error": message }))).into_response()
```

**Verification.** Re-running the request returned
`503 {"error":"market unavailable for ETH"}`, while a genuine `Internal` error
still returns only the generic `500` message. This also improved the saga's
`MARKET_UNAVAILABLE` rejection path, where that detail is meaningful.
