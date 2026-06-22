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

<!-- To be completed after manual review/testing. -->

## Example of incorrect AI output and how it was handled

<!-- To be completed after manual review/testing. -->
