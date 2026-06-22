//! Shared library for the Mini Exchange Portfolio System.
//!
//! Single source of truth for cross-service contracts and infra helpers.
//! Populated incrementally by the TODO tasks:
//! - `error` / `telemetry` / `http` — HTTP stack scaffolding (Task 3) ✅
//! - `domain` / `events` — domain models + Kafka event schemas (Task 4) ✅
//! - `db` — Postgres pool + readiness helpers (Task 5) ✅
//! - `kafka` — producer/consumer + topic admin (Task 6) ✅

pub mod db;
pub mod domain;
pub mod error;
pub mod events;
pub mod http;
pub mod kafka;
pub mod signal;
pub mod telemetry;

pub use error::{AppError, AppResult};
