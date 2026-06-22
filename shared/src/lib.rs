//! Shared library for the Mini Exchange Portfolio System.
//!
//! Single source of truth for cross-service contracts and infra helpers.
//! Populated incrementally by the TODO tasks:
//! - `error` / `telemetry` / `http` — HTTP stack scaffolding (Task 3) ✅
//! - `domain` / `events` — domain models + Kafka event schemas (Task 4) ✅
//! - Kafka producer/consumer + sqlx pool helpers — Tasks 5/6

pub mod domain;
pub mod error;
pub mod events;
pub mod http;
pub mod telemetry;

pub use error::{AppError, AppResult};
