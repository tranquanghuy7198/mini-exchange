//! Postgres connectivity shared by DB-backed services.
//!
//! Database-per-service: each service connects to its own database (e.g.
//! `portfolio`, `audit`) so migrations and tables never collide. Migrations
//! themselves live in each service crate (run via `sqlx::migrate!`).

use anyhow::Context;
use axum::{
    extract::State, http::StatusCode, response::IntoResponse, routing::get, Json, Router,
};
use serde_json::json;
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::time::Duration;

/// Create a connection pool for `database_url`, eagerly establishing one
/// connection so a misconfiguration fails fast at startup.
pub async fn init_pool(database_url: &str) -> anyhow::Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(Duration::from_secs(5))
        .connect(database_url)
        .await
        .context("failed to connect to Postgres")
}

/// Read a database URL from `var` (loading `.env` first) and build a pool.
pub async fn pool_from_env(var: &str) -> anyhow::Result<PgPool> {
    dotenvy::dotenv().ok();
    let url = std::env::var(var).with_context(|| format!("{var} must be set"))?;
    init_pool(&url).await
}

/// Liveness/readiness healthcheck query. `true` when the DB answers `SELECT 1`.
pub async fn ping(pool: &PgPool) -> bool {
    sqlx::query("SELECT 1").execute(pool).await.is_ok()
}

/// A router exposing `GET /ready` → 200 `{"status":"ready"}` when the database
/// is reachable, else 503. Merge onto a service's `health_router`.
pub fn readiness_router(pool: PgPool) -> Router {
    Router::new().route("/ready", get(ready)).with_state(pool)
}

async fn ready(State(pool): State<PgPool>) -> impl IntoResponse {
    if ping(&pool).await {
        (StatusCode::OK, Json(json!({ "status": "ready" })))
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "unavailable" })),
        )
    }
}
