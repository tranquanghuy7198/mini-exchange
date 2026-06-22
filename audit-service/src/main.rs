//! Audit Service (optional).
//!
//! A pure consumer: subscribes to every saga topic and records each event into
//! `audit_events` (idempotent via UNIQUE `event_id`). Emits no domain events.
//!
//! REST:
//!   GET /audit/orders/{order_id} — all recorded events for an order, in order
//!   GET /health, GET /ready

use axum::{
    extract::{Path, State},
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;
use shared::events::topics;
use shared::kafka::{EventConsumer, RawEvent};
use shared::{db, http, telemetry, AppResult};
use sqlx::PgPool;
use uuid::Uuid;

mod repo;

/// Minimal view of any saga event envelope (payload kept as opaque JSON).
#[derive(Deserialize)]
struct IncomingEnvelope {
    event_id: Uuid,
    order_id: Uuid,
    occurred_at: DateTime<Utc>,
    payload: Value,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init();

    let pool = db::pool_from_env("AUDIT_DATABASE_URL").await?;
    sqlx::migrate!("./migrations").run(&pool).await?;
    tracing::info!("migrations applied");

    // Subscribe to all saga topics; record every event. Runs alongside HTTP.
    let consumer = EventConsumer::from_env(
        "audit-service",
        &[
            topics::ORDER_CREATED,
            topics::PRICE_QUOTED,
            topics::ORDER_EXECUTED,
            topics::ORDER_REJECTED,
        ],
    )?;
    let c_pool = pool.clone();
    let consumer_task = tokio::spawn(async move {
        consumer
            .run(|raw| {
                let pool = c_pool.clone();
                async move { record_event(raw, &pool).await }
            })
            .await;
    });

    let app = http::health_router("audit-service")
        .merge(db::readiness_router(pool.clone()))
        .merge(read_routes(pool));
    let addr = http::addr_from_env("AUDIT_BIND", "0.0.0.0:8083");
    http::serve(app, addr).await?;

    consumer_task.await?;
    Ok(())
}

/// Parse and persist one consumed saga event.
async fn record_event(raw: RawEvent, pool: &PgPool) -> anyhow::Result<()> {
    let event_type = topic_to_event_type(&raw.topic);
    let env: IncomingEnvelope = serde_json::from_slice(&raw.payload)
        .map_err(|e| anyhow::anyhow!("failed to parse event on {}: {e}", raw.topic))?;

    let inserted = repo::record(
        pool,
        env.event_id,
        env.order_id,
        event_type,
        &env.payload,
        env.occurred_at,
    )
    .await?;

    if inserted {
        tracing::info!(order_id = %env.order_id, event_type, "audited event");
    } else {
        tracing::debug!(event_id = %env.event_id, "duplicate event ignored");
    }
    Ok(())
}

/// Map a topic to the audit `event_type` label (SCREAMING_SNAKE_CASE).
fn topic_to_event_type(topic: &str) -> &'static str {
    match topic {
        topics::ORDER_CREATED => "ORDER_CREATED",
        topics::PRICE_QUOTED => "PRICE_QUOTED",
        topics::ORDER_EXECUTED => "ORDER_EXECUTED",
        topics::ORDER_REJECTED => "ORDER_REJECTED",
        _ => "UNKNOWN",
    }
}

fn read_routes(pool: PgPool) -> Router {
    Router::new()
        .route("/audit/orders/{order_id}", get(list_for_order))
        .with_state(pool)
}

async fn list_for_order(
    State(pool): State<PgPool>,
    Path(order_id): Path<Uuid>,
) -> AppResult<Json<Vec<repo::AuditEvent>>> {
    Ok(Json(repo::list_for_order(&pool, order_id).await?))
}
