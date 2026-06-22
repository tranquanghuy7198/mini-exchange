//! Database access for the Audit Service. Owns the `audit_events` table.

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use sqlx::types::Json;
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

/// A recorded saga event, as returned by the read endpoint.
#[derive(Serialize, FromRow)]
pub struct AuditEvent {
    pub event_id: Uuid,
    pub order_id: Uuid,
    pub event_type: String,
    pub payload: Json<Value>,
    pub occurred_at: DateTime<Utc>,
    pub recorded_at: DateTime<Utc>,
}

/// Record an event. Idempotent: a duplicate `event_id` (redelivery) is ignored.
/// Returns `true` if a new row was inserted.
pub async fn record(
    pool: &PgPool,
    event_id: Uuid,
    order_id: Uuid,
    event_type: &str,
    payload: &Value,
    occurred_at: DateTime<Utc>,
) -> anyhow::Result<bool> {
    let res = sqlx::query(
        "INSERT INTO audit_events (event_id, order_id, event_type, payload, occurred_at) \
         VALUES ($1, $2, $3, $4, $5) ON CONFLICT (event_id) DO NOTHING",
    )
    .bind(event_id)
    .bind(order_id)
    .bind(event_type)
    .bind(Json(payload))
    .bind(occurred_at)
    .execute(pool)
    .await?;
    Ok(res.rows_affected() == 1)
}

/// List all events for an order, in the order they were recorded.
pub async fn list_for_order(pool: &PgPool, order_id: Uuid) -> anyhow::Result<Vec<AuditEvent>> {
    let rows = sqlx::query_as::<_, AuditEvent>(
        "SELECT event_id, order_id, event_type, payload, occurred_at, recorded_at \
         FROM audit_events WHERE order_id = $1 ORDER BY id",
    )
    .bind(order_id)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}
