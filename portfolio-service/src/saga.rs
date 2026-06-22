//! Saga consumer handlers for the Portfolio Service.
//!
//! Consumes `PriceQuoted` and `OrderRejected`, applies the local state change
//! transactionally (see [`crate::repo`]), and — for `PriceQuoted` — publishes
//! the next saga event (`OrderExecuted` / `OrderRejected`) after commit.

use shared::events::{
    EventEnvelope, OrderExecuted, OrderRejected, OrderRejectedEvent, PriceQuotedEvent,
};
use shared::kafka::EventProducer;
use sqlx::PgPool;

use crate::repo::{self, ExecOutcome};

/// Handle a `PriceQuoted`: execute or reject the order, then emit the outcome.
pub async fn on_price_quoted(
    pool: &PgPool,
    producer: &EventProducer,
    env: PriceQuotedEvent,
) -> anyhow::Result<()> {
    let order_id = env.order_id;
    match repo::apply_price_quoted(pool, order_id, env.event_id, env.payload.price).await? {
        ExecOutcome::Executed {
            user_id,
            symbol,
            side,
            quantity,
            price,
        } => {
            producer
                .send(&EventEnvelope::new(
                    order_id,
                    OrderExecuted {
                        user_id,
                        symbol,
                        side,
                        quantity,
                        price,
                    },
                ))
                .await?;
            tracing::info!(%order_id, %price, "order EXECUTED");
        }
        ExecOutcome::Rejected { reason, detail } => {
            producer
                .send(&EventEnvelope::new(
                    order_id,
                    OrderRejected {
                        reason,
                        detail: detail.clone(),
                    },
                ))
                .await?;
            tracing::info!(%order_id, ?reason, %detail, "order REJECTED");
        }
        ExecOutcome::Skipped => {
            tracing::debug!(%order_id, "price-quoted skipped (duplicate/late/unknown)");
        }
    }
    Ok(())
}

/// Handle an incoming `OrderRejected` (e.g. from the Market Service): mark a
/// still-open order rejected. Compensation only — emits nothing.
pub async fn on_order_rejected(pool: &PgPool, env: OrderRejectedEvent) -> anyhow::Result<()> {
    repo::apply_order_rejected(pool, env.order_id, env.event_id, &env.payload.detail).await?;
    tracing::debug!(order_id = %env.order_id, "processed OrderRejected");
    Ok(())
}
