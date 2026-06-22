//! Database access for the Portfolio Service.
//!
//! Uses sqlx's runtime-checked API (not the compile-time `query!` macro) so the
//! crate builds without a live database. The schema is owned by this service
//! (Task 5 migrations).

use std::str::FromStr;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use shared::domain::{
    Holding, NewOrder, Order, OrderStatus, Portfolio, RejectionReason, Side, Symbol, UserId,
};
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

/// A well-known demo user, seeded on startup so the API is usable immediately.
pub const DEMO_USER_ID: Uuid = Uuid::from_u128(0x1111_1111_1111_1111_1111_1111_1111_1111);
const DEMO_STARTING_CASH: &str = "100000.00";
const DEMO_HOLDING_SYMBOL: &str = "BTC";
const DEMO_HOLDING_QTY: &str = "1.0";

/// Fetch a user's portfolio (cash + non-zero holdings). `None` if the user has
/// no portfolio row.
pub async fn get_portfolio(pool: &PgPool, user_id: Uuid) -> anyhow::Result<Option<Portfolio>> {
    let cash: Option<Decimal> =
        sqlx::query_scalar("SELECT cash_balance FROM portfolios WHERE user_id = $1")
            .bind(user_id)
            .fetch_optional(pool)
            .await?;

    let Some(cash_balance) = cash else {
        return Ok(None);
    };

    let rows: Vec<(String, Decimal)> = sqlx::query_as(
        "SELECT symbol, quantity FROM holdings \
         WHERE user_id = $1 AND quantity > 0 ORDER BY symbol",
    )
    .bind(user_id)
    .fetch_all(pool)
    .await?;

    let holdings = rows
        .into_iter()
        .map(|(symbol, quantity)| Holding {
            symbol: Symbol::from(symbol.as_str()),
            quantity,
        })
        .collect();

    Ok(Some(Portfolio {
        user_id,
        cash_balance,
        holdings,
    }))
}

/// Row shape for the `orders` table.
#[derive(FromRow)]
struct OrderRow {
    id: Uuid,
    user_id: Uuid,
    symbol: String,
    side: String,
    quantity: Decimal,
    status: String,
    price: Option<Decimal>,
    reason: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl OrderRow {
    fn into_domain(self) -> anyhow::Result<Order> {
        Ok(Order {
            id: self.id,
            user_id: self.user_id,
            symbol: Symbol::from(self.symbol.as_str()),
            side: Side::from_str(&self.side)?,
            quantity: self.quantity,
            status: OrderStatus::from_str(&self.status)?,
            price: self.price,
            reason: self.reason,
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }
}

/// Fetch a single order by id. `None` if it doesn't exist.
pub async fn get_order(pool: &PgPool, order_id: Uuid) -> anyhow::Result<Option<Order>> {
    let row: Option<OrderRow> = sqlx::query_as(
        "SELECT id, user_id, symbol, side, quantity, status, price, reason, \
         created_at, updated_at FROM orders WHERE id = $1",
    )
    .bind(order_id)
    .fetch_optional(pool)
    .await?;

    row.map(OrderRow::into_domain).transpose()
}

/// Whether a portfolio exists for `user_id` (i.e. the user can trade).
pub async fn portfolio_exists(pool: &PgPool, user_id: Uuid) -> anyhow::Result<bool> {
    let exists: Option<i32> = sqlx::query_scalar("SELECT 1 FROM portfolios WHERE user_id = $1")
        .bind(user_id)
        .fetch_optional(pool)
        .await?;
    Ok(exists.is_some())
}

/// Insert a new order in `CREATED` state and return it.
pub async fn create_order(pool: &PgPool, new: &NewOrder) -> anyhow::Result<Order> {
    let id = Uuid::new_v4();
    let row: OrderRow = sqlx::query_as(
        "INSERT INTO orders (id, user_id, symbol, side, quantity, status) \
         VALUES ($1, $2, $3, $4, $5, 'CREATED') \
         RETURNING id, user_id, symbol, side, quantity, status, price, reason, \
                   created_at, updated_at",
    )
    .bind(id)
    .bind(new.user_id)
    .bind(new.symbol.as_str())
    .bind(new.side.as_str())
    .bind(new.quantity)
    .fetch_one(pool)
    .await?;
    row.into_domain()
}

/// Result of applying a `PriceQuoted` to an order.
pub enum ExecOutcome {
    /// Order executed; carries the data needed to emit `OrderExecuted`.
    Executed {
        user_id: UserId,
        symbol: Symbol,
        side: Side,
        quantity: Decimal,
        price: Decimal,
    },
    /// Order rejected (insufficient funds/asset); emit `OrderRejected`.
    Rejected {
        reason: RejectionReason,
        detail: String,
    },
    /// Duplicate/late/unknown — nothing to do, nothing to emit.
    Skipped,
}

/// Record an event_id in the idempotency ledger within `tx`. Returns `true` if
/// it was newly inserted (i.e. NOT a duplicate), `false` if already processed.
async fn mark_processed(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    event_id: Uuid,
) -> anyhow::Result<bool> {
    let res =
        sqlx::query("INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING")
            .bind(event_id)
            .execute(&mut **tx)
            .await?;
    Ok(res.rows_affected() == 1)
}

/// Apply a `PriceQuoted` to an order, atomically and idempotently:
/// - dedupe by `event_id`;
/// - only act if the order is still `CREATED`;
/// - BUY: lock the portfolio row, deduct cash + add asset if affordable;
/// - SELL: lock the holding, deduct asset + add cash if held;
/// - on shortfall, mark the order `REJECTED`.
///
/// All within one transaction; the outcome tells the caller what (if anything)
/// to publish after commit.
pub async fn apply_price_quoted(
    pool: &PgPool,
    order_id: Uuid,
    event_id: Uuid,
    price: Decimal,
) -> anyhow::Result<ExecOutcome> {
    let mut tx = pool.begin().await?;

    if !mark_processed(&mut tx, event_id).await? {
        tx.rollback().await?;
        return Ok(ExecOutcome::Skipped);
    }

    let order: Option<OrderRow> = sqlx::query_as(
        "SELECT id, user_id, symbol, side, quantity, status, price, reason, \
         created_at, updated_at FROM orders WHERE id = $1 FOR UPDATE",
    )
    .bind(order_id)
    .fetch_optional(&mut *tx)
    .await?;

    // Unknown order or already in a terminal state → idempotent no-op. Commit so
    // the event stays marked processed.
    let Some(order) = order else {
        tx.commit().await?;
        return Ok(ExecOutcome::Skipped);
    };
    let order = order.into_domain()?;
    if order.status != OrderStatus::Created {
        tx.commit().await?;
        return Ok(ExecOutcome::Skipped);
    }

    let qty = order.quantity;
    let value = price * qty;

    let outcome = match order.side {
        Side::Buy => {
            let cash: Decimal = sqlx::query_scalar(
                "SELECT cash_balance FROM portfolios WHERE user_id = $1 FOR UPDATE",
            )
            .bind(order.user_id)
            .fetch_one(&mut *tx)
            .await?;
            if cash >= value {
                sqlx::query(
                    "UPDATE portfolios SET cash_balance = cash_balance - $1, updated_at = now() \
                     WHERE user_id = $2",
                )
                .bind(value)
                .bind(order.user_id)
                .execute(&mut *tx)
                .await?;
                sqlx::query(
                    "INSERT INTO holdings (user_id, symbol, quantity) VALUES ($1, $2, $3) \
                     ON CONFLICT (user_id, symbol) \
                     DO UPDATE SET quantity = holdings.quantity + EXCLUDED.quantity",
                )
                .bind(order.user_id)
                .bind(order.symbol.as_str())
                .bind(qty)
                .execute(&mut *tx)
                .await?;
                mark_executed(&mut tx, order_id, price).await?;
                ExecOutcome::Executed {
                    user_id: order.user_id,
                    symbol: order.symbol,
                    side: order.side,
                    quantity: qty,
                    price,
                }
            } else {
                let detail = format!("insufficient balance: need {value}, have {cash}");
                mark_rejected(&mut tx, order_id, &detail).await?;
                ExecOutcome::Rejected {
                    reason: RejectionReason::InsufficientBalance,
                    detail,
                }
            }
        }
        Side::Sell => {
            let held: Decimal = sqlx::query_scalar(
                "SELECT quantity FROM holdings WHERE user_id = $1 AND symbol = $2 FOR UPDATE",
            )
            .bind(order.user_id)
            .bind(order.symbol.as_str())
            .fetch_optional(&mut *tx)
            .await?
            .unwrap_or(Decimal::ZERO);
            if held >= qty {
                sqlx::query(
                    "UPDATE holdings SET quantity = quantity - $1 WHERE user_id = $2 AND symbol = $3",
                )
                .bind(qty)
                .bind(order.user_id)
                .bind(order.symbol.as_str())
                .execute(&mut *tx)
                .await?;
                sqlx::query(
                    "UPDATE portfolios SET cash_balance = cash_balance + $1, updated_at = now() \
                     WHERE user_id = $2",
                )
                .bind(value)
                .bind(order.user_id)
                .execute(&mut *tx)
                .await?;
                mark_executed(&mut tx, order_id, price).await?;
                ExecOutcome::Executed {
                    user_id: order.user_id,
                    symbol: order.symbol,
                    side: order.side,
                    quantity: qty,
                    price,
                }
            } else {
                let detail = format!(
                    "insufficient asset: need {qty} {}, have {held}",
                    order.symbol
                );
                mark_rejected(&mut tx, order_id, &detail).await?;
                ExecOutcome::Rejected {
                    reason: RejectionReason::InsufficientAsset,
                    detail,
                }
            }
        }
    };

    tx.commit().await?;
    Ok(outcome)
}

/// Apply an incoming `OrderRejected` (typically from the Market Service, e.g.
/// unknown symbol / market unavailable). Compensation here is trivial — no cash
/// was reserved — so we just move a still-`CREATED` order to `REJECTED`.
/// Idempotent (dedupe by event_id; no-op if already terminal).
pub async fn apply_order_rejected(
    pool: &PgPool,
    order_id: Uuid,
    event_id: Uuid,
    detail: &str,
) -> anyhow::Result<()> {
    let mut tx = pool.begin().await?;

    if !mark_processed(&mut tx, event_id).await? {
        tx.rollback().await?;
        return Ok(());
    }

    let status: Option<String> =
        sqlx::query_scalar("SELECT status FROM orders WHERE id = $1 FOR UPDATE")
            .bind(order_id)
            .fetch_optional(&mut *tx)
            .await?;

    if status.as_deref() == Some(OrderStatus::Created.as_str()) {
        mark_rejected(&mut tx, order_id, detail).await?;
    }
    // Unknown order, or already EXECUTED/REJECTED → no-op.

    tx.commit().await?;
    Ok(())
}

async fn mark_executed(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    order_id: Uuid,
    price: Decimal,
) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE orders SET status = 'EXECUTED', price = $1, updated_at = now() WHERE id = $2",
    )
    .bind(price)
    .bind(order_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn mark_rejected(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    order_id: Uuid,
    detail: &str,
) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE orders SET status = 'REJECTED', reason = $1, updated_at = now() WHERE id = $2",
    )
    .bind(detail)
    .bind(order_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Seed the demo user, portfolio (starting cash), and one holding. Idempotent —
/// safe to run on every startup.
pub async fn seed_demo(pool: &PgPool) -> anyhow::Result<()> {
    let cash = Decimal::from_str(DEMO_STARTING_CASH)?;
    let qty = Decimal::from_str(DEMO_HOLDING_QTY)?;

    sqlx::query("INSERT INTO users (id) VALUES ($1) ON CONFLICT DO NOTHING")
        .bind(DEMO_USER_ID)
        .execute(pool)
        .await?;

    sqlx::query(
        "INSERT INTO portfolios (user_id, cash_balance) VALUES ($1, $2) \
         ON CONFLICT DO NOTHING",
    )
    .bind(DEMO_USER_ID)
    .bind(cash)
    .execute(pool)
    .await?;

    sqlx::query(
        "INSERT INTO holdings (user_id, symbol, quantity) VALUES ($1, $2, $3) \
         ON CONFLICT DO NOTHING",
    )
    .bind(DEMO_USER_ID)
    .bind(DEMO_HOLDING_SYMBOL)
    .bind(qty)
    .execute(pool)
    .await?;

    tracing::info!(user_id = %DEMO_USER_ID, "demo user seeded");
    Ok(())
}
