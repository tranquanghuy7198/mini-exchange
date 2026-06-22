//! Database access for the Portfolio Service.
//!
//! Uses sqlx's runtime-checked API (not the compile-time `query!` macro) so the
//! crate builds without a live database. The schema is owned by this service
//! (Task 5 migrations).

use std::str::FromStr;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use shared::domain::{Holding, Order, OrderStatus, Portfolio, Side, Symbol};
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
