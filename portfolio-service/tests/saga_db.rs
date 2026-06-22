//! Integration tests for the Portfolio saga state machine, against a real
//! Postgres (the `portfolio_test` database from docker-compose `db-init`).
//!
//! These exercise the deterministic, DB-backed core of the saga without Kafka:
//! - successful BUY / SELL and the resulting portfolio updates,
//! - insufficient balance / insufficient asset rejections (+ compensation),
//! - market-failure handling (an incoming `OrderRejected` from the Market Service),
//! - idempotent re-delivery.
//!
//! Marked `#[ignore]` so plain `cargo test` stays green without infra. Run with:
//!     docker compose up -d postgres db-init
//!     cargo test -p portfolio-service -- --ignored
//!
//! Each test uses a fresh random user id, so they're isolated and need no teardown.

use std::str::FromStr;

use portfolio_service::repo::{self, ExecOutcome};
use rust_decimal::Decimal;
use shared::domain::{NewOrder, OrderStatus, RejectionReason, Side, Symbol};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

fn d(s: &str) -> Decimal {
    Decimal::from_str(s).unwrap()
}

fn test_db_url() -> String {
    dotenvy::dotenv().ok();
    std::env::var("PORTFOLIO_TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://exchange:exchange@localhost:5432/portfolio_test".into())
}

/// Connect to the test DB and ensure the schema is present.
async fn setup() -> PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&test_db_url())
        .await
        .expect("connect to portfolio_test (is `docker compose up -d postgres db-init` running?)");
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();
    pool
}

async fn seed_user(pool: &PgPool, user: Uuid, cash: &str) {
    sqlx::query("INSERT INTO users (id) VALUES ($1) ON CONFLICT DO NOTHING")
        .bind(user)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO portfolios (user_id, cash_balance) VALUES ($1, $2)")
        .bind(user)
        .bind(d(cash))
        .execute(pool)
        .await
        .unwrap();
}

async fn set_holding(pool: &PgPool, user: Uuid, symbol: &str, qty: &str) {
    sqlx::query(
        "INSERT INTO holdings (user_id, symbol, quantity) VALUES ($1, $2, $3) \
         ON CONFLICT (user_id, symbol) DO UPDATE SET quantity = EXCLUDED.quantity",
    )
    .bind(user)
    .bind(symbol)
    .bind(d(qty))
    .execute(pool)
    .await
    .unwrap();
}

async fn cash_of(pool: &PgPool, user: Uuid) -> Decimal {
    repo::get_portfolio(pool, user)
        .await
        .unwrap()
        .unwrap()
        .cash_balance
}

async fn holding_of(pool: &PgPool, user: Uuid, symbol: &str) -> Decimal {
    repo::get_portfolio(pool, user)
        .await
        .unwrap()
        .unwrap()
        .holdings
        .into_iter()
        .find(|h| h.symbol.as_str() == symbol)
        .map(|h| h.quantity)
        .unwrap_or(Decimal::ZERO)
}

async fn status_of(pool: &PgPool, order_id: Uuid) -> OrderStatus {
    repo::get_order(pool, order_id)
        .await
        .unwrap()
        .unwrap()
        .status
}

#[tokio::test]
#[ignore = "requires Postgres (docker compose up -d postgres db-init)"]
async fn buy_executes_and_debits_cash_and_credits_asset() {
    let pool = setup().await;
    let user = Uuid::new_v4();
    seed_user(&pool, user, "10000").await;

    let order = repo::create_order(
        &pool,
        &NewOrder {
            user_id: user,
            symbol: Symbol::from("ETH"),
            side: Side::Buy,
            quantity: d("2"),
        },
    )
    .await
    .unwrap();

    let outcome = repo::apply_price_quoted(&pool, order.id, Uuid::new_v4(), d("1000"))
        .await
        .unwrap();

    assert!(matches!(outcome, ExecOutcome::Executed { .. }));
    assert_eq!(status_of(&pool, order.id).await, OrderStatus::Executed);
    assert_eq!(cash_of(&pool, user).await, d("8000")); // 10000 - 2*1000
    assert_eq!(holding_of(&pool, user, "ETH").await, d("2"));
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn buy_with_insufficient_balance_is_rejected_and_leaves_state_untouched() {
    let pool = setup().await;
    let user = Uuid::new_v4();
    seed_user(&pool, user, "100").await;

    let order = repo::create_order(
        &pool,
        &NewOrder {
            user_id: user,
            symbol: Symbol::from("ETH"),
            side: Side::Buy,
            quantity: d("1"),
        },
    )
    .await
    .unwrap();

    let outcome = repo::apply_price_quoted(&pool, order.id, Uuid::new_v4(), d("1000"))
        .await
        .unwrap();

    assert!(matches!(
        outcome,
        ExecOutcome::Rejected {
            reason: RejectionReason::InsufficientBalance,
            ..
        }
    ));
    assert_eq!(status_of(&pool, order.id).await, OrderStatus::Rejected);
    // compensation: nothing was deducted, no asset credited
    assert_eq!(cash_of(&pool, user).await, d("100"));
    assert_eq!(holding_of(&pool, user, "ETH").await, Decimal::ZERO);
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn sell_executes_and_credits_cash_and_debits_asset() {
    let pool = setup().await;
    let user = Uuid::new_v4();
    seed_user(&pool, user, "0").await;
    set_holding(&pool, user, "SOL", "5").await;

    let order = repo::create_order(
        &pool,
        &NewOrder {
            user_id: user,
            symbol: Symbol::from("SOL"),
            side: Side::Sell,
            quantity: d("2"),
        },
    )
    .await
    .unwrap();

    let outcome = repo::apply_price_quoted(&pool, order.id, Uuid::new_v4(), d("100"))
        .await
        .unwrap();

    assert!(matches!(outcome, ExecOutcome::Executed { .. }));
    assert_eq!(status_of(&pool, order.id).await, OrderStatus::Executed);
    assert_eq!(cash_of(&pool, user).await, d("200")); // 2*100
    assert_eq!(holding_of(&pool, user, "SOL").await, d("3")); // 5 - 2
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn sell_with_insufficient_asset_is_rejected() {
    let pool = setup().await;
    let user = Uuid::new_v4();
    seed_user(&pool, user, "0").await;
    set_holding(&pool, user, "SOL", "1").await;

    let order = repo::create_order(
        &pool,
        &NewOrder {
            user_id: user,
            symbol: Symbol::from("SOL"),
            side: Side::Sell,
            quantity: d("5"),
        },
    )
    .await
    .unwrap();

    let outcome = repo::apply_price_quoted(&pool, order.id, Uuid::new_v4(), d("100"))
        .await
        .unwrap();

    assert!(matches!(
        outcome,
        ExecOutcome::Rejected {
            reason: RejectionReason::InsufficientAsset,
            ..
        }
    ));
    assert_eq!(status_of(&pool, order.id).await, OrderStatus::Rejected);
    assert_eq!(holding_of(&pool, user, "SOL").await, d("1")); // unchanged
    assert_eq!(cash_of(&pool, user).await, d("0"));
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn market_failure_rejection_marks_order_rejected() {
    // Simulates the Market Service emitting OrderRejected (unknown symbol /
    // market unavailable) for a still-open order.
    let pool = setup().await;
    let user = Uuid::new_v4();
    seed_user(&pool, user, "10000").await;

    let order = repo::create_order(
        &pool,
        &NewOrder {
            user_id: user,
            symbol: Symbol::from("ZZZ"),
            side: Side::Buy,
            quantity: d("1"),
        },
    )
    .await
    .unwrap();
    assert_eq!(status_of(&pool, order.id).await, OrderStatus::Created);

    repo::apply_order_rejected(
        &pool,
        order.id,
        Uuid::new_v4(),
        "market unavailable for ZZZ",
    )
    .await
    .unwrap();

    let rejected = repo::get_order(&pool, order.id).await.unwrap().unwrap();
    assert_eq!(rejected.status, OrderStatus::Rejected);
    assert_eq!(
        rejected.reason.as_deref(),
        Some("market unavailable for ZZZ")
    );
    assert_eq!(cash_of(&pool, user).await, d("10000")); // untouched
}

#[tokio::test]
#[ignore = "requires Postgres"]
async fn duplicate_price_quoted_is_idempotent() {
    let pool = setup().await;
    let user = Uuid::new_v4();
    seed_user(&pool, user, "10000").await;

    let order = repo::create_order(
        &pool,
        &NewOrder {
            user_id: user,
            symbol: Symbol::from("ETH"),
            side: Side::Buy,
            quantity: d("2"),
        },
    )
    .await
    .unwrap();

    let event_id = Uuid::new_v4();
    let first = repo::apply_price_quoted(&pool, order.id, event_id, d("1000"))
        .await
        .unwrap();
    assert!(matches!(first, ExecOutcome::Executed { .. }));
    assert_eq!(cash_of(&pool, user).await, d("8000"));

    // Same event_id again → deduped by the idempotency ledger.
    let replay_same = repo::apply_price_quoted(&pool, order.id, event_id, d("1000"))
        .await
        .unwrap();
    assert!(matches!(replay_same, ExecOutcome::Skipped));

    // Fresh event_id but order already terminal → status guard skips.
    let replay_fresh = repo::apply_price_quoted(&pool, order.id, Uuid::new_v4(), d("1000"))
        .await
        .unwrap();
    assert!(matches!(replay_fresh, ExecOutcome::Skipped));

    // Cash was debited exactly once.
    assert_eq!(cash_of(&pool, user).await, d("8000"));
    assert_eq!(holding_of(&pool, user, "ETH").await, d("2"));
}
