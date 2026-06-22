//! Portfolio Service.
//!
//! REST:
//!   GET  /portfolio/{user_id}  — cash balance + holdings (404 if no such user)
//!   GET  /orders/{order_id}    — an order with its current saga status
//!   POST /orders               — place a market order (202 Accepted, async result)
//!
//! Saga (SAGA choreography over Kafka): on `POST /orders` the order is persisted
//! as `CREATED` and `OrderCreated` is emitted. A background consumer reacts to
//! `PriceQuoted` (execute/reject + emit outcome) and `OrderRejected` (mark a
//! still-open order rejected). State changes are transactional and idempotent.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};
use shared::domain::{NewOrder, Order, Portfolio};
use shared::events::{EventEnvelope, OrderCreated, OrderRejected, PriceQuoted, SagaEvent};
use shared::kafka::{EventConsumer, EventProducer, RawEvent};
use shared::{db, http, telemetry, AppError, AppResult};
use sqlx::PgPool;
use uuid::Uuid;

use portfolio_service::{repo, saga};

/// Shared state for HTTP handlers: DB pool + Kafka producer.
#[derive(Clone)]
struct AppState {
    pool: PgPool,
    producer: EventProducer,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init();

    let pool = db::pool_from_env("PORTFOLIO_DATABASE_URL").await?;
    sqlx::migrate!("./migrations").run(&pool).await?;
    tracing::info!("migrations applied");
    repo::seed_demo(&pool).await?;

    let producer = EventProducer::from_env()?;

    // Saga consumer: react to PriceQuoted and OrderRejected. Runs alongside the
    // HTTP server; both observe the same shutdown signal.
    let consumer = EventConsumer::from_env(
        "portfolio-service",
        &[PriceQuoted::TOPIC, OrderRejected::TOPIC],
    )?;
    let c_pool = pool.clone();
    let c_producer = producer.clone();
    let consumer_task = tokio::spawn(async move {
        consumer
            .run(|raw| {
                let pool = c_pool.clone();
                let producer = c_producer.clone();
                async move { dispatch(raw, &pool, &producer).await }
            })
            .await;
    });

    let state = AppState {
        pool: pool.clone(),
        producer,
    };
    let app = http::health_router("portfolio-service")
        .merge(db::readiness_router(pool))
        .merge(api_routes(state));
    let addr = http::addr_from_env("PORTFOLIO_BIND", "0.0.0.0:8082");
    http::serve(app, addr).await?;

    consumer_task.await?;
    Ok(())
}

/// Route a consumed Kafka message to the right saga handler by topic.
async fn dispatch(raw: RawEvent, pool: &PgPool, producer: &EventProducer) -> anyhow::Result<()> {
    match raw.topic.as_str() {
        PriceQuoted::TOPIC => saga::on_price_quoted(pool, producer, raw.deserialize()?).await,
        OrderRejected::TOPIC => saga::on_order_rejected(pool, raw.deserialize()?).await,
        other => {
            tracing::warn!(topic = other, "received message on unexpected topic");
            Ok(())
        }
    }
}

fn api_routes(state: AppState) -> Router {
    Router::new()
        .route("/portfolio/{user_id}", get(get_portfolio))
        .route("/orders/{order_id}", get(get_order))
        .route("/orders", post(create_order))
        .with_state(state)
}

async fn get_portfolio(
    State(state): State<AppState>,
    Path(user_id): Path<Uuid>,
) -> AppResult<Json<Portfolio>> {
    match repo::get_portfolio(&state.pool, user_id).await? {
        Some(portfolio) => Ok(Json(portfolio)),
        None => Err(AppError::NotFound(format!(
            "no portfolio for user {user_id}"
        ))),
    }
}

async fn get_order(
    State(state): State<AppState>,
    Path(order_id): Path<Uuid>,
) -> AppResult<Json<Order>> {
    match repo::get_order(&state.pool, order_id).await? {
        Some(order) => Ok(Json(order)),
        None => Err(AppError::NotFound(format!("no order {order_id}"))),
    }
}

/// Place a market order. Persists it as `CREATED`, emits `OrderCreated`, and
/// returns `202 Accepted` — the BUY/SELL result is resolved asynchronously by
/// the saga (poll `GET /orders/{id}`).
async fn create_order(
    State(state): State<AppState>,
    Json(new): Json<NewOrder>,
) -> AppResult<(StatusCode, Json<Value>)> {
    new.validate()?;

    if !repo::portfolio_exists(&state.pool, new.user_id).await? {
        return Err(AppError::NotFound(format!(
            "no portfolio for user {}",
            new.user_id
        )));
    }

    let order = repo::create_order(&state.pool, &new).await?;

    state
        .producer
        .send(&EventEnvelope::new(
            order.id,
            OrderCreated {
                user_id: order.user_id,
                symbol: order.symbol.clone(),
                side: order.side,
                quantity: order.quantity,
            },
        ))
        .await?;
    tracing::info!(order_id = %order.id, side = ?order.side, "order accepted -> OrderCreated");

    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "order_id": order.id, "status": order.status })),
    ))
}
