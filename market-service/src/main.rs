//! Market Service.
//!
//! REST (mocked prices):
//!   GET /symbols           — listed symbols
//!   GET /prices            — current price of every listed symbol
//!   GET /prices/{symbol}   — current price of one symbol (404 unknown, 503 unavailable)
//!
//! Saga participation (Kafka): consumes `OrderCreated`, and emits either
//! `PriceQuoted` (priced) or `OrderRejected` (unknown symbol / market unavailable).

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    routing::get,
    Json, Router,
};
use shared::domain::{Price, RejectionReason, Symbol};
use shared::events::{EventEnvelope, OrderCreated, OrderRejected, PriceQuoted, SagaEvent};
use shared::kafka::{EventConsumer, EventProducer, RawEvent};
use shared::{http, telemetry, AppError, AppResult};

mod price;
use price::{PriceEngine, Quote};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init();

    let engine = Arc::new(PriceEngine::from_env());
    let producer = EventProducer::from_env()?;

    // Saga consumer: price OrderCreated events. Runs alongside the HTTP server;
    // both observe the same shutdown signal.
    let consumer = EventConsumer::from_env("market-service", &[OrderCreated::TOPIC])?;
    let c_engine = engine.clone();
    let c_producer = producer.clone();
    let consumer_task = tokio::spawn(async move {
        consumer
            .run(|raw| {
                let engine = c_engine.clone();
                let producer = c_producer.clone();
                async move { on_order_created(raw, &engine, &producer).await }
            })
            .await;
    });

    let app = http::health_router("market-service").merge(rest_routes(engine));
    let addr = http::addr_from_env("MARKET_BIND", "0.0.0.0:8081");
    http::serve(app, addr).await?;

    consumer_task.await?;
    Ok(())
}

fn rest_routes(engine: Arc<PriceEngine>) -> Router {
    Router::new()
        .route("/symbols", get(get_symbols))
        .route("/prices", get(get_prices))
        .route("/prices/{symbol}", get(get_price))
        .with_state(engine)
}

async fn get_symbols(State(engine): State<Arc<PriceEngine>>) -> Json<Vec<Symbol>> {
    Json(engine.symbols())
}

async fn get_prices(State(engine): State<Arc<PriceEngine>>) -> Json<Vec<Price>> {
    Json(engine.all_prices())
}

async fn get_price(
    State(engine): State<Arc<PriceEngine>>,
    Path(symbol): Path<String>,
) -> AppResult<Json<Price>> {
    let symbol = Symbol::from(symbol.as_str());
    match engine.price(&symbol) {
        Some(price) => Ok(Json(price)),
        None => match engine.quote(&symbol) {
            Quote::Unavailable => Err(AppError::Unavailable(format!(
                "market unavailable for {symbol}"
            ))),
            _ => Err(AppError::NotFound(format!("unknown symbol {symbol}"))),
        },
    }
}

/// Handle an `OrderCreated`: price the symbol and emit the next saga event.
async fn on_order_created(
    raw: RawEvent,
    engine: &PriceEngine,
    producer: &EventProducer,
) -> anyhow::Result<()> {
    let event = raw.deserialize::<OrderCreated>()?;
    let order_id = event.order_id;
    let symbol = event.payload.symbol;

    match engine.quote(&symbol) {
        Quote::Priced(price) => {
            producer
                .send(&EventEnvelope::new(order_id, PriceQuoted { symbol, price }))
                .await?;
            tracing::info!(%order_id, %price, "priced order -> PriceQuoted");
        }
        Quote::Unknown => {
            let detail = format!("unknown symbol {symbol}");
            producer
                .send(&EventEnvelope::new(
                    order_id,
                    OrderRejected {
                        reason: RejectionReason::UnknownSymbol,
                        detail,
                    },
                ))
                .await?;
            tracing::warn!(%order_id, %symbol, "rejected order -> unknown symbol");
        }
        Quote::Unavailable => {
            let detail = format!("market unavailable for {symbol}");
            producer
                .send(&EventEnvelope::new(
                    order_id,
                    OrderRejected {
                        reason: RejectionReason::MarketUnavailable,
                        detail,
                    },
                ))
                .await?;
            tracing::warn!(%order_id, %symbol, "rejected order -> market unavailable");
        }
    }
    Ok(())
}
