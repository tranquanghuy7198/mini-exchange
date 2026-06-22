//! Portfolio Service.
//!
//! REST (Task 8 — read endpoints):
//!   GET /portfolio/{user_id}  — cash balance + holdings (404 if no such user)
//!   GET /orders/{order_id}    — an order with its current saga status (404 if none)
//!
//! Order intake + the choreographed saga are added in Task 9.

use axum::{
    extract::{Path, State},
    routing::get,
    Json, Router,
};
use shared::domain::{Order, Portfolio};
use shared::{db, http, telemetry, AppError, AppResult};
use sqlx::PgPool;
use uuid::Uuid;

mod repo;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init();

    let pool = db::pool_from_env("PORTFOLIO_DATABASE_URL").await?;
    sqlx::migrate!("./migrations").run(&pool).await?;
    tracing::info!("migrations applied");
    repo::seed_demo(&pool).await?;

    let app = http::health_router("portfolio-service")
        .merge(db::readiness_router(pool.clone()))
        .merge(read_routes(pool));
    let addr = http::addr_from_env("PORTFOLIO_BIND", "0.0.0.0:8082");
    http::serve(app, addr).await
}

fn read_routes(pool: PgPool) -> Router {
    Router::new()
        .route("/portfolio/{user_id}", get(get_portfolio))
        .route("/orders/{order_id}", get(get_order))
        .with_state(pool)
}

async fn get_portfolio(
    State(pool): State<PgPool>,
    Path(user_id): Path<Uuid>,
) -> AppResult<Json<Portfolio>> {
    match repo::get_portfolio(&pool, user_id).await? {
        Some(portfolio) => Ok(Json(portfolio)),
        None => Err(AppError::NotFound(format!("no portfolio for user {user_id}"))),
    }
}

async fn get_order(
    State(pool): State<PgPool>,
    Path(order_id): Path<Uuid>,
) -> AppResult<Json<Order>> {
    match repo::get_order(&pool, order_id).await? {
        Some(order) => Ok(Json(order)),
        None => Err(AppError::NotFound(format!("no order {order_id}"))),
    }
}
