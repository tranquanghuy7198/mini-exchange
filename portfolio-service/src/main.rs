//! Portfolio Service — portfolio/order REST + the choreographed order saga
//! (Tasks 8/9). Task 5: connects to its own Postgres DB, runs migrations, and
//! exposes `GET /health` (liveness) + `GET /ready` (DB readiness).

use shared::{db, http, telemetry};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init();

    let pool = db::pool_from_env("PORTFOLIO_DATABASE_URL").await?;
    sqlx::migrate!("./migrations").run(&pool).await?;
    tracing::info!("migrations applied");

    let app = http::health_router("portfolio-service").merge(db::readiness_router(pool));
    let addr = http::addr_from_env("PORTFOLIO_BIND", "0.0.0.0:8082");
    http::serve(app, addr).await
}
