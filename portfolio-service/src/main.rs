//! Portfolio Service — portfolio/order REST + the choreographed order saga
//! (Tasks 8/9). Task 3: serves `GET /health` to validate the HTTP stack.

use shared::{http, telemetry};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init();
    let app = http::health_router("portfolio-service");
    let addr = http::addr_from_env("PORTFOLIO_BIND", "0.0.0.0:8082");
    http::serve(app, addr).await
}
