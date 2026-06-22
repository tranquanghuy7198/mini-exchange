//! Market Service — symbols/prices over REST + prices orders via Kafka (Task 7).
//! Task 3: serves `GET /health` to validate the HTTP stack end to end.

use shared::{http, telemetry};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init();
    let app = http::health_router("market-service");
    let addr = http::addr_from_env("MARKET_BIND", "0.0.0.0:8081");
    http::serve(app, addr).await
}
