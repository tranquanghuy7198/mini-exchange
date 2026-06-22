//! Audit Service (optional) — consumes saga events into `audit_events` (Task 10).
//! Task 3: serves `GET /health` to validate the HTTP stack.

use shared::{http, telemetry};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init();
    let app = http::health_router("audit-service");
    let addr = http::addr_from_env("AUDIT_BIND", "0.0.0.0:8083");
    http::serve(app, addr).await
}
