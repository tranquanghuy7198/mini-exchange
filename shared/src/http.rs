//! Shared HTTP scaffolding: a standard `/health` route, request-tracing, a
//! graceful-shutdown server runner, and a helper to read the bind address.

use std::net::SocketAddr;

use axum::{routing::get, Json, Router};
use serde_json::json;
use tower_http::trace::TraceLayer;

/// A router exposing `GET /health` → `{ "status": "ok", "service": "<name>" }`.
/// Merge service-specific routes onto this.
pub fn health_router(service: &'static str) -> Router {
    Router::new().route(
        "/health",
        get(move || async move { Json(json!({ "status": "ok", "service": service })) }),
    )
}

/// Parse a `SocketAddr` from `var`, falling back to `default` when unset.
/// Panics on an unparseable value — a misconfigured bind address is fatal.
pub fn addr_from_env(var: &str, default: &str) -> SocketAddr {
    std::env::var(var)
        .unwrap_or_else(|_| default.to_string())
        .parse()
        .unwrap_or_else(|e| panic!("invalid socket address in {var}: {e}"))
}

/// Bind `addr` and serve `router` (with HTTP request tracing) until a shutdown
/// signal (Ctrl-C / SIGTERM) is received.
pub async fn serve(router: Router, addr: SocketAddr) -> anyhow::Result<()> {
    let app = router.layer(TraceLayer::new_for_http());
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "http server listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(crate::signal::shutdown())
        .await?;
    tracing::info!("http server stopped");
    Ok(())
}
