//! Tracing/logging setup shared by all services.

use tracing_subscriber::{fmt, EnvFilter};

/// Initialize the global tracing subscriber. Log level is driven by `RUST_LOG`
/// (e.g. `RUST_LOG=debug`); defaults to `info`. Safe to call once at startup.
pub fn init() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).init();
}
