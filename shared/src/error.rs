//! Application error type shared across services, with a uniform mapping to
//! HTTP responses (`{ "error": "<message>" }`).

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

/// Errors that a request handler may return. The variant determines the HTTP
/// status; the message becomes the JSON body.
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    /// Resource does not exist → 404.
    #[error("{0}")]
    NotFound(String),

    /// Malformed/invalid input → 400.
    #[error("{0}")]
    BadRequest(String),

    /// Valid request that conflicts with current state (e.g. insufficient
    /// balance/asset) → 409.
    #[error("{0}")]
    Conflict(String),

    /// Upstream dependency unavailable (e.g. market data) → 503.
    #[error("{0}")]
    Unavailable(String),

    /// Anything unexpected → 500. The underlying error is logged, not exposed.
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl AppError {
    fn status(&self) -> StatusCode {
        match self {
            AppError::NotFound(_) => StatusCode::NOT_FOUND,
            AppError::BadRequest(_) => StatusCode::BAD_REQUEST,
            AppError::Conflict(_) => StatusCode::CONFLICT,
            AppError::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            AppError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = self.status();
        // Only `Internal` is masked — it may wrap sensitive detail and is logged
        // server-side. All other variants carry safe, client-facing messages
        // (including `Unavailable`/503).
        let message = match &self {
            AppError::Internal(e) => {
                tracing::error!(error = %e, "request failed");
                "internal server error".to_string()
            }
            other => other.to_string(),
        };
        (status, Json(json!({ "error": message }))).into_response()
    }
}

/// Convenience alias for handler results.
pub type AppResult<T> = Result<T, AppError>;
