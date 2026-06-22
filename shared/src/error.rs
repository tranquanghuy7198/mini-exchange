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
        // Log server-side faults; never leak internal detail to the client.
        if status.is_server_error() {
            tracing::error!(error = %self, "request failed");
            return (status, Json(json!({ "error": "internal server error" }))).into_response();
        }
        (status, Json(json!({ "error": self.to_string() }))).into_response()
    }
}

/// Convenience alias for handler results.
pub type AppResult<T> = Result<T, AppError>;
