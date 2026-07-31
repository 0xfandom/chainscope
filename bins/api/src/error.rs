//! The API error envelope.
//!
//! One type every handler returns on failure, mapped once to an HTTP status and
//! a small JSON body, so a handler never has to think about response shaping and
//! a client always gets the same error format.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

#[derive(Debug)]
pub enum ApiError {
    /// The resource does not exist — 404.
    NotFound,
    /// The request was malformed (bad address, bad cursor, unknown enum) — 400.
    BadRequest(String),
    /// A dependency (the database) is down — 503.
    Unavailable,
    /// Anything unexpected — 500. The detail is logged, not returned.
    Internal(anyhow::Error),
}

impl ApiError {
    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self::BadRequest(msg.into())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::NotFound => (StatusCode::NOT_FOUND, "not found".to_owned()),
            ApiError::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
            ApiError::Unavailable => {
                (StatusCode::SERVICE_UNAVAILABLE, "service unavailable".to_owned())
            }
            ApiError::Internal(e) => {
                // The detail is for us, not the client.
                tracing::error!(error = %e, "internal error");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal error".to_owned())
            }
        };
        (status, Json(json!({ "error": message }))).into_response()
    }
}

impl From<sqlx::Error> for ApiError {
    fn from(e: sqlx::Error) -> Self {
        ApiError::Internal(e.into())
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        ApiError::Internal(e)
    }
}
