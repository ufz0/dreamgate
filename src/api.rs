use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use serde_json::json;

pub fn error(status: StatusCode, kind: &str, message: impl Into<String>) -> Response {
    (status, Json(json!({ "error": { "message": message.into(), "type": kind } }))).into_response()
}
