use std::fmt::Display;
use std::panic::Location;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use serde_json::json;
use uuid::Uuid;

pub fn error(status: StatusCode, kind: &str, message: impl Into<String>) -> Response {
    (
        status,
        Json(json!({ "error": { "message": message.into(), "type": kind } })),
    )
        .into_response()
}

// A failure inside the server is answered with a sentence and a reference rather than with its own
// text: a database error names tables, constraints and driver internals, which tell a client about
// the instance and tell the person nothing they can act on. The text is logged under the same
// reference, with the place in the code that answered, so the one can be found from the other.
#[track_caller]
pub fn server_error(err: impl Display) -> Response {
    let mut reference = Uuid::new_v4().simple().to_string();
    reference.truncate(8);
    tracing::error!(
        reference = %reference,
        at = %Location::caller(),
        "answered as a server error: {err}"
    );
    error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "server_error",
        format!("something went wrong on the server (logged as reference {reference})"),
    )
}
