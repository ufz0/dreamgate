use std::fmt::Display;
use std::panic::Location;

use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use serde_json::json;
use uuid::Uuid;

pub fn error(status: StatusCode, kind: &str, message: impl Into<String>) -> Response {
    let mut response = (
        status,
        Json(json!({ "error": { "message": message.into(), "type": kind } })),
    )
        .into_response();
    // The type rides on the response as well as in its body, so the count of answers can say why a
    // request was refused without reading the body back.
    response
        .extensions_mut()
        .insert(crate::metrics::ErrorType(kind.to_string()));
    response
}

// A failure inside the server is answered with a sentence and a reference rather than with its own
// text: a database error names tables, constraints and driver internals, which tell a client about
// the instance and tell the person nothing they can act on. The text is logged under the same
// reference, with the place in the code that answered, so the one can be found from the other.
#[track_caller]
pub fn server_error(err: impl Display) -> Response {
    logged(
        StatusCode::INTERNAL_SERVER_ERROR,
        "server_error",
        "something went wrong on the server",
        err,
    )
}

// The same answer for a failure that has a status and a sentence of its own, such as the gateway
// refusing a request whose limits it could not read, so every failure that is logged can be found
// from what the client was told.
#[track_caller]
pub fn logged(status: StatusCode, kind: &str, sentence: &str, err: impl Display) -> Response {
    let mut reference = Uuid::new_v4().simple().to_string();
    reference.truncate(8);
    tracing::error!(
        reference = %reference,
        at = %Location::caller(),
        "answered as {kind}: {err}"
    );
    error(
        status,
        kind,
        format!("{sentence} (logged as reference {reference})"),
    )
}
