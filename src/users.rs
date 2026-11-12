use axum::extract::{Path, State as AxumState};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use serde::Deserialize;
use serde_json::json;

use crate::api::error;
use crate::auth::Caller;
use crate::state::State;

#[derive(Deserialize)]
pub struct SetCap {
    // The account limit counts the current day only. Empty clears it; the account is then unlimited.
    pub daily_token_cap: Option<i64>,
}

pub async fn set_cap(AxumState(st): AxumState<State>, Path(id): Path<i64>, caller: Caller, Json(body): Json<SetCap>) -> Response {
    if caller.role != "admin" {
        return error(StatusCode::FORBIDDEN, "forbidden", "only an admin can set an account limit");
    }
    if body.daily_token_cap.is_some_and(|cap| cap < 1) {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "daily token limit must be a positive number, or empty to clear it",
        );
    }

    // Assigning directly rather than COALESCE so an empty value clears the limit.
    let result = sqlx::query("UPDATE users SET daily_token_cap = ? WHERE id = ?")
        .bind(body.daily_token_cap)
        .bind(id)
        .execute(&st.pool)
        .await;
    match result {
        Ok(_) => Json(json!({ "ok": true, "id": id, "daily_token_cap": body.daily_token_cap })).into_response(),
        Err(err) => error(StatusCode::INTERNAL_SERVER_ERROR, "server_error", err.to_string()),
    }
}
