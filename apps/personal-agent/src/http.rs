use crate::{Application, PageRequest, ResolveRequest, Submission};
use agent_rs::{Error, Result};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, Query, Request, State, rejection::JsonRejection},
    http::{HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use subtle::ConstantTimeEq;
use uuid::Uuid;

/// One local owner's bearer credential. Never serialized, logged, or persisted.
#[derive(Clone)]
pub struct AccessToken(Arc<str>);
impl AccessToken {
    pub fn new(value: String) -> Result<Self> {
        if !(32..=512).contains(&value.len()) || !value.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(Error::input(
                "access token must contain 32..=512 printable ASCII characters without spaces",
            ));
        }
        Ok(Self(value.into()))
    }
}

#[derive(Clone)]
struct AppState {
    app: Arc<Application>,
    token: AccessToken,
}

struct ApiError(StatusCode, &'static str, String);
/// Statuses follow the library's error variant, never its message. Only an `Input` message is
/// shown to the client: it describes the client's own request.
impl From<Error> for ApiError {
    fn from(error: Error) -> Self {
        match error {
            Error::Input(detail) => Self(StatusCode::BAD_REQUEST, "invalid_input", detail.to_string()),
            Error::NotFound => Self(StatusCode::NOT_FOUND, "not_found", "Conversation or run not found.".into()),
            Error::IdempotencyConflict => Self(StatusCode::CONFLICT, "idempotency_conflict", "This key already belongs to a different request.".into()),
            Error::ConversationBusy => Self(StatusCode::CONFLICT, "conversation_busy", "Finish or cancel the current run before submitting another message.".into()),
            Error::Stale | Error::NotWaiting => Self(StatusCode::CONFLICT, "stale_decision", "This decision has changed. Refresh and review the current state.".into()),
            Error::Incompatible(_) => Self(StatusCode::CONFLICT, "incompatible_configuration", "Restore the original agent configuration to recover pending work.".into()),
            // No route steers today; mapped so a run that has settled or is being cancelled is
            // a conflict with its current state, never an internal failure.
            Error::RunNotActive => Self(StatusCode::CONFLICT, "run_not_active", "This run has finished or is being cancelled. Start a new request instead.".into()),
            // Temporary: storage failed, or the host is shutting down or not yet free.
            Error::Storage(_) | Error::StoreBusy | Error::Interrupted => Self(StatusCode::SERVICE_UNAVAILABLE, "unavailable", "The application is unavailable. A submission may already be saved; retry it with the same key.".into()),
            // Configuration, provider and run-outcome errors do not reach these routes; a
            // variant added to the library lands here until it is mapped deliberately.
            _ => Self(StatusCode::INTERNAL_SERVER_ERROR, "internal", "The application failed unexpectedly.".into()),
        }
    }
}
impl From<JsonRejection> for ApiError {
    fn from(rejection: JsonRejection) -> Self {
        Self(
            rejection.status(),
            "invalid_json",
            "Send a JSON body matching the documented request format.".into(),
        )
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.0,
            Json(json!({"error": {"code": self.1, "message": self.2}})),
        )
            .into_response()
    }
}

/// Static assets contain no personal data. Every API route requires the owner token;
/// no cookies, query-string credentials, or cross-origin access are accepted.
pub fn router(app: Arc<Application>, token: AccessToken) -> Router {
    let state = AppState { app, token };
    let api = Router::new()
        .route("/session", get(session))
        .route(
            "/conversations",
            get(conversations).post(create_conversation),
        )
        .route("/conversations/{id}", get(conversation))
        .route("/conversations/{id}/runs", post(submit))
        .route("/inbox", get(inbox))
        .route("/runs/{id}", get(run))
        .route("/runs/{id}/resolve", post(resolve))
        .route("/runs/{id}/cancel", post(cancel))
        .fallback(|| async {
            ApiError(
                StatusCode::NOT_FOUND,
                "not_found",
                "API operation not found.".into(),
            )
        })
        .layer(DefaultBodyLimit::max(65_536))
        .layer(middleware::from_fn_with_state(state.clone(), authenticate))
        .with_state(state);
    Router::new()
        .route(
            "/",
            get(|| async { Html(include_str!("../web/index.html")) }),
        )
        .route(
            "/app.js",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
                    include_str!("../web/app.js"),
                )
            }),
        )
        .route(
            "/style.css",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
                    include_str!("../web/style.css"),
                )
            }),
        )
        .route(
            "/favicon.svg",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "image/svg+xml")],
                    include_str!("../web/favicon.svg"),
                )
            }),
        )
        .nest("/api", api)
        .layer(middleware::from_fn(security_headers))
}

async fn authenticate(State(state): State<AppState>, request: Request, next: Next) -> Response {
    let supplied = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "));
    if !supplied.is_some_and(|token| bool::from(token.as_bytes().ct_eq(state.token.0.as_bytes()))) {
        let mut response = ApiError(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "Enter the application access token to connect.".into(),
        )
        .into_response();
        response
            .headers_mut()
            .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        return response;
    }
    next.run(request).await
}

async fn security_headers(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    for (name, value) in [
        ("cache-control", "no-store"),
        (
            "content-security-policy",
            "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'",
        ),
        ("x-content-type-options", "nosniff"),
        ("referrer-policy", "no-referrer"),
    ] {
        response
            .headers_mut()
            .insert(name, HeaderValue::from_static(value));
    }
    response
}

async fn session(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(json!({"agent_id": state.app.agent_id(), "healthy": state.app.healthy().await}))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NewConversation {
    id: Uuid,
    title: String,
}

async fn create_conversation(
    State(state): State<AppState>,
    body: std::result::Result<Json<NewConversation>, JsonRejection>,
) -> std::result::Result<Response, ApiError> {
    let Json(body) = body?;
    Ok((
        StatusCode::CREATED,
        Json(state.app.create_conversation(body.id, body.title).await?),
    )
        .into_response())
}

async fn conversations(
    State(state): State<AppState>,
    Query(page): Query<PageRequest>,
) -> std::result::Result<Response, ApiError> {
    Ok(Json(state.app.conversations(page).await?).into_response())
}

async fn conversation(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Query(page): Query<PageRequest>,
) -> std::result::Result<Response, ApiError> {
    Ok(Json(state.app.conversation(id, page).await?).into_response())
}

async fn submit(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    body: std::result::Result<Json<Submission>, JsonRejection>,
) -> std::result::Result<Response, ApiError> {
    let Json(body) = body?;
    let id = state.app.submit(id, body).await?;
    Ok((StatusCode::ACCEPTED, Json(json!({"run_id": id}))).into_response())
}

async fn inbox(
    State(state): State<AppState>,
    Query(page): Query<PageRequest>,
) -> std::result::Result<Response, ApiError> {
    Ok(Json(state.app.inbox(page).await?).into_response())
}

async fn run(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> std::result::Result<Response, ApiError> {
    Ok(Json(state.app.run(id).await?).into_response())
}

async fn resolve(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    body: std::result::Result<Json<ResolveRequest>, JsonRejection>,
) -> std::result::Result<Response, ApiError> {
    let Json(body) = body?;
    state.app.resolve(id, body).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn cancel(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> std::result::Result<Response, ApiError> {
    state.app.cancel(id).await?;
    Ok(StatusCode::ACCEPTED.into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statuses_follow_the_error_variant() {
        let cases = [
            (
                Error::input("bad"),
                StatusCode::BAD_REQUEST,
                "invalid_input",
            ),
            (Error::NotFound, StatusCode::NOT_FOUND, "not_found"),
            (
                Error::IdempotencyConflict,
                StatusCode::CONFLICT,
                "idempotency_conflict",
            ),
            (
                Error::ConversationBusy,
                StatusCode::CONFLICT,
                "conversation_busy",
            ),
            (Error::Stale, StatusCode::CONFLICT, "stale_decision"),
            (Error::NotWaiting, StatusCode::CONFLICT, "stale_decision"),
            (
                Error::incompatible("binding"),
                StatusCode::CONFLICT,
                "incompatible_configuration",
            ),
            (Error::RunNotActive, StatusCode::CONFLICT, "run_not_active"),
            (
                Error::storage("disk"),
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
            ),
            (
                Error::StoreBusy,
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
            ),
            (
                Error::Interrupted,
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
            ),
            (
                Error::config("wrong"),
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
            ),
            (
                Error::provider("down"),
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
            ),
        ];
        for (error, status, code) in cases {
            let ApiError(actual, actual_code, _) = error.into();
            assert_eq!((actual, actual_code), (status, code));
        }
        // Only the client's own input error is echoed; internal detail is not.
        let ApiError(_, _, message) = Error::input("title must contain 1..=200 bytes").into();
        assert!(message.contains("title"));
        let ApiError(_, _, message) = Error::storage("/secret/path unreadable").into();
        assert!(!message.contains("secret"));
    }
}
