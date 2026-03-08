use std::path::Path;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::{
    Json, Router,
    routing::{get, post},
};
use serde::Serialize;
use tokio::net::UnixListener;

use crate::error::Error;
use crate::export::Export;

#[derive(Clone)]
struct AdminState {
    export: Arc<Export>,
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

pub async fn serve_admin(socket_path: &Path, export: Arc<Export>) -> crate::Result<()> {
    if socket_path.exists() {
        std::fs::remove_file(socket_path)?;
    }
    let listener = UnixListener::bind(socket_path)?;
    let router = Router::new()
        .route("/v1/status", get(get_status))
        .route("/v1/snapshot", post(post_snapshot))
        .route("/v1/compact", post(post_compact))
        .route("/v1/cache/reset", post(post_reset_cache))
        .with_state(AdminState { export });
    axum::serve(listener, router)
        .await
        .map_err(crate::Error::Io)
}

async fn get_status(State(state): State<AdminState>) -> Json<crate::export::Status> {
    Json(state.export.status().await)
}

async fn post_snapshot(
    State(state): State<AdminState>,
) -> std::result::Result<Json<crate::export::SnapshotResponse>, (StatusCode, Json<ErrorBody>)> {
    state
        .export
        .snapshot()
        .await
        .map(Json)
        .map_err(error_response)
}

async fn post_compact(
    State(state): State<AdminState>,
) -> std::result::Result<Json<crate::export::CompactResponse>, (StatusCode, Json<ErrorBody>)> {
    state
        .export
        .compact()
        .await
        .map(Json)
        .map_err(error_response)
}

async fn post_reset_cache(
    State(state): State<AdminState>,
) -> std::result::Result<Json<crate::export::ResetCacheResponse>, (StatusCode, Json<ErrorBody>)> {
    state
        .export
        .reset_cache()
        .await
        .map(Json)
        .map_err(error_response)
}

fn error_response(error: Error) -> (StatusCode, Json<ErrorBody>) {
    let status = match error {
        Error::OperationBusy => StatusCode::CONFLICT,
        Error::OutOfBounds { .. } | Error::InvalidRequest(_) | Error::InvalidManifest(_) => {
            StatusCode::BAD_REQUEST
        }
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (
        status,
        Json(ErrorBody {
            error: error.to_string(),
        }),
    )
}
