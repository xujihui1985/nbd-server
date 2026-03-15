use std::path::Path;
use std::sync::Arc;

use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use axum::{
    Json, Router,
    routing::{delete, get, post},
};
use serde::Serialize;
use tokio::net::UnixListener;

use crate::error::Error;
use crate::manager::{CloneExportRequest, CreateExportRequest, ExportManager, OpenExportRequest};

#[derive(Clone)]
struct ManagerAdminState {
    manager: Arc<ExportManager>,
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

pub async fn serve_manager_admin(
    socket_path: &Path,
    manager: Arc<ExportManager>,
) -> crate::Result<()> {
    if socket_path.exists() {
        std::fs::remove_file(socket_path)?;
    }
    let listener = UnixListener::bind(socket_path)?;
    let router = Router::new()
        .route("/v1/exports", get(get_exports))
        .route("/v1/exports/create", post(post_create_export))
        .route("/v1/exports/open", post(post_open_export))
        .route("/v1/exports/clone", post(post_clone_export))
        .route("/v1/exports/{export_id}", delete(delete_export))
        .route("/v1/exports/{export_id}/status", get(get_export_status))
        .route(
            "/v1/exports/{export_id}/snapshot",
            post(post_export_snapshot),
        )
        .route("/v1/exports/{export_id}/compact", post(post_export_compact))
        .route(
            "/v1/exports/{export_id}/cache/reset",
            post(post_export_reset_cache),
        )
        .with_state(ManagerAdminState { manager });
    axum::serve(listener, router)
        .await
        .map_err(crate::Error::Io)
}

async fn get_exports(
    State(state): State<ManagerAdminState>,
) -> Json<Vec<crate::manager::ExportSummary>> {
    Json(state.manager.list().await)
}

async fn post_create_export(
    State(state): State<ManagerAdminState>,
    Json(request): Json<CreateExportRequest>,
) -> std::result::Result<Json<crate::manager::ExportSummary>, (StatusCode, Json<ErrorBody>)> {
    state
        .manager
        .create_export(request)
        .await
        .map(Json)
        .map_err(error_response)
}

async fn post_open_export(
    State(state): State<ManagerAdminState>,
    Json(request): Json<OpenExportRequest>,
) -> std::result::Result<Json<crate::manager::ExportSummary>, (StatusCode, Json<ErrorBody>)> {
    state
        .manager
        .open_export(request)
        .await
        .map(Json)
        .map_err(error_response)
}

async fn post_clone_export(
    State(state): State<ManagerAdminState>,
    Json(request): Json<CloneExportRequest>,
) -> std::result::Result<Json<crate::manager::ExportSummary>, (StatusCode, Json<ErrorBody>)> {
    state
        .manager
        .clone_export(request)
        .await
        .map(Json)
        .map_err(error_response)
}

async fn delete_export(
    AxumPath(export_id): AxumPath<String>,
    State(state): State<ManagerAdminState>,
) -> std::result::Result<StatusCode, (StatusCode, Json<ErrorBody>)> {
    state
        .manager
        .remove_export(&export_id)
        .await
        .map(|_| StatusCode::NO_CONTENT)
        .map_err(error_response)
}

async fn get_export_status(
    AxumPath(export_id): AxumPath<String>,
    State(state): State<ManagerAdminState>,
) -> std::result::Result<Json<crate::export::Status>, (StatusCode, Json<ErrorBody>)> {
    state
        .manager
        .status(&export_id)
        .await
        .map(Json)
        .map_err(error_response)
}

async fn post_export_snapshot(
    AxumPath(export_id): AxumPath<String>,
    State(state): State<ManagerAdminState>,
) -> std::result::Result<Json<crate::export::SnapshotResponse>, (StatusCode, Json<ErrorBody>)> {
    state
        .manager
        .snapshot(&export_id)
        .await
        .map(Json)
        .map_err(error_response)
}

async fn post_export_compact(
    AxumPath(export_id): AxumPath<String>,
    State(state): State<ManagerAdminState>,
) -> std::result::Result<Json<crate::export::CompactResponse>, (StatusCode, Json<ErrorBody>)> {
    state
        .manager
        .compact(&export_id)
        .await
        .map(Json)
        .map_err(error_response)
}

async fn post_export_reset_cache(
    AxumPath(export_id): AxumPath<String>,
    State(state): State<ManagerAdminState>,
) -> std::result::Result<Json<crate::export::ResetCacheResponse>, (StatusCode, Json<ErrorBody>)> {
    state
        .manager
        .reset_cache(&export_id)
        .await
        .map(Json)
        .map_err(error_response)
}

fn error_response(error: Error) -> (StatusCode, Json<ErrorBody>) {
    let status = match error {
        Error::OperationBusy | Error::Conflict(_) => StatusCode::CONFLICT,
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
