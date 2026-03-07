use std::io;

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("s3 error: {0}")]
    S3(String),
    #[error("invalid manifest: {0}")]
    InvalidManifest(String),
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("range out of bounds: offset={offset} len={len} size={size}")]
    OutOfBounds { offset: u64, len: u64, size: u64 },
    #[error("operation already running")]
    OperationBusy,
    #[error("unsupported command: {0}")]
    UnsupportedCommand(u16),
    #[error("unsupported option: {0}")]
    UnsupportedOption(u32),
}
