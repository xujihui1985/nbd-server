pub mod r2;
pub mod s3;

#[cfg(test)]
pub mod memory;

use std::sync::Arc;

use crate::app::config::{StorageBackendKind, StorageConfig};
use crate::core::error::Result;
use crate::core::storage::object_store::ObjectStore;

pub async fn build_object_store(config: &StorageConfig) -> Result<Arc<dyn ObjectStore>> {
    match config.backend {
        StorageBackendKind::S3 => Ok(Arc::new(s3::build_s3_object_store(config).await?)),
        StorageBackendKind::R2 => r2::build_r2_object_store(config).await,
    }
}
