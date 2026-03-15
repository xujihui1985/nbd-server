pub mod object_store_volume_repository;
pub mod r2;
pub mod s3;

#[cfg(test)]
pub mod memory;

use std::sync::Arc;

use crate::app::config::{StorageBackendKind, StorageConfig};
use crate::core::error::Result;
use crate::core::storage::object_store::ObjectStore;
use crate::core::storage::volume_repository::VolumeRepository;

pub async fn build_object_store(config: &StorageConfig) -> Result<Arc<dyn ObjectStore>> {
    match config.backend {
        StorageBackendKind::S3 => Ok(Arc::new(s3::build_s3_object_store(config).await?)),
        StorageBackendKind::R2 => r2::build_r2_object_store(config).await,
    }
}

pub fn build_volume_repository(
    export_root: String,
    store: Arc<dyn ObjectStore>,
) -> Arc<dyn VolumeRepository> {
    Arc::new(object_store_volume_repository::ObjectStoreVolumeRepository::new(export_root, store))
}
