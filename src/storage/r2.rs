use std::sync::Arc;

use crate::app::config::StorageConfig;
use crate::core::error::{Error, Result};
use crate::core::storage::object_store::ObjectStore;
use crate::storage::s3::S3ObjectStore;

pub async fn build_r2_object_store(config: &StorageConfig) -> Result<Arc<dyn ObjectStore>> {
    let endpoint = match (&config.endpoint_url, &config.r2_account_id) {
        (Some(endpoint_url), _) => endpoint_url.clone(),
        (None, Some(account_id)) => format!("https://{account_id}.r2.cloudflarestorage.com"),
        (None, None) => {
            return Err(Error::InvalidRequest(
                "R2 backend requires either --endpoint-url or --r2-account-id".to_string(),
            ));
        }
    };

    Ok(Arc::new(
        S3ObjectStore::new(
            "r2",
            config.bucket.clone(),
            config.region.clone(),
            Some(endpoint),
            true,
        )
        .await?,
    ))
}
