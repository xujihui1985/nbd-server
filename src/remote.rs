use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::Builder as S3ConfigBuilder;
use aws_sdk_s3::primitives::ByteStream;
use aws_types::region::Region;
use bytes::Bytes;

use crate::config::{StorageBackendKind, StorageConfig};
use crate::error::{Error, Result};

#[async_trait]
pub trait StorageBackend: Send + Sync {
    async fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Bytes>;
    async fn get_object(&self, key: &str) -> Result<Bytes>;
    async fn put_bytes(&self, key: &str, body: Bytes) -> Result<()>;
    async fn put_file(&self, key: &str, path: &Path) -> Result<()>;
    async fn delete_object(&self, key: &str) -> Result<()>;
}

#[derive(Clone)]
pub struct S3CompatibleStorageBackend {
    bucket: String,
    client: Client,
}

impl S3CompatibleStorageBackend {
    pub async fn new(
        bucket: String,
        region: String,
        endpoint_url: Option<String>,
        force_path_style: bool,
    ) -> Result<Self> {
        let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(Region::new(region))
            .load()
            .await;
        let mut builder = S3ConfigBuilder::from(&shared_config).force_path_style(force_path_style);
        if let Some(endpoint_url) = endpoint_url {
            builder = builder.endpoint_url(endpoint_url);
        }

        Ok(Self {
            bucket,
            client: Client::from_conf(builder.build()),
        })
    }
}

#[async_trait]
impl StorageBackend for S3CompatibleStorageBackend {
    async fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Bytes> {
        if len == 0 {
            return Ok(Bytes::new());
        }
        let range = format!("bytes={offset}-{}", offset + len - 1);
        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .range(range)
            .send()
            .await
            .map_err(|error| Error::S3(error.to_string()))?;
        let body = response
            .body
            .collect()
            .await
            .map_err(|error| Error::S3(error.to_string()))?;
        Ok(body.into_bytes())
    }

    async fn get_object(&self, key: &str) -> Result<Bytes> {
        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|error| Error::S3(error.to_string()))?;
        let body = response
            .body
            .collect()
            .await
            .map_err(|error| Error::S3(error.to_string()))?;
        Ok(body.into_bytes())
    }

    async fn put_bytes(&self, key: &str, body: Bytes) -> Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(body))
            .send()
            .await
            .map_err(|error| Error::S3(error.to_string()))?;
        Ok(())
    }

    async fn put_file(&self, key: &str, path: &Path) -> Result<()> {
        let body = ByteStream::from_path(path.to_path_buf())
            .await
            .map_err(|error| Error::S3(error.to_string()))?;
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(body)
            .send()
            .await
            .map_err(|error| Error::S3(error.to_string()))?;
        Ok(())
    }

    async fn delete_object(&self, key: &str) -> Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|error| Error::S3(error.to_string()))?;
        Ok(())
    }
}

pub async fn build_storage_backend(config: &StorageConfig) -> Result<Arc<dyn StorageBackend>> {
    let (endpoint_url, force_path_style) = match config.backend {
        StorageBackendKind::S3 => (config.endpoint_url.clone(), false),
        StorageBackendKind::R2 => {
            let endpoint = match (&config.endpoint_url, &config.r2_account_id) {
                (Some(endpoint_url), _) => endpoint_url.clone(),
                (None, Some(account_id)) => {
                    format!("https://{account_id}.r2.cloudflarestorage.com")
                }
                (None, None) => {
                    return Err(Error::InvalidRequest(
                        "R2 backend requires either --endpoint-url or --r2-account-id".to_string(),
                    ));
                }
            };
            (Some(endpoint), true)
        }
    };

    Ok(Arc::new(
        S3CompatibleStorageBackend::new(
            config.bucket.clone(),
            config.region.clone(),
            endpoint_url,
            force_path_style,
        )
        .await?,
    ))
}
