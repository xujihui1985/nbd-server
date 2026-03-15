use std::path::Path;

use async_trait::async_trait;
use bytes::Bytes;

use crate::core::error::Result;

#[derive(Debug, Clone)]
pub struct StoredObject {
    pub body: Bytes,
    pub etag: Option<String>,
}

#[async_trait]
pub trait ObjectStore: Send + Sync {
    async fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Bytes>;
    async fn get_object(&self, key: &str) -> Result<Bytes>;
    async fn get_object_with_etag(&self, key: &str) -> Result<StoredObject>;
    async fn put_bytes(&self, key: &str, body: Bytes) -> Result<()>;
    async fn put_bytes_if_match(&self, key: &str, body: Bytes, etag: &str) -> Result<bool>;
    async fn put_bytes_if_absent(&self, key: &str, body: Bytes) -> Result<bool>;
    async fn put_file(&self, key: &str, path: &Path) -> Result<()>;
    async fn delete_object(&self, key: &str) -> Result<()>;
    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>>;
}
