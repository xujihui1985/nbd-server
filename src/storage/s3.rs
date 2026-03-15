use std::path::Path;

use async_trait::async_trait;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::Builder as S3ConfigBuilder;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_types::region::Region;
use bytes::Bytes;

use crate::app::config::StorageConfig;
use crate::core::error::{Error, Result};
use crate::core::storage::object_store::{ObjectStore, StoredObject};

#[derive(Clone)]
pub struct S3ObjectStore {
    backend_name: &'static str,
    bucket: String,
    region: String,
    endpoint_url: Option<String>,
    force_path_style: bool,
    client: Client,
}

impl S3ObjectStore {
    pub async fn new(
        backend_name: &'static str,
        bucket: String,
        region: String,
        endpoint_url: Option<String>,
        force_path_style: bool,
    ) -> Result<Self> {
        let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(Region::new(region.clone()))
            .load()
            .await;
        let mut builder = S3ConfigBuilder::from(&shared_config).force_path_style(force_path_style);
        if let Some(endpoint_url) = endpoint_url.clone() {
            builder = builder.endpoint_url(endpoint_url);
        }

        Ok(Self {
            backend_name,
            bucket,
            region,
            endpoint_url,
            force_path_style,
            client: Client::from_conf(builder.build()),
        })
    }

    fn format_error(
        &self,
        operation: &str,
        key: &str,
        extra: Option<&str>,
        error: &(dyn std::error::Error + 'static),
    ) -> Error {
        let endpoint = self
            .endpoint_url
            .as_deref()
            .unwrap_or("<aws-default-endpoint>");
        let extra = extra.map(|value| format!(" {value}")).unwrap_or_default();
        Error::Storage(format!(
            "backend={} bucket={} region={} endpoint={} path_style={} op={} key={}{} cause={}",
            self.backend_name,
            self.bucket,
            self.region,
            endpoint,
            self.force_path_style,
            operation,
            key,
            extra,
            format_error_chain(error),
        ))
    }
}

#[async_trait]
impl ObjectStore for S3ObjectStore {
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
            .map_err(|error| self.format_error("get_range", key, None, &error))?;
        let body = response
            .body
            .collect()
            .await
            .map_err(|error| self.format_error("collect_body", key, None, &error))?;
        Ok(body.into_bytes())
    }

    async fn get_object(&self, key: &str) -> Result<Bytes> {
        Ok(self.get_object_with_etag(key).await?.body)
    }

    async fn get_object_with_etag(&self, key: &str) -> Result<StoredObject> {
        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|error| self.format_error("get_object", key, None, &error))?;
        let etag = response.e_tag().map(ToOwned::to_owned);
        let body = response
            .body
            .collect()
            .await
            .map_err(|error| self.format_error("collect_body", key, None, &error))?;
        Ok(StoredObject {
            body: body.into_bytes(),
            etag,
        })
    }

    async fn put_bytes(&self, key: &str, body: Bytes) -> Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(body))
            .send()
            .await
            .map_err(|error| self.format_error("put_bytes", key, None, &error))?;
        Ok(())
    }

    async fn put_bytes_if_match(&self, key: &str, body: Bytes, etag: &str) -> Result<bool> {
        let response = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .if_match(etag)
            .body(ByteStream::from(body))
            .send()
            .await;
        match response {
            Ok(_) => Ok(true),
            Err(error) if is_conditional_write_failure(&error) => Ok(false),
            Err(error) => Err(self.format_error("put_bytes_if_match", key, None, &error)),
        }
    }

    async fn put_bytes_if_absent(&self, key: &str, body: Bytes) -> Result<bool> {
        let response = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .if_none_match("*")
            .body(ByteStream::from(body))
            .send()
            .await;
        match response {
            Ok(_) => Ok(true),
            Err(error) if is_conditional_write_failure(&error) => Ok(false),
            Err(error) => Err(self.format_error("put_bytes_if_absent", key, None, &error)),
        }
    }

    async fn put_file(&self, key: &str, path: &Path) -> Result<()> {
        let path_hint = format!("path={}", path.display());
        let body = ByteStream::from_path(path.to_path_buf())
            .await
            .map_err(|error| {
                self.format_error("read_upload_file", key, Some(&path_hint), &error)
            })?;
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(body)
            .send()
            .await
            .map_err(|error| self.format_error("put_file", key, Some(&path_hint), &error))?;
        Ok(())
    }

    async fn delete_object(&self, key: &str) -> Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|error| self.format_error("delete_object", key, None, &error))?;
        Ok(())
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let mut continuation_token = None;
        let mut keys = Vec::new();

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);
            if let Some(token) = continuation_token.clone() {
                request = request.continuation_token(token);
            }

            let response = request
                .send()
                .await
                .map_err(|error| self.format_error("list_prefix", prefix, None, &error))?;

            if let Some(contents) = response.contents {
                for object in contents {
                    if let Some(key) = object.key {
                        keys.push(key);
                    }
                }
            }

            if response.is_truncated.unwrap_or(false) {
                continuation_token = response.next_continuation_token;
            } else {
                break;
            }
        }

        Ok(keys)
    }
}

pub async fn build_s3_object_store(config: &StorageConfig) -> Result<S3ObjectStore> {
    S3ObjectStore::new(
        "s3",
        config.bucket.clone(),
        config.region.clone(),
        config.endpoint_url.clone(),
        false,
    )
    .await
}

fn format_error_chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(next) = source {
        message.push_str("; caused by: ");
        message.push_str(&next.to_string());
        source = next.source();
    }
    message
}

fn is_conditional_write_failure(
    error: &aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::put_object::PutObjectError>,
) -> bool {
    error
        .as_service_error()
        .and_then(|inner| inner.code())
        .is_some_and(|code| matches!(code, "PreconditionFailed" | "ConditionalRequestConflict"))
}
