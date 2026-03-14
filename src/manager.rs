use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};

use crate::config::{CloneSourceConfig, ServeConfig, ServerConfig};
use crate::error::{Error, Result};
use crate::export::{CompactResponse, Export, ResetCacheResponse, SnapshotResponse, Status};
use crate::remote::StorageBackend;
use crate::volume::{VolumeMetadata, export_prefix, volume_key};

#[derive(Debug, Clone, Deserialize)]
pub struct CreateExportRequest {
    pub export_id: String,
    pub size: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OpenExportRequest {
    pub export_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CloneExportRequest {
    pub export_id: String,
    pub source_export_id: String,
    pub source_snapshot_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExportSummary {
    pub export_id: String,
}

struct ManagedExport {
    export: Arc<Export>,
    volume: Mutex<VolumeMetadata>,
    volume_etag: Mutex<Option<String>>,
}

pub struct ExportManager {
    serve: ServeConfig,
    storage: Arc<dyn StorageBackend>,
    exports: RwLock<BTreeMap<String, Arc<ManagedExport>>>,
}

impl ExportManager {
    pub async fn new(serve: ServeConfig, storage: Arc<dyn StorageBackend>) -> Result<Arc<Self>> {
        let manager = Arc::new(Self {
            serve,
            storage,
            exports: RwLock::new(BTreeMap::new()),
        });
        manager.discover().await?;
        Ok(manager)
    }

    pub async fn list(&self) -> Vec<ExportSummary> {
        self.exports
            .read()
            .await
            .keys()
            .cloned()
            .map(|export_id| ExportSummary { export_id })
            .collect()
    }

    pub async fn create_export(&self, request: CreateExportRequest) -> Result<ExportSummary> {
        self.ensure_not_loaded(&request.export_id).await?;
        let config = self.server_config_for(&request.export_id, Some(request.size));
        let export = Export::create(config, self.storage.clone()).await?;
        let volume = VolumeMetadata::new_empty(
            request.export_id.clone(),
            request.size,
            self.serve.chunk_size,
        );
        let etag = self.create_remote_volume(&volume).await?;
        self.exports.write().await.insert(
            request.export_id.clone(),
            Arc::new(ManagedExport {
                export,
                volume: Mutex::new(volume),
                volume_etag: Mutex::new(Some(etag)),
            }),
        );
        Ok(ExportSummary {
            export_id: request.export_id,
        })
    }

    pub async fn open_export(&self, request: OpenExportRequest) -> Result<ExportSummary> {
        self.ensure_not_loaded(&request.export_id).await?;
        let (volume, etag) = self.load_volume_with_etag(&request.export_id).await?;
        let export = self.instantiate_from_volume(&volume).await?;
        self.exports.write().await.insert(
            request.export_id.clone(),
            Arc::new(ManagedExport {
                export,
                volume: Mutex::new(volume),
                volume_etag: Mutex::new(etag),
            }),
        );
        Ok(ExportSummary {
            export_id: request.export_id,
        })
    }

    pub async fn clone_export(&self, request: CloneExportRequest) -> Result<ExportSummary> {
        self.ensure_not_loaded(&request.export_id).await?;
        let (source_volume, _) = self
            .load_volume_with_etag(&request.source_export_id)
            .await?;
        let source_snapshot_id = request
            .source_snapshot_id
            .clone()
            .or_else(|| source_volume.current_snapshot_id.clone());
        let volume = VolumeMetadata::new_clone(
            request.export_id.clone(),
            source_volume.image_size,
            source_volume.chunk_size,
            request.source_export_id.clone(),
            source_snapshot_id.clone(),
        );
        let mut config = self.server_config_for(&request.export_id, None);
        config.clone_source = Some(CloneSourceConfig {
            prefix: export_prefix(&self.serve.export_root, &request.source_export_id),
            snapshot_id: source_snapshot_id.clone(),
        });
        let export = Export::clone_from_snapshot(config, self.storage.clone()).await?;
        let etag = self.create_remote_volume(&volume).await?;
        self.exports.write().await.insert(
            request.export_id.clone(),
            Arc::new(ManagedExport {
                export,
                volume: Mutex::new(volume),
                volume_etag: Mutex::new(Some(etag)),
            }),
        );
        Ok(ExportSummary {
            export_id: request.export_id,
        })
    }

    pub async fn remove_export(&self, export_id: &str) -> Result<()> {
        self.exports
            .write()
            .await
            .remove(export_id)
            .ok_or_else(|| Error::InvalidRequest(format!("unknown export {export_id}")))?;
        Ok(())
    }

    pub async fn get_export(&self, export_id: &str) -> Result<Arc<Export>> {
        Ok(self
            .exports
            .read()
            .await
            .get(export_id)
            .ok_or_else(|| Error::InvalidRequest(format!("unknown export {export_id}")))?
            .export
            .clone())
    }

    pub async fn status(&self, export_id: &str) -> Result<Status> {
        Ok(self.get_export(export_id).await?.status().await)
    }

    pub async fn snapshot(&self, export_id: &str) -> Result<SnapshotResponse> {
        let managed = self.get_managed(export_id).await?;
        let response = managed.export.snapshot().await?;
        if response.snapshot_created {
            let (volume, etag) = self.load_volume_with_etag(export_id).await?;
            *managed.volume.lock().await = volume;
            *managed.volume_etag.lock().await = etag;
        }
        Ok(response)
    }

    pub async fn compact(&self, export_id: &str) -> Result<CompactResponse> {
        let managed = self.get_managed(export_id).await?;
        let response = managed.export.compact().await?;
        let (volume, etag) = self.load_volume_with_etag(export_id).await?;
        *managed.volume.lock().await = volume;
        *managed.volume_etag.lock().await = etag;
        Ok(response)
    }

    pub async fn reset_cache(&self, export_id: &str) -> Result<ResetCacheResponse> {
        self.get_export(export_id).await?.reset_cache().await
    }

    pub async fn shutdown_all(&self) -> Result<()> {
        let exports: Vec<Arc<Export>> = self
            .exports
            .read()
            .await
            .values()
            .map(|managed| managed.export.clone())
            .collect();
        for export in exports {
            export.shutdown()?;
        }
        Ok(())
    }

    async fn discover(&self) -> Result<()> {
        let prefix = format!("{}/", self.serve.export_root.trim_end_matches('/'));
        let keys = self.storage.list_prefix(&prefix).await?;
        for key in keys {
            if !key.ends_with("/volume.json") {
                continue;
            }
            let stored = self.storage.get_object_with_etag(&key).await?;
            let volume: VolumeMetadata = serde_json::from_slice(&stored.body)?;
            volume.validate()?;
            let export = self.instantiate_from_volume(&volume).await?;
            self.exports.write().await.insert(
                volume.export_id.clone(),
                Arc::new(ManagedExport {
                    export,
                    volume: Mutex::new(volume),
                    volume_etag: Mutex::new(stored.etag),
                }),
            );
        }
        Ok(())
    }

    async fn get_managed(&self, export_id: &str) -> Result<Arc<ManagedExport>> {
        Ok(self
            .exports
            .read()
            .await
            .get(export_id)
            .cloned()
            .ok_or_else(|| Error::InvalidRequest(format!("unknown export {export_id}")))?)
    }

    async fn ensure_not_loaded(&self, export_id: &str) -> Result<()> {
        if self.exports.read().await.contains_key(export_id) {
            return Err(Error::InvalidRequest(format!(
                "export {export_id} is already loaded"
            )));
        }
        Ok(())
    }

    async fn load_volume_with_etag(
        &self,
        export_id: &str,
    ) -> Result<(VolumeMetadata, Option<String>)> {
        let key = volume_key(&self.serve.export_root, export_id);
        let stored = self.storage.get_object_with_etag(&key).await?;
        let volume: VolumeMetadata = serde_json::from_slice(&stored.body)?;
        volume.validate()?;
        Ok((volume, stored.etag))
    }

    async fn create_remote_volume(&self, volume: &VolumeMetadata) -> Result<String> {
        let key = volume_key(&self.serve.export_root, &volume.export_id);
        let created = self
            .storage
            .put_bytes_if_absent(&key, Bytes::from(serde_json::to_vec_pretty(volume)?))
            .await?;
        if !created {
            return Err(Error::Conflict(format!(
                "export {} already exists remotely",
                volume.export_id
            )));
        }
        let (_, etag) = self.load_volume_with_etag(&volume.export_id).await?;
        etag.ok_or_else(|| {
            Error::InvalidRequest(format!(
                "volume {} was created without an etag",
                volume.export_id
            ))
        })
    }

    async fn instantiate_from_volume(&self, volume: &VolumeMetadata) -> Result<Arc<Export>> {
        if let Some(snapshot_id) = &volume.current_snapshot_id {
            let mut config = self.server_config_for(&volume.export_id, None);
            config.snapshot_id = Some(snapshot_id.clone());
            return Export::open(config, self.storage.clone()).await;
        }

        if let Some(seed) = &volume.clone_seed {
            let mut config = self.server_config_for(&volume.export_id, None);
            config.clone_source = Some(CloneSourceConfig {
                prefix: export_prefix(&self.serve.export_root, &seed.source_export_id),
                snapshot_id: seed.source_snapshot_id.clone(),
            });
            return Export::clone_from_snapshot(config, self.storage.clone()).await;
        }

        Export::create(
            self.server_config_for(&volume.export_id, Some(volume.image_size)),
            self.storage.clone(),
        )
        .await
    }

    fn server_config_for(&self, export_id: &str, image_size: Option<u64>) -> ServerConfig {
        let mut storage = self.serve.storage.clone();
        storage.prefix = export_prefix(&self.serve.export_root, export_id);
        ServerConfig {
            export_id: export_id.to_string(),
            cache_dir: self.serve.cache_root.join(export_id),
            storage,
            listen: self.serve.listen,
            admin_sock: self.serve.admin_sock.clone(),
            chunk_size: self.serve.chunk_size,
            snapshot_id: None,
            image_size,
            clone_source: None,
            volume_key: Some(volume_key(&self.serve.export_root, export_id)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::{Arc, Mutex as StdMutex};

    use async_trait::async_trait;
    use bytes::Bytes;
    use tempfile::tempdir;

    use crate::config::{ServeConfig, StorageBackendKind, StorageConfig};
    use crate::remote::{StorageBackend, StoredObject};
    use crate::volume::{VolumeMetadata, volume_key};

    use super::{CreateExportRequest, ExportManager};

    #[derive(Clone)]
    struct MemoryObject {
        body: Vec<u8>,
        etag: String,
    }

    #[derive(Default)]
    struct MemoryRemote {
        objects: StdMutex<HashMap<String, MemoryObject>>,
        next_etag: StdMutex<u64>,
    }

    #[async_trait]
    impl StorageBackend for MemoryRemote {
        async fn get_range(&self, key: &str, offset: u64, len: u64) -> crate::Result<Bytes> {
            let objects = self.objects.lock().unwrap();
            let bytes = &objects.get(key).unwrap().body;
            Ok(Bytes::copy_from_slice(
                &bytes[offset as usize..(offset + len) as usize],
            ))
        }

        async fn get_object(&self, key: &str) -> crate::Result<Bytes> {
            Ok(self.get_object_with_etag(key).await?.body)
        }

        async fn get_object_with_etag(&self, key: &str) -> crate::Result<StoredObject> {
            let objects = self.objects.lock().unwrap();
            let object = objects
                .get(key)
                .ok_or_else(|| crate::Error::InvalidRequest(format!("missing object {key}")))?;
            Ok(StoredObject {
                body: Bytes::copy_from_slice(&object.body),
                etag: Some(object.etag.clone()),
            })
        }

        async fn put_bytes(&self, key: &str, body: Bytes) -> crate::Result<()> {
            self.put_object(key, body.to_vec());
            Ok(())
        }

        async fn put_bytes_if_match(
            &self,
            key: &str,
            body: Bytes,
            etag: &str,
        ) -> crate::Result<bool> {
            let mut objects = self.objects.lock().unwrap();
            let Some(current) = objects.get(key) else {
                return Ok(false);
            };
            if current.etag != etag {
                return Ok(false);
            }
            let next_etag = self.allocate_etag();
            objects.insert(
                key.to_string(),
                MemoryObject {
                    body: body.to_vec(),
                    etag: next_etag,
                },
            );
            Ok(true)
        }

        async fn put_bytes_if_absent(&self, key: &str, body: Bytes) -> crate::Result<bool> {
            let mut objects = self.objects.lock().unwrap();
            if objects.contains_key(key) {
                return Ok(false);
            }
            let next_etag = self.allocate_etag();
            objects.insert(
                key.to_string(),
                MemoryObject {
                    body: body.to_vec(),
                    etag: next_etag,
                },
            );
            Ok(true)
        }

        async fn put_file(&self, key: &str, path: &Path) -> crate::Result<()> {
            let bytes = std::fs::read(path)?;
            self.put_object(key, bytes);
            Ok(())
        }

        async fn delete_object(&self, key: &str) -> crate::Result<()> {
            self.objects.lock().unwrap().remove(key);
            Ok(())
        }

        async fn list_prefix(&self, prefix: &str) -> crate::Result<Vec<String>> {
            Ok(self
                .objects
                .lock()
                .unwrap()
                .keys()
                .filter(|key| key.starts_with(prefix))
                .cloned()
                .collect())
        }
    }

    impl MemoryRemote {
        fn put_object(&self, key: &str, body: Vec<u8>) {
            let etag = self.allocate_etag();
            self.objects
                .lock()
                .unwrap()
                .insert(key.to_string(), MemoryObject { body, etag });
        }

        fn allocate_etag(&self) -> String {
            let mut next = self.next_etag.lock().unwrap();
            *next += 1;
            format!("etag-{}", *next)
        }
    }

    fn serve_config(dir: &Path) -> ServeConfig {
        ServeConfig {
            cache_root: dir.join("cache-root"),
            export_root: "exports".to_string(),
            storage: StorageConfig {
                backend: StorageBackendKind::S3,
                bucket: "bucket".to_string(),
                prefix: "exports".to_string(),
                region: "us-east-1".to_string(),
                endpoint_url: None,
                r2_account_id: None,
            },
            listen: "127.0.0.1:10809".parse().unwrap(),
            admin_sock: dir.join("admin.sock"),
            chunk_size: 4,
        }
    }

    #[tokio::test]
    async fn create_export_persists_volume_metadata() {
        let dir = tempdir().unwrap();
        let remote = Arc::new(MemoryRemote::default());
        let manager = ExportManager::new(serve_config(dir.path()), remote.clone())
            .await
            .unwrap();

        manager
            .create_export(CreateExportRequest {
                export_id: "vm01".to_string(),
                size: 8,
            })
            .await
            .unwrap();

        let volume = remote
            .get_object(&volume_key("exports", "vm01"))
            .await
            .unwrap();
        let volume: VolumeMetadata = serde_json::from_slice(&volume).unwrap();
        assert_eq!(volume.export_id, "vm01");
        assert_eq!(manager.list().await.len(), 1);
    }

    #[tokio::test]
    async fn manager_discovers_remote_volumes_on_startup() {
        let dir = tempdir().unwrap();
        let remote = Arc::new(MemoryRemote::default());
        remote
            .put_bytes(
                &volume_key("exports", "vm01"),
                Bytes::from(
                    serde_json::to_vec_pretty(&VolumeMetadata {
                        version: 1,
                        export_id: "vm01".to_string(),
                        image_size: 8,
                        chunk_size: 4,
                        current_snapshot_id: None,
                        clone_seed: None,
                    })
                    .unwrap(),
                ),
            )
            .await
            .unwrap();

        let manager = ExportManager::new(serve_config(dir.path()), remote)
            .await
            .unwrap();

        let exports = manager.list().await;
        assert_eq!(exports.len(), 1);
        assert_eq!(exports[0].export_id, "vm01");
    }
}
