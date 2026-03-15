use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::app::config::ServeConfig;
use crate::core::engine::export::{
    CompactResponse, Export, ResetCacheResponse, SnapshotResponse, Status,
};
use crate::core::engine::spec::{CloneSource, ExportSpec};
use crate::core::error::{Error, Result};
use crate::core::model::volume::VolumeMetadata;
use crate::core::storage::object_store::ObjectStore;
use crate::core::storage::volume_repository::VolumeRepository;

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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct ExportId(pub String);

pub struct ExportManager {
    serve: ServeConfig,
    storage: Arc<dyn ObjectStore>,
    repository: Arc<dyn VolumeRepository>,
    exports: RwLock<BTreeMap<String, Arc<Export>>>,
}

impl ExportManager {
    pub async fn new(
        serve: ServeConfig,
        storage: Arc<dyn ObjectStore>,
        repository: Arc<dyn VolumeRepository>,
    ) -> Result<Arc<Self>> {
        let manager = Arc::new(Self {
            serve,
            storage,
            repository,
            exports: RwLock::new(BTreeMap::new()),
        });
        manager.discover().await?;
        Ok(manager)
    }

    pub async fn list(&self) -> Vec<ExportId> {
        self.exports
            .read()
            .await
            .keys()
            .cloned()
            .map(ExportId)
            .collect()
    }

    pub async fn create_export(&self, request: CreateExportRequest) -> Result<ExportId> {
        self.ensure_not_loaded(&request.export_id).await?;
        let config = self.export_spec_for(&request.export_id, Some(request.size));
        let export = Export::create(config, self.storage.clone(), self.repository.clone()).await?;
        let volume = VolumeMetadata::new_empty(
            request.export_id.clone(),
            request.size,
            self.serve.chunk_size,
        );
        self.repository.create_volume(&volume).await?;
        self.exports.write().await.insert(request.export_id.clone(), export);
        Ok(ExportId(request.export_id))
    }

    pub async fn open_export(&self, request: OpenExportRequest) -> Result<ExportId> {
        self.ensure_not_loaded(&request.export_id).await?;
        let loaded = self.repository.load_volume(&request.export_id).await?;
        let volume = loaded.metadata;
        let export = self.instantiate_from_volume(&volume).await?;
        self.exports.write().await.insert(request.export_id.clone(), export);
        Ok(ExportId(request.export_id))
    }

    pub async fn clone_export(&self, request: CloneExportRequest) -> Result<ExportId> {
        self.ensure_not_loaded(&request.export_id).await?;
        let source_volume = self
            .repository
            .load_volume(&request.source_export_id)
            .await?;
        let source_snapshot_id = request
            .source_snapshot_id
            .clone()
            .or_else(|| source_volume.metadata.current_snapshot_id.clone());
        let volume = VolumeMetadata::new_clone(
            request.export_id.clone(),
            source_volume.metadata.image_size,
            source_volume.metadata.chunk_size,
            request.source_export_id.clone(),
            source_snapshot_id.clone(),
        );
        let mut config = self.export_spec_for(&request.export_id, None);
        config.clone_source = Some(CloneSource {
            export_id: request.source_export_id.clone(),
            snapshot_id: source_snapshot_id.clone(),
        });
        let export =
            Export::clone_from_snapshot(config, self.storage.clone(), self.repository.clone())
                .await?;
        self.repository.create_volume(&volume).await?;
        self.exports.write().await.insert(request.export_id.clone(), export);
        Ok(ExportId(request.export_id))
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
        self.exports
            .read()
            .await
            .get(export_id)
            .cloned()
            .ok_or_else(|| Error::InvalidRequest(format!("unknown export {export_id}")))
    }

    pub async fn status(&self, export_id: &str) -> Result<Status> {
        Ok(self.get_export(export_id).await?.status().await)
    }

    pub async fn snapshot(&self, export_id: &str) -> Result<SnapshotResponse> {
        self.get_export(export_id).await?.snapshot().await
    }

    pub async fn compact(&self, export_id: &str) -> Result<CompactResponse> {
        self.get_export(export_id).await?.compact().await
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
            .cloned()
            .collect();
        for export in exports {
            export.shutdown()?;
        }
        Ok(())
    }

    async fn discover(&self) -> Result<()> {
        for loaded in self.repository.list_volumes().await? {
            let volume = loaded.metadata;
            let export = self.instantiate_from_volume(&volume).await?;
            self.exports.write().await.insert(volume.export_id.clone(), export);
        }
        Ok(())
    }

    async fn ensure_not_loaded(&self, export_id: &str) -> Result<()> {
        if self.exports.read().await.contains_key(export_id) {
            return Err(Error::InvalidRequest(format!(
                "export {export_id} is already loaded"
            )));
        }
        Ok(())
    }

    async fn instantiate_from_volume(&self, volume: &VolumeMetadata) -> Result<Arc<Export>> {
        if let Some(snapshot_id) = &volume.current_snapshot_id {
            let mut config = self.export_spec_for(&volume.export_id, None);
            config.snapshot_id = Some(snapshot_id.clone());
            return Export::open(config, self.storage.clone(), self.repository.clone()).await;
        }

        if let Some(seed) = &volume.clone_seed {
            let mut config = self.export_spec_for(&volume.export_id, None);
            config.clone_source = Some(CloneSource {
                export_id: seed.source_export_id.clone(),
                snapshot_id: seed.source_snapshot_id.clone(),
            });
            return Export::clone_from_snapshot(
                config,
                self.storage.clone(),
                self.repository.clone(),
            )
            .await;
        }

        Export::create(
            self.export_spec_for(&volume.export_id, Some(volume.image_size)),
            self.storage.clone(),
            self.repository.clone(),
        )
        .await
    }

    fn export_spec_for(&self, export_id: &str, image_size: Option<u64>) -> ExportSpec {
        ExportSpec {
            export_id: export_id.to_string(),
            cache_dir: self.serve.cache_root.join(export_id),
            chunk_size: self.serve.chunk_size,
            snapshot_id: None,
            image_size,
            clone_source: None,
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

    use crate::app::config::{ServeConfig, StorageBackendKind, StorageConfig};
    use crate::core::model::volume::{VolumeMetadata, volume_key};
    use crate::core::storage::object_store::{ObjectStore, StoredObject};
    use crate::storage::build_volume_repository;

    use super::{CreateExportRequest, ExportId, ExportManager};

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
    impl ObjectStore for MemoryRemote {
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
        let repository = build_volume_repository("exports".to_string(), remote.clone());
        let manager = ExportManager::new(serve_config(dir.path()), remote.clone(), repository)
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
        assert_eq!(manager.list().await[0], ExportId("vm01".to_string()));
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

        let repository = build_volume_repository("exports".to_string(), remote.clone());
        let manager = ExportManager::new(serve_config(dir.path()), remote, repository)
            .await
            .unwrap();

        let exports = manager.list().await;
        assert_eq!(exports.len(), 1);
        assert_eq!(exports[0], ExportId("vm01".to_string()));
    }
}
