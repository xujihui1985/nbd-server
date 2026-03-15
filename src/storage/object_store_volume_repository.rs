use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::core::error::{Error, Result};
use crate::core::model::manifest::Manifest;
use crate::core::model::volume::{VolumeMetadata, manifest_key_from_snapshot_id, volume_key};
use crate::core::storage::object_store::ObjectStore;
use crate::core::storage::volume_repository::{
    LoadedVolume, ResolvedManifest, SnapshotLayout, VolumeRepository,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CurrentRef {
    generation: u64,
    #[serde(default)]
    snapshot_id: Option<String>,
    manifest_key: String,
}

pub struct ObjectStoreVolumeRepository {
    export_root: String,
    store: Arc<dyn ObjectStore>,
}

impl ObjectStoreVolumeRepository {
    pub fn new(export_root: String, store: Arc<dyn ObjectStore>) -> Self {
        Self {
            export_root: export_root.trim_end_matches('/').to_string(),
            store,
        }
    }

    fn current_ref_key(&self, export_id: &str) -> String {
        format!("{}/{}/refs/current.json", self.export_root, export_id)
    }

    fn manifest_key(&self, export_id: &str, snapshot_id: &str) -> String {
        manifest_key_from_snapshot_id(&self.export_root, export_id, snapshot_id)
    }
}

#[async_trait]
impl VolumeRepository for ObjectStoreVolumeRepository {
    async fn list_volumes(&self) -> Result<Vec<LoadedVolume>> {
        let prefix = format!("{}/", self.export_root);
        let keys = self.store.list_prefix(&prefix).await?;
        let mut volumes = Vec::new();
        for key in keys {
            if !key.ends_with("/volume.json") {
                continue;
            }
            let stored = self.store.get_object_with_etag(&key).await?;
            let volume: VolumeMetadata = serde_json::from_slice(&stored.body)?;
            volume.validate()?;
            volumes.push(LoadedVolume {
                metadata: volume,
                etag: stored.etag,
            });
        }
        Ok(volumes)
    }

    async fn load_volume(&self, export_id: &str) -> Result<LoadedVolume> {
        let key = volume_key(&self.export_root, export_id);
        let stored = self.store.get_object_with_etag(&key).await?;
        let volume: VolumeMetadata = serde_json::from_slice(&stored.body)?;
        volume.validate()?;
        Ok(LoadedVolume {
            metadata: volume,
            etag: stored.etag,
        })
    }

    async fn create_volume(&self, volume: &VolumeMetadata) -> Result<LoadedVolume> {
        let key = volume_key(&self.export_root, &volume.export_id);
        let created = self
            .store
            .put_bytes_if_absent(&key, Bytes::from(serde_json::to_vec_pretty(volume)?))
            .await?;
        if !created {
            return Err(Error::Conflict(format!(
                "export {} already exists remotely",
                volume.export_id
            )));
        }
        let loaded = self.load_volume(&volume.export_id).await?;
        if loaded.etag.is_none() {
            return Err(Error::InvalidRequest(format!(
                "volume {} was created without an etag",
                volume.export_id
            )));
        }
        Ok(loaded)
    }

    async fn load_manifest(
        &self,
        export_id: &str,
        snapshot_id: Option<&str>,
    ) -> Result<ResolvedManifest> {
        let manifest_key = if let Some(snapshot_id) = snapshot_id {
            self.manifest_key(export_id, snapshot_id)
        } else {
            let current_ref: CurrentRef = serde_json::from_slice(
                &self
                    .store
                    .get_object(&self.current_ref_key(export_id))
                    .await?,
            )?;
            current_ref.manifest_key
        };
        let manifest: Manifest =
            serde_json::from_slice(&self.store.get_object(&manifest_key).await?)?;
        manifest.validate()?;
        Ok(ResolvedManifest {
            manifest_key,
            manifest,
        })
    }

    async fn load_manifest_by_key(&self, manifest_key: &str) -> Result<ResolvedManifest> {
        let manifest: Manifest =
            serde_json::from_slice(&self.store.get_object(manifest_key).await?)?;
        manifest.validate()?;
        Ok(ResolvedManifest {
            manifest_key: manifest_key.to_string(),
            manifest,
        })
    }

    async fn put_manifest(
        &self,
        export_id: &str,
        snapshot_id: &str,
        manifest: &Manifest,
    ) -> Result<String> {
        let manifest_key = self.manifest_key(export_id, snapshot_id);
        self.store
            .put_bytes(
                &manifest_key,
                Bytes::from(serde_json::to_vec_pretty(manifest)?),
            )
            .await?;
        Ok(manifest_key)
    }

    async fn publish_snapshot(
        &self,
        export_id: &str,
        snapshot_id: &str,
        generation: u64,
        manifest_key: &str,
    ) -> Result<()> {
        let volume_key = volume_key(&self.export_root, export_id);
        let stored = self.store.get_object_with_etag(&volume_key).await?;
        let current: VolumeMetadata = serde_json::from_slice(&stored.body)?;
        current.validate()?;
        let next = current.with_current_snapshot_id(snapshot_id.to_string());
        let etag = stored.etag.ok_or_else(|| {
            Error::InvalidRequest(format!(
                "volume metadata {} is missing an etag; conditional publish is unavailable",
                volume_key
            ))
        })?;
        let updated = self
            .store
            .put_bytes_if_match(
                &volume_key,
                Bytes::from(serde_json::to_vec_pretty(&next)?),
                &etag,
            )
            .await?;
        if !updated {
            return Err(Error::Conflict(format!(
                "volume head changed while publishing snapshot {} for export {}",
                snapshot_id, export_id
            )));
        }

        self.store
            .put_bytes(
                &self.current_ref_key(export_id),
                Bytes::from(serde_json::to_vec_pretty(&CurrentRef {
                    generation,
                    snapshot_id: Some(snapshot_id.to_string()),
                    manifest_key: manifest_key.to_string(),
                })?),
            )
            .await?;
        Ok(())
    }

    fn snapshot_layout(&self, export_id: &str, snapshot_id: &str) -> SnapshotLayout {
        SnapshotLayout {
            manifest_key: self.manifest_key(export_id, snapshot_id),
            delta_key: format!(
                "{}/{}/snapshots/{}/delta.blob",
                self.export_root, export_id, snapshot_id
            ),
            base_key: format!(
                "{}/{}/snapshots/{}/base.blob",
                self.export_root, export_id, snapshot_id
            ),
            current_ref_key: self.current_ref_key(export_id),
        }
    }
}
