use async_trait::async_trait;

use crate::core::error::Result;
use crate::core::model::manifest::Manifest;
use crate::core::model::volume::VolumeMetadata;

#[derive(Debug, Clone)]
pub struct LoadedVolume {
    pub metadata: VolumeMetadata,
    pub etag: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ResolvedManifest {
    pub manifest_key: String,
    pub manifest: Manifest,
}

#[derive(Debug, Clone)]
pub struct SnapshotLayout {
    pub manifest_key: String,
    pub delta_key: String,
    pub base_key: String,
    pub current_ref_key: String,
}

#[async_trait]
pub trait VolumeRepository: Send + Sync {
    async fn list_volumes(&self) -> Result<Vec<LoadedVolume>>;
    async fn load_volume(&self, export_id: &str) -> Result<LoadedVolume>;
    async fn create_volume(&self, volume: &VolumeMetadata) -> Result<LoadedVolume>;
    async fn load_manifest(
        &self,
        export_id: &str,
        snapshot_id: Option<&str>,
    ) -> Result<ResolvedManifest>;
    async fn load_manifest_by_key(&self, manifest_key: &str) -> Result<ResolvedManifest>;
    async fn put_manifest(
        &self,
        export_id: &str,
        snapshot_id: &str,
        manifest: &Manifest,
    ) -> Result<String>;
    async fn publish_snapshot(
        &self,
        export_id: &str,
        snapshot_id: &str,
        generation: u64,
        manifest_key: &str,
    ) -> Result<()>;
    fn snapshot_layout(&self, export_id: &str, snapshot_id: &str) -> SnapshotLayout;
}
