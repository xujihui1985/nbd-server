use serde::{Deserialize, Serialize};

use crate::core::error::{Error, Result};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CloneSeedMetadata {
    pub source_export_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_snapshot_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VolumeMetadata {
    pub version: u32,
    pub export_id: String,
    pub image_size: u64,
    pub chunk_size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_snapshot_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clone_seed: Option<CloneSeedMetadata>,
}

impl VolumeMetadata {
    pub fn new_empty(export_id: String, image_size: u64, chunk_size: u64) -> Self {
        Self {
            version: 1,
            export_id,
            image_size,
            chunk_size,
            current_snapshot_id: None,
            clone_seed: None,
        }
    }

    pub fn new_clone(
        export_id: String,
        image_size: u64,
        chunk_size: u64,
        source_export_id: String,
        source_snapshot_id: Option<String>,
    ) -> Self {
        Self {
            version: 1,
            export_id,
            image_size,
            chunk_size,
            current_snapshot_id: None,
            clone_seed: Some(CloneSeedMetadata {
                source_export_id,
                source_snapshot_id,
            }),
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.version != 1 {
            return Err(Error::InvalidRequest(format!(
                "unsupported volume metadata version {}",
                self.version
            )));
        }
        if self.export_id.is_empty() {
            return Err(Error::InvalidRequest(
                "volume export_id must not be empty".to_string(),
            ));
        }
        if self.image_size == 0 {
            return Err(Error::InvalidRequest(
                "volume image_size must be greater than zero".to_string(),
            ));
        }
        if self.chunk_size == 0 {
            return Err(Error::InvalidRequest(
                "volume chunk_size must be greater than zero".to_string(),
            ));
        }
        Ok(())
    }

    pub fn with_current_snapshot_id(&self, snapshot_id: String) -> Self {
        let mut next = self.clone();
        next.current_snapshot_id = Some(snapshot_id);
        next.clone_seed = None;
        next
    }
}

pub fn volume_key(export_root: &str, export_id: &str) -> String {
    format!(
        "{}/{}/volume.json",
        export_root.trim_end_matches('/'),
        export_id
    )
}

pub fn export_prefix(export_root: &str, export_id: &str) -> String {
    format!("{}/{}", export_root.trim_end_matches('/'), export_id)
}

pub fn manifest_key_from_snapshot_id(
    export_root: &str,
    export_id: &str,
    snapshot_id: &str,
) -> String {
    format!(
        "{}/{}/snapshots/{}/manifest.json",
        export_root.trim_end_matches('/'),
        export_id,
        snapshot_id
    )
}

#[cfg(test)]
mod tests {
    use super::{VolumeMetadata, export_prefix, manifest_key_from_snapshot_id, volume_key};

    #[test]
    fn volume_paths_are_derived_from_export_id() {
        assert_eq!(volume_key("exports", "vm01"), "exports/vm01/volume.json");
        assert_eq!(export_prefix("exports", "vm01"), "exports/vm01");
        assert_eq!(
            manifest_key_from_snapshot_id("exports", "vm01", "abc123"),
            "exports/vm01/snapshots/abc123/manifest.json"
        );
    }

    #[test]
    fn clone_metadata_round_trip_shape_is_valid() {
        let metadata = VolumeMetadata::new_clone(
            "vm02".to_string(),
            1024,
            4096,
            "vm01".to_string(),
            Some("abcd".to_string()),
        );
        metadata.validate().unwrap();
        assert_eq!(metadata.clone_seed.unwrap().source_export_id, "vm01");
    }
}
