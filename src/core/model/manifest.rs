use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::core::error::{Error, Result};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RefEntry {
    pub id: u32,
    pub path: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub index: u64,
    #[serde(rename = "ref")]
    pub ref_id: u32,
    pub offset: u64,
    pub len: u32,
    pub blake3: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u32,
    pub export_id: String,
    pub generation: u64,
    pub image_size: u64,
    pub chunk_size: u64,
    pub chunk_count: u64,
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_ref: Option<u32>,
    pub refs: Vec<RefEntry>,
    pub entries: Vec<ManifestEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChunkSource {
    Zero,
    Ref,
}

#[derive(Clone, Debug)]
pub struct ChunkLocation {
    pub source: ChunkSource,
    pub object_key: Option<String>,
    pub object_offset: u64,
    pub logical_len: u32,
}

#[derive(Clone, Debug)]
pub struct ReplacementChunk {
    pub object_offset: u64,
    pub logical_len: u64,
    pub checksum: String,
}

impl Manifest {
    pub fn empty(
        export_id: String,
        generation: u64,
        image_size: u64,
        chunk_size: u64,
    ) -> Result<Self> {
        Ok(Self {
            version: 2,
            export_id,
            generation,
            image_size,
            chunk_size,
            chunk_count: chunk_count(image_size, chunk_size),
            created_at: now_rfc3339()?,
            base_ref: None,
            refs: Vec::new(),
            entries: Vec::new(),
        })
    }

    pub fn validate(&self) -> Result<()> {
        if self.version != 2 {
            return Err(Error::InvalidManifest(format!(
                "unsupported manifest version {}",
                self.version
            )));
        }

        if self.chunk_count != chunk_count(self.image_size, self.chunk_size) {
            return Err(Error::InvalidManifest(format!(
                "chunk count {} does not match image/chunk size derived count {}",
                self.chunk_count,
                chunk_count(self.image_size, self.chunk_size)
            )));
        }

        let refs: BTreeMap<u32, &RefEntry> =
            self.refs.iter().map(|entry| (entry.id, entry)).collect();
        if let Some(base_ref) = self.base_ref
            && !refs.contains_key(&base_ref)
        {
            return Err(Error::InvalidManifest(format!(
                "base_ref {} missing from refs",
                base_ref
            )));
        }

        let mut last_index = None;
        for entry in &self.entries {
            if entry.index >= self.chunk_count {
                return Err(Error::InvalidManifest(format!(
                    "entry index {} out of bounds for chunk count {}",
                    entry.index, self.chunk_count
                )));
            }
            if let Some(previous) = last_index
                && entry.index <= previous
            {
                return Err(Error::InvalidManifest(format!(
                    "entries not strictly ordered: {} after {}",
                    entry.index, previous
                )));
            }
            last_index = Some(entry.index);

            if !refs.contains_key(&entry.ref_id) {
                return Err(Error::InvalidManifest(format!(
                    "entry {} references unknown ref {}",
                    entry.index, entry.ref_id
                )));
            }
            if entry.len as u64 != chunk_len(self.image_size, self.chunk_size, entry.index) {
                return Err(Error::InvalidManifest(format!(
                    "entry {} has invalid len {}",
                    entry.index, entry.len
                )));
            }
            if entry.blake3.is_empty() {
                return Err(Error::InvalidManifest(format!(
                    "entry {} missing checksum",
                    entry.index
                )));
            }
        }

        Ok(())
    }

    pub fn chunk_location(&self, index: u64) -> Result<ChunkLocation> {
        if index >= self.chunk_count {
            return Err(Error::InvalidManifest(format!("chunk {index} missing")));
        }

        if let Some(entry) = self.entries.iter().find(|entry| entry.index == index) {
            let reference = self
                .refs
                .iter()
                .find(|reference| reference.id == entry.ref_id)
                .ok_or_else(|| {
                    Error::InvalidManifest(format!(
                        "entry {} references unknown ref {}",
                        entry.index, entry.ref_id
                    ))
                })?;
            return Ok(ChunkLocation {
                source: ChunkSource::Ref,
                object_key: Some(reference.path.clone()),
                object_offset: entry.offset,
                logical_len: entry.len,
            });
        }

        if let Some(base_ref) = self.base_ref {
            let reference = self
                .refs
                .iter()
                .find(|reference| reference.id == base_ref)
                .ok_or_else(|| {
                    Error::InvalidManifest(format!("base_ref {} missing from refs", base_ref))
                })?;
            return Ok(ChunkLocation {
                source: ChunkSource::Ref,
                object_key: Some(reference.path.clone()),
                object_offset: index * self.chunk_size,
                logical_len: chunk_len(self.image_size, self.chunk_size, index) as u32,
            });
        }

        Ok(ChunkLocation {
            source: ChunkSource::Zero,
            object_key: None,
            object_offset: 0,
            logical_len: chunk_len(self.image_size, self.chunk_size, index) as u32,
        })
    }

    pub fn next_ref_id(&self) -> u32 {
        self.refs.iter().map(|entry| entry.id).max().unwrap_or(0) + 1
    }

    pub fn referenced_object_keys(&self) -> BTreeSet<String> {
        self.refs.iter().map(|entry| entry.path.clone()).collect()
    }

    pub fn from_full_base(
        export_id: String,
        generation: u64,
        image_size: u64,
        chunk_size: u64,
        base_path: String,
    ) -> Result<Self> {
        Ok(Self {
            version: 2,
            export_id,
            generation,
            image_size,
            chunk_size,
            chunk_count: chunk_count(image_size, chunk_size),
            created_at: now_rfc3339()?,
            base_ref: Some(1),
            refs: vec![RefEntry {
                id: 1,
                path: base_path,
            }],
            entries: Vec::new(),
        })
    }

    pub fn with_new_ref(
        &self,
        generation: u64,
        ref_path: String,
        replacements: BTreeMap<u64, ReplacementChunk>,
    ) -> Result<Self> {
        let mut refs = self.refs.clone();
        let ref_id = self.next_ref_id();
        refs.push(RefEntry {
            id: ref_id,
            path: ref_path,
        });

        let mut entries: BTreeMap<u64, ManifestEntry> = self
            .entries
            .iter()
            .cloned()
            .map(|entry| (entry.index, entry))
            .collect();

        for (index, replacement) in replacements {
            entries.insert(
                index,
                ManifestEntry {
                    index,
                    ref_id,
                    offset: replacement.object_offset,
                    len: replacement.logical_len as u32,
                    blake3: replacement.checksum,
                },
            );
        }

        Ok(Self {
            version: 2,
            export_id: self.export_id.clone(),
            generation,
            image_size: self.image_size,
            chunk_size: self.chunk_size,
            chunk_count: self.chunk_count,
            created_at: now_rfc3339()?,
            base_ref: self.base_ref,
            refs,
            entries: entries.into_values().collect(),
        })
    }
}

fn now_rfc3339() -> Result<String> {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .map_err(|error| Error::InvalidManifest(error.to_string()))
}

pub fn chunk_count(image_size: u64, chunk_size: u64) -> u64 {
    image_size.div_ceil(chunk_size)
}

pub fn chunk_offset(chunk_size: u64, index: u64) -> u64 {
    chunk_size * index
}

pub fn chunk_len(image_size: u64, chunk_size: u64, index: u64) -> u64 {
    let start = chunk_offset(chunk_size, index);
    image_size.saturating_sub(start).min(chunk_size)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{Manifest, ReplacementChunk};

    #[test]
    fn sparse_manifest_rewrites_only_selected_entries() {
        let base = Manifest::empty("export".to_string(), 1, 8, 4).unwrap();
        let mut replacements = BTreeMap::new();
        replacements.insert(
            1,
            ReplacementChunk {
                object_offset: 0,
                logical_len: 4,
                checksum: "c".to_string(),
            },
        );

        let updated = base
            .with_new_ref(2, "snapshots/2/data.blob".to_string(), replacements)
            .unwrap();

        assert_eq!(updated.entries.len(), 1);
        assert_eq!(updated.entries[0].index, 1);
        assert_eq!(updated.entries[0].ref_id, 1);
    }
}
