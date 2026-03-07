use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::error::{Error, Result};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ObjectKind {
    Base,
    Delta,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObjectEntry {
    pub id: u32,
    pub kind: ObjectKind,
    pub generation: u64,
    pub key: String,
    pub size: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChunkSource {
    Zero,
    Object,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChunkEntry {
    pub index: u64,
    pub source: ChunkSource,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_id: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_offset: Option<u64>,
    pub logical_len: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blake3: Option<String>,
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
    pub objects: Vec<ObjectEntry>,
    pub chunks: Vec<ChunkEntry>,
}

#[derive(Clone, Debug)]
pub struct ChunkLocation {
    pub source: ChunkSource,
    pub object_key: Option<String>,
    pub object_offset: u64,
    pub logical_len: u32,
}

impl Manifest {
    pub fn validate(&self) -> Result<()> {
        if self.version != 1 {
            return Err(Error::InvalidManifest(format!(
                "unsupported manifest version {}",
                self.version
            )));
        }

        if self.chunk_count != self.chunks.len() as u64 {
            return Err(Error::InvalidManifest(format!(
                "chunk count {} does not match entry count {}",
                self.chunk_count,
                self.chunks.len()
            )));
        }

        let objects: BTreeMap<u32, &ObjectEntry> = self
            .objects
            .iter()
            .map(|object| (object.id, object))
            .collect();

        for (expected_index, chunk) in self.chunks.iter().enumerate() {
            if chunk.index != expected_index as u64 {
                return Err(Error::InvalidManifest(format!(
                    "chunk index {} out of sequence at position {}",
                    chunk.index, expected_index
                )));
            }

            match chunk.source {
                ChunkSource::Zero => {
                    if chunk.object_id.is_some()
                        || chunk.object_offset.is_some()
                        || chunk.blake3.is_some()
                    {
                        return Err(Error::InvalidManifest(format!(
                            "zero chunk {} unexpectedly references an object",
                            chunk.index
                        )));
                    }
                }
                ChunkSource::Object => {
                    let object_id = chunk.object_id.ok_or_else(|| {
                        Error::InvalidManifest(format!("chunk {} missing object id", chunk.index))
                    })?;
                    let offset = chunk.object_offset.ok_or_else(|| {
                        Error::InvalidManifest(format!(
                            "chunk {} missing object offset",
                            chunk.index
                        ))
                    })?;
                    let object = objects.get(&object_id).ok_or_else(|| {
                        Error::InvalidManifest(format!(
                            "chunk {} references unknown object {}",
                            chunk.index, object_id
                        ))
                    })?;
                    let end = offset
                        .checked_add(chunk.logical_len as u64)
                        .ok_or_else(|| Error::InvalidManifest("chunk overflow".to_string()))?;
                    if end > object.size {
                        return Err(Error::InvalidManifest(format!(
                            "chunk {} extends beyond object {}",
                            chunk.index, object_id
                        )));
                    }
                    if chunk.blake3.is_none() {
                        return Err(Error::InvalidManifest(format!(
                            "chunk {} missing checksum",
                            chunk.index
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    pub fn chunk_location(&self, index: u64) -> Result<ChunkLocation> {
        let chunk = self
            .chunks
            .get(index as usize)
            .ok_or_else(|| Error::InvalidManifest(format!("chunk {index} missing")))?;

        match chunk.source {
            ChunkSource::Zero => Ok(ChunkLocation {
                source: ChunkSource::Zero,
                object_key: None,
                object_offset: 0,
                logical_len: chunk.logical_len,
            }),
            ChunkSource::Object => {
                let object_id = chunk.object_id.ok_or_else(|| {
                    Error::InvalidManifest(format!("chunk {} missing object id", chunk.index))
                })?;
                let object = self
                    .objects
                    .iter()
                    .find(|object| object.id == object_id)
                    .ok_or_else(|| {
                        Error::InvalidManifest(format!(
                            "chunk {} references unknown object {}",
                            chunk.index, object_id
                        ))
                    })?;
                Ok(ChunkLocation {
                    source: ChunkSource::Object,
                    object_key: Some(object.key.clone()),
                    object_offset: chunk.object_offset.unwrap_or_default(),
                    logical_len: chunk.logical_len,
                })
            }
        }
    }

    pub fn next_object_id(&self) -> u32 {
        self.objects
            .iter()
            .map(|object| object.id)
            .max()
            .unwrap_or(0)
            + 1
    }

    pub fn referenced_object_keys(&self) -> BTreeSet<String> {
        self.objects
            .iter()
            .map(|object| object.key.clone())
            .collect()
    }

    pub fn from_full_base(
        export_id: String,
        generation: u64,
        image_size: u64,
        chunk_size: u64,
        base_key: String,
        checksums: Vec<String>,
    ) -> Result<Self> {
        let chunk_count = chunk_count(image_size, chunk_size);
        if checksums.len() != chunk_count as usize {
            return Err(Error::InvalidManifest(
                "full snapshot checksum count mismatch".to_string(),
            ));
        }

        let mut chunks = Vec::with_capacity(chunk_count as usize);
        for index in 0..chunk_count {
            chunks.push(ChunkEntry {
                index,
                source: ChunkSource::Object,
                object_id: Some(1),
                object_offset: Some(index * chunk_size),
                logical_len: chunk_len(image_size, chunk_size, index) as u32,
                blake3: Some(checksums[index as usize].clone()),
            });
        }

        Ok(Self {
            version: 1,
            export_id,
            generation,
            image_size,
            chunk_size,
            chunk_count,
            created_at: OffsetDateTime::now_utc()
                .format(&Rfc3339)
                .map_err(|error| Error::InvalidManifest(error.to_string()))?,
            objects: vec![ObjectEntry {
                id: 1,
                kind: ObjectKind::Base,
                generation,
                key: base_key,
                size: image_size,
            }],
            chunks,
        })
    }

    pub fn with_delta(
        &self,
        generation: u64,
        delta_key: String,
        delta_len: u64,
        replacements: BTreeMap<u64, ReplacementChunk>,
    ) -> Result<Self> {
        let mut objects = self.objects.clone();
        let new_object_id = self.next_object_id();
        objects.push(ObjectEntry {
            id: new_object_id,
            kind: ObjectKind::Delta,
            generation,
            key: delta_key,
            size: delta_len,
        });

        let mut chunks = self.chunks.clone();
        for (index, replacement) in replacements {
            let chunk = chunks
                .get_mut(index as usize)
                .ok_or_else(|| Error::InvalidManifest(format!("chunk {index} missing")))?;
            chunk.source = ChunkSource::Object;
            chunk.object_id = Some(new_object_id);
            chunk.object_offset = Some(replacement.object_offset);
            chunk.logical_len = replacement.logical_len as u32;
            chunk.blake3 = Some(replacement.checksum);
        }

        let referenced: BTreeSet<u32> = chunks.iter().filter_map(|chunk| chunk.object_id).collect();
        objects.retain(|object| referenced.contains(&object.id));

        Ok(Self {
            version: 1,
            export_id: self.export_id.clone(),
            generation,
            image_size: self.image_size,
            chunk_size: self.chunk_size,
            chunk_count: self.chunk_count,
            created_at: OffsetDateTime::now_utc()
                .format(&Rfc3339)
                .map_err(|error| Error::InvalidManifest(error.to_string()))?,
            objects,
            chunks,
        })
    }
}

#[derive(Clone, Debug)]
pub struct ReplacementChunk {
    pub object_offset: u64,
    pub logical_len: u64,
    pub checksum: String,
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
    fn delta_manifest_rewrites_only_selected_chunks() {
        let base = Manifest::from_full_base(
            "export".to_string(),
            1,
            8,
            4,
            "base/full.blob".to_string(),
            vec!["a".to_string(), "b".to_string()],
        )
        .unwrap();

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
            .with_delta(2, "snapshots/2/delta.blob".to_string(), 4, replacements)
            .unwrap();

        assert_eq!(updated.chunks[0].blake3.as_deref(), Some("a"));
        assert_eq!(updated.chunks[1].blake3.as_deref(), Some("c"));
        assert_eq!(updated.generation, 2);
    }
}
