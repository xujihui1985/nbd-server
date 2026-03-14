use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions, remove_file};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};

use crate::cache::LocalCache;
use crate::config::{CloneSourceConfig, ServerConfig};
use crate::error::{Error, Result};
use crate::journal::{JournalOperation, JournalRecord};
use crate::manifest::{
    ChunkLocation, ChunkSource, Manifest, ReplacementChunk, chunk_len, chunk_offset,
};
use crate::remote::StorageBackend;

#[derive(Clone)]
enum ReadSource {
    Zero { image_size: u64, chunk_size: u64 },
    Manifest(Manifest),
}

impl ReadSource {
    fn generation(&self) -> u64 {
        match self {
            Self::Zero { .. } => 0,
            Self::Manifest(manifest) => manifest.generation,
        }
    }

    fn chunk_location(&self, index: u64) -> Result<ChunkLocation> {
        match self {
            Self::Zero {
                image_size,
                chunk_size,
                ..
            } => Ok(ChunkLocation {
                source: ChunkSource::Zero,
                object_key: None,
                object_offset: 0,
                logical_len: chunk_len(*image_size, *chunk_size, index) as u32,
            }),
            Self::Manifest(manifest) => manifest.chunk_location(index),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct Status {
    pub export_id: String,
    pub image_size: u64,
    pub chunk_size: u64,
    pub chunk_count: u64,
    pub resident_chunks: usize,
    pub dirty_chunks: usize,
    pub snapshot_generation: u64,
    pub remote_head_generation: u64,
    pub operation_state: String,
}

#[derive(Debug, Serialize)]
pub struct SnapshotResponse {
    pub snapshot_created: bool,
    pub generation: u64,
    pub garbage_collected_objects: usize,
}

#[derive(Debug, Serialize)]
pub struct CompactResponse {
    pub generation: u64,
    pub garbage_collected_objects: usize,
}

#[derive(Debug, Serialize)]
pub struct ResetCacheResponse {
    pub discarded_dirty_chunks: usize,
    pub discarded_resident_chunks: usize,
    pub manifest_generation: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct CurrentRef {
    generation: u64,
    manifest_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CloneSeedRecord {
    version: u32,
    source_manifest_key: String,
}

#[derive(Clone)]
struct ResolvedManifest {
    manifest_key: String,
    manifest: Manifest,
}

const SINGLE_PUT_LIMIT_BYTES: u64 = 5 * 1024 * 1024 * 1024;

pub struct Export {
    config: ServerConfig,
    cache: Arc<LocalCache>,
    remote: Arc<dyn StorageBackend>,
    read_source: RwLock<Arc<ReadSource>>,
    published_manifest: RwLock<Option<Manifest>>,
    write_gate: RwLock<()>,
    operation_running: AtomicBool,
    operation_name: StdMutex<Option<&'static str>>,
    journal_path: std::path::PathBuf,
    clone_seed_path: std::path::PathBuf,
    chunk_locks: Vec<Mutex<()>>,
}

impl Export {
    pub async fn create(
        config: ServerConfig,
        remote: Arc<dyn StorageBackend>,
    ) -> Result<Arc<Self>> {
        if config.snapshot_id.is_some() {
            return Err(Error::InvalidRequest(
                "--snapshot-id is only valid with the open command".to_string(),
            ));
        }
        if config.clone_source.is_some() {
            return Err(Error::InvalidRequest(
                "clone source is only valid with the clone command".to_string(),
            ));
        }
        if config.image_size.is_none() {
            return Err(Error::InvalidRequest(
                "--size is required when creating a new export".to_string(),
            ));
        }

        let image_size = config.image_size.unwrap();
        let chunk_size = config.chunk_size;
        let cache = if config.cache_dir.join("cache.meta").exists() {
            let cache = LocalCache::open(&config.cache_dir)?;
            cache.validate_layout(&config.export_id, image_size, chunk_size)?;
            cache
        } else {
            LocalCache::create(
                &config.cache_dir,
                config.export_id.clone(),
                image_size,
                chunk_size,
            )?
        };
        Self::recover_local_state(&config.cache_dir, &cache)?;
        cache.set_clean_shutdown(false)?;

        let chunk_total = cache.chunk_count();
        Ok(Arc::new(Self {
            journal_path: config.cache_dir.join("snapshot.journal.json"),
            clone_seed_path: config.cache_dir.join("clone.seed.json"),
            config,
            cache: Arc::new(cache),
            remote,
            read_source: RwLock::new(Arc::new(ReadSource::Zero {
                image_size,
                chunk_size,
            })),
            published_manifest: RwLock::new(None),
            write_gate: RwLock::new(()),
            operation_running: AtomicBool::new(false),
            operation_name: StdMutex::new(None),
            chunk_locks: (0..chunk_total).map(|_| Mutex::new(())).collect(),
        }))
    }

    pub async fn open(config: ServerConfig, remote: Arc<dyn StorageBackend>) -> Result<Arc<Self>> {
        if config.clone_source.is_some() {
            return Err(Error::InvalidRequest(
                "clone source is only valid with the clone command".to_string(),
            ));
        }
        let resolved =
            resolve_manifest(&*remote, &config.storage.prefix, config.snapshot_id).await?;
        let manifest = resolved.manifest;

        let cache = if config.cache_dir.join("cache.meta").exists() {
            let cache = LocalCache::open(&config.cache_dir)?;
            cache.validate_layout(&config.export_id, manifest.image_size, manifest.chunk_size)?;
            cache
        } else {
            LocalCache::create(
                &config.cache_dir,
                config.export_id.clone(),
                manifest.image_size,
                manifest.chunk_size,
            )?
        };
        Self::recover_local_state(&config.cache_dir, &cache)?;
        cache.rebind_manifest_generation(manifest.generation)?;
        cache.set_clean_shutdown(false)?;

        let chunk_total = cache.chunk_count();
        Ok(Arc::new(Self {
            journal_path: config.cache_dir.join("snapshot.journal.json"),
            clone_seed_path: config.cache_dir.join("clone.seed.json"),
            config,
            cache: Arc::new(cache),
            remote,
            read_source: RwLock::new(Arc::new(ReadSource::Manifest(manifest.clone()))),
            published_manifest: RwLock::new(Some(manifest)),
            write_gate: RwLock::new(()),
            operation_running: AtomicBool::new(false),
            operation_name: StdMutex::new(None),
            chunk_locks: (0..chunk_total).map(|_| Mutex::new(())).collect(),
        }))
    }

    pub async fn clone_from_snapshot(
        config: ServerConfig,
        remote: Arc<dyn StorageBackend>,
    ) -> Result<Arc<Self>> {
        if config.snapshot_id.is_some() {
            return Err(Error::InvalidRequest(
                "--snapshot-id is only valid with the open command".to_string(),
            ));
        }
        if config.image_size.is_some() {
            return Err(Error::InvalidRequest(
                "--size is not valid with the clone command".to_string(),
            ));
        }
        let clone_source = config
            .clone_source
            .clone()
            .ok_or_else(|| Error::InvalidRequest("missing clone source".to_string()))?;

        let clone_seed_path = config.cache_dir.join("clone.seed.json");
        let cache_exists = config.cache_dir.join("cache.meta").exists();
        if cache_exists {
            let cache = LocalCache::open(&config.cache_dir)?;
            Self::recover_local_state(&config.cache_dir, &cache)?;
            if cache.manifest_generation() != 0 {
                return Err(Error::InvalidRequest(
                    "clone can only be resumed before the first target snapshot; use open for an initialized export".to_string(),
                ));
            }
            let seed = CloneSeedRecord::load(&clone_seed_path)?.ok_or_else(|| {
                Error::InvalidRequest(
                    "cache directory is missing clone.seed.json; cannot safely resume clone state"
                        .to_string(),
                )
            })?;
            let resolved = resolve_manifest_by_key(&*remote, &seed.source_manifest_key).await?;
            cache.validate_layout(
                &config.export_id,
                resolved.manifest.image_size,
                resolved.manifest.chunk_size,
            )?;
            cache.set_clean_shutdown(false)?;
            let chunk_total = cache.chunk_count();
            return Ok(Arc::new(Self {
                journal_path: config.cache_dir.join("snapshot.journal.json"),
                clone_seed_path,
                config,
                cache: Arc::new(cache),
                remote,
                read_source: RwLock::new(Arc::new(ReadSource::Manifest(resolved.manifest))),
                published_manifest: RwLock::new(None),
                write_gate: RwLock::new(()),
                operation_running: AtomicBool::new(false),
                operation_name: StdMutex::new(None),
                chunk_locks: (0..chunk_total).map(|_| Mutex::new(())).collect(),
            }));
        } else {
            let resolved = resolve_manifest_for_clone(&*remote, &clone_source).await?;
            CloneSeedRecord {
                version: 1,
                source_manifest_key: resolved.manifest_key.clone(),
            }
            .persist(&clone_seed_path)?;
            let cache = LocalCache::create(
                &config.cache_dir,
                config.export_id.clone(),
                resolved.manifest.image_size,
                resolved.manifest.chunk_size,
            )?;
            Self::recover_local_state(&config.cache_dir, &cache)?;
            cache.set_clean_shutdown(false)?;

            let chunk_total = cache.chunk_count();
            return Ok(Arc::new(Self {
                journal_path: config.cache_dir.join("snapshot.journal.json"),
                clone_seed_path,
                config,
                cache: Arc::new(cache),
                remote,
                read_source: RwLock::new(Arc::new(ReadSource::Manifest(resolved.manifest))),
                published_manifest: RwLock::new(None),
                write_gate: RwLock::new(()),
                operation_running: AtomicBool::new(false),
                operation_name: StdMutex::new(None),
                chunk_locks: (0..chunk_total).map(|_| Mutex::new(())).collect(),
            }));
        }
    }

    pub fn image_size(&self) -> u64 {
        self.cache.image_size()
    }

    pub fn export_name(&self) -> &str {
        &self.config.export_id
    }

    pub fn export_description(&self) -> String {
        format!(
            "lazy object-storage-backed export {}",
            self.config.export_id
        )
    }

    pub async fn read(&self, offset: u64, len: u32) -> Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        self.validate_range(offset, len as u64)?;
        let mut buffer = vec![0_u8; len as usize];
        let chunk_size = self.cache.chunk_size();
        let start_chunk = offset / chunk_size;
        let end_chunk = (offset + len as u64 - 1) / chunk_size;

        for index in start_chunk..=end_chunk {
            let _chunk_guard = self.chunk_locks[index as usize].lock().await;
            if !self.cache.is_resident(index as usize) {
                self.materialize_chunk(index, true).await?;
            }
            let chunk_start = chunk_offset(chunk_size, index);
            let read_start = offset.max(chunk_start);
            let read_end = (offset + len as u64)
                .min(chunk_start + chunk_len(self.image_size(), chunk_size, index));
            let copy_len = (read_end - read_start) as usize;
            let data = self.cache.read_exact_at(read_start, copy_len)?;
            let out_offset = (read_start - offset) as usize;
            buffer[out_offset..out_offset + copy_len].copy_from_slice(&data);
        }

        Ok(buffer)
    }

    pub async fn write(&self, offset: u64, data: &[u8], fua: bool) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        self.validate_range(offset, data.len() as u64)?;
        let _write_guard = self.write_gate.read().await;
        let chunk_size = self.cache.chunk_size();
        let start_chunk = offset / chunk_size;
        let end_chunk = (offset + data.len() as u64 - 1) / chunk_size;

        for index in start_chunk..=end_chunk {
            let _chunk_guard = self.chunk_locks[index as usize].lock().await;
            let chunk_start = chunk_offset(chunk_size, index);
            let chunk_end = chunk_start + chunk_len(self.image_size(), chunk_size, index);
            let write_start = offset.max(chunk_start);
            let write_end = (offset + data.len() as u64).min(chunk_end);
            let in_offset = (write_start - offset) as usize;
            let write_len = (write_end - write_start) as usize;
            let full_chunk_write = write_start == chunk_start && write_end == chunk_end;

            if !self.cache.is_resident(index as usize) && !full_chunk_write {
                self.materialize_chunk(index, false).await?;
            }

            self.cache
                .write_all_at(write_start, &data[in_offset..in_offset + write_len])?;
            self.cache.mark_dirty(index as usize)?;
        }

        if fua {
            self.flush().await?;
        }

        Ok(())
    }

    pub async fn flush(&self) -> Result<()> {
        self.cache.sync_data()?;
        self.cache
            .snapshot_meta()
            .persist(&self.config.cache_dir.join("cache.meta"), true)?;
        Ok(())
    }

    pub async fn snapshot(&self) -> Result<SnapshotResponse> {
        self.begin_operation("snapshot")?;
        let result = self.snapshot_inner().await;
        self.end_operation();
        result
    }

    pub async fn compact(&self) -> Result<CompactResponse> {
        self.begin_operation("compact")?;
        let result = self.compact_inner().await;
        self.end_operation();
        result
    }

    pub async fn reset_cache(&self) -> Result<ResetCacheResponse> {
        self.begin_operation("reset-cache")?;
        let result = self.reset_cache_inner().await;
        self.end_operation();
        result
    }

    pub async fn status(&self) -> Status {
        let read_source = self.read_source.read().await;
        let operation_state = self
            .operation_name
            .lock()
            .unwrap()
            .map(str::to_string)
            .unwrap_or_else(|| "idle".to_string());

        Status {
            export_id: self.config.export_id.clone(),
            image_size: self.cache.image_size(),
            chunk_size: self.cache.chunk_size(),
            chunk_count: self.cache.chunk_count() as u64,
            resident_chunks: self.cache.resident_count(),
            dirty_chunks: self.cache.dirty_count(),
            snapshot_generation: self.cache.manifest_generation(),
            remote_head_generation: read_source.generation(),
            operation_state,
        }
    }

    pub fn shutdown(&self) -> Result<()> {
        self.cache.set_clean_shutdown(true)
    }

    fn begin_operation(&self, name: &'static str) -> Result<()> {
        if self
            .operation_running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(Error::OperationBusy);
        }
        *self.operation_name.lock().unwrap() = Some(name);
        Ok(())
    }

    fn end_operation(&self) {
        *self.operation_name.lock().unwrap() = None;
        self.operation_running.store(false, Ordering::SeqCst);
    }

    async fn snapshot_inner(&self) -> Result<SnapshotResponse> {
        let _snapshot_guard = self.write_gate.write().await;
        self.cache.set_snapshot_in_progress(true)?;
        self.cache.sync_data()?;
        let next_generation = self.cache.manifest_generation() + 1;
        let read_source = self.read_source.read().await.clone();
        let published_manifest = self.published_manifest.read().await.clone();
        let previous_keys = published_manifest
            .as_ref()
            .map(|manifest| manifest.referenced_object_keys());

        let result = if self.cache.manifest_generation() == 0 {
            match read_source.as_ref() {
                ReadSource::Zero { .. } => {
                    self.publish_initial_sparse_snapshot(next_generation).await
                }
                ReadSource::Manifest(_) => {
                    self.materialize_all_chunks().await?;
                    self.cache.sync_data()?;
                    self.publish_full_snapshot(
                        next_generation,
                        JournalOperation::Snapshot,
                        format!(
                            "{}/snapshots/{next_generation}/base.blob",
                            self.config.storage.prefix
                        ),
                        None,
                    )
                    .await
                }
            }
        } else {
            self.publish_delta_snapshot(
                next_generation,
                published_manifest.ok_or_else(|| {
                    Error::InvalidManifest("missing published manifest".to_string())
                })?,
                previous_keys,
            )
            .await
        };

        self.finish_publish(result)
    }

    async fn compact_inner(&self) -> Result<CompactResponse> {
        let _compact_guard = self.write_gate.write().await;
        self.cache.set_snapshot_in_progress(true)?;
        self.materialize_all_chunks().await?;
        self.cache.sync_data()?;
        let generation = self.cache.manifest_generation() + 1;
        let previous_keys = self
            .published_manifest
            .read()
            .await
            .as_ref()
            .map(|manifest| manifest.referenced_object_keys());

        let result = self
            .publish_full_snapshot(
                generation,
                JournalOperation::Compact,
                format!(
                    "{}/snapshots/{generation}/base.blob",
                    self.config.storage.prefix
                ),
                previous_keys,
            )
            .await;
        let result = self.finish_publish(result)?;

        Ok(CompactResponse {
            generation: result.generation,
            garbage_collected_objects: result.garbage_collected_objects,
        })
    }

    async fn reset_cache_inner(&self) -> Result<ResetCacheResponse> {
        let _reset_guard = self.write_gate.write().await;
        let (discarded_dirty_chunks, discarded_resident_chunks) = self.cache.reset_local_state()?;
        Ok(ResetCacheResponse {
            discarded_dirty_chunks,
            discarded_resident_chunks,
            manifest_generation: self.cache.manifest_generation(),
        })
    }

    fn finish_publish(&self, result: Result<SnapshotResponse>) -> Result<SnapshotResponse> {
        let clear_journal = JournalRecord::clear(&self.journal_path);
        let clear_flag = self.cache.set_snapshot_in_progress(false);

        match result {
            Ok(response) => {
                clear_journal?;
                clear_flag?;
                if response.snapshot_created {
                    if self.cache.manifest_generation() == 0 && response.generation == 1 {
                        remove_file(&self.clone_seed_path).ok();
                    }
                    self.cache.set_manifest_generation(response.generation)?;
                    self.cache.clear_dirty_all()?;
                }
                Ok(response)
            }
            Err(error) => {
                let _ = clear_journal;
                let _ = clear_flag;
                Err(error)
            }
        }
    }

    async fn publish_full_snapshot(
        &self,
        generation: u64,
        operation: JournalOperation,
        object_key: String,
        previous_keys: Option<BTreeSet<String>>,
    ) -> Result<SnapshotResponse> {
        let image_size = self.cache.image_size();
        if image_size > SINGLE_PUT_LIMIT_BYTES {
            return Err(Error::InvalidRequest(format!(
                "full snapshot requires uploading {} bytes to {}, but single-put support is limited to {} bytes; multipart upload is not implemented",
                image_size, object_key, SINGLE_PUT_LIMIT_BYTES
            )));
        }

        let manifest_key = manifest_key(&self.config.storage.prefix, generation);
        JournalRecord {
            version: 1,
            operation: operation.clone(),
            generation,
            staging_path: None,
            object_key: object_key.clone(),
            manifest_key: manifest_key.clone(),
        }
        .persist(&self.journal_path)?;

        tracing::info!(
            generation,
            operation = ?operation,
            image_size,
            object_key = %object_key,
            "starting full snapshot upload"
        );
        self.remote
            .put_file(&object_key, self.cache.raw_path())
            .await?;
        tracing::info!(
            generation,
            operation = ?operation,
            object_key = %object_key,
            "finished full snapshot upload"
        );
        let manifest = Manifest::from_full_base(
            self.config.export_id.clone(),
            generation,
            image_size,
            self.cache.chunk_size(),
            object_key,
        )?;
        let gc = self.publish_manifest(manifest, previous_keys).await?;
        Ok(SnapshotResponse {
            snapshot_created: true,
            generation,
            garbage_collected_objects: gc,
        })
    }

    async fn publish_initial_sparse_snapshot(&self, generation: u64) -> Result<SnapshotResponse> {
        let dirty = self.cache.dirty_indices();
        if dirty.is_empty() {
            tracing::info!(
                generation,
                image_size = self.cache.image_size(),
                "publishing initial zero-backed manifest without data objects"
            );
            let manifest = Manifest::empty(
                self.config.export_id.clone(),
                generation,
                self.cache.image_size(),
                self.cache.chunk_size(),
            )?;
            let gc = self.publish_manifest(manifest, None).await?;
            return Ok(SnapshotResponse {
                snapshot_created: true,
                generation,
                garbage_collected_objects: gc,
            });
        }

        let delta_path = self
            .config
            .cache_dir
            .join(format!("snapshot-{generation}.delta.blob"));
        let delta_key = format!(
            "{}/snapshots/{generation}/delta.blob",
            self.config.storage.prefix
        );
        let manifest_key = manifest_key(&self.config.storage.prefix, generation);

        let mut delta_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&delta_path)?;
        let mut replacements = BTreeMap::new();
        let mut cursor = 0_u64;
        for index in dirty {
            let _chunk_guard = self.chunk_locks[index].lock().await;
            let logical_len = chunk_len(
                self.cache.image_size(),
                self.cache.chunk_size(),
                index as u64,
            );
            let bytes = self.cache.read_exact_at(
                chunk_offset(self.cache.chunk_size(), index as u64),
                logical_len as usize,
            )?;
            delta_file.write_all(&bytes)?;
            replacements.insert(
                index as u64,
                ReplacementChunk {
                    object_offset: cursor,
                    logical_len,
                    checksum: blake3::hash(&bytes).to_hex().to_string(),
                },
            );
            cursor += logical_len;
        }
        delta_file.sync_all()?;

        JournalRecord {
            version: 1,
            operation: JournalOperation::Snapshot,
            generation,
            staging_path: Some(delta_path.display().to_string()),
            object_key: delta_key.clone(),
            manifest_key: manifest_key.clone(),
        }
        .persist(&self.journal_path)?;

        tracing::info!(
            generation,
            dirty_chunks = replacements.len(),
            delta_bytes = cursor,
            object_key = %delta_key,
            "starting initial sparse snapshot upload"
        );
        self.remote.put_file(&delta_key, &delta_path).await?;
        tracing::info!(
            generation,
            dirty_chunks = replacements.len(),
            delta_bytes = cursor,
            object_key = %delta_key,
            "finished initial sparse snapshot upload"
        );

        let manifest = Manifest::empty(
            self.config.export_id.clone(),
            generation,
            self.cache.image_size(),
            self.cache.chunk_size(),
        )?
        .with_new_ref(generation, delta_key, replacements)?;
        let gc = self.publish_manifest(manifest, None).await?;
        remove_file(&delta_path).ok();

        Ok(SnapshotResponse {
            snapshot_created: true,
            generation,
            garbage_collected_objects: gc,
        })
    }

    async fn publish_delta_snapshot(
        &self,
        generation: u64,
        published_manifest: Manifest,
        previous_keys: Option<BTreeSet<String>>,
    ) -> Result<SnapshotResponse> {
        let dirty = self.cache.dirty_indices();
        if dirty.is_empty() {
            return Ok(SnapshotResponse {
                snapshot_created: false,
                generation: self.cache.manifest_generation(),
                garbage_collected_objects: 0,
            });
        }

        let delta_path = self
            .config
            .cache_dir
            .join(format!("snapshot-{generation}.delta.blob"));
        let delta_key = format!(
            "{}/snapshots/{generation}/delta.blob",
            self.config.storage.prefix
        );
        let manifest_key = manifest_key(&self.config.storage.prefix, generation);

        let mut delta_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&delta_path)?;
        let mut replacements = BTreeMap::new();
        let mut cursor = 0_u64;
        for index in dirty {
            let _chunk_guard = self.chunk_locks[index].lock().await;
            let logical_len = chunk_len(
                self.cache.image_size(),
                self.cache.chunk_size(),
                index as u64,
            );
            let bytes = self.cache.read_exact_at(
                chunk_offset(self.cache.chunk_size(), index as u64),
                logical_len as usize,
            )?;
            delta_file.write_all(&bytes)?;
            replacements.insert(
                index as u64,
                ReplacementChunk {
                    object_offset: cursor,
                    logical_len,
                    checksum: blake3::hash(&bytes).to_hex().to_string(),
                },
            );
            cursor += logical_len;
        }
        delta_file.sync_all()?;

        JournalRecord {
            version: 1,
            operation: JournalOperation::Snapshot,
            generation,
            staging_path: Some(delta_path.display().to_string()),
            object_key: delta_key.clone(),
            manifest_key: manifest_key.clone(),
        }
        .persist(&self.journal_path)?;

        tracing::info!(
            generation,
            dirty_chunks = replacements.len(),
            delta_bytes = cursor,
            object_key = %delta_key,
            "starting delta snapshot upload"
        );
        self.remote.put_file(&delta_key, &delta_path).await?;
        tracing::info!(
            generation,
            dirty_chunks = replacements.len(),
            delta_bytes = cursor,
            object_key = %delta_key,
            "finished delta snapshot upload"
        );
        let manifest = published_manifest.with_new_ref(generation, delta_key, replacements)?;
        let gc = self.publish_manifest(manifest, previous_keys).await?;
        remove_file(&delta_path).ok();

        Ok(SnapshotResponse {
            snapshot_created: true,
            generation,
            garbage_collected_objects: gc,
        })
    }

    async fn publish_manifest(
        &self,
        manifest: Manifest,
        previous_keys: Option<BTreeSet<String>>,
    ) -> Result<usize> {
        let manifest_key = manifest_key(&self.config.storage.prefix, manifest.generation);
        self.remote
            .put_bytes(
                &manifest_key,
                Bytes::from(serde_json::to_vec_pretty(&manifest)?),
            )
            .await?;

        self.remote
            .put_bytes(
                &current_ref_key(&self.config.storage.prefix),
                Bytes::from(serde_json::to_vec_pretty(&CurrentRef {
                    generation: manifest.generation,
                    manifest_key: manifest_key.clone(),
                })?),
            )
            .await?;

        let new_keys = manifest.referenced_object_keys();
        *self.read_source.write().await = Arc::new(ReadSource::Manifest(manifest.clone()));
        *self.published_manifest.write().await = Some(manifest);
        Ok(self
            .garbage_collect_unreferenced_objects(previous_keys, &new_keys)
            .await)
    }

    async fn garbage_collect_unreferenced_objects(
        &self,
        previous_keys: Option<BTreeSet<String>>,
        current_keys: &BTreeSet<String>,
    ) -> usize {
        let Some(previous_keys) = previous_keys else {
            return 0;
        };

        let mut deleted = 0;
        for key in previous_keys.difference(current_keys) {
            match self.remote.delete_object(key).await {
                Ok(()) => deleted += 1,
                Err(error) => tracing::warn!("failed to delete stale object {key}: {error}"),
            }
        }
        deleted
    }

    async fn materialize_all_chunks(&self) -> Result<()> {
        let mut newly_resident = Vec::new();
        for index in 0..self.cache.chunk_count() {
            let _chunk_guard = self.chunk_locks[index].lock().await;
            if self.cache.is_resident(index) {
                continue;
            }
            let bytes = self.fetch_chunk_bytes(index as u64).await?;
            self.cache
                .write_all_at(chunk_offset(self.cache.chunk_size(), index as u64), &bytes)?;
            newly_resident.push(index);
        }
        self.cache.mark_resident_many(&newly_resident)?;
        Ok(())
    }

    async fn materialize_chunk(&self, index: u64, persist_resident: bool) -> Result<()> {
        if self.cache.is_resident(index as usize) {
            return Ok(());
        }
        let bytes = self.fetch_chunk_bytes(index).await?;
        self.cache
            .write_all_at(chunk_offset(self.cache.chunk_size(), index), &bytes)?;
        if persist_resident {
            self.cache.mark_resident(index as usize)?;
        }
        Ok(())
    }

    async fn fetch_chunk_bytes(&self, index: u64) -> Result<Vec<u8>> {
        let read_source = self.read_source.read().await.clone();
        let location = read_source.chunk_location(index)?;
        match location.source {
            ChunkSource::Zero => Ok(vec![0_u8; location.logical_len as usize]),
            ChunkSource::Ref => Ok(self
                .remote
                .get_range(
                    location
                        .object_key
                        .as_deref()
                        .ok_or_else(|| Error::InvalidManifest("missing object key".to_string()))?,
                    location.object_offset,
                    location.logical_len as u64,
                )
                .await?
                .to_vec()),
        }
    }

    fn validate_range(&self, offset: u64, len: u64) -> Result<()> {
        let size = self.cache.image_size();
        if offset.checked_add(len).is_none() || offset + len > size {
            return Err(Error::OutOfBounds { offset, len, size });
        }
        Ok(())
    }

    fn recover_local_state(cache_dir: &Path, cache: &LocalCache) -> Result<()> {
        cache.apply_crash_recovery()?;
        let journal_path = cache_dir.join("snapshot.journal.json");
        if let Some(record) = JournalRecord::load(&journal_path)? {
            if let Some(staging_path) = record.staging_path {
                remove_file(staging_path).ok();
            }
            JournalRecord::clear(&journal_path)?;
            cache.set_snapshot_in_progress(false)?;
        }
        Ok(())
    }
}

impl CloneSeedRecord {
    fn load(path: &Path) -> Result<Option<Self>> {
        if !path.exists() {
            return Ok(None);
        }
        let mut bytes = Vec::new();
        File::open(path)?.read_to_end(&mut bytes)?;
        Ok(Some(serde_json::from_slice(&bytes)?))
    }

    fn persist(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = File::create(path)?;
        file.write_all(&serde_json::to_vec_pretty(self)?)?;
        file.sync_all()?;
        Ok(())
    }
}

async fn resolve_manifest(
    remote: &dyn StorageBackend,
    prefix: &str,
    snapshot_id: Option<u64>,
) -> Result<ResolvedManifest> {
    let manifest_key = if let Some(snapshot_id) = snapshot_id {
        manifest_key(prefix, snapshot_id)
    } else {
        let current_ref: CurrentRef =
            serde_json::from_slice(&remote.get_object(&current_ref_key(prefix)).await?)?;
        current_ref.manifest_key
    };
    resolve_manifest_by_key(remote, &manifest_key).await
}

async fn resolve_manifest_for_clone(
    remote: &dyn StorageBackend,
    clone_source: &CloneSourceConfig,
) -> Result<ResolvedManifest> {
    resolve_manifest(remote, &clone_source.prefix, clone_source.snapshot_id).await
}

async fn resolve_manifest_by_key(
    remote: &dyn StorageBackend,
    manifest_key: &str,
) -> Result<ResolvedManifest> {
    let manifest: Manifest = serde_json::from_slice(&remote.get_object(manifest_key).await?)?;
    manifest.validate()?;
    Ok(ResolvedManifest {
        manifest_key: manifest_key.to_string(),
        manifest,
    })
}

fn current_ref_key(prefix: &str) -> String {
    format!("{prefix}/refs/current.json")
}

fn manifest_key(prefix: &str, generation: u64) -> String {
    format!("{prefix}/snapshots/{generation}/manifest.json")
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use bytes::Bytes;
    use tempfile::tempdir;

    use crate::config::ServerConfig;
    use crate::journal::{JournalOperation, JournalRecord};
    use crate::remote::StorageBackend;

    use super::Export;

    #[derive(Default)]
    struct MemoryRemote {
        objects: Mutex<HashMap<String, Vec<u8>>>,
        deleted: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl StorageBackend for MemoryRemote {
        async fn get_range(&self, key: &str, offset: u64, len: u64) -> crate::Result<Bytes> {
            let objects = self.objects.lock().unwrap();
            let bytes = objects.get(key).unwrap();
            Ok(Bytes::copy_from_slice(
                &bytes[offset as usize..(offset + len) as usize],
            ))
        }

        async fn get_object(&self, key: &str) -> crate::Result<Bytes> {
            let objects = self.objects.lock().unwrap();
            Ok(Bytes::copy_from_slice(objects.get(key).unwrap()))
        }

        async fn put_bytes(&self, key: &str, body: Bytes) -> crate::Result<()> {
            self.objects
                .lock()
                .unwrap()
                .insert(key.to_string(), body.to_vec());
            Ok(())
        }

        async fn put_file(&self, key: &str, path: &Path) -> crate::Result<()> {
            let bytes = std::fs::read(path)?;
            self.objects.lock().unwrap().insert(key.to_string(), bytes);
            Ok(())
        }

        async fn delete_object(&self, key: &str) -> crate::Result<()> {
            self.objects.lock().unwrap().remove(key);
            self.deleted.lock().unwrap().push(key.to_string());
            Ok(())
        }
    }

    fn base_config(dir: &Path) -> ServerConfig {
        ServerConfig {
            export_id: "export".to_string(),
            cache_dir: dir.join("cache"),
            storage: crate::config::StorageConfig {
                backend: crate::config::StorageBackendKind::S3,
                bucket: "bucket".to_string(),
                prefix: "exports/export".to_string(),
                region: "us-east-1".to_string(),
                endpoint_url: None,
                r2_account_id: None,
            },
            listen: "127.0.0.1:10809".parse().unwrap(),
            admin_sock: dir.join("admin.sock"),
            chunk_size: 4,
            snapshot_id: None,
            image_size: Some(8),
            clone_source: None,
        }
    }

    fn clone_config(dir: &Path) -> ServerConfig {
        ServerConfig {
            export_id: "clone".to_string(),
            cache_dir: dir.join("clone-cache"),
            storage: crate::config::StorageConfig {
                backend: crate::config::StorageBackendKind::S3,
                bucket: "bucket".to_string(),
                prefix: "exports/clone".to_string(),
                region: "us-east-1".to_string(),
                endpoint_url: None,
                r2_account_id: None,
            },
            listen: "127.0.0.1:10810".parse().unwrap(),
            admin_sock: dir.join("clone-admin.sock"),
            chunk_size: 4,
            snapshot_id: None,
            image_size: None,
            clone_source: Some(crate::config::CloneSourceConfig {
                prefix: "exports/export".to_string(),
                snapshot_id: Some(2),
            }),
        }
    }

    #[tokio::test]
    async fn partial_write_materializes_remote_chunk() {
        let dir = tempdir().unwrap();
        let remote = Arc::new(MemoryRemote::default());
        remote
            .put_bytes(
                "exports/export/base/full.blob",
                Bytes::from_static(b"abcdefgh"),
            )
            .await
            .unwrap();
        remote
            .put_bytes(
                "exports/export/refs/current.json",
                Bytes::from_static(
                    br#"{"generation":1,"manifest_key":"exports/export/snapshots/1/manifest.json"}"#,
                ),
            )
            .await
            .unwrap();
        remote
            .put_bytes(
                "exports/export/snapshots/1/manifest.json",
                Bytes::from_static(
                    br#"{
                    "version":2,
                    "export_id":"export",
                    "generation":1,
                    "image_size":8,
                    "chunk_size":4,
                    "chunk_count":2,
                    "created_at":"2026-03-07T00:00:00Z",
                    "base_ref":1,
                    "refs":[{"id":1,"path":"exports/export/base/full.blob"}],
                    "entries":[]
                }"#,
                ),
            )
            .await
            .unwrap();

        let export = Export::open(base_config(dir.path()), remote).await.unwrap();
        export.write(1, b"ZZ", false).await.unwrap();
        let data = export.read(0, 4).await.unwrap();
        assert_eq!(&data, b"aZZd");
    }

    #[tokio::test]
    async fn snapshot_publishes_delta_only_for_dirty_chunks() {
        let dir = tempdir().unwrap();
        let remote = Arc::new(MemoryRemote::default());
        let export = Export::create(base_config(dir.path()), remote.clone())
            .await
            .unwrap();
        export.write(0, b"abcd", false).await.unwrap();
        let first = export.snapshot().await.unwrap();
        assert!(first.snapshot_created);

        export.write(4, b"wxyz", false).await.unwrap();
        let second = export.snapshot().await.unwrap();
        assert_eq!(second.generation, 2);

        let manifest = remote
            .get_object("exports/export/snapshots/2/manifest.json")
            .await
            .unwrap();
        let manifest: serde_json::Value = serde_json::from_slice(&manifest).unwrap();
        assert_eq!(manifest["generation"], 2);
    }

    #[tokio::test]
    async fn open_uses_explicit_snapshot_id_instead_of_current_head() {
        let dir = tempdir().unwrap();
        let remote = Arc::new(MemoryRemote::default());
        remote
            .put_bytes(
                "exports/export/base/snapshot-1.blob",
                Bytes::from_static(b"abcdefgh"),
            )
            .await
            .unwrap();
        remote
            .put_bytes(
                "exports/export/base/snapshot-2.blob",
                Bytes::from_static(b"wxyz1234"),
            )
            .await
            .unwrap();
        remote
            .put_bytes(
                "exports/export/refs/current.json",
                Bytes::from_static(
                    br#"{"generation":2,"manifest_key":"exports/export/snapshots/2/manifest.json"}"#,
                ),
            )
            .await
            .unwrap();
        remote
            .put_bytes(
                "exports/export/snapshots/1/manifest.json",
                Bytes::from_static(
                    br#"{
                    "version":2,
                    "export_id":"export",
                    "generation":1,
                    "image_size":8,
                    "chunk_size":4,
                    "chunk_count":2,
                    "created_at":"2026-03-07T00:00:00Z",
                    "base_ref":1,
                    "refs":[{"id":1,"path":"exports/export/base/snapshot-1.blob"}],
                    "entries":[]
                }"#,
                ),
            )
            .await
            .unwrap();
        remote
            .put_bytes(
                "exports/export/snapshots/2/manifest.json",
                Bytes::from_static(
                    br#"{
                    "version":2,
                    "export_id":"export",
                    "generation":2,
                    "image_size":8,
                    "chunk_size":4,
                    "chunk_count":2,
                    "created_at":"2026-03-07T00:00:00Z",
                    "base_ref":1,
                    "refs":[{"id":1,"path":"exports/export/base/snapshot-2.blob"}],
                    "entries":[]
                }"#,
                ),
            )
            .await
            .unwrap();

        let mut config = base_config(dir.path());
        config.snapshot_id = Some(1);
        let export = Export::open(config, remote).await.unwrap();

        let status = export.status().await;
        assert_eq!(status.remote_head_generation, 1);
        assert_eq!(export.read(0, 8).await.unwrap(), b"abcdefgh");
    }

    #[tokio::test]
    async fn clone_reads_from_source_snapshot_and_starts_at_generation_zero() {
        let dir = tempdir().unwrap();
        let remote = Arc::new(MemoryRemote::default());
        remote
            .put_bytes(
                "exports/export/base/full.blob",
                Bytes::from_static(b"abcdefgh"),
            )
            .await
            .unwrap();
        remote
            .put_bytes(
                "exports/export/snapshots/2/manifest.json",
                Bytes::from_static(
                    br#"{
                    "version":2,
                    "export_id":"export",
                    "generation":2,
                    "image_size":8,
                    "chunk_size":4,
                    "chunk_count":2,
                    "created_at":"2026-03-07T00:00:00Z",
                    "base_ref":1,
                    "refs":[{"id":1,"path":"exports/export/base/full.blob"}],
                    "entries":[]
                }"#,
                ),
            )
            .await
            .unwrap();

        let clone = Export::clone_from_snapshot(clone_config(dir.path()), remote)
            .await
            .unwrap();

        let status = clone.status().await;
        assert_eq!(status.snapshot_generation, 0);
        assert_eq!(status.remote_head_generation, 2);
        assert_eq!(clone.read(0, 8).await.unwrap(), b"abcdefgh");
    }

    #[tokio::test]
    async fn first_clone_snapshot_uploads_full_base_blob_and_cuts_over_to_target_lineage() {
        let dir = tempdir().unwrap();
        let remote = Arc::new(MemoryRemote::default());
        remote
            .put_bytes(
                "exports/export/base/full.blob",
                Bytes::from_static(b"abcdefgh"),
            )
            .await
            .unwrap();
        remote
            .put_bytes(
                "exports/export/snapshots/2/manifest.json",
                Bytes::from_static(
                    br#"{
                    "version":2,
                    "export_id":"export",
                    "generation":2,
                    "image_size":8,
                    "chunk_size":4,
                    "chunk_count":2,
                    "created_at":"2026-03-07T00:00:00Z",
                    "base_ref":1,
                    "refs":[{"id":1,"path":"exports/export/base/full.blob"}],
                    "entries":[]
                }"#,
                ),
            )
            .await
            .unwrap();

        let clone = Export::clone_from_snapshot(clone_config(dir.path()), remote.clone())
            .await
            .unwrap();
        clone.write(0, b"WXYZ", false).await.unwrap();

        let snapshot = clone.snapshot().await.unwrap();
        assert_eq!(snapshot.generation, 1);

        let status = clone.status().await;
        assert_eq!(status.snapshot_generation, 1);
        assert_eq!(status.remote_head_generation, 1);

        let manifest = remote
            .get_object("exports/clone/snapshots/1/manifest.json")
            .await
            .unwrap();
        let manifest: serde_json::Value = serde_json::from_slice(&manifest).unwrap();
        assert_eq!(manifest["generation"], 1);
        assert_eq!(manifest["base_ref"], 1);
        assert_eq!(
            manifest["refs"][0]["path"],
            "exports/clone/snapshots/1/base.blob"
        );
        assert_eq!(
            remote
                .get_object("exports/clone/snapshots/1/base.blob")
                .await
                .unwrap(),
            Bytes::from_static(b"WXYZefgh")
        );
        assert!(
            !dir.path()
                .join("clone-cache")
                .join("clone.seed.json")
                .exists()
        );
    }

    #[tokio::test]
    async fn reopening_same_cache_dir_on_new_snapshot_invalidates_clean_resident_chunks() {
        let dir = tempdir().unwrap();
        let remote = Arc::new(MemoryRemote::default());
        remote
            .put_bytes(
                "exports/export/base/snapshot-1.blob",
                Bytes::from_static(b"abcdefgh"),
            )
            .await
            .unwrap();
        remote
            .put_bytes(
                "exports/export/base/snapshot-2.blob",
                Bytes::from_static(b"wxyz1234"),
            )
            .await
            .unwrap();
        remote
            .put_bytes(
                "exports/export/snapshots/1/manifest.json",
                Bytes::from_static(
                    br#"{
                    "version":2,
                    "export_id":"export",
                    "generation":1,
                    "image_size":8,
                    "chunk_size":4,
                    "chunk_count":2,
                    "created_at":"2026-03-07T00:00:00Z",
                    "base_ref":1,
                    "refs":[{"id":1,"path":"exports/export/base/snapshot-1.blob"}],
                    "entries":[]
                }"#,
                ),
            )
            .await
            .unwrap();
        remote
            .put_bytes(
                "exports/export/snapshots/2/manifest.json",
                Bytes::from_static(
                    br#"{
                    "version":2,
                    "export_id":"export",
                    "generation":2,
                    "image_size":8,
                    "chunk_size":4,
                    "chunk_count":2,
                    "created_at":"2026-03-07T00:00:00Z",
                    "base_ref":1,
                    "refs":[{"id":1,"path":"exports/export/base/snapshot-2.blob"}],
                    "entries":[]
                }"#,
                ),
            )
            .await
            .unwrap();

        let mut first_config = base_config(dir.path());
        first_config.snapshot_id = Some(1);
        let first = Export::open(first_config, remote.clone()).await.unwrap();
        assert_eq!(first.read(0, 8).await.unwrap(), b"abcdefgh");
        first.shutdown().unwrap();
        drop(first);

        let mut second_config = base_config(dir.path());
        second_config.snapshot_id = Some(2);
        let second = Export::open(second_config, remote).await.unwrap();
        assert_eq!(second.read(0, 8).await.unwrap(), b"wxyz1234");
    }

    #[tokio::test]
    async fn opening_different_snapshot_with_dirty_local_cache_is_rejected() {
        let dir = tempdir().unwrap();
        let remote = Arc::new(MemoryRemote::default());
        remote
            .put_bytes(
                "exports/export/base/snapshot-1.blob",
                Bytes::from_static(b"abcdefgh"),
            )
            .await
            .unwrap();
        remote
            .put_bytes(
                "exports/export/base/snapshot-2.blob",
                Bytes::from_static(b"wxyz1234"),
            )
            .await
            .unwrap();
        remote
            .put_bytes(
                "exports/export/snapshots/1/manifest.json",
                Bytes::from_static(
                    br#"{
                    "version":2,
                    "export_id":"export",
                    "generation":1,
                    "image_size":8,
                    "chunk_size":4,
                    "chunk_count":2,
                    "created_at":"2026-03-07T00:00:00Z",
                    "base_ref":1,
                    "refs":[{"id":1,"path":"exports/export/base/snapshot-1.blob"}],
                    "entries":[]
                }"#,
                ),
            )
            .await
            .unwrap();
        remote
            .put_bytes(
                "exports/export/snapshots/2/manifest.json",
                Bytes::from_static(
                    br#"{
                    "version":2,
                    "export_id":"export",
                    "generation":2,
                    "image_size":8,
                    "chunk_size":4,
                    "chunk_count":2,
                    "created_at":"2026-03-07T00:00:00Z",
                    "base_ref":1,
                    "refs":[{"id":1,"path":"exports/export/base/snapshot-2.blob"}],
                    "entries":[]
                }"#,
                ),
            )
            .await
            .unwrap();

        let mut first_config = base_config(dir.path());
        first_config.snapshot_id = Some(1);
        let first = Export::open(first_config, remote.clone()).await.unwrap();
        first.write(0, b"ZZZZ", false).await.unwrap();
        first.shutdown().unwrap();
        drop(first);

        let mut second_config = base_config(dir.path());
        second_config.snapshot_id = Some(2);
        let error = match Export::open(second_config, remote).await {
            Ok(_) => panic!("expected snapshot switch with dirty cache to fail"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("cannot switch to generation 2"));
    }

    #[tokio::test]
    async fn reset_cache_discards_local_state_and_allows_switching_snapshots() {
        let dir = tempdir().unwrap();
        let remote = Arc::new(MemoryRemote::default());
        remote
            .put_bytes(
                "exports/export/base/snapshot-1.blob",
                Bytes::from_static(b"abcdefgh"),
            )
            .await
            .unwrap();
        remote
            .put_bytes(
                "exports/export/base/snapshot-2.blob",
                Bytes::from_static(b"wxyz1234"),
            )
            .await
            .unwrap();
        remote
            .put_bytes(
                "exports/export/snapshots/1/manifest.json",
                Bytes::from_static(
                    br#"{
                    "version":2,
                    "export_id":"export",
                    "generation":1,
                    "image_size":8,
                    "chunk_size":4,
                    "chunk_count":2,
                    "created_at":"2026-03-07T00:00:00Z",
                    "base_ref":1,
                    "refs":[{"id":1,"path":"exports/export/base/snapshot-1.blob"}],
                    "entries":[]
                }"#,
                ),
            )
            .await
            .unwrap();
        remote
            .put_bytes(
                "exports/export/snapshots/2/manifest.json",
                Bytes::from_static(
                    br#"{
                    "version":2,
                    "export_id":"export",
                    "generation":2,
                    "image_size":8,
                    "chunk_size":4,
                    "chunk_count":2,
                    "created_at":"2026-03-07T00:00:00Z",
                    "base_ref":1,
                    "refs":[{"id":1,"path":"exports/export/base/snapshot-2.blob"}],
                    "entries":[]
                }"#,
                ),
            )
            .await
            .unwrap();

        let mut first_config = base_config(dir.path());
        first_config.snapshot_id = Some(1);
        let first = Export::open(first_config, remote.clone()).await.unwrap();
        assert_eq!(first.read(0, 8).await.unwrap(), b"abcdefgh");
        first.write(0, b"ZZZZ", false).await.unwrap();

        let reset = first.reset_cache().await.unwrap();
        assert_eq!(reset.discarded_dirty_chunks, 1);
        assert!(reset.discarded_resident_chunks >= 1);

        let status = first.status().await;
        assert_eq!(status.dirty_chunks, 0);
        assert_eq!(status.resident_chunks, 0);

        first.shutdown().unwrap();
        drop(first);

        let mut second_config = base_config(dir.path());
        second_config.snapshot_id = Some(2);
        let second = Export::open(second_config, remote).await.unwrap();
        assert_eq!(second.read(0, 8).await.unwrap(), b"wxyz1234");
    }

    #[tokio::test]
    async fn first_snapshot_of_new_export_stores_only_dirty_chunks() {
        let dir = tempdir().unwrap();
        let remote = Arc::new(MemoryRemote::default());
        let export = Export::create(base_config(dir.path()), remote.clone())
            .await
            .unwrap();

        export.write(0, b"abcd", false).await.unwrap();
        let first = export.snapshot().await.unwrap();
        assert_eq!(first.generation, 1);

        let manifest = remote
            .get_object("exports/export/snapshots/1/manifest.json")
            .await
            .unwrap();
        let manifest: serde_json::Value = serde_json::from_slice(&manifest).unwrap();
        assert_eq!(manifest["base_ref"], serde_json::Value::Null);
        assert_eq!(manifest["entries"].as_array().unwrap().len(), 1);
        assert_eq!(manifest["entries"][0]["index"], 0);
        assert!(
            !remote
                .objects
                .lock()
                .unwrap()
                .contains_key("exports/export/base/full.blob")
        );
        assert!(
            remote
                .objects
                .lock()
                .unwrap()
                .contains_key("exports/export/snapshots/1/delta.blob")
        );
    }

    #[tokio::test]
    async fn compact_rewrites_full_base_and_collects_old_delta() {
        let dir = tempdir().unwrap();
        let remote = Arc::new(MemoryRemote::default());
        let export = Export::create(base_config(dir.path()), remote.clone())
            .await
            .unwrap();

        export.write(0, b"abcd", false).await.unwrap();
        export.snapshot().await.unwrap();
        export.write(4, b"wxyz", false).await.unwrap();
        export.snapshot().await.unwrap();

        let compact = export.compact().await.unwrap();
        assert_eq!(compact.generation, 3);
        assert!(
            remote
                .deleted
                .lock()
                .unwrap()
                .iter()
                .any(|key| key == "exports/export/snapshots/1/delta.blob")
        );
        assert!(
            remote
                .objects
                .lock()
                .unwrap()
                .contains_key("exports/export/snapshots/3/base.blob")
        );
    }

    #[tokio::test]
    async fn startup_recovery_cleans_journal_and_keeps_dirty_cache() {
        let dir = tempdir().unwrap();
        let remote = Arc::new(MemoryRemote::default());
        let export = Export::create(base_config(dir.path()), remote.clone())
            .await
            .unwrap();
        export.write(0, b"abcd", false).await.unwrap();

        let journal_path = dir.path().join("cache").join("snapshot.journal.json");
        let staging_path = dir.path().join("cache").join("pending.delta");
        std::fs::write(&staging_path, b"stale").unwrap();
        JournalRecord {
            version: 1,
            operation: JournalOperation::Snapshot,
            generation: 1,
            staging_path: Some(staging_path.display().to_string()),
            object_key: "exports/export/snapshots/1/delta.blob".to_string(),
            manifest_key: "exports/export/snapshots/1/manifest.json".to_string(),
        }
        .persist(&journal_path)
        .unwrap();

        drop(export);

        let reopened = Export::create(base_config(dir.path()), remote)
            .await
            .unwrap();
        let status = reopened.status().await;
        assert_eq!(status.dirty_chunks, 1);
        assert!(!journal_path.exists());
        assert!(!staging_path.exists());
    }
}
