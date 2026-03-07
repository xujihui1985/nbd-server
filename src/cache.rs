use std::fs::{File, OpenOptions, create_dir_all};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::bitmap::Bitmap;
use crate::error::{Error, Result};
use crate::manifest::chunk_count;
use crate::meta::CacheMeta;

#[derive(Debug)]
pub struct LocalCache {
    raw_path: PathBuf,
    meta_path: PathBuf,
    raw_file: File,
    meta: Mutex<CacheMeta>,
}

impl LocalCache {
    pub fn create(
        path: &Path,
        export_id: String,
        image_size: u64,
        chunk_size: u64,
    ) -> Result<Self> {
        create_dir_all(path)?;
        let raw_path = path.join("cache.raw");
        let meta_path = path.join("cache.meta");
        let raw_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&raw_path)?;
        raw_file.set_len(image_size)?;

        let meta = CacheMeta::new(
            export_id,
            image_size,
            chunk_size,
            chunk_count(image_size, chunk_size),
        );
        meta.persist(&meta_path, true)?;

        Ok(Self {
            raw_path,
            meta_path,
            raw_file,
            meta: Mutex::new(meta),
        })
    }

    pub fn open(path: &Path) -> Result<Self> {
        let raw_path = path.join("cache.raw");
        let meta_path = path.join("cache.meta");
        let raw_file = OpenOptions::new().read(true).write(true).open(&raw_path)?;
        let meta = CacheMeta::load(&meta_path)?;

        Ok(Self {
            raw_path,
            meta_path,
            raw_file,
            meta: Mutex::new(meta),
        })
    }

    pub fn raw_path(&self) -> &Path {
        &self.raw_path
    }

    pub fn chunk_count(&self) -> usize {
        self.meta.lock().unwrap().chunk_count as usize
    }

    pub fn chunk_size(&self) -> u64 {
        self.meta.lock().unwrap().chunk_size
    }

    pub fn image_size(&self) -> u64 {
        self.meta.lock().unwrap().image_size
    }

    pub fn manifest_generation(&self) -> u64 {
        self.meta.lock().unwrap().manifest_generation
    }

    pub fn read_exact_at(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let mut buffer = vec![0_u8; len];
        let mut read = 0;
        while read < len {
            let count = self
                .raw_file
                .read_at(&mut buffer[read..], offset + read as u64)?;
            if count == 0 {
                return Err(Error::Io(std::io::Error::from(
                    std::io::ErrorKind::UnexpectedEof,
                )));
            }
            read += count;
        }
        Ok(buffer)
    }

    pub fn write_all_at(&self, offset: u64, data: &[u8]) -> Result<()> {
        let mut written = 0;
        while written < data.len() {
            let count = self
                .raw_file
                .write_at(&data[written..], offset + written as u64)?;
            if count == 0 {
                return Err(Error::Io(std::io::Error::from(
                    std::io::ErrorKind::WriteZero,
                )));
            }
            written += count;
        }
        Ok(())
    }

    pub fn sync_data(&self) -> Result<()> {
        self.raw_file.sync_data()?;
        Ok(())
    }

    pub fn snapshot_meta(&self) -> CacheMeta {
        self.meta.lock().unwrap().clone()
    }

    pub fn apply_crash_recovery(&self) -> Result<()> {
        let mut meta = self.meta.lock().unwrap();
        if !meta.clean_shutdown {
            let mut recovered = Bitmap::new(meta.chunk_count as usize);
            for index in meta.dirty.iter_set_bits() {
                recovered.set(index, true);
            }
            meta.resident = recovered;
            meta.snapshot_in_progress = false;
            meta.persist(&self.meta_path, true)?;
        }
        Ok(())
    }

    pub fn set_clean_shutdown(&self, clean_shutdown: bool) -> Result<()> {
        let mut meta = self.meta.lock().unwrap();
        meta.clean_shutdown = clean_shutdown;
        meta.persist(&self.meta_path, true)
    }

    pub fn set_snapshot_in_progress(&self, value: bool) -> Result<()> {
        let mut meta = self.meta.lock().unwrap();
        meta.snapshot_in_progress = value;
        meta.persist(&self.meta_path, true)
    }

    pub fn set_manifest_generation(&self, generation: u64) -> Result<()> {
        let mut meta = self.meta.lock().unwrap();
        meta.manifest_generation = generation;
        meta.persist(&self.meta_path, true)
    }

    pub fn is_resident(&self, index: usize) -> bool {
        self.meta.lock().unwrap().resident.get(index)
    }

    pub fn mark_resident(&self, index: usize) -> Result<()> {
        let mut meta = self.meta.lock().unwrap();
        meta.resident.set(index, true);
        meta.persist(&self.meta_path, false)
    }

    pub fn mark_resident_many(&self, indices: &[usize]) -> Result<()> {
        if indices.is_empty() {
            return Ok(());
        }
        let mut meta = self.meta.lock().unwrap();
        for index in indices {
            meta.resident.set(*index, true);
        }
        meta.persist(&self.meta_path, false)
    }

    pub fn mark_dirty(&self, index: usize) -> Result<()> {
        let mut meta = self.meta.lock().unwrap();
        meta.resident.set(index, true);
        meta.dirty.set(index, true);
        meta.persist(&self.meta_path, true)
    }

    pub fn clear_dirty_all(&self) -> Result<()> {
        let mut meta = self.meta.lock().unwrap();
        meta.dirty.clear_all();
        meta.persist(&self.meta_path, true)
    }

    pub fn dirty_indices(&self) -> Vec<usize> {
        self.meta.lock().unwrap().dirty.iter_set_bits().collect()
    }

    pub fn dirty_count(&self) -> usize {
        self.meta.lock().unwrap().dirty.count_ones()
    }

    pub fn resident_count(&self) -> usize {
        self.meta.lock().unwrap().resident.count_ones()
    }

    pub fn validate_layout(&self, export_id: &str, image_size: u64, chunk_size: u64) -> Result<()> {
        let meta = self.meta.lock().unwrap();
        if meta.export_id != export_id {
            return Err(Error::InvalidRequest(format!(
                "cache belongs to export {}, expected {}",
                meta.export_id, export_id
            )));
        }
        if meta.image_size != image_size {
            return Err(Error::InvalidRequest(format!(
                "cache image size {} does not match {}",
                meta.image_size, image_size
            )));
        }
        if meta.chunk_size != chunk_size {
            return Err(Error::InvalidRequest(format!(
                "cache chunk size {} does not match {}",
                meta.chunk_size, chunk_size
            )));
        }
        Ok(())
    }
}
