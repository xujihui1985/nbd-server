use std::fs::{File, create_dir_all, rename};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::bitmap::Bitmap;
use crate::error::{Error, Result};

const META_MAGIC: &[u8; 8] = b"LZYNBDM1";

#[derive(Clone, Debug)]
pub struct CacheMeta {
    pub export_id: String,
    pub image_size: u64,
    pub chunk_size: u64,
    pub chunk_count: u64,
    pub manifest_generation: u64,
    pub clean_shutdown: bool,
    pub snapshot_in_progress: bool,
    pub dirty: Bitmap,
    pub resident: Bitmap,
}

impl CacheMeta {
    pub fn new(export_id: String, image_size: u64, chunk_size: u64, chunk_count: u64) -> Self {
        Self {
            export_id,
            image_size,
            chunk_size,
            chunk_count,
            manifest_generation: 0,
            clean_shutdown: false,
            snapshot_in_progress: false,
            dirty: Bitmap::new(chunk_count as usize),
            resident: Bitmap::new(chunk_count as usize),
        }
    }

    pub fn load(path: &Path) -> Result<Self> {
        let mut file = File::open(path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        Self::decode(&bytes)
    }

    pub fn persist(&self, path: &Path, sync: bool) -> Result<()> {
        create_dir_all(
            path.parent()
                .ok_or_else(|| Error::InvalidRequest("metadata path has no parent".to_string()))?,
        )?;
        let tmp_path = temp_meta_path(path);
        let encoded = self.encode()?;
        {
            let mut file = File::create(&tmp_path)?;
            file.write_all(&encoded)?;
            if sync {
                file.sync_all()?;
            }
        }
        rename(&tmp_path, path)?;
        if sync {
            File::open(path.parent().ok_or_else(|| {
                Error::InvalidRequest("metadata path has no parent".to_string())
            })?)?
            .sync_all()?;
        }
        Ok(())
    }

    fn encode(&self) -> Result<Vec<u8>> {
        let export_bytes = self.export_id.as_bytes();
        if export_bytes.len() > u16::MAX as usize {
            return Err(Error::InvalidRequest("export id too long".to_string()));
        }

        let mut bytes = Vec::new();
        bytes.extend_from_slice(META_MAGIC);
        bytes.extend_from_slice(&(1_u32).to_le_bytes());
        bytes.extend_from_slice(&(export_bytes.len() as u16).to_le_bytes());
        bytes.extend_from_slice(export_bytes);
        bytes.extend_from_slice(&self.image_size.to_le_bytes());
        bytes.extend_from_slice(&self.chunk_size.to_le_bytes());
        bytes.extend_from_slice(&self.chunk_count.to_le_bytes());
        bytes.extend_from_slice(&self.manifest_generation.to_le_bytes());
        bytes.push(self.clean_shutdown as u8);
        bytes.push(self.snapshot_in_progress as u8);
        bytes.extend_from_slice(&(self.dirty.bytes().len() as u32).to_le_bytes());
        bytes.extend_from_slice(self.dirty.bytes());
        bytes.extend_from_slice(self.resident.bytes());
        Ok(bytes)
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let mut cursor = 0;

        let magic = take(bytes, &mut cursor, META_MAGIC.len())?;
        if magic != META_MAGIC {
            return Err(Error::InvalidRequest("invalid metadata magic".to_string()));
        }

        let version = u32::from_le_bytes(take_array::<4>(bytes, &mut cursor)?);
        if version != 1 {
            return Err(Error::InvalidRequest(format!(
                "unsupported metadata version {version}"
            )));
        }

        let export_len = u16::from_le_bytes(take_array::<2>(bytes, &mut cursor)?) as usize;
        let export_id = String::from_utf8(take(bytes, &mut cursor, export_len)?.to_vec())
            .map_err(|error| Error::InvalidRequest(error.to_string()))?;
        let image_size = u64::from_le_bytes(take_array::<8>(bytes, &mut cursor)?);
        let chunk_size = u64::from_le_bytes(take_array::<8>(bytes, &mut cursor)?);
        let chunk_count = u64::from_le_bytes(take_array::<8>(bytes, &mut cursor)?);
        let manifest_generation = u64::from_le_bytes(take_array::<8>(bytes, &mut cursor)?);
        let clean_shutdown = take(bytes, &mut cursor, 1)?[0] != 0;
        let snapshot_in_progress = take(bytes, &mut cursor, 1)?[0] != 0;
        let bitmap_len = u32::from_le_bytes(take_array::<4>(bytes, &mut cursor)?) as usize;
        let dirty = Bitmap::from_bytes(
            chunk_count as usize,
            take(bytes, &mut cursor, bitmap_len)?.to_vec(),
        );
        let resident = Bitmap::from_bytes(
            chunk_count as usize,
            take(bytes, &mut cursor, bitmap_len)?.to_vec(),
        );

        Ok(Self {
            export_id,
            image_size,
            chunk_size,
            chunk_count,
            manifest_generation,
            clean_shutdown,
            snapshot_in_progress,
            dirty,
            resident,
        })
    }
}

fn take<'a>(bytes: &'a [u8], cursor: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| Error::InvalidRequest("metadata overflow".to_string()))?;
    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| Error::InvalidRequest("truncated metadata".to_string()))?;
    *cursor = end;
    Ok(slice)
}

fn take_array<const N: usize>(bytes: &[u8], cursor: &mut usize) -> Result<[u8; N]> {
    let slice = take(bytes, cursor, N)?;
    let mut array = [0_u8; N];
    array.copy_from_slice(slice);
    Ok(array)
}

fn temp_meta_path(path: &Path) -> PathBuf {
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".tmp");
    PathBuf::from(tmp)
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::CacheMeta;

    #[test]
    fn metadata_round_trip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("cache.meta");

        let mut meta = CacheMeta::new("export".to_string(), 1024, 256, 4);
        meta.dirty.set(1, true);
        meta.resident.set(1, true);
        meta.persist(&path, true).unwrap();

        let restored = CacheMeta::load(&path).unwrap();
        assert_eq!(restored.export_id, "export");
        assert!(restored.dirty.get(1));
        assert!(restored.resident.get(1));
    }
}
