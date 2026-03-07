use std::fs::{File, create_dir_all, remove_file, rename};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JournalOperation {
    Snapshot,
    Compact,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JournalRecord {
    pub version: u32,
    pub operation: JournalOperation,
    pub generation: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub staging_path: Option<String>,
    pub object_key: String,
    pub manifest_key: String,
}

impl JournalRecord {
    pub fn persist(&self, path: &Path) -> Result<()> {
        create_dir_all(
            path.parent()
                .ok_or_else(|| Error::InvalidRequest("journal path has no parent".to_string()))?,
        )?;
        let tmp = temp_path(path);
        {
            let mut file = File::create(&tmp)?;
            file.write_all(&serde_json::to_vec_pretty(self)?)?;
            file.sync_all()?;
        }
        rename(&tmp, path)?;
        File::open(
            path.parent()
                .ok_or_else(|| Error::InvalidRequest("journal path has no parent".to_string()))?,
        )?
        .sync_all()?;
        Ok(())
    }

    pub fn load(path: &Path) -> Result<Option<Self>> {
        if !path.exists() {
            return Ok(None);
        }
        let mut bytes = Vec::new();
        File::open(path)?.read_to_end(&mut bytes)?;
        let record: Self = serde_json::from_slice(&bytes)?;
        if record.version != 1 {
            return Err(Error::InvalidRequest(format!(
                "unsupported journal version {}",
                record.version
            )));
        }
        Ok(Some(record))
    }

    pub fn clear(path: &Path) -> Result<()> {
        if path.exists() {
            remove_file(path)?;
        }
        Ok(())
    }
}

fn temp_path(path: &Path) -> PathBuf {
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".tmp");
    PathBuf::from(tmp)
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{JournalOperation, JournalRecord};

    #[test]
    fn journal_round_trip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("snapshot.journal.json");
        let record = JournalRecord {
            version: 1,
            operation: JournalOperation::Snapshot,
            generation: 2,
            staging_path: Some("/tmp/staging.blob".to_string()),
            object_key: "exports/export/snapshots/2/delta.blob".to_string(),
            manifest_key: "exports/export/snapshots/2/manifest.json".to_string(),
        };

        record.persist(&path).unwrap();
        let restored = JournalRecord::load(&path).unwrap().unwrap();
        assert_eq!(restored.generation, 2);
        assert_eq!(restored.operation, JournalOperation::Snapshot);
    }
}
