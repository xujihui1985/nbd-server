use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;

use crate::core::error::{Error, Result};
use crate::core::storage::object_store::{ObjectStore, StoredObject};

#[derive(Clone)]
struct MemoryObject {
    body: Vec<u8>,
    etag: String,
}

#[derive(Default)]
pub struct MemoryObjectStore {
    objects: Mutex<HashMap<String, MemoryObject>>,
    next_etag: Mutex<u64>,
}

#[async_trait]
impl ObjectStore for MemoryObjectStore {
    async fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Bytes> {
        let objects = self.objects.lock().unwrap();
        let bytes = &objects
            .get(key)
            .ok_or_else(|| Error::InvalidRequest(format!("missing object {key}")))?
            .body;
        Ok(Bytes::copy_from_slice(
            &bytes[offset as usize..(offset + len) as usize],
        ))
    }

    async fn get_object(&self, key: &str) -> Result<Bytes> {
        Ok(self.get_object_with_etag(key).await?.body)
    }

    async fn get_object_with_etag(&self, key: &str) -> Result<StoredObject> {
        let objects = self.objects.lock().unwrap();
        let object = objects
            .get(key)
            .ok_or_else(|| Error::InvalidRequest(format!("missing object {key}")))?;
        Ok(StoredObject {
            body: Bytes::copy_from_slice(&object.body),
            etag: Some(object.etag.clone()),
        })
    }

    async fn put_bytes(&self, key: &str, body: Bytes) -> Result<()> {
        self.put_object(key, body.to_vec());
        Ok(())
    }

    async fn put_bytes_if_match(&self, key: &str, body: Bytes, etag: &str) -> Result<bool> {
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

    async fn put_bytes_if_absent(&self, key: &str, body: Bytes) -> Result<bool> {
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

    async fn put_file(&self, key: &str, path: &Path) -> Result<()> {
        let bytes = std::fs::read(path)?;
        self.put_object(key, bytes);
        Ok(())
    }

    async fn delete_object(&self, key: &str) -> Result<()> {
        self.objects.lock().unwrap().remove(key);
        Ok(())
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
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

impl MemoryObjectStore {
    pub fn put_object(&self, key: &str, body: Vec<u8>) {
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
