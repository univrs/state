//! In-memory state store implementation.
//!
//! Implements from storage.dol:
//! - `storage.memory_backend` gene: in-process HashMap, for testing only
//!
//! This implementation is NOT durable - data is lost on process exit.
//! Use for testing and development only.

use async_trait::async_trait;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::SystemTime;

use crate::error::{Result, StateError};
use crate::store::{validate_key, Entry, StateStore, TransactionOp};
use crate::watch::{EventType, WatchEvent, WatchSender, WatchStream};

/// In-memory implementation of StateStore.
///
/// Uses a BTreeMap for ordered key iteration and RwLock for concurrency.
/// Suitable for testing and development only.
pub struct MemoryStore {
    data: Arc<RwLock<BTreeMap<String, Entry>>>,
    watcher: WatchSender,
}

impl MemoryStore {
    /// Create a new empty in-memory store.
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(BTreeMap::new())),
            watcher: WatchSender::new(1024),
        }
    }

    /// Get the number of entries in the store.
    pub fn len(&self) -> usize {
        self.data.read().len()
    }

    /// Check if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.data.read().is_empty()
    }

    /// Clear all entries.
    pub fn clear(&self) {
        self.data.write().clear();
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StateStore for MemoryStore {
    async fn get(&self, key: &str) -> Result<Option<Entry>> {
        validate_key(key)?;
        Ok(self.data.read().get(key).cloned())
    }

    async fn set(&self, key: &str, value: Vec<u8>) -> Result<u64> {
        validate_key(key)?;

        let mut data = self.data.write();
        let now = SystemTime::now();

        let (new_version, event_type, old_value) = if let Some(existing) = data.get(key) {
            let old_val = existing.value.clone();
            (existing.version + 1, EventType::Updated, Some(old_val))
        } else {
            (1, EventType::Created, None)
        };

        let entry = Entry {
            key: key.to_string(),
            value: value.clone(),
            version: new_version,
            created_at: data
                .get(key)
                .map(|e| e.created_at)
                .unwrap_or(now),
            updated_at: now,
        };

        data.insert(key.to_string(), entry);

        // Send watch event
        let event = match event_type {
            EventType::Created => WatchEvent::created(key, value, new_version),
            EventType::Updated => {
                WatchEvent::updated(key, old_value.unwrap(), value, new_version)
            }
            _ => unreachable!(),
        };
        self.watcher.send(event);

        Ok(new_version)
    }

    async fn delete(&self, key: &str) -> Result<()> {
        validate_key(key)?;

        let mut data = self.data.write();
        if let Some(entry) = data.remove(key) {
            self.watcher.send(WatchEvent::deleted(key, entry.value));
        }
        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let data = self.data.read();
        let keys: Vec<String> = data
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        Ok(keys)
    }

    async fn compare_and_set(
        &self,
        key: &str,
        expected_version: u64,
        value: Vec<u8>,
    ) -> Result<u64> {
        validate_key(key)?;

        let mut data = self.data.write();
        let current_version = data.get(key).map(|e| e.version).unwrap_or(0);

        if current_version != expected_version {
            return Err(StateError::VersionConflict {
                expected: expected_version,
                found: current_version,
            });
        }

        let now = SystemTime::now();
        let new_version = current_version + 1;

        let (event_type, old_value) = if let Some(existing) = data.get(key) {
            (EventType::Updated, Some(existing.value.clone()))
        } else {
            (EventType::Created, None)
        };

        let entry = Entry {
            key: key.to_string(),
            value: value.clone(),
            version: new_version,
            created_at: data.get(key).map(|e| e.created_at).unwrap_or(now),
            updated_at: now,
        };

        data.insert(key.to_string(), entry);

        // Send watch event
        let event = match event_type {
            EventType::Created => WatchEvent::created(key, value, new_version),
            EventType::Updated => {
                WatchEvent::updated(key, old_value.unwrap(), value, new_version)
            }
            _ => unreachable!(),
        };
        self.watcher.send(event);

        Ok(new_version)
    }

    async fn create_if_not_exists(&self, key: &str, value: Vec<u8>) -> Result<u64> {
        validate_key(key)?;

        let mut data = self.data.write();
        if data.contains_key(key) {
            return Err(StateError::AlreadyExists(key.to_string()));
        }

        let now = SystemTime::now();
        let entry = Entry {
            key: key.to_string(),
            value: value.clone(),
            version: 1,
            created_at: now,
            updated_at: now,
        };

        data.insert(key.to_string(), entry);
        self.watcher.send(WatchEvent::created(key, value, 1));

        Ok(1)
    }

    async fn transaction(&self, ops: Vec<TransactionOp>) -> Result<()> {
        let mut data = self.data.write();

        // First, validate all CheckVersion operations
        for op in &ops {
            if let TransactionOp::CheckVersion {
                key,
                expected_version,
            } = op
            {
                let current_version = data.get(key).map(|e| e.version).unwrap_or(0);
                if current_version != *expected_version {
                    return Err(StateError::VersionConflict {
                        expected: *expected_version,
                        found: current_version,
                    });
                }
            }
        }

        // Then apply all Set and Delete operations
        let now = SystemTime::now();
        let mut events = Vec::new();

        for op in ops {
            match op {
                TransactionOp::Set { key, value } => {
                    validate_key(&key)?;

                    let (new_version, event_type, old_value) = if let Some(existing) = data.get(&key)
                    {
                        let old_val = existing.value.clone();
                        (existing.version + 1, EventType::Updated, Some(old_val))
                    } else {
                        (1, EventType::Created, None)
                    };

                    let entry = Entry {
                        key: key.clone(),
                        value: value.clone(),
                        version: new_version,
                        created_at: data.get(&key).map(|e| e.created_at).unwrap_or(now),
                        updated_at: now,
                    };

                    data.insert(key.clone(), entry);

                    let event = match event_type {
                        EventType::Created => WatchEvent::created(&key, value, new_version),
                        EventType::Updated => {
                            WatchEvent::updated(&key, old_value.unwrap(), value, new_version)
                        }
                        _ => unreachable!(),
                    };
                    events.push(event);
                }
                TransactionOp::Delete { key } => {
                    validate_key(&key)?;
                    if let Some(entry) = data.remove(&key) {
                        events.push(WatchEvent::deleted(&key, entry.value));
                    }
                }
                TransactionOp::CheckVersion { .. } => {
                    // Already validated above
                }
            }
        }

        // Send all events after successful transaction
        for event in events {
            self.watcher.send(event);
        }

        Ok(())
    }

    fn watch(&self, pattern: &str) -> Result<WatchStream> {
        Ok(self.watcher.subscribe(pattern))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_set_and_get() {
        let store = MemoryStore::new();

        let version = store.set("/test/key", b"value".to_vec()).await.unwrap();
        assert_eq!(version, 1);

        let entry = store.get("/test/key").await.unwrap().unwrap();
        assert_eq!(entry.key, "/test/key");
        assert_eq!(entry.value, b"value");
        assert_eq!(entry.version, 1);
    }

    #[tokio::test]
    async fn test_set_increments_version() {
        let store = MemoryStore::new();

        let v1 = store.set("/key", b"v1".to_vec()).await.unwrap();
        let v2 = store.set("/key", b"v2".to_vec()).await.unwrap();
        let v3 = store.set("/key", b"v3".to_vec()).await.unwrap();

        assert_eq!(v1, 1);
        assert_eq!(v2, 2);
        assert_eq!(v3, 3);
    }

    #[tokio::test]
    async fn test_get_nonexistent() {
        let store = MemoryStore::new();
        let result = store.get("/nonexistent").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_delete() {
        let store = MemoryStore::new();

        store.set("/key", b"value".to_vec()).await.unwrap();
        assert!(store.exists("/key").await.unwrap());

        store.delete("/key").await.unwrap();
        assert!(!store.exists("/key").await.unwrap());
    }

    #[tokio::test]
    async fn test_delete_nonexistent() {
        let store = MemoryStore::new();
        // Should not error
        store.delete("/nonexistent").await.unwrap();
    }

    #[tokio::test]
    async fn test_list() {
        let store = MemoryStore::new();

        store.set("/workloads/a", b"1".to_vec()).await.unwrap();
        store.set("/workloads/b", b"2".to_vec()).await.unwrap();
        store.set("/nodes/n1", b"3".to_vec()).await.unwrap();

        let workloads = store.list("/workloads/").await.unwrap();
        assert_eq!(workloads.len(), 2);
        assert!(workloads.contains(&"/workloads/a".to_string()));
        assert!(workloads.contains(&"/workloads/b".to_string()));
    }

    #[tokio::test]
    async fn test_compare_and_set_success() {
        let store = MemoryStore::new();

        store.set("/key", b"v1".to_vec()).await.unwrap();
        let v2 = store
            .compare_and_set("/key", 1, b"v2".to_vec())
            .await
            .unwrap();
        assert_eq!(v2, 2);
    }

    #[tokio::test]
    async fn test_compare_and_set_conflict() {
        let store = MemoryStore::new();

        store.set("/key", b"v1".to_vec()).await.unwrap();
        store.set("/key", b"v2".to_vec()).await.unwrap(); // version is now 2

        let result = store.compare_and_set("/key", 1, b"v3".to_vec()).await;
        assert!(matches!(
            result,
            Err(StateError::VersionConflict {
                expected: 1,
                found: 2
            })
        ));
    }

    #[tokio::test]
    async fn test_create_if_not_exists_success() {
        let store = MemoryStore::new();

        let version = store
            .create_if_not_exists("/key", b"value".to_vec())
            .await
            .unwrap();
        assert_eq!(version, 1);
    }

    #[tokio::test]
    async fn test_create_if_not_exists_already_exists() {
        let store = MemoryStore::new();

        store.set("/key", b"v1".to_vec()).await.unwrap();
        let result = store.create_if_not_exists("/key", b"v2".to_vec()).await;
        assert!(matches!(result, Err(StateError::AlreadyExists(_))));
    }

    #[tokio::test]
    async fn test_transaction_success() {
        let store = MemoryStore::new();

        store.set("/a", b"1".to_vec()).await.unwrap();
        store.set("/b", b"2".to_vec()).await.unwrap();

        store
            .transaction(vec![
                TransactionOp::check_version("/a", 1),
                TransactionOp::check_version("/b", 1),
                TransactionOp::set("/a", b"new-a".to_vec()),
                TransactionOp::set("/b", b"new-b".to_vec()),
            ])
            .await
            .unwrap();

        let a = store.get("/a").await.unwrap().unwrap();
        let b = store.get("/b").await.unwrap().unwrap();
        assert_eq!(a.value, b"new-a");
        assert_eq!(b.value, b"new-b");
    }

    #[tokio::test]
    async fn test_transaction_conflict() {
        let store = MemoryStore::new();

        store.set("/a", b"1".to_vec()).await.unwrap();
        store.set("/a", b"2".to_vec()).await.unwrap(); // version 2

        let result = store
            .transaction(vec![
                TransactionOp::check_version("/a", 1), // wrong version
                TransactionOp::set("/a", b"3".to_vec()),
            ])
            .await;

        assert!(matches!(result, Err(StateError::VersionConflict { .. })));

        // Value should be unchanged
        let a = store.get("/a").await.unwrap().unwrap();
        assert_eq!(a.value, b"2");
    }

    #[tokio::test]
    async fn test_json_operations() {
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct TestData {
            name: String,
            value: i32,
        }

        let store = MemoryStore::new();
        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        store.set_json("/data", &data).await.unwrap();
        let loaded: TestData = store.get_json("/data").await.unwrap().unwrap();
        assert_eq!(data, loaded);
    }

    #[tokio::test]
    async fn test_invalid_key() {
        let store = MemoryStore::new();

        let result = store.set("no-leading-slash", b"v".to_vec()).await;
        assert!(matches!(result, Err(StateError::InvalidKey(_))));
    }
}
