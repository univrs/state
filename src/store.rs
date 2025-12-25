//! StateStore trait - the core abstraction for all storage backends.
//!
//! Implements from storage.dol:
//! - `storage.key` gene: hierarchical, path-like keys
//! - `storage.value` gene: opaque bytes
//! - `storage.version` gene: u64 monotonic version per key
//! - `storage.entry` gene: key + value + version + timestamps
//! - Core operations: get, set, delete, list
//! - Conditional operations: compare_and_set, create_if_not_exists
//! - Transactions: atomic multi-key operations
//! - Watch: change notifications

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::time::SystemTime;

use crate::error::{Result, StateError};
use crate::watch::WatchStream;

/// Maximum key length in bytes.
pub const MAX_KEY_LENGTH: usize = 1024;

/// A stored entry with metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    /// The key.
    pub key: String,
    /// The value (opaque bytes).
    pub value: Vec<u8>,
    /// Monotonically increasing version (per key).
    pub version: u64,
    /// When the entry was created.
    pub created_at: SystemTime,
    /// When the entry was last updated.
    pub updated_at: SystemTime,
}

impl Entry {
    /// Deserialize the value as JSON.
    pub fn value_json<T: DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_slice(&self.value).map_err(Into::into)
    }
}

/// An operation within a transaction.
#[derive(Debug, Clone)]
pub enum TransactionOp {
    /// Set a key to a value.
    Set { key: String, value: Vec<u8> },
    /// Delete a key.
    Delete { key: String },
    /// Check that a key has a specific version (fails transaction if not).
    CheckVersion { key: String, expected_version: u64 },
}

impl TransactionOp {
    /// Create a Set operation.
    pub fn set(key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        Self::Set {
            key: key.into(),
            value: value.into(),
        }
    }

    /// Create a Set operation with JSON-encoded value.
    pub fn set_json<T: Serialize>(key: impl Into<String>, value: &T) -> Result<Self> {
        let bytes = serde_json::to_vec(value)?;
        Ok(Self::Set {
            key: key.into(),
            value: bytes,
        })
    }

    /// Create a Delete operation.
    pub fn delete(key: impl Into<String>) -> Self {
        Self::Delete { key: key.into() }
    }

    /// Create a CheckVersion operation.
    pub fn check_version(key: impl Into<String>, expected_version: u64) -> Self {
        Self::CheckVersion {
            key: key.into(),
            expected_version,
        }
    }
}

/// Validate that a key is well-formed.
pub fn validate_key(key: &str) -> Result<()> {
    if key.is_empty() {
        return Err(StateError::InvalidKey("key cannot be empty".to_string()));
    }
    if key.len() > MAX_KEY_LENGTH {
        return Err(StateError::InvalidKey(format!(
            "key exceeds maximum length of {} bytes",
            MAX_KEY_LENGTH
        )));
    }
    if !key.starts_with('/') {
        return Err(StateError::InvalidKey(
            "key must start with '/'".to_string(),
        ));
    }
    Ok(())
}

/// The core state storage trait.
///
/// All storage backends (SQLite, memory, etcd) implement this trait.
/// Code should depend on this trait, not specific implementations.
#[async_trait]
pub trait StateStore: Send + Sync {
    /// Get an entry by key.
    ///
    /// Returns `None` if the key does not exist.
    async fn get(&self, key: &str) -> Result<Option<Entry>>;

    /// Get an entry and deserialize as JSON.
    async fn get_json<T: DeserializeOwned + Send>(&self, key: &str) -> Result<Option<T>> {
        match self.get(key).await? {
            Some(entry) => {
                let value: T = serde_json::from_slice(&entry.value)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Set a key to a value.
    ///
    /// Creates the key if it doesn't exist, or updates it if it does.
    /// Returns the new version number.
    async fn set(&self, key: &str, value: Vec<u8>) -> Result<u64>;

    /// Set a key to a JSON-encoded value.
    async fn set_json<T: Serialize + Send + Sync>(&self, key: &str, value: &T) -> Result<u64> {
        let bytes = serde_json::to_vec(value)?;
        self.set(key, bytes).await
    }

    /// Delete a key.
    ///
    /// Returns `Ok(())` if the key was deleted or didn't exist.
    async fn delete(&self, key: &str) -> Result<()>;

    /// List keys matching a prefix.
    ///
    /// Returns keys in lexicographic order.
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;

    /// Compare-and-set: update only if the current version matches expected.
    ///
    /// Returns the new version on success, or `VersionConflict` if the
    /// current version doesn't match.
    async fn compare_and_set(
        &self,
        key: &str,
        expected_version: u64,
        value: Vec<u8>,
    ) -> Result<u64>;

    /// Create a key only if it doesn't already exist.
    ///
    /// Returns the new version (1) on success, or `AlreadyExists` if the
    /// key already exists.
    async fn create_if_not_exists(&self, key: &str, value: Vec<u8>) -> Result<u64>;

    /// Execute a transaction atomically.
    ///
    /// All operations succeed or all fail. CheckVersion operations are
    /// evaluated first; if any fail, the transaction is aborted.
    async fn transaction(&self, ops: Vec<TransactionOp>) -> Result<()>;

    /// Watch for changes matching a pattern.
    ///
    /// Pattern can be an exact key or end with `*` for prefix matching.
    fn watch(&self, pattern: &str) -> Result<WatchStream>;

    /// Check if a key exists.
    async fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.get(key).await?.is_some())
    }

    /// Get the current version of a key (0 if not exists).
    async fn version(&self, key: &str) -> Result<u64> {
        Ok(self.get(key).await?.map(|e| e.version).unwrap_or(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_key_valid() {
        assert!(validate_key("/test").is_ok());
        assert!(validate_key("/workloads/abc-123").is_ok());
        assert!(validate_key("/nodes/n1/containers/c1").is_ok());
    }

    #[test]
    fn test_validate_key_empty() {
        let err = validate_key("").unwrap_err();
        assert!(matches!(err, StateError::InvalidKey(_)));
    }

    #[test]
    fn test_validate_key_no_slash() {
        let err = validate_key("test").unwrap_err();
        assert!(matches!(err, StateError::InvalidKey(_)));
    }

    #[test]
    fn test_validate_key_too_long() {
        let key = format!("/{}", "a".repeat(MAX_KEY_LENGTH));
        let err = validate_key(&key).unwrap_err();
        assert!(matches!(err, StateError::InvalidKey(_)));
    }

    #[test]
    fn test_transaction_op_creation() {
        let op = TransactionOp::set("/key", vec![1, 2, 3]);
        assert!(matches!(op, TransactionOp::Set { .. }));

        let op = TransactionOp::delete("/key");
        assert!(matches!(op, TransactionOp::Delete { .. }));

        let op = TransactionOp::check_version("/key", 5);
        assert!(matches!(
            op,
            TransactionOp::CheckVersion {
                expected_version: 5,
                ..
            }
        ));
    }

    #[test]
    fn test_transaction_op_json() {
        #[derive(Serialize)]
        struct TestData {
            name: String,
        }

        let data = TestData {
            name: "test".to_string(),
        };
        let op = TransactionOp::set_json("/key", &data).unwrap();
        if let TransactionOp::Set { value, .. } = op {
            assert!(String::from_utf8_lossy(&value).contains("test"));
        } else {
            panic!("expected Set operation");
        }
    }
}
