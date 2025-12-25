//! Error types for state storage operations.
//!
//! Implements from storage.dol:
//! - `storage.errors` trait: key_not_found, version_conflict, already_exists,
//!   transaction_failed, connection_error, storage_full

use thiserror::Error;

/// Errors that can occur during state storage operations.
#[derive(Debug, Error)]
pub enum StateError {
    /// Key does not exist in the store.
    #[error("key not found: {0}")]
    KeyNotFound(String),

    /// Expected version does not match current version (optimistic concurrency conflict).
    #[error("version conflict: expected {expected}, found {found}")]
    VersionConflict { expected: u64, found: u64 },

    /// Key already exists (for create_if_not_exists operations).
    #[error("key already exists: {0}")]
    AlreadyExists(String),

    /// Transaction failed due to one or more check failures.
    #[error("transaction failed: {0}")]
    TransactionFailed(String),

    /// Cannot connect to or communicate with storage backend.
    #[error("connection error: {0}")]
    ConnectionError(String),

    /// Storage is full, no space remaining.
    #[error("storage full: {0}")]
    StorageFull(String),

    /// Serialization or deserialization error.
    #[error("serialization error: {0}")]
    SerializationError(String),

    /// Invalid key format.
    #[error("invalid key: {0}")]
    InvalidKey(String),

    /// Database error from SQLx.
    #[error("database error: {0}")]
    DatabaseError(#[from] sqlx::Error),

    /// I/O error.
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),
}

/// Result type alias for state operations.
pub type Result<T> = std::result::Result<T, StateError>;

impl StateError {
    /// Returns true if this error is retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            StateError::VersionConflict { .. }
                | StateError::TransactionFailed(_)
                | StateError::ConnectionError(_)
        )
    }
}

impl From<serde_json::Error> for StateError {
    fn from(err: serde_json::Error) -> Self {
        StateError::SerializationError(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = StateError::KeyNotFound("/test/key".to_string());
        assert!(err.to_string().contains("key not found"));
        assert!(err.to_string().contains("/test/key"));
    }

    #[test]
    fn test_version_conflict_display() {
        let err = StateError::VersionConflict {
            expected: 5,
            found: 7,
        };
        assert!(err.to_string().contains("expected 5"));
        assert!(err.to_string().contains("found 7"));
    }

    #[test]
    fn test_retryable_errors() {
        assert!(StateError::VersionConflict {
            expected: 1,
            found: 2
        }
        .is_retryable());
        assert!(StateError::TransactionFailed("test".to_string()).is_retryable());
        assert!(StateError::ConnectionError("test".to_string()).is_retryable());
        assert!(!StateError::KeyNotFound("test".to_string()).is_retryable());
        assert!(!StateError::StorageFull("test".to_string()).is_retryable());
    }
}
