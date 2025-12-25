//! SQLite state store implementation.
//!
//! Implements from storage.dol:
//! - `storage.sqlite_backend` gene: embedded database, single file, ACID transactions
//!
//! Features:
//! - WAL mode for concurrent readers
//! - Atomic transactions
//! - Watch via in-memory channels (per-process)

use async_trait::async_trait;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use std::path::Path;
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, info};

use crate::error::{Result, StateError};
use crate::store::{validate_key, Entry, StateStore, TransactionOp};
use crate::watch::{EventType, WatchEvent, WatchSender, WatchStream};

/// SQLite implementation of StateStore.
///
/// Uses WAL mode for performance and durability.
/// Watch notifications are in-process only (not shared across processes).
pub struct SqliteStore {
    pool: SqlitePool,
    watcher: WatchSender,
}

impl SqliteStore {
    /// Open or create a SQLite store at the given path.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        info!("Opening SQLite store at {:?}", path);

        let options = SqliteConnectOptions::from_str(&format!("sqlite:{}", path.display()))
            .map_err(|e| StateError::ConnectionError(e.to_string()))?
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(30));

        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await
            .map_err(|e| StateError::ConnectionError(e.to_string()))?;

        let store = Self {
            pool,
            watcher: WatchSender::new(1024),
        };

        store.init_schema().await?;
        Ok(store)
    }

    /// Create an in-memory SQLite store (for testing).
    pub async fn in_memory() -> Result<Self> {
        let options = SqliteConnectOptions::from_str("sqlite::memory:")
            .map_err(|e| StateError::ConnectionError(e.to_string()))?
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .map_err(|e| StateError::ConnectionError(e.to_string()))?;

        let store = Self {
            pool,
            watcher: WatchSender::new(1024),
        };

        store.init_schema().await?;
        Ok(store)
    }

    /// Initialize the database schema.
    async fn init_schema(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS kv_store (
                key TEXT PRIMARY KEY NOT NULL,
                value BLOB NOT NULL,
                version INTEGER NOT NULL DEFAULT 1,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_kv_store_prefix ON kv_store(key);
            "#,
        )
        .execute(&self.pool)
        .await?;

        debug!("SQLite schema initialized");
        Ok(())
    }

    /// Get current Unix timestamp.
    fn now_unix() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    /// Convert Unix timestamp to SystemTime.
    fn unix_to_system_time(unix: i64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(unix as u64)
    }
}

#[async_trait]
impl StateStore for SqliteStore {
    async fn get(&self, key: &str) -> Result<Option<Entry>> {
        validate_key(key)?;

        let row: Option<(String, Vec<u8>, i64, i64, i64)> = sqlx::query_as(
            "SELECT key, value, version, created_at, updated_at FROM kv_store WHERE key = ?",
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|(key, value, version, created_at, updated_at)| Entry {
            key,
            value,
            version: version as u64,
            created_at: Self::unix_to_system_time(created_at),
            updated_at: Self::unix_to_system_time(updated_at),
        }))
    }

    async fn set(&self, key: &str, value: Vec<u8>) -> Result<u64> {
        validate_key(key)?;

        let now = Self::now_unix();

        // Get existing entry for versioning and watch events
        let existing = self.get(key).await?;

        let new_version = existing.as_ref().map(|e| e.version + 1).unwrap_or(1);

        sqlx::query(
            r#"
            INSERT INTO kv_store (key, value, version, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                version = version + 1,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(key)
        .bind(&value)
        .bind(new_version as i64)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;

        // Send watch event
        let event = if let Some(old) = existing {
            WatchEvent::updated(key, old.value, value, new_version)
        } else {
            WatchEvent::created(key, value, new_version)
        };
        self.watcher.send(event);

        Ok(new_version)
    }

    async fn delete(&self, key: &str) -> Result<()> {
        validate_key(key)?;

        let existing = self.get(key).await?;

        sqlx::query("DELETE FROM kv_store WHERE key = ?")
            .bind(key)
            .execute(&self.pool)
            .await?;

        if let Some(entry) = existing {
            self.watcher.send(WatchEvent::deleted(key, entry.value));
        }

        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let pattern = format!("{}%", prefix);
        let rows: Vec<(String,)> =
            sqlx::query_as("SELECT key FROM kv_store WHERE key LIKE ? ORDER BY key")
                .bind(&pattern)
                .fetch_all(&self.pool)
                .await?;

        Ok(rows.into_iter().map(|(k,)| k).collect())
    }

    async fn compare_and_set(
        &self,
        key: &str,
        expected_version: u64,
        value: Vec<u8>,
    ) -> Result<u64> {
        validate_key(key)?;

        let existing = self.get(key).await?;
        let current_version = existing.as_ref().map(|e| e.version).unwrap_or(0);

        if current_version != expected_version {
            return Err(StateError::VersionConflict {
                expected: expected_version,
                found: current_version,
            });
        }

        let now = Self::now_unix();
        let new_version = current_version + 1;

        if expected_version == 0 {
            // Create new entry
            sqlx::query(
                "INSERT INTO kv_store (key, value, version, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            )
            .bind(key)
            .bind(&value)
            .bind(new_version as i64)
            .bind(now)
            .bind(now)
            .execute(&self.pool)
            .await?;

            self.watcher
                .send(WatchEvent::created(key, value, new_version));
        } else {
            // Update existing
            sqlx::query(
                "UPDATE kv_store SET value = ?, version = ?, updated_at = ? WHERE key = ? AND version = ?",
            )
            .bind(&value)
            .bind(new_version as i64)
            .bind(now)
            .bind(key)
            .bind(expected_version as i64)
            .execute(&self.pool)
            .await?;

            if let Some(old) = existing {
                self.watcher
                    .send(WatchEvent::updated(key, old.value, value, new_version));
            }
        }

        Ok(new_version)
    }

    async fn create_if_not_exists(&self, key: &str, value: Vec<u8>) -> Result<u64> {
        validate_key(key)?;

        if self.exists(key).await? {
            return Err(StateError::AlreadyExists(key.to_string()));
        }

        let now = Self::now_unix();

        sqlx::query(
            "INSERT INTO kv_store (key, value, version, created_at, updated_at) VALUES (?, ?, 1, ?, ?)",
        )
        .bind(key)
        .bind(&value)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            // Handle unique constraint violation
            if e.to_string().contains("UNIQUE constraint") {
                StateError::AlreadyExists(key.to_string())
            } else {
                e.into()
            }
        })?;

        self.watcher.send(WatchEvent::created(key, value, 1));
        Ok(1)
    }

    async fn transaction(&self, ops: Vec<TransactionOp>) -> Result<()> {
        // Start a transaction
        let mut tx = self.pool.begin().await?;

        // First validate all CheckVersion operations
        for op in &ops {
            if let TransactionOp::CheckVersion {
                key,
                expected_version,
            } = op
            {
                let row: Option<(i64,)> =
                    sqlx::query_as("SELECT version FROM kv_store WHERE key = ?")
                        .bind(key)
                        .fetch_optional(&mut *tx)
                        .await?;

                let current_version = row.map(|(v,)| v as u64).unwrap_or(0);
                if current_version != *expected_version {
                    return Err(StateError::VersionConflict {
                        expected: *expected_version,
                        found: current_version,
                    });
                }
            }
        }

        // Collect events to send after commit
        let mut events = Vec::new();
        let now = Self::now_unix();

        // Apply all Set and Delete operations
        for op in ops {
            match op {
                TransactionOp::Set { key, value } => {
                    validate_key(&key)?;

                    // Get existing for versioning
                    let row: Option<(Vec<u8>, i64)> =
                        sqlx::query_as("SELECT value, version FROM kv_store WHERE key = ?")
                            .bind(&key)
                            .fetch_optional(&mut *tx)
                            .await?;

                    let (new_version, event_type, old_value) = if let Some((old_val, ver)) = row {
                        ((ver + 1) as u64, EventType::Updated, Some(old_val))
                    } else {
                        (1, EventType::Created, None)
                    };

                    sqlx::query(
                        r#"
                        INSERT INTO kv_store (key, value, version, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(key) DO UPDATE SET
                            value = excluded.value,
                            version = version + 1,
                            updated_at = excluded.updated_at
                        "#,
                    )
                    .bind(&key)
                    .bind(&value)
                    .bind(new_version as i64)
                    .bind(now)
                    .bind(now)
                    .execute(&mut *tx)
                    .await?;

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

                    let row: Option<(Vec<u8>,)> =
                        sqlx::query_as("SELECT value FROM kv_store WHERE key = ?")
                            .bind(&key)
                            .fetch_optional(&mut *tx)
                            .await?;

                    sqlx::query("DELETE FROM kv_store WHERE key = ?")
                        .bind(&key)
                        .execute(&mut *tx)
                        .await?;

                    if let Some((old_value,)) = row {
                        events.push(WatchEvent::deleted(&key, old_value));
                    }
                }
                TransactionOp::CheckVersion { .. } => {
                    // Already validated above
                }
            }
        }

        // Commit transaction
        tx.commit().await?;

        // Send events after successful commit
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
    async fn test_sqlite_set_and_get() {
        let store = SqliteStore::in_memory().await.unwrap();

        let version = store.set("/test/key", b"value".to_vec()).await.unwrap();
        assert_eq!(version, 1);

        let entry = store.get("/test/key").await.unwrap().unwrap();
        assert_eq!(entry.key, "/test/key");
        assert_eq!(entry.value, b"value");
        assert_eq!(entry.version, 1);
    }

    #[tokio::test]
    async fn test_sqlite_version_increment() {
        let store = SqliteStore::in_memory().await.unwrap();

        let v1 = store.set("/key", b"v1".to_vec()).await.unwrap();
        let v2 = store.set("/key", b"v2".to_vec()).await.unwrap();

        assert_eq!(v1, 1);
        assert_eq!(v2, 2);
    }

    #[tokio::test]
    async fn test_sqlite_delete() {
        let store = SqliteStore::in_memory().await.unwrap();

        store.set("/key", b"value".to_vec()).await.unwrap();
        assert!(store.exists("/key").await.unwrap());

        store.delete("/key").await.unwrap();
        assert!(!store.exists("/key").await.unwrap());
    }

    #[tokio::test]
    async fn test_sqlite_list() {
        let store = SqliteStore::in_memory().await.unwrap();

        store.set("/workloads/a", b"1".to_vec()).await.unwrap();
        store.set("/workloads/b", b"2".to_vec()).await.unwrap();
        store.set("/nodes/n1", b"3".to_vec()).await.unwrap();

        let workloads = store.list("/workloads/").await.unwrap();
        assert_eq!(workloads.len(), 2);
    }

    #[tokio::test]
    async fn test_sqlite_compare_and_set() {
        let store = SqliteStore::in_memory().await.unwrap();

        store.set("/key", b"v1".to_vec()).await.unwrap();

        let v2 = store
            .compare_and_set("/key", 1, b"v2".to_vec())
            .await
            .unwrap();
        assert_eq!(v2, 2);

        // Conflict
        let result = store.compare_and_set("/key", 1, b"v3".to_vec()).await;
        assert!(matches!(result, Err(StateError::VersionConflict { .. })));
    }

    #[tokio::test]
    async fn test_sqlite_create_if_not_exists() {
        let store = SqliteStore::in_memory().await.unwrap();

        let v1 = store
            .create_if_not_exists("/key", b"v1".to_vec())
            .await
            .unwrap();
        assert_eq!(v1, 1);

        let result = store.create_if_not_exists("/key", b"v2".to_vec()).await;
        assert!(matches!(result, Err(StateError::AlreadyExists(_))));
    }

    #[tokio::test]
    async fn test_sqlite_transaction() {
        let store = SqliteStore::in_memory().await.unwrap();

        store.set("/a", b"1".to_vec()).await.unwrap();
        store.set("/b", b"2".to_vec()).await.unwrap();

        store
            .transaction(vec![
                TransactionOp::check_version("/a", 1),
                TransactionOp::set("/a", b"new-a".to_vec()),
                TransactionOp::set("/c", b"new-c".to_vec()),
            ])
            .await
            .unwrap();

        let a = store.get("/a").await.unwrap().unwrap();
        assert_eq!(a.value, b"new-a");
        assert!(store.exists("/c").await.unwrap());
    }

    #[tokio::test]
    async fn test_sqlite_transaction_rollback() {
        let store = SqliteStore::in_memory().await.unwrap();

        store.set("/a", b"1".to_vec()).await.unwrap();
        store.set("/a", b"2".to_vec()).await.unwrap(); // version 2

        let result = store
            .transaction(vec![
                TransactionOp::check_version("/a", 1), // wrong version
                TransactionOp::set("/a", b"3".to_vec()),
                TransactionOp::set("/b", b"new".to_vec()),
            ])
            .await;

        assert!(matches!(result, Err(StateError::VersionConflict { .. })));

        // Transaction should have rolled back
        let a = store.get("/a").await.unwrap().unwrap();
        assert_eq!(a.value, b"2");
        assert!(!store.exists("/b").await.unwrap());
    }
}
