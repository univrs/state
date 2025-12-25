//! # univrs-state
//!
//! Unified state storage abstraction for the Univrs ecosystem.
//!
//! This crate implements the storage system specified in `storage.dol`,
//! providing a backend-agnostic key-value store with:
//!
//! - **Hierarchical keys**: Path-like keys (`/workloads/abc`, `/nodes/n1`)
//! - **Versioned entries**: Optimistic concurrency control
//! - **Atomic transactions**: Multi-key operations
//! - **Watch/notifications**: Reactive change streams
//!
//! ## Backends
//!
//! - [`SqliteStore`]: Embedded SQLite database (default, production-ready)
//! - [`MemoryStore`]: In-memory store (testing and development)
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use univrs_state::{MemoryStore, StateStore};
//!
//! #[tokio::main]
//! async fn main() -> univrs_state::Result<()> {
//!     let store = MemoryStore::new();
//!
//!     // Set a value
//!     let version = store.set("/workloads/my-app", b"running".to_vec()).await?;
//!     println!("Version: {}", version);
//!
//!     // Get a value
//!     if let Some(entry) = store.get("/workloads/my-app").await? {
//!         println!("Value: {:?}", String::from_utf8_lossy(&entry.value));
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! ## JSON Values
//!
//! ```rust,no_run
//! use serde::{Deserialize, Serialize};
//! use univrs_state::{MemoryStore, StateStore};
//!
//! #[derive(Debug, Serialize, Deserialize)]
//! struct Workload {
//!     name: String,
//!     replicas: u32,
//! }
//!
//! #[tokio::main]
//! async fn main() -> univrs_state::Result<()> {
//!     let store = MemoryStore::new();
//!
//!     let workload = Workload {
//!         name: "my-app".to_string(),
//!         replicas: 3,
//!     };
//!
//!     store.set_json("/workloads/my-app", &workload).await?;
//!
//!     let loaded: Workload = store.get_json("/workloads/my-app").await?.unwrap();
//!     println!("{:?}", loaded);
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Transactions
//!
//! ```rust,no_run
//! use univrs_state::{MemoryStore, StateStore, TransactionOp};
//!
//! #[tokio::main]
//! async fn main() -> univrs_state::Result<()> {
//!     let store = MemoryStore::new();
//!
//!     // Set up initial state
//!     store.set("/nodes/a/workloads", b"[w1]".to_vec()).await?;
//!     store.set("/nodes/b/workloads", b"[]".to_vec()).await?;
//!
//!     // Atomically move workload from node A to node B
//!     store.transaction(vec![
//!         TransactionOp::check_version("/nodes/a/workloads", 1),
//!         TransactionOp::check_version("/nodes/b/workloads", 1),
//!         TransactionOp::set("/nodes/a/workloads", b"[]".to_vec()),
//!         TransactionOp::set("/nodes/b/workloads", b"[w1]".to_vec()),
//!     ]).await?;
//!
//!     Ok(())
//! }
//! ```

pub mod error;
pub mod memory;
pub mod sqlite;
pub mod store;
pub mod watch;

// Re-export main types
pub use error::{Result, StateError};
pub use memory::MemoryStore;
pub use sqlite::SqliteStore;
pub use store::{Entry, StateStore, TransactionOp, MAX_KEY_LENGTH};
pub use watch::{EventType, WatchEvent, WatchStream};

/// Prelude for convenient imports.
pub mod prelude {
    pub use crate::error::{Result, StateError};
    pub use crate::memory::MemoryStore;
    pub use crate::sqlite::SqliteStore;
    pub use crate::store::{Entry, StateStore, TransactionOp};
    pub use crate::watch::{EventType, WatchEvent, WatchStream};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_memory_store_basic() {
        let store = MemoryStore::new();

        store.set("/test", b"value".to_vec()).await.unwrap();
        let entry = store.get("/test").await.unwrap().unwrap();
        assert_eq!(entry.value, b"value");
    }

    #[tokio::test]
    async fn test_stores_are_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}

        assert_send_sync::<MemoryStore>();
        // SqliteStore is also Send + Sync
    }
}
