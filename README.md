# univrs-state

Unified state storage abstraction for the Univrs ecosystem.

> [!CAUTION]
> This project is a research demonstrator. It is in early development and may change significantly. Using permissive Univrs tools in your repository requires careful attention to security considerations and careful human supervision, and even then things can still go wrong. Use it with caution, and at your own risk. See [Disclaimer](#disclaimer).

## Overview

`univrs-state` provides a backend-agnostic key-value store designed for distributed systems. It serves as the persistence layer for all Univrs components, offering a consistent API whether you're storing workload definitions, peer information, credit relationships, or any other structured data.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Application Layer                       │
│         (univrs-orchestrator, mycelial-network, etc.)        │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                      StateStore Trait                        │
│    get, set, delete, list, compare_and_set, transaction     │
└─────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
┌──────────────────────────┐    ┌──────────────────────────┐
│      SqliteStore         │    │      MemoryStore         │
│   (Production, Durable)  │    │   (Testing, Ephemeral)   │
└──────────────────────────┘    └──────────────────────────┘
```

## Key Concepts

### Hierarchical Keys

Keys are path-like strings that enable logical organization and efficient prefix queries:

```rust
/workloads/my-app-abc123
/workloads/my-app-abc123/instances/i1
/nodes/node-1/status
/peers/QmXyz.../reputation
/credits/alice/bob
```

### Versioned Entries

Every key has a monotonically increasing version number, enabling:
- **Optimistic concurrency control**: Prevent lost updates with `compare_and_set`
- **Change detection**: Know when data was last modified
- **Conflict resolution**: Detect concurrent modifications

```rust
// First write: version = 1
store.set("/config", b"v1".to_vec()).await?;

// Second write: version = 2
store.set("/config", b"v2".to_vec()).await?;

// Conditional update: only succeeds if version matches
store.compare_and_set("/config", 2, b"v3".to_vec()).await?;
```

### Atomic Transactions

Multi-key operations that either all succeed or all fail:

```rust
store.transaction(vec![
    TransactionOp::check_version("/nodes/a/workloads", 5),
    TransactionOp::check_version("/nodes/b/workloads", 3),
    TransactionOp::set("/nodes/a/workloads", new_a_workloads),
    TransactionOp::set("/nodes/b/workloads", new_b_workloads),
]).await?;
```

### Watch Streams

Reactive change notifications for building event-driven systems:

```rust
let mut stream = store.watch("/workloads/*")?;

while let Some(event) = stream.next().await {
    match event.event_type {
        EventType::Created => println!("New workload: {}", event.key),
        EventType::Updated => println!("Workload updated: {}", event.key),
        EventType::Deleted => println!("Workload removed: {}", event.key),
    }
}
```

## Storage Backends

### SqliteStore (Production)

The primary backend for production deployments:

- **Durability**: WAL mode ensures data survives crashes
- **Performance**: Optimized for concurrent readers
- **Portability**: Single-file database, easy to backup/migrate
- **ACID Transactions**: Full transactional guarantees

```rust
use univrs_state::SqliteStore;

let store = SqliteStore::open("./data/state.db").await?;
```

### MemoryStore (Testing)

In-memory backend for unit tests and development:

- **Fast**: No I/O overhead
- **Isolated**: Each test gets a fresh store
- **Same API**: Identical interface to SqliteStore

```rust
use univrs_state::MemoryStore;

let store = MemoryStore::new();
```

## Database Schema

The SQLite backend uses a simple, efficient schema:

```sql
CREATE TABLE kv_store (
    key TEXT PRIMARY KEY NOT NULL,    -- Hierarchical path
    value BLOB NOT NULL,               -- Opaque bytes (JSON/CBOR)
    version INTEGER NOT NULL,          -- Monotonic version
    created_at INTEGER NOT NULL,       -- Unix timestamp
    updated_at INTEGER NOT NULL        -- Unix timestamp
);

CREATE INDEX idx_kv_store_prefix ON kv_store(key);
```

## Use Cases in Univrs Ecosystem

### Orchestrator State

```rust
// Store workload definitions
store.set_json("/workloads/my-app", &workload_spec).await?;

// Track node assignments
store.set_json("/nodes/node-1/workloads", &["my-app"]).await?;

// Atomic workload migration
store.transaction(vec![
    TransactionOp::set("/nodes/old/workloads", remove_workload),
    TransactionOp::set("/nodes/new/workloads", add_workload),
]).await?;
```

### P2P Network State

```rust
// Peer discovery cache
store.set_json("/peers/QmXyz/info", &peer_info).await?;

// Credit relationships
store.set_json("/credits/alice/bob", &credit_line).await?;

// Reputation scores
store.set_json("/reputation/QmXyz", &reputation).await?;
```

### Configuration Management

```rust
// Cluster configuration with version control
let config = store.get_json::<ClusterConfig>("/config/cluster").await?;

// Safe configuration updates
store.compare_and_set("/config/cluster", config.version, new_config).await?;
```

## API Reference

### Core Operations

| Method | Description |
|--------|-------------|
| `get(key)` | Retrieve an entry by key |
| `get_json<T>(key)` | Retrieve and deserialize as JSON |
| `set(key, value)` | Create or update a key |
| `set_json(key, value)` | Serialize and store as JSON |
| `delete(key)` | Remove a key |
| `exists(key)` | Check if a key exists |
| `version(key)` | Get the current version (0 if missing) |
| `list(prefix)` | List all keys matching a prefix |

### Conditional Operations

| Method | Description |
|--------|-------------|
| `compare_and_set(key, version, value)` | Update only if version matches |
| `create_if_not_exists(key, value)` | Create only if key doesn't exist |
| `transaction(ops)` | Execute multiple operations atomically |

### Watch Operations

| Method | Description |
|--------|-------------|
| `watch(pattern)` | Subscribe to changes matching pattern |

Patterns support exact keys (`/workloads/my-app`) or prefix wildcards (`/workloads/*`).

## Error Handling

```rust
use univrs_state::{StateError, Result};

match store.compare_and_set("/key", 1, value).await {
    Ok(new_version) => println!("Updated to version {}", new_version),
    Err(StateError::VersionConflict { expected, found }) => {
        println!("Conflict: expected v{}, found v{}", expected, found);
    }
    Err(StateError::InvalidKey(msg)) => {
        println!("Bad key: {}", msg);
    }
    Err(e) => return Err(e.into()),
}
```

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
univrs-state = { git = "https://github.com/univrs/state" }
```

Or as a workspace dependency:

```toml
[workspace.dependencies]
univrs-state = { path = "../univrs-state" }
```

## Quick Start

```rust
use univrs_state::{SqliteStore, StateStore};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Workload {
    name: String,
    replicas: u32,
    image: String,
}

#[tokio::main]
async fn main() -> univrs_state::Result<()> {
    // Open or create the database
    let store = SqliteStore::open("./state.db").await?;

    // Store a workload
    let workload = Workload {
        name: "api-server".to_string(),
        replicas: 3,
        image: "myapp:latest".to_string(),
    };

    let version = store.set_json("/workloads/api-server", &workload).await?;
    println!("Stored workload at version {}", version);

    // Retrieve it
    let loaded: Workload = store.get_json("/workloads/api-server").await?.unwrap();
    println!("Loaded: {:?}", loaded);

    // List all workloads
    let keys = store.list("/workloads/").await?;
    println!("Workloads: {:?}", keys);

    // Watch for changes
    let mut stream = store.watch("/workloads/*")?;

    // In another task, changes would be received:
    // while let Some(event) = stream.next().await { ... }

    Ok(())
}
```

## Disclaimer

> [!IMPORTANT]
> **This is an experimental system. _We break things frequently_.**

- Not accepting contributions yet (but we plan to!)
- No stability guarantees
- Pin commits if you need consistency
- This is a learning resource, not production software
- **No support provided** - See [SUPPORT.md](SUPPORT.md)

---

## License

MIT License - see [LICENSE](LICENSE) for details.
