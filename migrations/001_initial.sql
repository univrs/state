-- Initial schema for univrs-state SQLite backend
-- Implements storage.dol key-value store

-- Main key-value store table
CREATE TABLE IF NOT EXISTS kv_store (
    -- Hierarchical path-like key (e.g., /workloads/abc-123)
    key TEXT PRIMARY KEY NOT NULL,

    -- Opaque value bytes (typically JSON or CBOR encoded)
    value BLOB NOT NULL,

    -- Monotonically increasing version per key
    -- Enables optimistic concurrency control
    version INTEGER NOT NULL DEFAULT 1,

    -- Timestamp when entry was first created
    created_at INTEGER NOT NULL,

    -- Timestamp of last update
    updated_at INTEGER NOT NULL
);

-- Index for efficient prefix queries (list operations)
CREATE INDEX IF NOT EXISTS idx_kv_store_prefix ON kv_store(key);

-- Schema version tracking
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY NOT NULL,
    value TEXT NOT NULL
);

-- Initialize schema version
INSERT OR IGNORE INTO meta (key, value) VALUES ('schema_version', '1');
