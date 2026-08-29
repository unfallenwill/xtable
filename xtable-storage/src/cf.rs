//! Column-family / redb table definitions.
//!
//! Phase 1 tables:
//! - `versions`: object_key → bincode-encoded VersionRecord
//! - `meta`: singleton bookkeeping keys → u64
//!
//! Phase 2 tables (added lazily, on open):
//! - `wal`: (seq_be_u128, txn_id) → bincode-encoded WalRecord
//! - `txn_state`: txn_id → bincode-encoded TxnStateRecord
//! - `read_set`: (txn_id, key) → bincode-encoded ReadSetEntry
//! - `write_set`: (txn_id, key) → bincode-encoded WriteSetEntry
//! - `staged_blobs`: body_handle → bincode-encoded BlobRecord
//! - `multipart`: upload_id → bincode-encoded MultipartState

use redb::TableDefinition;

/// `versions`: object_key (UTF-8) → bincode-encoded VersionRecord bytes.
pub const TBL_VERSIONS: TableDefinition<&str, &[u8]> = TableDefinition::new("xtable.versions");

/// `version_chains`: object_key (UTF-8) → bincode-encoded VersionChain bytes.
/// MVCC: per-key chain of versions, sorted by commit_version ascending.
pub const TBL_VERSION_CHAINS: TableDefinition<&str, &[u8]> = TableDefinition::new("xtable.version_chains");

/// `active_snapshots`: snapshot_version (u64) → ref-count (u64).
/// V9 fix: ref-count so multiple txns sharing a snapshot don't accidentally
/// release the pin when the first one commits.
pub const TBL_ACTIVE_SNAPSHOTS: TableDefinition<u64, u64> = TableDefinition::new("xtable.active_snapshots");

/// `meta`: singleton bookkeeping keys (string) → u64 value.
pub const TBL_META: TableDefinition<&str, u64> = TableDefinition::new("xtable.meta");

/// `wal`: WAL entries. Key is the WAL record's monotonic sequence (u64).
pub const TBL_WAL: TableDefinition<u64, &[u8]> = TableDefinition::new("xtable.wal");

/// `txn_state`: txn_id (ULID string) → bincode TxnStateRecord.
pub const TBL_TXN_STATE: TableDefinition<&str, &[u8]> = TableDefinition::new("xtable.txn_state");

/// `read_set`: (txn_id, key) → bincode ReadSetEntry.
pub const TBL_READ_SET: TableDefinition<(&str, &str), &[u8]> = TableDefinition::new("xtable.read_set");

/// `write_set`: (txn_id, key) → bincode WriteSetEntry.
pub const TBL_WRITE_SET: TableDefinition<(&str, &str), &[u8]> = TableDefinition::new("xtable.write_set");

/// `staged_blobs`: body_handle → bincode BlobRecord.
pub const TBL_STAGED_BLOBS: TableDefinition<&str, &[u8]> = TableDefinition::new("xtable.staged_blobs");

/// `multipart`: upload_id → bincode MultipartState.
pub const TBL_MULTIPART: TableDefinition<&str, &[u8]> = TableDefinition::new("xtable.multipart");

/// Record index for structured-data-space layer.
/// Key = (space, table, record_id) — bincode-encoded [`RecordIndexEntry`].
/// Maintained by post-commit hooks; consulted by reads/queries that don't
/// want to walk every S3 object.
pub const TBL_RECORD_INDEX: TableDefinition<(&str, &str, &str), &[u8]> =
    TableDefinition::new("xtable.record_index");

/// Schema index for structured-data-space layer.
/// Key = (space, schema_name) → bincode [`SchemaIndexEntry`].
/// Tracks the latest versioned schema document.
pub const TBL_SCHEMA_INDEX: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("xtable.schema_index");

/// Meta table singleton keys.
pub mod meta_key {
    pub const GLOBAL_VERSION: &str = "global_version";
    pub const LAST_WAL_SEQ: &str = "last_wal_seq";
}