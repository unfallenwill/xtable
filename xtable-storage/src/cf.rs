//! Column-family / redb table definitions.
//!
//! Phase 1 tables:
//! - `versions`: object_key → bincode-encoded VersionRecord
//! - `meta`: singleton bookkeeping keys → u64
//!
//! Phase 2 tables (added lazily, on open):
//! - `wal`: (seq_be_u128, txn_id) → bincode-encoded WalRecord
//! - `txn_state`: txn_id → bincode-encoded TxnStateRecord
//! - `write_set`: (txn_id, key) → bincode-encoded WriteSetEntry
//! - `staged_blobs`: body_handle → bincode-encoded BlobRecord
//! - `multipart`: upload_id → bincode-encoded MultipartState
//!
//! Phase 4 (LSM-tree rewrite):
//! - `chunk_index`: chunk_id (ULID string) → bincode ChunkIndexEntry

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

/// `chunk_index`: chunk_id (ULID string) → bincode [`ChunkIndexEntry`].
///
/// Each chunk represents one flushed immutable memtable. The entry points
/// to the S3 object key where the chunk bytes live, along with key min/max,
/// commit-version range, WAL sequence range, and an embedded bloom filter
/// for negative-lookup optimization. See `chunk.rs` for the file format
/// and `flush.rs` for the write path.
pub const TBL_CHUNK_INDEX: TableDefinition<&str, &[u8]> =
    TableDefinition::new("xtable.chunk_index");

// =========================================================================
// Cahill SSI tables (PR #2 / Phase 5)
//
// Each table exists for a specific purpose in the SI lock manager:
// - TBL_SI_READ: per-txn SIRead locks
// - TBL_SI_WRITE: per-txn SIWrite locks
// - TBL_SI_IN_EDGES_BY_TJ: index from peer txn to set of txns that have
//   an in-edge from this peer
// - TBL_SI_RECENT: rolling window of recently-committed txns (for
//   cycle detection across commit boundaries)
// - TBL_SI_EDGES: full per-edge audit trail
// =========================================================================

/// `si_read`: (txn_id, key) → bincode SIReadLock.
pub const TBL_SI_READ: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("xtable.si_read");

/// `si_write`: (txn_id, key) → bincode SIWriteLock.
pub const TBL_SI_WRITE: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("xtable.si_write");

/// `si_in_edges_by_tj`: (peer_txn, own_txn) → bincode InEdgeSummary.
/// Used for fast "who has an in-edge from a recently-committed Tj"
/// queries during commit validation and GC.
pub const TBL_SI_IN_EDGES_BY_TJ: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("xtable.si_in_edges_by_tj");

/// `si_recent`: commit_version → bincode RecentlyCommittedTxn.
/// Bounded retention: GC drops entries whose version <
/// global_version - WINDOW.
pub const TBL_SI_RECENT: TableDefinition<u64, &[u8]> =
    TableDefinition::new("xtable.si_recent");

/// `si_edges`: (owning_txn_id, direction, peer_txn_id, key) → bincode
/// SIEdge. `direction` is "in" or "out". Diagnostics + cycle traversal.
pub const TBL_SI_EDGES: TableDefinition<(&str, &str, &str, &str), &[u8]> =
    TableDefinition::new("xtable.si_edges");

/// Lifecycle status of a chunk in `TBL_CHUNK_INDEX`.
///
/// - `Live`:      Normal serving state; reads can target this chunk.
/// - `Compacting`: Reserved for a future compaction step (currently unused;
///                 future levels may carry this state).
/// - `Deleted`:   Marked for removal; the GC sweep will issue
///                `DeleteObjects` and remove the row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ChunkStatus {
    Live,
    Compacting,
    Deleted,
}

/// Meta table singleton keys.
pub mod meta_key {
    pub const GLOBAL_VERSION: &str = "global_version";
    pub const LAST_WAL_SEQ: &str = "last_wal_seq";
    /// Highest WAL sequence number that has been proven durable in a chunk
    /// (i.e., the corresponding `WalRecord::MemtableFlushed` has landed).
    /// Used by recovery and GC to know what WAL rows are safe to truncate.
    pub const LAST_FLUSHED_SEQ: &str = "last_flushed_seq";
}