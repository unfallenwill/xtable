//! xtable-storage: redb-backed local state.
//!
//! Phase 1 surfaces: version index, meta.
//! Phase 2 surfaces: WAL, txn_state, read/write sets, staged blobs.
//! Phase 4 surfaces (LSM-tree rewrite): MemTable, chunks, flush, read.

#![recursion_limit = "256"]

pub mod blob;
pub mod cf;
pub mod chunk;
pub mod flush;
pub mod locks;
pub mod memtable;
pub mod read;
pub mod store;
pub mod txn_state;
pub mod version_chain;
pub mod version_index;
pub mod wal;

pub use chunk::{ChunkEntry, ChunkFooter, ChunkHeader, ChunkIndexEntry, ChunkWriter, KeyIndexEntry};
pub use locks::{
    EdgeDirection, InEdgeSummary, PeerAction, RecentlyCommittedTxn, RECENT_WINDOW, SIEdge,
    SIEdgeSet, SIReadLock, SIWriteLock, SiTxnLocks, SiTxnPhase,
};
pub use memtable::{
    MemEntry, MemTable, MemTableSet, RecordKey, RecordValue, FlushPolicy, SerializedEntry,
    INVISIBLE,
};
pub use store::LocalStore;
pub use txn_state::{
    BlobRecord, MultipartState, RecordIndexEntry, SchemaIndexEntry, StoredRecord,
    TxnStateRecord, WriteSetEntry,
};
pub use version_chain::{VersionChain, VersionEntry};
pub use version_index::VersionRecord;
pub use wal::{encode_seq, WalRecord};