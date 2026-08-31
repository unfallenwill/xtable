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

/// Test-only helpers for forcing chunk flushes.
///
/// Kept out of the production surface (no `pub use` above). Production
/// code triggers flushes via size / age thresholds in `flush_loop`; the
/// helpers here exist so tests can drive a commit → rotate → flush loop
/// deterministically.
pub mod test_helpers {
    use std::sync::Arc;

    use crate::flush;
    use crate::memtable::{MemTable, MemTableSet};
    use crate::store::LocalStore;
    use xtable_backend::BackendClient;
    use xtable_core::XtableResult;

    /// Atomically swap the active memtable for a fresh empty one and
    /// return the just-rotated-out memtable. Use alongside
    /// [`flush_one`] to drive a complete commit → flush cycle without
    /// running the long-lived `flush_loop` task.
    pub fn rotate_for_test(mems: &MemTableSet) -> Arc<MemTable> {
        let new_id = mems.active.read().id + 1;
        let new_active = MemTable::new(new_id);
        let mut w = mems.active.write();
        let old = std::mem::replace(&mut *w, new_active);
        drop(w);
        mems.flushing.lock().push(old.clone());
        mems.flush_notify.notify_one();
        old
    }

    /// Take all currently-queued immutable memtables and flush each one
    /// in turn. Returns the count flushed.
    pub async fn flush_to_chunks(
        mems: &MemTableSet,
        store: &LocalStore,
        backend: Arc<BackendClient>,
    ) -> XtableResult<usize> {
        let immutables = mems.take_immutables().await;
        let n = immutables.len();
        for mt in immutables {
            flush::flush_one(&mt, store, backend.clone()).await?;
        }
        Ok(n)
    }

    /// Re-export of `flush_one` so callers don't need to import
    /// `xtable_storage::flush` directly (which would couple them to
    /// production-only flush internals).
    pub use crate::flush::flush_one;
}