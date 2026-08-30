//! LocalStore: redb-backed local state.
//!
//! Phase 1 surfaces:
//! - version index (object → latest version + metadata)
//! - meta singleton (global version counter)
//!
//! Phase 2 surfaces:
//! - WAL (append-only, monotonic seq)
//! - txn_state (per txn lifecycle)
//! - read_set / write_set (OCC bookkeeping)
//! - staged_blobs (body spill metadata)
//! - multipart (Phase 3 in-flight uploads)

use std::path::Path;
use std::sync::{Arc, OnceLock};

use redb::{Database, ReadableTable};

use crate::cf::{
    meta_key, ChunkStatus, TBL_ACTIVE_SNAPSHOTS, TBL_CHUNK_INDEX, TBL_META, TBL_MULTIPART,
    TBL_RECORD_INDEX, TBL_SCHEMA_INDEX, TBL_SI_EDGES, TBL_SI_IN_EDGES_BY_TJ, TBL_SI_READ,
    TBL_SI_RECENT, TBL_SI_WRITE, TBL_STAGED_BLOBS, TBL_TXN_STATE, TBL_VERSION_CHAINS,
    TBL_VERSIONS, TBL_WAL, TBL_WRITE_SET,
};
use crate::chunk::ChunkIndexEntry;
use crate::txn_state::{
    BlobRecord, MultipartState, RecordIndexEntry, SchemaIndexEntry, StoredRecord, TxnStateRecord,
    WriteSetEntry,
};
use crate::version_chain::{VersionChain, VersionEntry};
use crate::version_index::VersionRecord;
use crate::wal::WalRecord;
use xtable_core::headers::TxnStatus;
use xtable_core::{ObjectKey, XtableError, XtableResult};
use xtable_telemetry::metrics::Metrics;
use xtable_telemetry::timed::Timed;
use xtable_telemetry::KeyValue;

/// Lazily-initialised `Metrics` bound to the global OTel meter.
fn metrics() -> &'static Metrics {
    static METRICS: OnceLock<Metrics> = OnceLock::new();
    METRICS.get_or_init(Metrics::default)
}

pub(crate) fn redb_err<E: std::fmt::Display>(e: E) -> XtableError {
    XtableError::Storage(e.to_string())
}

/// Handle to the local store. Cheap to clone (Arc-wrapped DB).
#[derive(Clone)]
pub struct LocalStore {
    db: Arc<Database>,
}

impl std::fmt::Debug for LocalStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalStore").finish_non_exhaustive()
    }
}

impl LocalStore {
    /// Open (or create) a local store at the given directory.
    pub fn open(data_dir: &Path) -> XtableResult<Self> {
        std::fs::create_dir_all(data_dir)?;
        let path = data_dir.join("xtable.redb");
        Self::open_path(&path)
    }

    /// Open with an explicit database file path (used by tests).
    pub fn open_path(path: &Path) -> XtableResult<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let db = Database::create(path).map_err(redb_err)?;
        let store = Self { db: Arc::new(db) };

        // Initialize required tables.
        {
            let txn = store.db.begin_write().map_err(redb_err)?;
            {
                let _ = txn.open_table(TBL_VERSIONS).map_err(redb_err)?;
                let _ = txn.open_table(TBL_META).map_err(redb_err)?;
                let _ = txn.open_table(TBL_WAL).map_err(redb_err)?;
                let _ = txn.open_table(TBL_TXN_STATE).map_err(redb_err)?;
                let _ = txn.open_table(TBL_WRITE_SET).map_err(redb_err)?;
                let _ = txn.open_table(TBL_STAGED_BLOBS).map_err(redb_err)?;
                let _ = txn.open_table(TBL_MULTIPART).map_err(redb_err)?;
                let _ = txn.open_table(TBL_VERSION_CHAINS).map_err(redb_err)?;
                let _ = txn.open_table(TBL_ACTIVE_SNAPSHOTS).map_err(redb_err)?;
                let _ = txn.open_table(TBL_RECORD_INDEX).map_err(redb_err)?;
                let _ = txn.open_table(TBL_SCHEMA_INDEX).map_err(redb_err)?;
                let _ = txn.open_table(TBL_CHUNK_INDEX).map_err(redb_err)?;
                let _ = txn.open_table(TBL_SI_READ).map_err(redb_err)?;
                let _ = txn.open_table(TBL_SI_WRITE).map_err(redb_err)?;
                let _ = txn.open_table(TBL_SI_IN_EDGES_BY_TJ).map_err(redb_err)?;
                let _ = txn.open_table(TBL_SI_RECENT).map_err(redb_err)?;
                let _ = txn.open_table(TBL_SI_EDGES).map_err(redb_err)?;
                let mut meta = txn.open_table(TBL_META).map_err(redb_err)?;
                if meta.get(meta_key::GLOBAL_VERSION).map_err(redb_err)?.is_none() {
                    meta.insert(meta_key::GLOBAL_VERSION, 0u64).map_err(redb_err)?;
                }
                if meta.get(meta_key::LAST_WAL_SEQ).map_err(redb_err)?.is_none() {
                    meta.insert(meta_key::LAST_WAL_SEQ, 0u64).map_err(redb_err)?;
                }
            }
            txn.commit().map_err(redb_err)?;
        }
        Ok(store)
    }

    pub fn with_read<R>(&self, f: impl FnOnce(&redb::ReadTransaction) -> XtableResult<R>) -> XtableResult<R> {
        let txn = self.db.begin_read().map_err(redb_err)?;
        f(&txn)
    }

    pub fn with_write<R>(&self, f: impl FnOnce(&redb::WriteTransaction) -> XtableResult<R>) -> XtableResult<R> {
        let txn = self.db.begin_write().map_err(redb_err)?;
        let r = f(&txn)?;
        txn.commit().map_err(redb_err)?;
        Ok(r)
    }

    // ----- meta / version helpers (Phase 1) -----

    pub fn next_global_version(&self) -> XtableResult<u64> {
        self.with_write(|txn| {
            let mut meta = txn.open_table(TBL_META).map_err(redb_err)?;
            let cur = meta.get(meta_key::GLOBAL_VERSION).map_err(redb_err)?.map(|v| v.value()).unwrap_or(0);
            let next = cur + 1;
            meta.insert(meta_key::GLOBAL_VERSION, next).map_err(redb_err)?;
            Ok(next)
        })
    }

    pub fn current_global_version(&self) -> XtableResult<u64> {
        self.with_read(|txn| {
            let meta = txn.open_table(TBL_META).map_err(redb_err)?;
            Ok(meta.get(meta_key::GLOBAL_VERSION).map_err(redb_err)?.map(|v| v.value()).unwrap_or(0))
        })
    }

    pub fn get_version(&self, key: &ObjectKey) -> XtableResult<Option<VersionRecord>> {
        self.with_read(|txn| {
            let tbl = txn.open_table(TBL_VERSIONS).map_err(redb_err)?;
            match tbl.get(key.as_str()).map_err(redb_err)? {
                Some(v) => {
                    let bytes = v.value();
                    let rec: VersionRecord = bincode::deserialize(bytes).map_err(XtableError::from)?;
                    Ok(Some(rec))
                }
                None => Ok(None),
            }
        })
    }

    /// Insert or update a version-record row.
    #[tracing::instrument(level = "debug", skip_all, fields(op = "store.put"), err)]
    pub fn put_version(
        &self,
        key: &ObjectKey,
        record: &VersionRecord,
    ) -> XtableResult<()> {
        let bytes: Vec<u8> = bincode::serialize(record).map_err(XtableError::from)?;
        self.with_write(|txn| {
            let mut tbl = txn.open_table(TBL_VERSIONS).map_err(redb_err)?;
            tbl.insert(key.as_str(), bytes.as_slice()).map_err(redb_err)?;
            Ok(())
        })
    }

    pub fn put_versions_bulk(
        &self,
        updates: &[(ObjectKey, VersionRecord)],
    ) -> XtableResult<()> {
        if updates.is_empty() {
            return Ok(());
        }
        // Serialize all up front so the redb txn is short.
        let mut prepared: Vec<(String, Vec<u8>)> = Vec::with_capacity(updates.len());
        for (k, r) in updates {
            let bytes = bincode::serialize(r).map_err(XtableError::from)?;
            prepared.push((k.as_str().to_string(), bytes));
        }
        self.with_write(|txn| {
            let mut tbl = txn.open_table(TBL_VERSIONS).map_err(redb_err)?;
            for (k, bytes) in &prepared {
                tbl.insert(k.as_str(), bytes.as_slice()).map_err(redb_err)?;
            }
            Ok(())
        })
    }

    // ----- WAL (Phase 2) -----

    /// Allocate the next WAL sequence number (atomically).
    pub fn next_wal_seq(&self) -> XtableResult<u64> {
        self.with_write(|txn| {
            let mut meta = txn.open_table(TBL_META).map_err(redb_err)?;
            let cur = meta.get(meta_key::LAST_WAL_SEQ).map_err(redb_err)?.map(|v| v.value()).unwrap_or(0);
            let next = cur + 1;
            meta.insert(meta_key::LAST_WAL_SEQ, next).map_err(redb_err)?;
            Ok(next)
        })
    }

    /// Append a WAL record. Returns the seq number used.
    #[tracing::instrument(level = "info", name = "wal.append", skip_all, fields(op = "wal.append"), err)]
    pub fn append_wal(&self, record: &WalRecord) -> XtableResult<u64> {
        let _timed = Timed::new(
            &metrics().wal_append_duration,
            vec![KeyValue::new("op", "wal.append")],
        );
        let bytes = bincode::serialize(record).map_err(XtableError::from)?;
        self.with_write(|txn| {
            let mut meta = txn.open_table(TBL_META).map_err(redb_err)?;
            let cur = meta.get(meta_key::LAST_WAL_SEQ).map_err(redb_err)?.map(|v| v.value()).unwrap_or(0);
            let seq = cur + 1;
            meta.insert(meta_key::LAST_WAL_SEQ, seq).map_err(redb_err)?;
            let mut wal = txn.open_table(TBL_WAL).map_err(redb_err)?;
            wal.insert(seq, bytes.as_slice()).map_err(redb_err)?;
            Ok(seq)
        })
    }

    /// Iterate all WAL records in order. Returns (seq, record).
    pub fn iter_wal(&self) -> XtableResult<Vec<(u64, WalRecord)>> {
        let mut out = Vec::new();
        self.with_read(|txn| {
            let wal = txn.open_table(TBL_WAL).map_err(redb_err)?;
            for entry in wal.iter().map_err(redb_err)? {
                let (k, v) = entry.map_err(redb_err)?;
                let rec: WalRecord = bincode::deserialize(v.value()).map_err(XtableError::from)?;
                out.push((k.value(), rec));
            }
            Ok(())
        })?;
        out.sort_by_key(|(seq, _)| *seq);
        Ok(out)
    }

    /// Last WAL sequence number (0 if empty).
    pub fn last_wal_seq(&self) -> XtableResult<u64> {
        self.with_read(|txn| {
            let meta = txn.open_table(TBL_META).map_err(redb_err)?;
            Ok(meta.get(meta_key::LAST_WAL_SEQ).map_err(redb_err)?.map(|v| v.value()).unwrap_or(0))
        })
    }

    // ----- txn_state (Phase 2) -----

    pub fn put_txn_state(&self, txn_id: &str, rec: &TxnStateRecord) -> XtableResult<()> {
        let bytes = bincode::serialize(rec).map_err(XtableError::from)?;
        self.with_write(|txn| {
            let mut tbl = txn.open_table(TBL_TXN_STATE).map_err(redb_err)?;
            tbl.insert(txn_id, bytes.as_slice()).map_err(redb_err)?;
            Ok(())
        })
    }

    pub fn get_txn_state(&self, txn_id: &str) -> XtableResult<Option<TxnStateRecord>> {
        self.with_read(|txn| {
            let tbl = txn.open_table(TBL_TXN_STATE).map_err(redb_err)?;
            match tbl.get(txn_id).map_err(redb_err)? {
                Some(v) => {
                    let rec: TxnStateRecord = bincode::deserialize(v.value()).map_err(XtableError::from)?;
                    Ok(Some(rec))
                }
                None => Ok(None),
            }
        })
    }

    pub fn delete_txn_state(&self, txn_id: &str) -> XtableResult<()> {
        self.with_write(|txn| {
            let mut tbl = txn.open_table(TBL_TXN_STATE).map_err(redb_err)?;
            tbl.remove(txn_id).map_err(redb_err)?;
            Ok(())
        })
    }

    // ----- write_set (Phase 2 / MVCC) -----

    pub fn put_write_entry(&self, txn_id: &str, key: &str, entry: &WriteSetEntry) -> XtableResult<()> {
        let bytes = bincode::serialize(entry).map_err(XtableError::from)?;
        self.with_write(|txn| {
            let mut tbl = txn.open_table(TBL_WRITE_SET).map_err(redb_err)?;
            tbl.insert((txn_id, key), bytes.as_slice()).map_err(redb_err)?;
            Ok(())
        })
    }

    pub fn delete_write_entry(&self, txn_id: &str, key: &str) -> XtableResult<()> {
        self.with_write(|txn| {
            let mut tbl = txn.open_table(TBL_WRITE_SET).map_err(redb_err)?;
            tbl.remove((txn_id, key)).map_err(redb_err)?;
            Ok(())
        })
    }

    /// Iterate all write entries for a txn.
    pub fn iter_write_set(&self, txn_id: &str) -> XtableResult<Vec<(String, WriteSetEntry)>> {
        self.with_read(|txn| {
            let tbl = txn.open_table(TBL_WRITE_SET).map_err(redb_err)?;
            let mut out = Vec::new();
            for entry in tbl.iter().map_err(redb_err)? {
                let (k, v) = entry.map_err(redb_err)?;
                let (t, key) = k.value();
                if t == txn_id {
                    let rec: WriteSetEntry = bincode::deserialize(v.value()).map_err(XtableError::from)?;
                    out.push((key.to_string(), rec));
                }
            }
            Ok(out)
        })
    }

    /// Iterate all write entries across all txns (for GC / cleanup).
    pub fn iter_all_write_sets(&self) -> XtableResult<Vec<(String, String, WriteSetEntry)>> {
        self.with_read(|txn| {
            let tbl = txn.open_table(TBL_WRITE_SET).map_err(redb_err)?;
            let mut out = Vec::new();
            for entry in tbl.iter().map_err(redb_err)? {
                let (k, v) = entry.map_err(redb_err)?;
                let (t, key) = k.value();
                let rec: WriteSetEntry = bincode::deserialize(v.value()).map_err(XtableError::from)?;
                out.push((t.to_string(), key.to_string(), rec));
            }
            Ok(out)
        })
    }

    // ----- staged blobs (Phase 2) -----

    pub fn put_blob(&self, handle: &str, rec: &BlobRecord) -> XtableResult<()> {
        let bytes = bincode::serialize(rec).map_err(XtableError::from)?;
        self.with_write(|txn| {
            let mut tbl = txn.open_table(TBL_STAGED_BLOBS).map_err(redb_err)?;
            tbl.insert(handle, bytes.as_slice()).map_err(redb_err)?;
            Ok(())
        })
    }

    pub fn get_blob(&self, handle: &str) -> XtableResult<Option<BlobRecord>> {
        self.with_read(|txn| {
            let tbl = txn.open_table(TBL_STAGED_BLOBS).map_err(redb_err)?;
            match tbl.get(handle).map_err(redb_err)? {
                Some(v) => {
                    let rec: BlobRecord = bincode::deserialize(v.value()).map_err(XtableError::from)?;
                    Ok(Some(rec))
                }
                None => Ok(None),
            }
        })
    }

    pub fn delete_blob(&self, handle: &str) -> XtableResult<()> {
        self.with_write(|txn| {
            let mut tbl = txn.open_table(TBL_STAGED_BLOBS).map_err(redb_err)?;
            tbl.remove(handle).map_err(redb_err)?;
            Ok(())
        })
    }

    // ----- multipart (Phase 3) -----

    pub fn put_multipart(&self, upload_id: &str, rec: &MultipartState) -> XtableResult<()> {
        let bytes = bincode::serialize(rec).map_err(XtableError::from)?;
        self.with_write(|txn| {
            let mut tbl = txn.open_table(TBL_MULTIPART).map_err(redb_err)?;
            tbl.insert(upload_id, bytes.as_slice()).map_err(redb_err)?;
            Ok(())
        })
    }

    pub fn get_multipart(&self, upload_id: &str) -> XtableResult<Option<MultipartState>> {
        self.with_read(|txn| {
            let tbl = txn.open_table(TBL_MULTIPART).map_err(redb_err)?;
            match tbl.get(upload_id).map_err(redb_err)? {
                Some(v) => {
                    let rec: MultipartState = bincode::deserialize(v.value()).map_err(XtableError::from)?;
                    Ok(Some(rec))
                }
                None => Ok(None),
            }
        })
    }

    pub fn delete_multipart(&self, upload_id: &str) -> XtableResult<()> {
        self.with_write(|txn| {
            let mut tbl = txn.open_table(TBL_MULTIPART).map_err(redb_err)?;
            tbl.remove(upload_id).map_err(redb_err)?;
            Ok(())
        })
    }

    pub fn iter_all_multipart(&self) -> XtableResult<Vec<(String, MultipartState)>> {
        self.with_read(|txn| {
            let tbl = txn.open_table(TBL_MULTIPART).map_err(redb_err)?;
            let mut out = Vec::new();
            for entry in tbl.iter().map_err(redb_err)? {
                let (k, v) = entry.map_err(redb_err)?;
                let rec: MultipartState = bincode::deserialize(v.value()).map_err(XtableError::from)?;
                out.push((k.value().to_string(), rec));
            }
            Ok(out)
        })
    }

    /// Force `TxnStatus::Committed` (used by idempotent CommitTxn replay).
    pub fn mark_committed(&self, txn_id: &str, commit_version: u64) -> XtableResult<()> {
        let mut rec = match self.get_txn_state(txn_id)? {
            Some(r) => r,
            None => return Ok(()),
        };
        rec.status = TxnStatus::Committed;
        self.put_txn_state(txn_id, &rec)?;
        // Don't re-publish versions — already done in the original commit.
        let _ = commit_version;
        Ok(())
    }

    // ===== MVCC version-chain operations =====

    /// Read the full chain for a key.
    #[tracing::instrument(level = "debug", skip_all, fields(op = "store.scan"), err)]
    pub fn read_chain(&self, key: &str) -> XtableResult<VersionChain> {
        self.with_read(|txn| {
            let tbl = txn.open_table(TBL_VERSION_CHAINS).map_err(redb_err)?;
            match tbl.get(key).map_err(redb_err)? {
                Some(v) => {
                    let bytes = v.value();
                    let chain: VersionChain = bincode::deserialize(bytes).map_err(XtableError::from)?;
                    Ok(chain)
                }
                None => Ok(VersionChain::new(key.to_string())),
            }
        })
    }

    /// Read the latest visible entry for a key at a snapshot version.
    /// Implements invariant I3.
    pub fn read_at_snapshot(&self, key: &str, snapshot: u64) -> XtableResult<Option<VersionEntry>> {
        let chain = self.read_chain(key)?;
        Ok(chain.read_at_snapshot(snapshot).cloned())
    }

    /// Read the latest entry overall (no snapshot filtering — for non-txn reads).
    pub fn read_latest(&self, key: &str) -> XtableResult<Option<VersionEntry>> {
        let chain = self.read_chain(key)?;
        Ok(chain.entries.last().cloned())
    }

    /// Append a new entry to a key's chain. Used by CommitTxn (atomic with
    /// all other writes for the same txn in a single redb write txn).
    pub fn append_chain_entry(&self, key: &str, entry: &VersionEntry) -> XtableResult<()> {
        let mut chain = self.read_chain(key)?;
        let latest = chain.latest_commit_version();
        if entry.commit_version <= latest && !chain.entries.is_empty() {
            return Err(XtableError::internal(format!(
                "chain append not monotonic: latest={} new={}", latest, entry.commit_version
            )));
        }
        chain.append(entry.clone());
        let bytes = bincode::serialize(&chain).map_err(XtableError::from)?;
        self.with_write(|txn| {
            let mut tbl = txn.open_table(TBL_VERSION_CHAINS).map_err(redb_err)?;
            tbl.insert(key, bytes.as_slice()).map_err(redb_err)?;
            Ok(())
        })
    }

    /// Bulk-append multiple entries in a single redb write txn. Used by
    /// CommitTxn for atomicity (I6).
    ///
    /// PR-Fix9.1: each entry carries the txn's `snapshot_version`.
    /// Inside the atomic redb write txn, we check that
    /// `chain[K].latest_commit_version <= snapshot_version` BEFORE
    /// applying the append. If any key has been written by a concurrent
    /// txn after our snapshot, the entire bulk append is rolled back
    /// (via redb's transaction semantics) and we return
    /// `XtableError::Conflict(key)`. This catches lost-update: two txns
    /// at the same snapshot writing the same key cannot both succeed.
    pub fn append_chain_entries_bulk(
        &self,
        entries: &[(String, VersionEntry, u64)], // (key, entry, snapshot_version)
    ) -> XtableResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut sorted = entries.to_vec();
        sorted.sort_by(|a, b| a.0.cmp(&b.0));
        let mut prepared: Vec<(String, Vec<u8>)> = Vec::with_capacity(sorted.len());
        for (key, entry, snapshot_version) in &sorted {
            let mut chain = self.read_chain(key)?;
            let latest = chain.latest_commit_version();
            // PR-Fix9.1: snapshot conflict check. If the chain already
            // has an entry past our snapshot, a concurrent txn wrote
            // after our snapshot — we must NOT overwrite (lost-update).
            // Rolling back via redb aborts the entire bulk append so
            // partial state is never published.
            if !chain.entries.is_empty() && latest > *snapshot_version {
                return Err(XtableError::Conflict(format!(
                    "{}: snapshot {} < chain latest {}",
                    key, snapshot_version, latest
                )));
            }
            // Monotonicity check (kept for defense-in-depth).
            if entry.commit_version <= latest && !chain.entries.is_empty() {
                return Err(XtableError::internal(format!(
                    "chain append not monotonic for key {}: latest={} new={}", key, latest, entry.commit_version
                )));
            }
            chain.append(entry.clone());
            let bytes = bincode::serialize(&chain).map_err(XtableError::from)?;
            prepared.push((key.clone(), bytes));
        }
        self.with_write(|txn| {
            let mut tbl = txn.open_table(TBL_VERSION_CHAINS).map_err(redb_err)?;
            for (k, bytes) in &prepared {
                tbl.insert(k.as_str(), bytes.as_slice()).map_err(redb_err)?;
            }
            Ok(())
        })
    }

    /// Prune (GC) entries strictly below `min_snapshot` that are not the newest.
    /// Returns (chains_visited, total_entries_removed).
    /// `chains_visited` counts every chain GC iterated over, including ones
    /// where nothing was pruned (e.g., when `min_snapshot` is below every
    /// entry, or a single-entry chain). Callers (regression tests,
    /// `gc::gc_version_chains`) rely on this — `chains_visited == 1`
    /// means "we looked at the chain", not "we modified it".
    /// Implements invariant I8.
    pub fn gc_chains(&self, min_snapshot: u64) -> XtableResult<(usize, usize)> {
        let chains = self.iter_all_chains()?;
        let mut visited = 0usize;
        let mut total_removed = 0usize;
        let mut to_write: Vec<(String, Vec<u8>)> = Vec::new();
        for (key, mut chain) in chains {
            visited += 1;
            let removed = chain.prune_below(min_snapshot);
            if removed > 0 {
                total_removed += removed;
                debug_assert!(!chain.entries.is_empty(), "GC left empty chain for {}", key);
                let bytes = bincode::serialize(&chain).map_err(XtableError::from)?;
                to_write.push((key, bytes));
            }
        }
        if !to_write.is_empty() {
            self.with_write(|txn| {
                let mut tbl = txn.open_table(TBL_VERSION_CHAINS).map_err(redb_err)?;
                for (k, bytes) in &to_write {
                    tbl.insert(k.as_str(), bytes.as_slice()).map_err(redb_err)?;
                }
                Ok(())
            })?;
        }
        Ok((visited, total_removed))
    }

    /// Iterate all chains (used by GC and admin).
    pub fn iter_all_chains(&self) -> XtableResult<Vec<(String, VersionChain)>> {
        self.with_read(|txn| {
            let tbl = txn.open_table(TBL_VERSION_CHAINS).map_err(redb_err)?;
            let mut out = Vec::new();
            for entry in tbl.iter().map_err(redb_err)? {
                let (k, v) = entry.map_err(redb_err)?;
                let chain: VersionChain = bincode::deserialize(v.value()).map_err(XtableError::from)?;
                out.push((k.value().to_string(), chain));
            }
            Ok(out)
        })
    }

    // ===== Active snapshot registry =====

    /// Register a snapshot_version as held by an active transaction.
    /// V9 fix: ref-count. Multiple txns at the same snapshot increment the count;
    /// each must call `unregister_snapshot` to decrement.
    pub fn register_snapshot(&self, snapshot: u64) -> XtableResult<()> {
        self.with_write(|txn| {
            let mut tbl = txn.open_table(TBL_ACTIVE_SNAPSHOTS).map_err(redb_err)?;
            let cur = tbl.get(snapshot).map_err(redb_err)?.map(|v| v.value()).unwrap_or(0);
            tbl.insert(snapshot, cur + 1).map_err(redb_err)?;
            Ok(())
        })
    }

    /// Decrement the snapshot's ref-count. If it reaches 0, the snapshot is
    /// no longer active and is removed.
    pub fn unregister_snapshot(&self, snapshot: u64) -> XtableResult<()> {
        self.with_write(|txn| {
            let mut tbl = txn.open_table(TBL_ACTIVE_SNAPSHOTS).map_err(redb_err)?;
            let cur = tbl.get(snapshot).map_err(redb_err)?.map(|v| v.value()).unwrap_or(0);
            if cur <= 1 {
                tbl.remove(snapshot).map_err(redb_err)?;
            } else {
                tbl.insert(snapshot, cur - 1).map_err(redb_err)?;
            }
            Ok(())
        })
    }

    /// Return the minimum active snapshot, or u64::MAX if no active snapshots.
    /// Used by GC to know the safe-to-prune threshold.
    pub fn min_active_snapshot(&self) -> XtableResult<u64> {
        self.with_read(|txn| {
            let tbl = txn.open_table(TBL_ACTIVE_SNAPSHOTS).map_err(redb_err)?;
            let mut min = u64::MAX;
            for entry in tbl.iter().map_err(redb_err)? {
                let (k, _) = entry.map_err(redb_err)?;
                let v = k.value();
                if v < min {
                    min = v;
                }
            }
            Ok(min)
        })
    }

    /// Count of active snapshots (sum of all ref-counts).
    pub fn count_active_snapshots(&self) -> XtableResult<usize> {
        self.with_read(|txn| {
            let tbl = txn.open_table(TBL_ACTIVE_SNAPSHOTS).map_err(redb_err)?;
            let mut n = 0usize;
            for entry in tbl.iter().map_err(redb_err)? {
                let (_, v) = entry.map_err(redb_err)?;
                n += v.value() as usize;
            }
            Ok(n)
        })
    }

    // ===== Structured-data-space indexes =====

    pub fn put_record_index(
        &self,
        space: &str,
        table: &str,
        record_id: &str,
        entry: &RecordIndexEntry,
    ) -> XtableResult<()> {
        let bytes = bincode::serialize(entry).map_err(XtableError::from)?;
        self.with_write(|txn| {
            let mut tbl = txn.open_table(TBL_RECORD_INDEX).map_err(redb_err)?;
            tbl.insert((space, table, record_id), bytes.as_slice())
                .map_err(redb_err)?;
            Ok(())
        })
    }

    pub fn get_record_index(
        &self,
        space: &str,
        table: &str,
        record_id: &str,
    ) -> XtableResult<Option<RecordIndexEntry>> {
        self.with_read(|txn| {
            let tbl = txn.open_table(TBL_RECORD_INDEX).map_err(redb_err)?;
            match tbl.get((space, table, record_id)).map_err(redb_err)? {
                Some(v) => {
                    let rec: RecordIndexEntry =
                        bincode::deserialize(v.value()).map_err(XtableError::from)?;
                    Ok(Some(rec))
                }
                None => Ok(None),
            }
        })
    }

    /// Body-carrying variant of [`put_record_index`]. Stores the entry
    /// alongside the JSON body so listing queries don't need to fetch
    /// every record from the backend S3.
    pub fn put_record_index_with_body(
        &self,
        space: &str,
        table: &str,
        record_id: &str,
        entry: &RecordIndexEntry,
        body: &serde_json::Value,
    ) -> XtableResult<()> {
        // Pre-serialize body to JSON text — bincode's serde integration
        // does not reliably roundtrip `serde_json::Value`.
        let body_json = serde_json::to_string(body).map_err(XtableError::from)?;
        let entry_owned = entry.clone();
        let bytes = bincode::serialize(&StoredRecord { entry: entry_owned, body_json }).map_err(XtableError::from)?;
        self.with_write(|txn| {
            let mut tbl = txn.open_table(TBL_RECORD_INDEX).map_err(redb_err)?;
            tbl.insert((space, table, record_id), bytes.as_slice())
                .map_err(redb_err)?;
            Ok(())
        })
    }

    /// Read both index entry and stored body. Tries the body-carrying
    /// format first, falls back to the legacy format for compatibility.
    pub fn get_record_index_with_body(
        &self,
        space: &str,
        table: &str,
        record_id: &str,
    ) -> XtableResult<Option<(RecordIndexEntry, serde_json::Value)>> {
        self.with_read(|txn| {
            let tbl = txn.open_table(TBL_RECORD_INDEX).map_err(redb_err)?;
            let raw = match tbl.get((space, table, record_id)).map_err(redb_err)? {
                Some(v) => v.value().to_vec(),
                None => return Ok(None),
            };
            if let Ok(s) = bincode::deserialize::<StoredRecord>(&raw) {
                if s.body_json.is_empty() {
                    return Ok(Some((s.entry, serde_json::Value::Null)));
                }
                let body: serde_json::Value =
                    serde_json::from_str(&s.body_json).map_err(XtableError::from)?;
                return Ok(Some((s.entry, body)));
            }
            if let Ok(entry) = bincode::deserialize::<RecordIndexEntry>(&raw) {
                return Ok(Some((entry, serde_json::Value::Null)));
            }
            Err(XtableError::Storage("record index: decode failed".into()))
        })
    }

    #[tracing::instrument(level = "debug", skip_all, fields(op = "store.delete"), err)]
    pub fn delete_record_index(&self, space: &str, table: &str, record_id: &str) -> XtableResult<()> {
        self.with_write(|txn| {
            let mut tbl = txn.open_table(TBL_RECORD_INDEX).map_err(redb_err)?;
            tbl.remove((space, table, record_id)).map_err(redb_err)?;
            Ok(())
        })
    }

    /// Iterate all record index entries for a (space, table). Yields
    /// (record_id, RecordIndexEntry) — caller filters by snapshot.
    pub fn iter_record_index(
        &self,
        space: &str,
        table: &str,
    ) -> XtableResult<Vec<(String, RecordIndexEntry)>> {
        let mut out = Vec::new();
        self.with_read(|txn| {
            let tbl = txn.open_table(TBL_RECORD_INDEX).map_err(redb_err)?;
            for entry in tbl.iter().map_err(redb_err)? {
                let (k, v) = entry.map_err(redb_err)?;
                let (s, t, rid) = k.value();
                if s == space && t == table {
                    let rec: RecordIndexEntry =
                        bincode::deserialize(v.value()).map_err(XtableError::from)?;
                    out.push((rid.to_string(), rec));
                }
            }
            Ok(())
        })?;
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    pub fn put_schema_index(
        &self,
        space: &str,
        name: &str,
        entry: &SchemaIndexEntry,
    ) -> XtableResult<()> {
        let bytes = bincode::serialize(entry).map_err(XtableError::from)?;
        self.with_write(|txn| {
            let mut tbl = txn.open_table(TBL_SCHEMA_INDEX).map_err(redb_err)?;
            tbl.insert((space, name), bytes.as_slice()).map_err(redb_err)?;
            Ok(())
        })
    }

    pub fn get_schema_index(
        &self,
        space: &str,
        name: &str,
    ) -> XtableResult<Option<SchemaIndexEntry>> {
        self.with_read(|txn| {
            let tbl = txn.open_table(TBL_SCHEMA_INDEX).map_err(redb_err)?;
            match tbl.get((space, name)).map_err(redb_err)? {
                Some(v) => {
                    let rec: SchemaIndexEntry =
                        bincode::deserialize(v.value()).map_err(XtableError::from)?;
                    Ok(Some(rec))
                }
                None => Ok(None),
            }
        })
    }

    pub fn iter_schema_index(&self, space: &str) -> XtableResult<Vec<(String, SchemaIndexEntry)>> {
        let mut out = Vec::new();
        self.with_read(|txn| {
            let tbl = txn.open_table(TBL_SCHEMA_INDEX).map_err(redb_err)?;
            for entry in tbl.iter().map_err(redb_err)? {
                let (k, v) = entry.map_err(redb_err)?;
                let (s, n) = k.value();
                if s == space {
                    let rec: SchemaIndexEntry =
                        bincode::deserialize(v.value()).map_err(XtableError::from)?;
                    out.push((n.to_string(), rec));
                }
            }
            Ok(())
        })?;
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    // ===== Chunk index (Phase 4 / PR #1) =====

    pub fn put_chunk_index(&self, chunk_id: &str, entry: &ChunkIndexEntry) -> XtableResult<()> {
        let bytes = bincode::serialize(entry).map_err(XtableError::from)?;
        self.with_write(|txn| {
            let mut tbl = txn.open_table(TBL_CHUNK_INDEX).map_err(redb_err)?;
            tbl.insert(chunk_id, bytes.as_slice()).map_err(redb_err)?;
            Ok(())
        })
    }

    pub fn get_chunk_index(&self, chunk_id: &str) -> XtableResult<Option<ChunkIndexEntry>> {
        self.with_read(|txn| {
            let tbl = txn.open_table(TBL_CHUNK_INDEX).map_err(redb_err)?;
            match tbl.get(chunk_id).map_err(redb_err)? {
                Some(v) => {
                    let rec: ChunkIndexEntry =
                        bincode::deserialize(v.value()).map_err(XtableError::from)?;
                    Ok(Some(rec))
                }
                None => Ok(None),
            }
        })
    }

    pub fn delete_chunk_index(&self, chunk_id: &str) -> XtableResult<()> {
        self.with_write(|txn| {
            let mut tbl = txn.open_table(TBL_CHUNK_INDEX).map_err(redb_err)?;
            tbl.remove(chunk_id).map_err(redb_err)?;
            Ok(())
        })
    }

    /// Iterate all chunk index entries (used by GC and admin).
    pub fn iter_all_chunk_index(&self) -> XtableResult<Vec<(String, ChunkIndexEntry)>> {
        let mut out = Vec::new();
        self.with_read(|txn| {
            let tbl = txn.open_table(TBL_CHUNK_INDEX).map_err(redb_err)?;
            for entry in tbl.iter().map_err(redb_err)? {
                let (k, v) = entry.map_err(redb_err)?;
                let rec: ChunkIndexEntry =
                    bincode::deserialize(v.value()).map_err(XtableError::from)?;
                out.push((k.value().to_string(), rec));
            }
            Ok(())
        })?;
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    /// Mark chunks as `Deleted` so the GC sweep can issue
    /// `DeleteObjects` and remove the rows.
    pub fn mark_chunks_deleted(&self, chunk_ids: &[String]) -> XtableResult<()> {
        if chunk_ids.is_empty() {
            return Ok(());
        }
        let mut updated: Vec<(String, Vec<u8>)> = Vec::with_capacity(chunk_ids.len());
        self.with_read(|txn| {
            let tbl = txn.open_table(TBL_CHUNK_INDEX).map_err(redb_err)?;
            for id in chunk_ids {
                if let Some(v) = tbl.get(id.as_str()).map_err(redb_err)? {
                    let mut rec: ChunkIndexEntry =
                        bincode::deserialize(v.value()).map_err(XtableError::from)?;
                    rec.status = ChunkStatus::Deleted;
                    let bytes = bincode::serialize(&rec).map_err(XtableError::from)?;
                    updated.push((id.clone(), bytes));
                }
            }
            Ok(())
        })?;
        if !updated.is_empty() {
            self.with_write(|txn| {
                let mut tbl = txn.open_table(TBL_CHUNK_INDEX).map_err(redb_err)?;
                for (id, bytes) in &updated {
                    tbl.insert(id.as_str(), bytes.as_slice()).map_err(redb_err)?;
                }
                Ok(())
            })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn open_and_increment_global_version() {
        let tmp = TempDir::new().unwrap();
        let store = LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap();
        assert_eq!(store.current_global_version().unwrap(), 0);
        let v1 = store.next_global_version().unwrap();
        let v2 = store.next_global_version().unwrap();
        assert_eq!(v1, 1);
        assert_eq!(v2, 2);
    }

    #[test]
    fn reopen_preserves_state() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("xt.redb");
        {
            let store = LocalStore::open_path(&path).unwrap();
            let _ = store.next_global_version().unwrap();
            let _ = store.next_global_version().unwrap();
        }
        let store2 = LocalStore::open_path(&path).unwrap();
        assert_eq!(store2.current_global_version().unwrap(), 2);
    }

    #[test]
    fn put_and_get_version() {
        let tmp = TempDir::new().unwrap();
        let store = LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap();
        let key = ObjectKey::new("foo/bar");
        let rec = VersionRecord {
            latest_version: xtable_core::Version(7),
            latest_etag: "etag-7".into(),
            latest_backend_key: "foo/bar".into(),
            last_writer_txn_id: String::new(),
            tombstone: false,
            size: 42,
            last_modified_unix_ms: 0,
        };
        store.put_version(&key, &rec).unwrap();
        let got = store.get_version(&key).unwrap().unwrap();
        assert_eq!(got.latest_version.as_u64(), 7);
        assert_eq!(got.latest_etag, "etag-7");
        assert_eq!(got.size, 42);
    }

    #[test]
    fn get_version_missing_returns_none() {
        let tmp = TempDir::new().unwrap();
        let store = LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap();
        let key = ObjectKey::new("missing");
        assert!(store.get_version(&key).unwrap().is_none());
    }

    #[test]
    fn wal_append_and_iter() {
        let tmp = TempDir::new().unwrap();
        let store = LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap();
        let r1 = WalRecord::Begin { txn_id: "T1".into(), snapshot_version: 0, idempotency_key: None };
        let r2 = WalRecord::Commit { txn_id: "T1".into(), commit_version: 1, write_keys: vec![] };
        store.append_wal(&r1).unwrap();
        store.append_wal(&r2).unwrap();
        let log = store.iter_wal().unwrap();
        assert_eq!(log.len(), 2);
        assert_eq!(log[0].1, r1);
        assert_eq!(log[1].1, r2);
        assert_eq!(store.last_wal_seq().unwrap(), 2);
    }

    #[test]
    fn txn_state_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let store = LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap();
        let r = TxnStateRecord::new_active(7, Some("idem".into()), 100);
        store.put_txn_state("T", &r).unwrap();
        let got = store.get_txn_state("T").unwrap().unwrap();
        assert_eq!(got.snapshot_version, 7);
        assert_eq!(got.idempotency_key.as_deref(), Some("idem"));
    }

    #[test]
    fn put_versions_bulk_works() {
        let tmp = TempDir::new().unwrap();
        let store = LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap();
        let rec = VersionRecord {
            latest_version: xtable_core::Version(1),
            latest_etag: "e".into(),
            latest_backend_key: "k".into(),
            last_writer_txn_id: String::new(),
            tombstone: false,
            size: 10,
            last_modified_unix_ms: 0,
        };
        let updates = vec![
            (ObjectKey::new("a"), rec.clone()),
            (ObjectKey::new("b"), rec.clone()),
        ];
        store.put_versions_bulk(&updates).unwrap();
        assert_eq!(store.get_version(&ObjectKey::new("a")).unwrap().unwrap().latest_version, xtable_core::Version(1));
        assert_eq!(store.get_version(&ObjectKey::new("b")).unwrap().unwrap().latest_version, xtable_core::Version(1));
    }

    #[test]
    fn write_set_isolation_between_txns() {
        let tmp = TempDir::new().unwrap();
        let store = LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap();
        let e1 = WriteSetEntry {
            backend_key: "k1".into(),
            body_handle: None,
            inline_body: None,
            size: 1,
            content_type: None,
            user_meta: vec![],
            deleted: false,
        };
        let e2 = WriteSetEntry {
            backend_key: "k2".into(),
            body_handle: None,
            inline_body: None,
            size: 2,
            content_type: None,
            user_meta: vec![],
            deleted: false,
        };
        store.put_write_entry("T1", "k1", &e1).unwrap();
        store.put_write_entry("T2", "k2", &e2).unwrap();
        let only_t1 = store.iter_write_set("T1").unwrap();
        assert_eq!(only_t1.len(), 1);
        assert_eq!(only_t1[0].0, "k1");
    }

    #[test]
    fn record_index_with_body_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let store = LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap();
        let entry = RecordIndexEntry {
            commit_version: 42,
            deleted: false,
            backend_key: "k".into(),
            schema_version: 3,
            txn_id: "TX".into(),
            updated_ms: 1_700_000_000_000,
        };
        let body = serde_json::json!({"a": 1, "b": "hi", "c": [true, false]});
        store.put_record_index_with_body("s", "t", "r", &entry, &body).unwrap();
        let (got_entry, got_body) = store.get_record_index_with_body("s", "t", "r").unwrap().unwrap();
        assert_eq!(got_entry, entry);
        assert_eq!(got_body, body);

        // iter_record_index decodes only the entry (body dropped by serde).
        let iter = store.iter_record_index("s", "t").unwrap();
        assert_eq!(iter.len(), 1);
        assert_eq!(iter[0].0, "r");
        assert_eq!(iter[0].1.commit_version, 42);
    }
}