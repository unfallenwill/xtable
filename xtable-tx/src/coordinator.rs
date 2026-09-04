//! Transaction coordinator — MVCC + Cahill SSI state machine.
//!
//! Protocol order (critical for crash-safety):
//! 1. BeginTxn → TxnId + snapshot_version, WAL `Begin` + register SI txn
//! 2. Stage (per PutObject in txn) → WAL `Stage` + WriteSetEntry +
//!    register SI write intent (lock_manager.register_write)
//! 3. CommitTxn:
//!    a. Cahill cycle detection (lock_manager.find_dangerous_structure)
//!       → abort on dangerous structure (Conflict)
//!    b. (Spec §5.1 — per-record S3 PUT removed; chain append + WAL are the
//!       durability boundary. The MemTable is the only writer of structured
//!       data; the flush loop uploads it as a chunk later.)
//!    c. Atomic redb write txn: append_chain_entries_bulk with
//!       snapshot-conflict check (prevents lost-update) + memtable publish
//!    d. WAL `Committed` + Mark committed on SI lock manager
//!    e. Fire post-commit hooks (record_index update)
//! 4. On any failure, mark aborted + return conflict / 503; no per-record
//!    S3 state to roll back because nothing was PUT per-record.
//!    WAL `Aborted`, return 409 / 503.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use xtable_backend::BackendClient;
use xtable_core::headers::TxnStatus;
use xtable_core::{ObjectKey, TxnId, Version, XtableError, XtableResult};
use xtable_storage::{
    BlobRecord, LocalStore, MemEntry, MemTableSet, RecordValue, TxnStateRecord, VersionRecord,
    WalRecord, WriteSetEntry,
};
use xtable_telemetry::metrics::global as metrics;
use xtable_telemetry::timed::Timed;
use xtable_telemetry::KeyValue;

use crate::error::TxnError;
use crate::si_lock_manager::SiLockManager;

/// Outcome of a successful CommitTxn.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitOutcome {
    pub commit_version: u64,
}

/// Per-write payload handed to post-commit hooks.
#[derive(Debug, Clone)]
pub struct CommitWrite {
    pub key: String,
    pub commit_version: u64,
    pub deleted: bool,
    pub size: u64,
}

/// Event delivered to post-commit hooks after a successful commit.
/// `writes` lists every key the txn appended to the chain, in commit order.
#[derive(Debug, Clone)]
pub struct CommitEvent {
    pub txn_id: String,
    pub commit_version: u64,
    pub writes: Vec<CommitWrite>,
}

/// Sync post-commit hook signature. Hooks run inside the commit critical
/// section AFTER chain-append + WAL Committed have succeeded. They run
/// synchronously so a failure can be logged; they cannot fail the commit
/// (the chain is already published). Implementation MUST be fast and
/// non-blocking on IO.
pub type PostCommitHook = Arc<dyn Fn(&CommitEvent) + Send + Sync>;

/// Transaction coordinator.
#[derive(Clone)]
pub struct TxnCoordinator {
    store: Arc<LocalStore>,
    backend: Arc<BackendClient>,
    spill_dir: Arc<std::path::PathBuf>,
    /// PR #4: Cahill SSI lock manager. Tracks per-txn SIRead/SIWrite locks
    /// and rw-antidependency edges; commit-time cycle detection aborts
    /// txns that participate in dangerous structures.
    lock_manager: Arc<SiLockManager>,
    /// PR #4: in-memory MemTable set. Commit publishes to memtable; a
    /// background flush task uploads chunks to S3.
    memtable_set: Arc<MemTableSet>,
    /// Post-commit hooks (e.g., index maintenance for the structured-data-space layer).
    post_commit_hooks: Arc<std::sync::RwLock<Vec<PostCommitHook>>>,
    /// Serialize commit and abort state-machine transitions within this
    /// coordinator. The durable compare-and-set in LocalStore also protects
    /// callers that use more than one coordinator instance for a store.
    commit_lock: Arc<tokio::sync::Mutex<()>>,
}

impl std::fmt::Debug for TxnCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxnCoordinator").finish_non_exhaustive()
    }
}

impl TxnCoordinator {
    pub fn new(
        store: Arc<LocalStore>,
        backend: Arc<BackendClient>,
        spill_dir: std::path::PathBuf,
        _upload_concurrency: usize,
    ) -> Self {
        std::fs::create_dir_all(&spill_dir).ok();
        Self {
            store,
            backend,
            spill_dir: Arc::new(spill_dir),
            // PR #4: default SI lock manager + memtable set.
            lock_manager: SiLockManager::new(),
            memtable_set: MemTableSet::new(
                xtable_storage::MemTable::new(0),
                xtable_storage::FlushPolicy::default(),
            ),
            post_commit_hooks: Arc::new(std::sync::RwLock::new(Vec::new())),
            commit_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    /// Construct with explicit SI lock manager and memtable set (used by
    /// tests and by server startup to wire shared instances).
    pub fn with_lock_and_memtable(
        store: Arc<LocalStore>,
        backend: Arc<BackendClient>,
        spill_dir: std::path::PathBuf,
        _upload_concurrency: usize,
        lock_manager: Arc<SiLockManager>,
        memtable_set: Arc<MemTableSet>,
    ) -> Self {
        std::fs::create_dir_all(&spill_dir).ok();
        Self {
            store,
            backend,
            spill_dir: Arc::new(spill_dir),
            lock_manager,
            memtable_set,
            post_commit_hooks: Arc::new(std::sync::RwLock::new(Vec::new())),
            commit_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub fn lock_manager(&self) -> &Arc<SiLockManager> {
        &self.lock_manager
    }

    pub fn memtable_set(&self) -> &Arc<MemTableSet> {
        &self.memtable_set
    }

    pub fn store(&self) -> &LocalStore {
        &self.store
    }

    /// Return the exact snapshot captured for a transaction. Structured
    /// callers must use this value rather than sampling the global counter a
    /// second time after `begin`.
    pub fn transaction_snapshot(&self, txn_id: &str) -> XtableResult<u64> {
        self.store
            .get_txn_state(txn_id)?
            .map(|state| state.snapshot_version)
            .ok_or_else(|| TxnError::UnknownTxn(txn_id.to_string()).into())
    }

    pub fn backend(&self) -> &BackendClient {
        &self.backend
    }

    /// Register a post-commit hook. Returns the registration index so the
    /// caller can deregister later (rarely needed in production).
    pub fn register_post_commit_hook(&self, hook: PostCommitHook) -> usize {
        let mut hooks = self.post_commit_hooks.write().expect("hooks poisoned");
        let idx = hooks.len();
        hooks.push(hook);
        idx
    }

    fn fire_post_commit_hooks(&self, ev: &CommitEvent) {
        let hooks = self.post_commit_hooks.read().expect("hooks poisoned");
        for h in hooks.iter() {
            h(ev);
        }
    }

    /// Allocate a ULID-based transaction id.
    pub fn next_txn_id() -> String {
        TxnId::new().as_string()
    }

    /// Begin a new transaction.
    #[tracing::instrument(
        level = "info",
        name = "txn.begin",
        skip_all,
        fields(txn.id = tracing::field::Empty, op = "begin"),
        err,
    )]
    pub async fn begin(&self, idempotency_key: Option<String>) -> XtableResult<String> {
        let _timed = Timed::new(
            &metrics().txn_begin_duration,
            vec![KeyValue::new("op", "begin")],
        );
        let txn_id = Self::next_txn_id();
        tracing::Span::current().record("txn.id", tracing::field::display(&txn_id));
        // Capture and pin the exact snapshot in one storage transaction. A
        // separate current_global_version()+register_snapshot() pair lets GC
        // race a reader between those operations.
        let snapshot_version = self.store.capture_and_register_snapshot()?;
        let now_ms = Utc::now().timestamp_millis();
        let rec = TxnStateRecord::new_active(snapshot_version, idempotency_key.clone(), now_ms);
        if let Err(err) = self.store.put_txn_state(&txn_id, &rec) {
            let _ = self.store.unregister_snapshot(snapshot_version);
            return Err(err);
        }
        // PR-Fix1.1: register the txn in the SI lock manager so that
        // `register_read` / `register_write` / `find_dangerous_structure`
        // see it. Without this the lock manager stays empty and SSI is dead.
        if let Err(err) = self.lock_manager.begin_txn(&txn_id, snapshot_version) {
            let _ = self.store.unregister_snapshot(snapshot_version);
            let _ = self.store.delete_txn_state(&txn_id);
            return Err(err);
        }
        if let Err(err) = self.store.append_wal(&WalRecord::Begin {
            txn_id: txn_id.clone(),
            snapshot_version,
            idempotency_key,
        }) {
            self.lock_manager.mark_aborted(&txn_id);
            let _ = self.store.unregister_snapshot(snapshot_version);
            let _ = self.store.delete_txn_state(&txn_id);
            return Err(err);
        }
        debug!(txn = %txn_id, version = snapshot_version, "BeginTxn");
        metrics()
            .txn_begin_total
            .add(1, &[KeyValue::new("outcome", "ok")]);
        Ok(txn_id)
    }

    /// Stage a write within a transaction.
    /// The body is held in-memory here (caller passes bytes) and may spill to
    /// disk if it exceeds the threshold (default 256 KiB).
    #[tracing::instrument(
        level = "info",
        name = "txn.stage",
        skip_all,
        fields(txn.id = %txn_id, op = "stage"),
        err,
    )]
    pub async fn stage(
        &self,
        txn_id: &str,
        key: &ObjectKey,
        body: Vec<u8>,
        content_type: Option<String>,
        user_meta: HashMap<String, String>,
        deleted: bool,
    ) -> XtableResult<()> {
        let mut txn = self.require_active(txn_id)?;

        // Note: no threshold check — the threshold concept was a mis-design
        // that caused V18 (every txn after the first got rejected).

        // Spill body if large.
        let body_handle = if body.len() > 256 * 1024 {
            let handle = format!("{}-{}", txn_id, uuid_like(key.as_str()));
            let path = self.spill_dir.join(&handle);
            tokio::fs::write(&path, &body).await?;
            let sha = sha256_hex(&body);
            let rec = BlobRecord {
                path: path.to_string_lossy().to_string(),
                size: body.len() as u64,
                sha256: sha,
                created_at_ms: Utc::now().timestamp_millis(),
            };
            self.store.put_blob(&handle, &rec)?;
            Some(handle)
        } else {
            None
        };

        // Append WAL + write_set.
        self.store.append_wal(&WalRecord::Stage {
            txn_id: txn_id.to_string(),
            key: key.as_str().to_string(),
            body_handle: body_handle.clone(),
        })?;

        let entry = WriteSetEntry {
            backend_key: key.as_str().to_string(),
            body_handle: body_handle.clone(),
            inline_body: if body_handle.is_none() {
                Some(body.clone())
            } else {
                None
            },
            size: body.len() as u64,
            content_type,
            user_meta: user_meta.into_iter().collect(),
            deleted,
        };
        self.store.put_write_entry(txn_id, key.as_str(), &entry)?;

        if !txn.write_keys.iter().any(|k| k == key.as_str()) {
            txn.write_keys.push(key.as_str().to_string());
            self.store.put_txn_state(txn_id, &txn)?;
        }
        // PR #4: register the SI write intent. The version we register
        // is `txn.snapshot_version + 1` — this is the commit_version that
        // would be allocated at commit time (an over-estimate is fine; the
        // actual commit_version is decided atomically in `commit`).
        let next_version = txn.snapshot_version.saturating_add(1);
        self.lock_manager
            .register_write(txn_id, key.as_str(), next_version);
        Ok(())
    }

    /// Touch a key for read tracking (within txn).
    ///
    /// PR-Fix8.2: actually register the read with the SI lock manager
    /// so Cahill cycle detection sees it. Without this, write-skew
    /// scenarios (T1 reads X/Y + writes X; T2 reads X/Y + writes Y)
    /// would commit on both sides and break serializability.
    #[tracing::instrument(
        level = "debug",
        name = "txn.read",
        skip_all,
        fields(txn.id = %txn_id, op = "read"),
        err,
    )]
    pub async fn read(
        &self,
        txn_id: &str,
        key: &ObjectKey,
        observed_version: Version,
        observed_etag: String,
    ) -> XtableResult<()> {
        let mut txn = self.require_active(txn_id)?;
        // PR-Fix8.2: SI lock acquisition.
        self.lock_manager.register_read(
            txn_id,
            key.as_str(),
            observed_version.as_u64(),
            observed_etag,
        );
        // Observability: keep `txn.read_keys` so admin tools / debug can
        // see what was read.
        if !txn.read_keys.iter().any(|k| k == key.as_str()) {
            txn.read_keys.push(key.as_str().to_string());
            self.store.put_txn_state(txn_id, &txn)?;
        }
        Ok(())
    }

    /// Commit a transaction. Runs Cahill cycle detection, S3 uploads,
    /// atomic chain append, and MemTable publish — all within a single
    /// crash-safe protocol (see `commit_inner` for the step-by-step).
    #[tracing::instrument(
        level = "info",
        name = "txn.commit",
        skip_all,
        fields(txn.id = %txn_id, op = "commit"),
        err,
    )]
    pub async fn commit(&self, txn_id: &str) -> XtableResult<CommitOutcome> {
        let _commit_guard = self.commit_lock.lock().await;
        let m = metrics();
        m.txn_commit_active.add(1, &[KeyValue::new("op", "commit")]);
        let _timed = Timed::new(&m.txn_commit_duration, vec![KeyValue::new("op", "commit")]);
        let mut owns_committing_state = false;
        let result = self.commit_inner(txn_id, &mut owns_committing_state).await;
        if owns_committing_state {
            if let Err(err) = &result {
                // Do not leave a transaction permanently in Committing when a
                // fallible step fails. If the atomic chain append already landed,
                // the durable outcome is committed; otherwise it is safe to
                // abort and release the snapshot pin.
                self.reconcile_failed_commit(txn_id, err);
            }
        }
        m.txn_commit_active
            .add(-1, &[KeyValue::new("op", "commit")]);
        m.txn_commit_total.add(
            1,
            &[KeyValue::new(
                "outcome",
                if result.is_ok() { "ok" } else { "err" },
            )],
        );
        result
    }

    fn reconcile_failed_commit(&self, txn_id: &str, cause: &XtableError) {
        let Ok(Some(mut txn)) = self.store.get_txn_state(txn_id) else {
            return;
        };
        if txn.status != TxnStatus::Committing {
            return;
        }

        let published = !txn.alloc_versions.is_empty()
            && txn.alloc_versions.iter().all(|(key, version)| {
                self.store
                    .read_chain(key)
                    .map(|chain| {
                        chain
                            .entries
                            .iter()
                            .any(|entry| entry.txn_id == txn_id && entry.commit_version == *version)
                    })
                    .unwrap_or(false)
            });

        if published {
            let commit_version = txn
                .alloc_versions
                .iter()
                .map(|(_, version)| *version)
                .max()
                .unwrap_or(txn.snapshot_version);
            txn.status = TxnStatus::Committed;
            let _ = self.store.put_txn_state(txn_id, &txn);
            self.lock_manager.mark_committed(txn_id, commit_version);
            let _ = self.store.unregister_snapshot(txn.snapshot_version);
        } else {
            let _ = self.store.append_wal(&WalRecord::Aborted {
                txn_id: txn_id.to_string(),
                reason: format!("commit failed before publish: {cause}"),
            });
            txn.status = TxnStatus::Aborted;
            let _ = self.store.put_txn_state(txn_id, &txn);
            self.lock_manager.mark_aborted(txn_id);
            let _ = self.store.unregister_snapshot(txn.snapshot_version);
        }
    }

    async fn commit_inner(
        &self,
        txn_id: &str,
        owns_committing_state: &mut bool,
    ) -> XtableResult<CommitOutcome> {
        // 1. Idempotent replay.
        if let Some(rec) = self.store.get_txn_state(txn_id)? {
            if rec.status == TxnStatus::Committed {
                // Return last known commit version from alloc_versions.
                let v = rec
                    .alloc_versions
                    .iter()
                    .map(|(_, v)| *v)
                    .max()
                    .unwrap_or(rec.snapshot_version);
                return Ok(CommitOutcome { commit_version: v });
            }
            if rec.status == TxnStatus::Aborted {
                return Err(TxnError::Aborted("txn already aborted".into()).into());
            }
            if rec.status == TxnStatus::Committing {
                // Mid-flight from a previous crashed instance — conservative abort.
                return Err(
                    TxnError::InvalidState(format!("txn in {:?} state", rec.status)).into(),
                );
            }
        } else {
            return Err(TxnError::UnknownTxn(txn_id.to_string()).into());
        }

        // Make the state transition durable and conditional. This protects
        // against a second coordinator instance committing the same txn after
        // both instances have read the old Active row.
        if !self.store.compare_and_set_txn_status(
            txn_id,
            TxnStatus::Active,
            TxnStatus::Committing,
        )? {
            let state = self
                .store
                .get_txn_state(txn_id)?
                .ok_or_else(|| TxnError::UnknownTxn(txn_id.to_string()))?;
            return match state.status {
                TxnStatus::Committed => {
                    let v = state
                        .alloc_versions
                        .iter()
                        .map(|(_, v)| *v)
                        .max()
                        .unwrap_or(state.snapshot_version);
                    Ok(CommitOutcome { commit_version: v })
                }
                TxnStatus::Aborted => Err(TxnError::Aborted("txn already aborted".into()).into()),
                status => Err(TxnError::InvalidState(format!("txn in {:?} state", status)).into()),
            };
        }
        *owns_committing_state = true;
        let mut txn = self
            .store
            .get_txn_state(txn_id)?
            .ok_or_else(|| TxnError::UnknownTxn(txn_id.to_string()))?;
        let write_entries = self.store.iter_write_set(txn_id)?;

        // Materialize spill files before the chain publish. The memtable is
        // the source for the eventual chunk, so publishing an empty body for
        // a large staged value would make the value disappear after flush.
        // Doing this before chain append also lets the normal failure
        // reconciliation abort without leaving a committed chain entry.
        let mut staged_bodies: HashMap<String, bytes::Bytes> = HashMap::new();
        for (key, write_entry) in &write_entries {
            let body = match &write_entry.inline_body {
                Some(body) => bytes::Bytes::copy_from_slice(body),
                None => match &write_entry.body_handle {
                    Some(handle) => {
                        let blob = self.store.get_blob(handle)?.ok_or_else(|| {
                            XtableError::Storage(format!("staged blob missing: {handle}"))
                        })?;
                        bytes::Bytes::from(tokio::fs::read(&blob.path).await?)
                    }
                    None => bytes::Bytes::new(),
                },
            };
            staged_bodies.insert(key.clone(), body);
        }

        // PR #4: Cahill cycle detection. Reads in-edges and out-edges on
        // this txn; if any peer appears in both, abort.
        if let Some(peer) = self.lock_manager.find_dangerous_structure(txn_id) {
            metrics().txn_ssi_conflict_total.add(1, &[]);
            self.store.append_wal(&WalRecord::Aborted {
                txn_id: txn_id.to_string(),
                reason: format!("Cahill cycle with {}", peer),
            })?;
            txn.status = TxnStatus::Aborted;
            self.store.put_txn_state(txn_id, &txn)?;
            self.lock_manager.mark_aborted(txn_id);
            let _ = self.store.unregister_snapshot(txn.snapshot_version);
            return Err(TxnError::Conflict(format!("SSI cycle with {}", peer)).into());
        }

        // 4. Allocate one commit version for the whole transaction. Every
        // key in a multi-table / multi-schema commit must share this epoch;
        // otherwise a historical snapshot between per-key versions could
        // observe only part of the transaction.
        let mut sorted_keys: Vec<String> = txn.write_keys.clone();
        sorted_keys.sort();
        // Empty transactions are successful no-ops and must not advance the
        // global version. A transaction with writes gets one shared epoch.
        let commit_version = if sorted_keys.is_empty() {
            txn.snapshot_version
        } else {
            self.store.next_global_version()?
        };
        let alloc_versions: Vec<(String, u64)> = sorted_keys
            .iter()
            .map(|key| (key.clone(), commit_version))
            .collect();

        // V7 fix: write WAL Committing BEFORE any uploads. The Committing
        // record's upload_keys field is the full intended set; on crash,
        // recovery uses this list to compensate-delete exactly those keys
        // whose uploads may have succeeded. Without this ordering, a crash
        // between upload and WAL Committing looks like "no uploads" to
        // recovery and orphans + dirty-read.
        // PR-Fix9.2: pre-upload snapshot conflict check. If any write_key
        // has been written by a concurrent txn AFTER our snapshot, we must
        // refuse BEFORE uploading — otherwise the upload would overwrite
        // the live key and the chain rollback wouldn't recover the S3 state.
        // Cheap read-only check; protects against lost-update.
        for key in &sorted_keys {
            let chain = self.store.read_chain(key)?;
            let latest = chain.latest_commit_version();
            if !chain.entries.is_empty() && latest > txn.snapshot_version {
                self.store.append_wal(&WalRecord::Aborted {
                    txn_id: txn_id.to_string(),
                    reason: format!(
                        "snapshot conflict on {}: our snapshot {}, chain latest {}",
                        key, txn.snapshot_version, latest
                    ),
                })?;
                txn.status = TxnStatus::Aborted;
                self.store.put_txn_state(txn_id, &txn)?;
                let _ = self.store.unregister_snapshot(txn.snapshot_version);
                return Err(XtableError::Conflict(format!(
                    "{}: snapshot {} < chain latest {}",
                    key, txn.snapshot_version, latest
                )));
            }
        }

        self.store.append_wal(&WalRecord::Committing {
            txn_id: txn_id.to_string(),
            upload_keys: alloc_versions.iter().map(|(k, _)| k.clone()).collect(),
        })?;

        // 5. Spec §5.1: the per-record PUT to S3 is removed. Per-record
        // bodies no longer leave through the backend on commit. The
        // single writer of structured data is the MemTable (see step 9
        // below); the flush loop encodes MemTable contents into a chunk
        // and uploads it. The chain append + WAL Committed (step 8)
        // remain the durability boundary — atomicity depends on them,
        // not on per-record S3 PUT.
        txn.alloc_versions = alloc_versions.clone();
        // No uploaded_keys — there are no per-record S3 uploads any more.
        txn.uploaded_keys.clear();
        self.store.put_txn_state(txn_id, &txn)?;

        // 6/7. WAL Committing already written above. Below is the
        // chain-publish + WAL Committed stage.

        // 8. MVCC: append new VersionEntry to each chain atomically.
        // Invariants satisfied here:
        //  - I1 (chain monotonic): enforced by append_chain_entries_bulk
        //  - I6 (atomicity): all entries appended in a single redb write txn
        // V10: deleted entries get a tombstone VersionEntry.
        let mut entries: Vec<(String, xtable_storage::VersionEntry, u64)> =
            Vec::with_capacity(alloc_versions.len());
        for (k, v) in &alloc_versions {
            let write_entry = write_entries.iter().find(|(kk, _)| kk == k);
            let is_deleted = write_entry.map(|(_, e)| e.deleted).unwrap_or(false);
            let size = write_entry.map(|(_, e)| e.size).unwrap_or(0);

            let entry = if is_deleted {
                let mut e =
                    xtable_storage::VersionEntry::tombstone(*v, k.clone(), txn_id.to_string());
                e.size = 0;
                e
            } else {
                xtable_storage::VersionEntry::new(
                    *v,
                    String::new(),
                    k.clone(),
                    txn_id.to_string(),
                    size,
                )
            };
            // PR-Fix9.2: include this txn's snapshot_version so the
            // bulk append can detect snapshot conflicts atomically.
            entries.push((k.clone(), entry, txn.snapshot_version));
        }
        self.store.append_chain_entries_bulk(&entries)?;

        // V4 fix: keep TBL_VERSIONS in sync with the chain. The chain is
        // the authoritative log; TBL_VERSIONS mirrors it for compensation
        // (V3 — needs the prior backend_key to restore on partial-failure
        // aborts) and for the rebuild path (single source of truth per
        // object).
        let now_ms = Utc::now().timestamp_millis();
        let mut version_updates: Vec<(ObjectKey, VersionRecord)> =
            Vec::with_capacity(alloc_versions.len());
        for (k, v) in &alloc_versions {
            let write_entry = write_entries.iter().find(|(kk, _)| kk == k);
            let is_deleted = write_entry.map(|(_, e)| e.deleted).unwrap_or(false);
            let size = write_entry.map(|(_, e)| e.size).unwrap_or(0);
            version_updates.push((
                ObjectKey::new(k),
                VersionRecord {
                    latest_version: Version(*v),
                    latest_etag: String::new(),
                    latest_backend_key: k.clone(),
                    last_writer_txn_id: txn_id.to_string(),
                    tombstone: is_deleted,
                    size,
                    last_modified_unix_ms: now_ms,
                },
            ));
        }
        self.store.put_versions_bulk(&version_updates)?;

        // 9. Mark committed (WAL + TxnState).
        self.store.append_wal(&WalRecord::Committed {
            txn_id: txn_id.to_string(),
            commit_version,
        })?;
        self.store.append_wal(&WalRecord::CommitResult {
            txn_id: txn_id.to_string(),
            commit_version,
            success: true,
        })?;
        txn.status = TxnStatus::Committed;
        self.store.put_txn_state(txn_id, &txn)?;

        // PR #4: publish entries to memtable. Each write becomes visible
        // at `commit_version` for reads at-or-after that snapshot. A
        // background flush task encodes the immutable memtable into a
        // chunk and uploads to S3 (see `flush_loop`).
        //
        // Memtable publish. Spec §5.1: this is the only writer of
        // structured data to the LSM layer. Per-record keys are parsed
        // from the staged `backend_key` so chunk layout is keyed by the
        // real (space, table). Records: `_xtable/{space}/{table}/{rid}`.
        // Schemas (Task 4): `_xtable/{space}/_schema/{name}/v{N}.json`.
        // Non-structured keys fall back to ("" , "" , key) so they stay
        // isolated from the structured-data path.
        for (key, we) in &write_entries {
            let body = staged_bodies.get(key).cloned().unwrap_or_default();
            let (space, table, record_id) = parse_record_key(key)
                .unwrap_or_else(|| (String::new(), String::new(), key.clone()));
            let mem_key: xtable_storage::memtable::RecordKey = (space, table, record_id);
            let cv_atomic = Arc::new(std::sync::atomic::AtomicU64::new(commit_version));
            let mem_entry = MemEntry {
                key: mem_key.clone(),
                value: Arc::new(RecordValue { bytes: body }),
                commit_version: cv_atomic,
                txn_id: txn_id.to_string(),
                deleted: we.deleted,
                content_type: we.content_type.clone(),
                user_meta: we.user_meta.clone(),
                schema_version: 0,
                wal_seq: commit_version,
                size_bytes: we.size,
            };
            // Memtable write is best-effort — chain append is already durable.
            let _ = self.memtable_set.put_invisible(mem_entry);
            self.memtable_set
                .publish(&mem_key, commit_version, commit_version);
        }

        // PR #4: mark the txn as recently committed in the SI lock
        // manager so future commits can still detect dangerous structures.
        self.lock_manager.mark_committed(txn_id, commit_version);

        // 9b. Fire post-commit hooks. After this point observers can
        // reconcile their own indexes (record / schema index in the
        // structured-data-space layer).
        let writes = entries
            .iter()
            .map(|(k, e, _snap)| CommitWrite {
                key: k.clone(),
                commit_version: e.commit_version,
                deleted: e.deleted,
                size: e.size,
            })
            .collect::<Vec<_>>();
        self.fire_post_commit_hooks(&CommitEvent {
            txn_id: txn_id.to_string(),
            commit_version,
            writes,
        });

        // MVCC: release the snapshot pin so GC can prune old versions.
        let _ = self.store.unregister_snapshot(txn.snapshot_version);

        info!(txn = %txn_id, version = commit_version, "CommitTxn ok");

        // 10. GC staged bodies (best-effort).
        // V12 fix: get the BlobRecord FIRST (so we have the path), then delete
        // the blob metadata row, then remove the file. The previous order was
        // delete-then-get, which left the spill file on disk forever.
        for (_, entry) in &write_entries {
            if let Some(handle) = &entry.body_handle {
                let rec_path = self.store.get_blob(handle).ok().flatten().map(|r| r.path);
                let _ = self.store.delete_blob(handle);
                if let Some(path) = rec_path {
                    let _ = std::fs::remove_file(&path);
                }
                let _ = self.store.delete_write_entry(txn_id, &entry.backend_key);
            }
        }

        Ok(CommitOutcome { commit_version })
    }

    /// Abort a transaction. Drop staged bodies, mark aborted in WAL.
    #[tracing::instrument(
        level = "info",
        name = "txn.abort",
        skip_all,
        fields(txn.id = %txn_id, op = "abort"),
        err,
    )]
    pub async fn abort(&self, txn_id: &str) -> XtableResult<()> {
        let _commit_guard = self.commit_lock.lock().await;
        let m = metrics();
        let _timed = Timed::new(&m.txn_abort_duration, vec![KeyValue::new("op", "abort")]);
        let result = self.abort_inner(txn_id).await;
        m.txn_abort_total.add(
            1,
            &[KeyValue::new(
                "outcome",
                if result.is_ok() { "ok" } else { "err" },
            )],
        );
        result
    }

    async fn abort_inner(&self, txn_id: &str) -> XtableResult<()> {
        let txn = match self.store.get_txn_state(txn_id)? {
            Some(t) => t,
            None => return Err(TxnError::UnknownTxn(txn_id.to_string()).into()),
        };
        if txn.status == TxnStatus::Committed {
            return Err(TxnError::InvalidState("txn already committed".into()).into());
        }
        if txn.status == TxnStatus::Aborted {
            return Ok(());
        }
        if txn.status != TxnStatus::Active {
            return Err(
                TxnError::InvalidState(format!("txn not abortable: {:?}", txn.status)).into(),
            );
        }
        if !self
            .store
            .compare_and_set_txn_status(txn_id, TxnStatus::Active, TxnStatus::Aborted)?
        {
            return Err(TxnError::InvalidState("txn state changed while aborting".into()).into());
        }
        // Drop staged blobs.
        let writes = self.store.iter_write_set(txn_id)?;
        for (_, entry) in writes {
            if let Some(handle) = &entry.body_handle {
                if let Ok(Some(rec)) = self.store.get_blob(handle) {
                    let _ = std::fs::remove_file(&rec.path);
                }
                let _ = self.store.delete_blob(handle);
            }
            let _ = self.store.delete_write_entry(txn_id, &entry.backend_key);
        }
        self.store.append_wal(&WalRecord::Aborted {
            txn_id: txn_id.to_string(),
            reason: "explicit abort".into(),
        })?;
        // MVCC: release the snapshot pin.
        let _ = self.store.unregister_snapshot(txn.snapshot_version);
        self.lock_manager.mark_aborted(txn_id);
        Ok(())
    }

    /// Look up transaction status.
    pub async fn status(&self, txn_id: &str) -> XtableResult<TxnStatus> {
        match self.store.get_txn_state(txn_id)? {
            Some(t) => Ok(t.status),
            None => Err(TxnError::UnknownTxn(txn_id.to_string()).into()),
        }
    }

    /// Heartbeat a transaction (refresh last_heartbeat).
    pub async fn heartbeat(&self, txn_id: &str) -> XtableResult<()> {
        let mut txn = self.require_active(txn_id)?;
        txn.last_heartbeat_ms = Utc::now().timestamp_millis();
        self.store.put_txn_state(txn_id, &txn)
    }

    /// Read body for txn-staged upload (from redb inline bytes or spill file).
    pub async fn stage_body(&self, txn_id: &str, key: &str) -> XtableResult<Option<Vec<u8>>> {
        let entries = self.store.iter_write_set(txn_id)?;
        for (k, e) in entries {
            if k == key {
                if let Some(inline) = &e.inline_body {
                    return Ok(Some(inline.clone()));
                }
                if let Some(handle) = &e.body_handle {
                    let rec = self
                        .store
                        .get_blob(handle)?
                        .ok_or_else(|| XtableError::Storage(format!("blob missing: {}", handle)))?;
                    let bytes = tokio::fs::read(&rec.path).await?;
                    return Ok(Some(bytes));
                }
                return Ok(None);
            }
        }
        Ok(None)
    }

    fn require_active(&self, txn_id: &str) -> XtableResult<TxnStateRecord> {
        match self.store.get_txn_state(txn_id)? {
            Some(t) if t.status == TxnStatus::Active => Ok(t),
            Some(t) => {
                Err(TxnError::InvalidState(format!("txn not active: {:?}", t.status)).into())
            }
            None => Err(TxnError::UnknownTxn(txn_id.to_string()).into()),
        }
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(bytes);
    hex::encode(h.finalize())
}

/// Produce a UUID-like hex suffix from a key (avoid pulling uuid crate).
fn uuid_like(s: &str) -> String {
    let mut hasher = sha2::Sha256::new();
    use sha2::Digest;
    hasher.update(s.as_bytes());
    hex::encode(&hasher.finalize()[..12])
}

/// Parse a structured-data key into `(space, table, record_id)`.
///
/// Recognised shapes:
///   - Records: `_xtable/{space}/{table}/{record_id}`
///     → `(space, table, record_id)` (record_id captures everything
///     after the second slash so paths like `_xtable/acme/users/u/1.json`
///     still parse cleanly).
///   - Schemas: `_xtable/{space}/_schema/{name}/v{N}.json`
///     → `(space, "_schema", "{name}/v{N}")`
///
/// Returns `None` if the key doesn't match either shape. Non-structured
/// keys (no `_xtable/` prefix) also return `None`; callers fall back to
/// using the full key as the `record_id` with empty space/table so the
/// entry stays isolated from the structured-data path.
fn parse_record_key(backend_key: &str) -> Option<(String, String, String)> {
    let stripped = backend_key.strip_prefix("_xtable/")?;
    let mut it = stripped.splitn(3, '/');
    let space = it.next()?.to_string();
    let second = it.next()?.to_string();
    let rest = it.next()?.to_string();
    if second == "_schema" {
        // `_xtable/{space}/_schema/{name}/v{N}.json` → record_id is
        // `{name}/v{N}`. The splitn above already grouped the tail.
        Some((space, "_schema".to_string(), rest))
    } else {
        // `_xtable/{space}/{table}/{record_id}.json`. Mirror the
        // schema-engine parser: drop the trailing `.json` so the
        // memtable key matches what callers (and the record index)
        // use as the bare `record_id`.
        let record_id = rest.strip_suffix(".json")?.to_string();
        Some((space, second, record_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn build_for_test() -> (TxnCoordinator, TempDir) {
        let tmp = TempDir::new().unwrap();
        let store = Arc::new(LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap());
        let backend = Arc::new(
            xtable_backend::BackendClient::dummy_for_test_async()
                .await
                .expect("dummy backend"),
        );
        let coord = TxnCoordinator::new(store, backend, tmp.path().join("staged"), 4);
        (coord, tmp)
    }

    #[tokio::test]
    async fn begin_assigns_unique_txn_id_and_snapshot() {
        let (coord, _tmp) = build_for_test().await;
        let t1 = coord.begin(None).await.unwrap();
        let t2 = coord.begin(None).await.unwrap();
        assert_ne!(t1, t2);
        let status1 = coord.status(&t1).await.unwrap();
        assert_eq!(status1, TxnStatus::Active);
    }

    #[tokio::test]
    async fn abort_marks_terminal() {
        let (coord, _tmp) = build_for_test().await;
        let t = coord.begin(None).await.unwrap();
        coord.abort(&t).await.unwrap();
        let status = coord.status(&t).await.unwrap();
        assert_eq!(status, TxnStatus::Aborted);
    }

    #[tokio::test]
    async fn unknown_txn_returns_unknown_error() {
        let (coord, _tmp) = build_for_test().await;
        let err = coord.status("does-not-exist").await.unwrap_err();
        assert_eq!(err.http_status(), 404);
    }

    #[tokio::test]
    async fn commit_with_no_writes_succeeds_idempotent() {
        let (coord, _tmp) = build_for_test().await;
        let t = coord.begin(None).await.unwrap();
        let out = coord.commit(&t).await.unwrap();
        assert_eq!(out.commit_version, 0);
        let out2 = coord.commit(&t).await.unwrap();
        assert_eq!(out2.commit_version, 0);
    }

    #[tokio::test]
    async fn concurrent_commit_calls_are_idempotent() {
        let (coord, _tmp) = build_for_test().await;
        let t = coord.begin(None).await.unwrap();
        coord
            .stage(
                &t,
                &ObjectKey::new("k"),
                b"value".to_vec(),
                None,
                HashMap::new(),
                false,
            )
            .await
            .unwrap();

        let (first, second) = tokio::join!(coord.commit(&t), coord.commit(&t));
        let first = first.unwrap();
        let second = second.unwrap();
        assert_eq!(first.commit_version, second.commit_version);
        assert_eq!(coord.store().read_chain("k").unwrap().entries.len(), 1);
    }

    #[tokio::test]
    async fn heartbeat_refreshes_state() {
        let (coord, _tmp) = build_for_test().await;
        let t = coord.begin(None).await.unwrap();
        coord.heartbeat(&t).await.unwrap();
        let status = coord.status(&t).await.unwrap();
        assert_eq!(status, TxnStatus::Active);
    }
}
