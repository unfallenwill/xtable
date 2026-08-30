//! Transaction coordinator — MVCC + Cahill SSI state machine.
//!
//! Protocol order (critical for crash-safety):
//! 1. BeginTxn → TxnId + snapshot_version, WAL `Begin` + register SI txn
//! 2. Stage (per PutObject in txn) → WAL `Stage` + WriteSetEntry +
//!    register SI write intent (lock_manager.register_write)
//! 3. CommitTxn:
//!    a. Cahill cycle detection (lock_manager.find_dangerous_structure)
//!       → abort on dangerous structure (Conflict)
//!    b. Upload all keys to backend S3 (single PUT or multipart)
//!    c. Atomic redb write txn: append_chain_entries_bulk with
//!       snapshot-conflict check (prevents lost-update) + memtable publish
//!    d. WAL `Committed` + Mark committed on SI lock manager
//!    e. Fire post-commit hooks (record_index update)
//! 4. On any failure during upload, compensating-delete already-uploaded keys,
//!    WAL `Aborted`, return 409 / 503.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use chrono::Utc;
use futures_util::stream::{FuturesUnordered, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use xtable_backend::{BackendClient, BackendError};
use xtable_core::headers::TxnStatus;
use xtable_core::{ObjectKey, TxnId, Version, XtableError, XtableResult};
use xtable_storage::{
    BlobRecord, LocalStore, MemEntry, MemTableSet, RecordValue, TxnStateRecord, VersionRecord,
    WalRecord, WriteSetEntry,
};

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
    /// Optional concurrency limit for parallel backend uploads.
    upload_concurrency: Arc<Semaphore>,
    /// PR #4: Cahill SSI lock manager. Tracks per-txn SIRead/SIWrite locks
    /// and rw-antidependency edges; commit-time cycle detection aborts
    /// txns that participate in dangerous structures.
    lock_manager: Arc<SiLockManager>,
    /// PR #4: in-memory MemTable set. Commit publishes to memtable; a
    /// background flush task uploads chunks to S3.
    memtable_set: Arc<MemTableSet>,
    /// Post-commit hooks (e.g., index maintenance for the structured-data-space layer).
    post_commit_hooks: Arc<std::sync::RwLock<Vec<PostCommitHook>>>,
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
        upload_concurrency: usize,
    ) -> Self {
        std::fs::create_dir_all(&spill_dir).ok();
        Self {
            store,
            backend,
            spill_dir: Arc::new(spill_dir),
            upload_concurrency: Arc::new(Semaphore::new(upload_concurrency.max(1))),
            // PR #4: default SI lock manager + memtable set.
            lock_manager: SiLockManager::new(),
            memtable_set: MemTableSet::new(
                xtable_storage::MemTable::new(0),
                xtable_storage::FlushPolicy::default(),
            ),
            post_commit_hooks: Arc::new(std::sync::RwLock::new(Vec::new())),
        }
    }

    /// Construct with explicit SI lock manager and memtable set (used by
    /// tests and by server startup to wire shared instances).
    pub fn with_lock_and_memtable(
        store: Arc<LocalStore>,
        backend: Arc<BackendClient>,
        spill_dir: std::path::PathBuf,
        upload_concurrency: usize,
        lock_manager: Arc<SiLockManager>,
        memtable_set: Arc<MemTableSet>,
    ) -> Self {
        std::fs::create_dir_all(&spill_dir).ok();
        Self {
            store,
            backend,
            spill_dir: Arc::new(spill_dir),
            upload_concurrency: Arc::new(Semaphore::new(upload_concurrency.max(1))),
            lock_manager,
            memtable_set,
            post_commit_hooks: Arc::new(std::sync::RwLock::new(Vec::new())),
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
    pub async fn begin(&self, idempotency_key: Option<String>) -> XtableResult<String> {
        let txn_id = Self::next_txn_id();
        let snapshot_version = self.store.current_global_version()?;
        let now_ms = Utc::now().timestamp_millis();
        let rec = TxnStateRecord::new_active(snapshot_version, idempotency_key.clone(), now_ms);
        self.store.put_txn_state(&txn_id, &rec)?;
        // PR-Fix1.1: register the txn in the SI lock manager so that
        // `register_read` / `register_write` / `find_dangerous_structure`
        // see it. Without this the lock manager stays empty and SSI is dead.
        self.lock_manager.begin_txn(&txn_id, snapshot_version)?;
        // MVCC: register the snapshot so it pins old versions from GC.
        self.store.register_snapshot(snapshot_version)?;
        self.store.append_wal(&WalRecord::Begin {
            txn_id: txn_id.clone(),
            snapshot_version,
            idempotency_key,
        })?;
        debug!(txn = %txn_id, version = snapshot_version, "BeginTxn");
        Ok(txn_id)
    }

    /// Stage a write within a transaction.
    /// The body is held in-memory here (caller passes bytes) and may spill to
    /// disk if it exceeds the threshold (default 256 KiB).
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
            let handle = format!("{}-{}", txn_id, uuid_like(&key.as_str()));
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
            inline_body: if body_handle.is_none() { Some(body.clone()) } else { None },
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
        self.lock_manager.register_write(
            txn_id,
            key.as_str(),
            next_version,
        );
        Ok(())
    }

    /// Touch a key for read tracking (within txn).
    ///
    /// PR-Fix8.2: actually register the read with the SI lock manager
    /// so Cahill cycle detection sees it. Without this, write-skew
    /// scenarios (T1 reads X/Y + writes X; T2 reads X/Y + writes Y)
    /// would commit on both sides and break serializability.
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

    /// Commit a transaction. PR #3 removed the OCC validate phase; PR #4
    /// wires the SI lock manager + MemTable publish into `commit_inner`.
    pub async fn commit(&self, txn_id: &str) -> XtableResult<CommitOutcome> {
        // PR #3: `commit_lock` removed. The SI lock manager's interior
        // mutex provides equivalent serialization for the per-txn
        // critical section. Cross-txn serialization on the same key is
        // handled by `append_chain_entries_bulk`'s monotonicity check.
        self.commit_inner(txn_id).await
    }

    async fn commit_inner(&self, txn_id: &str) -> XtableResult<CommitOutcome> {
        // 1. Idempotent replay.
        if let Some(rec) = self.store.get_txn_state(txn_id)? {
            if rec.status == TxnStatus::Committed {
                // Return last known commit version from alloc_versions.
                let v = rec.alloc_versions.iter().map(|(_, v)| *v).max().unwrap_or(rec.snapshot_version);
                return Ok(CommitOutcome { commit_version: v });
            }
            if rec.status == TxnStatus::Aborted {
                return Err(TxnError::Aborted("txn already aborted".into()).into());
            }
            if rec.status == TxnStatus::Committing {
                // Mid-flight from a previous crashed instance — conservative abort.
                return Err(TxnError::InvalidState(format!("txn in {:?} state", rec.status)).into());
            }
        } else {
            return Err(TxnError::UnknownTxn(txn_id.to_string()).into());
        }

        let mut txn = self.store.get_txn_state(txn_id)?
            .ok_or_else(|| TxnError::UnknownTxn(txn_id.to_string()))?;
        let write_entries = self.store.iter_write_set(txn_id)?;

        // PR #4: Cahill cycle detection. Reads in-edges and out-edges on
        // this txn; if any peer appears in both, abort.
        if let Some(peer) = self.lock_manager.find_dangerous_structure(txn_id) {
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

        txn.status = TxnStatus::Committing;
        self.store.put_txn_state(txn_id, &txn)?;

        // PR #3: OCC validate removed. Conflict detection moves to the
        // SI lock manager via `find_dangerous_structure()` at commit.

        // 4. Allocate new versions per key. Sort for deterministic ordering.
        let mut sorted_keys: Vec<String> = txn.write_keys.clone();
        sorted_keys.sort();
        let mut alloc_versions: Vec<(String, u64)> = Vec::with_capacity(sorted_keys.len());
        for k in &sorted_keys {
            // We're committing, so global_version must advance at least once.
            let v = self.store.next_global_version()?;
            alloc_versions.push((k.clone(), v));
        }

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
                )).into());
            }
        }

        self.store.append_wal(&WalRecord::Committing {
            txn_id: txn_id.to_string(),
            upload_keys: alloc_versions.iter().map(|(k, _)| k.clone()).collect(),
        })?;

        // 5. Upload all bodies to a per-txn staging path in S3, NOT to the
        // final key paths. This is the V3 fix: if any upload fails, we can
        // abort cleanly by deleting staging copies without ever having
        // overwritten the live (T0) data. On full success we promote each
        // staging object to its final key.
        let upload_keys = Arc::new(sorted_keys);
        let staging_prefix = format!("xtable-txn-staging/{}/", txn_id);
        let upload_results = self
            .upload_all(txn_id, &write_entries, &alloc_versions, upload_keys.clone(), &staging_prefix)
            .await;

        let mut uploaded: Vec<String> = Vec::new();
        let mut failed: Vec<(String, String)> = Vec::new();
        for (key, res) in upload_results {
            match res {
                Ok(()) => uploaded.push(key),
                Err(e) => failed.push((key, format!("{}", e))),
            }
        }

        // Record committed-uploads list for recovery / compensation.
        txn.uploaded_keys = uploaded.clone();
        txn.alloc_versions = alloc_versions.clone();
        self.store.put_txn_state(txn_id, &txn)?;

        // 6. If any uploads failed, abort cleanly: delete every staging
        // copy we did manage to write, leave the live backend untouched,
        // and never append the chain entry.
        if !failed.is_empty() {
            warn!(txn = %txn_id, failed_keys = ?failed, "uploads failed; cleaning up staging copies");
            for (key, _alloc_v) in &alloc_versions {
                if !uploaded.contains(key) {
                    continue;
                }
                let staging_key = format!("{}{}", staging_prefix, key);
                let _ = self.backend.delete_object(&ObjectKey::new(&staging_key)).await;
            }
            self.store.append_wal(&WalRecord::Aborted {
                txn_id: txn_id.to_string(),
                reason: format!("upload failures: {:?}", failed),
            })?;
            txn.status = TxnStatus::Aborted;
            self.store.put_txn_state(txn_id, &txn)?;
            return Err(XtableError::Backend(format!("txn {} aborted: upload failures", txn_id)));
        }

        // 6b. All staging uploads succeeded — promote each to its final
        // key. For deleted=true entries, the staging delete now takes
        // effect on the live key. For normal entries, we copy the staged
        // body to the final key (preserving xtable metadata), then delete
        // the staging copy.
        for (key, alloc_v) in &alloc_versions {
            let staging_key = format!("{}{}", staging_prefix, key);
            let staging_obj = ObjectKey::new(&staging_key);
            let final_obj = ObjectKey::new(key);
            let write_entry = write_entries.iter().find(|(kk, _)| kk == key);
            let is_deleted = write_entry.map(|(_, e)| e.deleted).unwrap_or(false);

            if is_deleted {
                // V10: actually delete the live key.
                let _ = self.backend.delete_object(&final_obj).await;
            } else {
                match self.backend.get_object(&staging_obj).await {
                    Ok(got) => {
                        let mut meta = HashMap::new();
                        meta.insert(
                            "x-amz-meta-xtable-version".to_string(),
                            alloc_v.to_string(),
                        );
                        meta.insert(
                            "x-amz-meta-xtable-txn-id".to_string(),
                            txn_id.to_string(),
                        );
                        let _ = self
                            .backend
                            .put_object(&final_obj, got.bytes, None, meta)
                            .await;
                    }
                    Err(_) => {
                        // We just uploaded this; if it's gone now the
                        // backend is in trouble. Don't overwrite live
                        // data speculatively.
                        warn!(key = %key, "could not fetch staging body for promotion");
                    }
                }
            }
            // Clean up staging copy regardless.
            let _ = self.backend.delete_object(&staging_obj).await;
        }

        // 7. V7 fix: WAL Committing already written above (before uploads).
        // Below is the chain-publish + WAL Committed stage.

        // 8. MVCC: append new VersionEntry to each chain atomically.
        // Invariants satisfied here:
        //  - I1 (chain monotonic): enforced by append_chain_entries_bulk
        //  - I6 (atomicity): all entries appended in a single redb write txn
        // V10: deleted entries get a tombstone VersionEntry.
        let mut entries: Vec<(String, xtable_storage::VersionEntry, u64)> = Vec::with_capacity(alloc_versions.len());
        for (k, v) in &alloc_versions {
            let write_entry = write_entries.iter().find(|(kk, _)| kk == k);
            let is_deleted = write_entry.map(|(_, e)| e.deleted).unwrap_or(false);
            let size = write_entry.map(|(_, e)| e.size).unwrap_or(0);

            let entry = if is_deleted {
                let mut e = xtable_storage::VersionEntry::tombstone(*v, k.clone(), txn_id.to_string());
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

        // V4 fix: keep TBL_VERSIONS in sync with the chain. Even though
        // OCC validation now reads the chain directly, TBL_VERSIONS is
        // still load-bearing for compensation (V3 — needs the prior
        // backend_key to restore on partial-failure aborts) and for the
        // rebuild path (single source of truth per object).
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

        let commit_version = alloc_versions.iter().map(|(_, v)| *v).max().unwrap_or(txn.snapshot_version);

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
        // Memtable uses space="" / table="" for non-structured records.
        // Structured records (which have a space/table) get the same
        // empty pair here; the structured layer maintains its own index
        // via post-commit hooks.
        for (key, we) in &write_entries {
            let body = match &we.inline_body {
                Some(b) => bytes::Bytes::copy_from_slice(b.as_slice()),
                None => match &we.body_handle {
                    Some(_) => bytes::Bytes::new(), // spill file — not loaded here
                    None => bytes::Bytes::new(),
                },
            };
            let mem_key: xtable_storage::memtable::RecordKey =
                (String::new(), String::new(), key.clone());
            let cv_atomic = Arc::new(std::sync::atomic::AtomicU64::new(commit_version));
            let mem_entry = MemEntry {
                key: mem_key,
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
            self.memtable_set.publish(
                &(String::new(), String::new(), key.clone()),
                commit_version,
                commit_version,
            );
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
    pub async fn abort(&self, txn_id: &str) -> XtableResult<()> {
        let mut txn = match self.store.get_txn_state(txn_id)? {
            Some(t) => t,
            None => return Err(TxnError::UnknownTxn(txn_id.to_string()).into()),
        };
        if txn.status == TxnStatus::Committed {
            return Err(TxnError::InvalidState("txn already committed".into()).into());
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
        txn.status = TxnStatus::Aborted;
        self.store.put_txn_state(txn_id, &txn)?;
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
                    let rec = self.store.get_blob(handle)?
                        .ok_or_else(|| XtableError::Storage(format!("blob missing: {}", handle)))?;
                    let bytes = tokio::fs::read(&rec.path).await?;
                    return Ok(Some(bytes));
                }
                return Ok(None);
            }
        }
        Ok(None)
    }

    /// Upload all staged writes to the backend S3 in parallel.
    /// For `deleted=true` entries (V10 fix), call DeleteObject on the backend
    /// instead of PutObject.
    /// `key_prefix` (e.g. `"xtable-txn-staging/{txn_id}/"`) is prepended to
    /// every S3 key so callers can stage writes to a side location and
    /// promote them only after the whole txn is confirmed. Empty string
    /// means "publish to final key path" (legacy behavior).
    /// Returns per-key results for compensation on failure.
    async fn upload_all(
        &self,
        txn_id: &str,
        write_entries: &[(String, WriteSetEntry)],
        alloc_versions: &[(String, u64)],
        upload_keys: Arc<Vec<String>>,
        key_prefix: &str,
    ) -> Vec<(String, Result<(), BackendError>)> {
        let mut futures: FuturesUnordered<Pin<Box<dyn std::future::Future<Output = (String, Result<(), BackendError>)> + Send>>> = FuturesUnordered::new();
        for (key, entry) in write_entries {
            let key_str = key.clone();
            let alloc_v = alloc_versions.iter().find(|(k, _)| k == &key_str).map(|(_, v)| *v).unwrap_or(0);

            let meta_map = entry.user_meta.iter().cloned().collect::<HashMap<_, _>>();
            let backend = Arc::clone(&self.backend);
            let permit_sem = Arc::clone(&self.upload_concurrency);
            let key_for_task = key_str.clone();
            let deleted = entry.deleted;
            let txn_id_owned = txn_id.to_string();
            let s3_key = format!("{}{}", key_prefix, key_for_task);

            if deleted {
                // V10: transactional delete → DeleteObject (NOT PutObject with empty body).
                let fut = async move {
                    let _permit = permit_sem.acquire_owned().await.expect("semaphore closed");
                    let res = backend.delete_object(&ObjectKey::new(&s3_key)).await;
                    (key_for_task, res.map(|_| ()))
                };
                futures.push(Box::pin(fut));
                continue;
            }

            let body = if let Some(inline) = &entry.inline_body {
                inline.clone()
            } else if let Some(handle) = &entry.body_handle {
                let rec = match self.store.get_blob(handle) {
                    Ok(Some(r)) => r,
                    _ => return vec![(key_str, Err(BackendError::Internal("blob missing".into())))],
                };
                match std::fs::read(&rec.path) {
                    Ok(b) => b,
                    Err(_) => return vec![(key_str, Err(BackendError::Internal("blob read fail".into())))],
                }
            } else {
                // Should not happen — non-deleted entries must have a body.
                Vec::new()
            };

            let fut = async move {
                let _permit = permit_sem.acquire_owned().await.expect("semaphore closed");
                let mut meta = meta_map;
                meta.insert(
                    "x-amz-meta-xtable-version".to_string(),
                    alloc_v.to_string(),
                );
                meta.insert(
                    "x-amz-meta-xtable-txn-id".to_string(),
                    txn_id_owned.to_string(),
                );
                let res = backend
                    .put_object(
                        &ObjectKey::new(&s3_key),
                        body,
                        entry.content_type.as_deref(),
                        meta,
                    )
                    .await;
                (key_for_task, res.map(|_| ()))
            };
            futures.push(Box::pin(fut));
        }

        let mut results: Vec<(String, Result<(), BackendError>)> = Vec::new();
        while let Some(item) = futures.next().await {
            let _ = upload_keys;
            results.push(item);
        }
        results
    }

    fn require_active(&self, txn_id: &str) -> XtableResult<TxnStateRecord> {
        match self.store.get_txn_state(txn_id)? {
            Some(t) if t.status == TxnStatus::Active => Ok(t),
            Some(t) => Err(TxnError::InvalidState(format!("txn not active: {:?}", t.status)).into()),
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
    async fn heartbeat_refreshes_state() {
        let (coord, _tmp) = build_for_test().await;
        let t = coord.begin(None).await.unwrap();
        coord.heartbeat(&t).await.unwrap();
        let status = coord.status(&t).await.unwrap();
        assert_eq!(status, TxnStatus::Active);
    }
}