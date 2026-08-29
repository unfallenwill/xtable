//! Transaction coordinator — OCC state machine.
//!
//! Protocol order (critical for crash-safety):
//! 1. BeginTxn → TxnId + snapshot_version, WAL `Begin`
//! 2. Stage (per PutObject in txn) → WAL `Stage` + WriteSetEntry
//! 3. CommitTxn:
//!    a. Validate: every write_key's `version_at_read` must equal `current_version`
//!    b. Upload all keys to backend S3 with `x-amz-meta-xtable-version` metadata
//!       (in parallel via JoinSet)
//!    c. Bulk-put version records to redb versions table (single write txn)
//!    d. WAL `Committing` → `Committed` → `CommitResult` (single write txn)
//!    e. Schedule staged-body GC
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
    BlobRecord, LocalStore, ReadSetEntry, TxnStateRecord, VersionRecord, WalRecord,
    WriteSetEntry,
};

use crate::error::TxnError;

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
    /// V5 fix: per-coordinator commit mutex serializes the commit
    /// critical section (validate → upload → chain append → WAL Committed).
    /// redb already serializes individual ops, but without this lock
    /// the validate → upload window leaves room for concurrent commits on
    /// the same key to interleave and silently overwrite each other.
    commit_lock: Arc<tokio::sync::Mutex<()>>,
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
            // V5 fix: per-coordinator commit mutex. Within a single
            // xtable-server process, only one commit can be in its
            // critical section at a time. redb still provides per-key
            // atomicity at the storage layer.
            commit_lock: Arc::new(tokio::sync::Mutex::new(())),
            post_commit_hooks: Arc::new(std::sync::RwLock::new(Vec::new())),
        }
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

        // V16 fix: version_at_read must be the txn's snapshot_version
        // (captured at begin), not the chain's latest_commit_version at
        // stage time. Otherwise a concurrent commit between begin and stage
        // would shift version_at_read, hiding lost updates (write skew).
        let version_at_read = txn.snapshot_version;
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
            version_at_read,
        })?;

        let entry = WriteSetEntry {
            backend_key: key.as_str().to_string(),
            body_handle: body_handle.clone(),
            inline_body: if body_handle.is_none() { Some(body.clone()) } else { None },
            size: body.len() as u64,
            content_type,
            user_meta: user_meta.into_iter().collect(),
            version_at_read,
            deleted,
        };
        self.store.put_write_entry(txn_id, key.as_str(), &entry)?;

        if !txn.write_keys.iter().any(|k| k == key.as_str()) {
            txn.write_keys.push(key.as_str().to_string());
            self.store.put_txn_state(txn_id, &txn)?;
        }
        Ok(())
    }

    /// Touch a key for read tracking (within txn).
    pub async fn read(&self, txn_id: &str, key: &ObjectKey, observed_version: Version, observed_etag: String) -> XtableResult<()> {
        let mut txn = self.require_active(txn_id)?;
        let entry = ReadSetEntry { version_observed: observed_version.as_u64(), etag_observed: observed_etag };
        self.store.put_read_entry(txn_id, key.as_str(), &entry)?;
        if !txn.read_keys.iter().any(|k| k == key.as_str()) {
            txn.read_keys.push(key.as_str().to_string());
            self.store.put_txn_state(txn_id, &txn)?;
        }
        Ok(())
    }

    /// Commit a transaction. Implements the OCC validate-then-publish protocol.
    pub async fn commit(&self, txn_id: &str) -> XtableResult<CommitOutcome> {
        // V5 fix: serialize the commit critical section across the whole
        // coordinator. Combined with V4 (OCC reads chain) and V16
        // (version_at_read = snapshot_version), concurrent commits on the
        // same key now strictly serialize at this point.
        let _guard = self.commit_lock.lock().await;
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
            if rec.status == TxnStatus::Validating || rec.status == TxnStatus::Committing {
                // Mid-flight from a previous crashed instance — conservative abort.
                return Err(TxnError::InvalidState(format!("txn in {:?} state", rec.status)).into());
            }
        } else {
            return Err(TxnError::UnknownTxn(txn_id.to_string()).into());
        }

        let mut txn = self.store.get_txn_state(txn_id)?
            .ok_or_else(|| TxnError::UnknownTxn(txn_id.to_string()))?;
        txn.status = TxnStatus::Validating;
        self.store.put_txn_state(txn_id, &txn)?;

        // 2. OCC validation: read current versions from the MVCC chain (NOT the
        // legacy TBL_VERSIONS, which is no longer kept in sync). This is the
        // fix for V4: commit validation must read the same source of truth
        // that commit publishes to.
        //
        // OCC semantics: a write conflict exists if and only if the key was
        // modified *after* this txn's snapshot_version. So we flag conflict
        // when `current > version_at_read`. Keys that are unchanged since
        // our snapshot (current ≤ version_at_read, including brand-new keys
        // where current = 0) are fine.
        let write_entries = self.store.iter_write_set(txn_id)?;
        let mut conflict_keys: Vec<String> = Vec::new();
        for (key, entry) in &write_entries {
            let current = self.store
                .read_chain(key)
                .map(|c| c.latest_commit_version())?;
            if current > entry.version_at_read {
                conflict_keys.push(key.clone());
            }
        }
        if !conflict_keys.is_empty() {
            self.store.append_wal(&WalRecord::Aborted {
                txn_id: txn_id.to_string(),
                reason: format!("OCC conflict on keys: {}", conflict_keys.join(",")),
            })?;
            txn.status = TxnStatus::Aborted;
            self.store.put_txn_state(txn_id, &txn)?;
            return Err(TxnError::Conflict(conflict_keys.join(",")).into());
        }

        // 3. ValidateOk — about to upload.
        txn.status = TxnStatus::Committing;
        self.store.append_wal(&WalRecord::ValidateOk {
            txn_id: txn_id.to_string(),
            write_keys: txn.write_keys.clone(),
        })?;

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
        let mut entries: Vec<(String, xtable_storage::VersionEntry)> = Vec::with_capacity(alloc_versions.len());
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
            entries.push((k.clone(), entry));
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

        // 9b. Fire post-commit hooks. After this point observers can
        // reconcile their own indexes (record / schema index in the
        // structured-data-space layer).
        let writes = entries
            .iter()
            .map(|(k, e)| CommitWrite {
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