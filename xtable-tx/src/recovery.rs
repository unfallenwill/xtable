//! Crash recovery on startup.
//!
//! V2 fix: the previous logic treated any `Committing` without `Committed` as
//! "incomplete — compensate-delete everything". This was wrong because the
//! chain may already have the version published (chain append happens before
//! WAL Committed in the original commit path). Compensating-delete would
//! then destroy data that is genuinely committed.
//!
//! Correct semantics:
//! - If WAL ends with `Committed` / `CommitResult` → already terminal. Skip.
//! - If WAL ends with `Committing` and the chain[k] ALREADY has our txn's
//!   version entry → treat as committed (chain won the race with WAL).
//!   Write the missing WAL `Committed` and `CommitResult`. Do NOT delete.
//! - If WAL ends with `Committing` and the chain does NOT have our txn's
//!   entry → genuinely incomplete; mark aborted. Chunk-only refactor
//!   (fixup after final review) removed the V3 compensating-delete loop:
//!   structured records ride chunks, so there is no per-record S3 PUT to
//!   delete. The chunk pipeline reconciles on the next flush.

use std::collections::HashMap;
use std::sync::OnceLock;
use tracing::{info, warn};

use xtable_core::headers::TxnStatus;
use xtable_core::XtableResult;
use xtable_storage::{LocalStore, WalRecord};
use xtable_telemetry::metrics::Metrics;
use xtable_telemetry::timed::Timed;
use xtable_telemetry::KeyValue;

use crate::error::TxnError;

/// Lazily-initialised `Metrics` bound to the global OTel meter.
fn metrics() -> &'static Metrics {
    static METRICS: OnceLock<Metrics> = OnceLock::new();
    METRICS.get_or_init(Metrics::default)
}

/// Outcome counts reported after a recovery sweep.
#[derive(Debug, Default, Clone, Copy)]
pub struct RecoveryReport {
    pub already_committed: usize,
    pub chain_won_wal_race: usize, // V2 fix: chain published, WAL missing → backfill WAL
    pub partial_uploads_aborted: usize,
}

pub async fn recover(store: &LocalStore) -> XtableResult<RecoveryReport> {
    let _timed = Timed::new(
        &metrics().recovery_replay_duration,
        vec![KeyValue::new("op", "recover")],
    );
    recover_inner(store).await
}

#[tracing::instrument(level = "info", name = "tx.recover", skip_all, err)]
async fn recover_inner(store: &LocalStore) -> XtableResult<RecoveryReport> {
    let log = store.iter_wal()?;
    let mut last_status: HashMap<String, TxnStatus> = HashMap::new();
    let mut last_uploaded: HashMap<String, Vec<String>> = HashMap::new();

    for (_seq, rec) in &log {
        // Per-txn variants carry a txn_id; `MemtableFlushed` is a global
        // record and is skipped (its presence already implies the WAL
        // truncator has run for everything before `up_to_seq`).
        let Some(txn_id) = rec.txn_id().map(str::to_string) else {
            continue;
        };
        match rec {
            WalRecord::Begin { .. } => {
                last_status
                    .entry(txn_id.clone())
                    .or_insert(TxnStatus::Active);
            }
            WalRecord::Stage { .. } => {
                last_status
                    .entry(txn_id.clone())
                    .or_insert(TxnStatus::Active);
            }
            WalRecord::Committing { upload_keys, .. } => {
                last_status.insert(txn_id.clone(), TxnStatus::Committing);
                last_uploaded.insert(txn_id.clone(), upload_keys.clone());
            }
            WalRecord::Committed { .. } => {
                last_status.insert(txn_id.clone(), TxnStatus::Committed);
            }
            WalRecord::CommitResult { .. } => {
                last_status.insert(txn_id.clone(), TxnStatus::Committed);
            }
            WalRecord::Aborted { .. } => {
                last_status.insert(txn_id.clone(), TxnStatus::Aborted);
            }
            WalRecord::MemtableFlushed { .. } => {
                // Global; no per-txn status update.
            }
        }
    }

    let mut report = RecoveryReport::default();

    let txn_ids: Vec<String> = last_status.keys().cloned().collect();
    for txn_id in txn_ids {
        let status = last_status[&txn_id];
        let stored = store.get_txn_state(&txn_id)?;
        if let Some(rec) = stored {
            if rec.status == TxnStatus::Committed || rec.status == TxnStatus::Aborted {
                if rec.status == TxnStatus::Committed {
                    report.already_committed += 1;
                }
                continue;
            }
            // Non-terminal. Decide based on chain state.
            match status {
                TxnStatus::Active => {
                    // No backend uploads happened. Safe abort.
                    abort_txn_no_uploads(store, &txn_id)?;
                }
                TxnStatus::Committing => {
                    // V2 fix: check the chain BEFORE compensating. If the chain
                    // already has an entry with our alloc_version for the
                    // uploaded keys, the commit was effectively successful —
                    // backfill WAL Committed, do NOT delete backend objects.
                    let uploaded = last_uploaded.get(&txn_id).cloned().unwrap_or_default();
                    let alloc_vers = rec.alloc_versions.clone();
                    let mut chain_published_count = 0usize;
                    let mut pending_uploads: Vec<String> = Vec::new();
                    for (key, alloc_v) in &alloc_vers {
                        if !uploaded.contains(key) {
                            continue;
                        }
                        let chain = store.read_chain(key).map(|c| c.latest_commit_version())?;
                        if chain == *alloc_v {
                            chain_published_count += 1;
                        } else {
                            pending_uploads.push(key.clone());
                        }
                    }
                    if !pending_uploads.is_empty() {
                        // Chunk-only world (fixup after final review):
                        // there are no per-record S3 PUTs to compensate
                        // anymore — structured records ride chunks, so a
                        // backend.delete_object here would target a key
                        // that was never written. We keep the
                        // abort-and-counter path as a defensive marker
                        // for any future per-record write that might be
                        // reintroduced; the V2/V3 delete loop is removed
                        // because it would lie to readers and is
                        // operationally a no-op (S3/MockS3 treat delete
                        // of missing keys as no-op).
                        warn!(
                            txn = %txn_id,
                            pending = ?pending_uploads,
                            "partial CommitTxn with chain-not-published; aborting without compensation"
                        );
                        abort_txn_no_uploads(store, &txn_id)?;
                        report.partial_uploads_aborted += 1;
                    } else if chain_published_count > 0 {
                        // All uploaded keys are in the chain at our alloc_version.
                        // V2 fix: this is a successful commit; backfill WAL only.
                        info!(
                            txn = %txn_id,
                            keys = chain_published_count,
                            "CommitTxn completed before crash (chain won WAL race); backfilling WAL"
                        );
                        let commit_version = alloc_vers
                            .iter()
                            .map(|(_, v)| *v)
                            .max()
                            .unwrap_or(rec.snapshot_version);
                        store.append_wal(&WalRecord::Committed {
                            txn_id: txn_id.clone(),
                            commit_version,
                        })?;
                        store.append_wal(&WalRecord::CommitResult {
                            txn_id: txn_id.clone(),
                            commit_version,
                            success: true,
                        })?;
                        let mut new_state = rec.clone();
                        new_state.status = TxnStatus::Committed;
                        store.put_txn_state(&txn_id, &new_state)?;
                        report.chain_won_wal_race += 1;
                    } else {
                        // No uploads, no chain entries — safe abort.
                        abort_txn_no_uploads(store, &txn_id)?;
                    }
                }
                _ => {}
            }
        }
    }

    info!(
        already_committed = report.already_committed,
        chain_won_wal_race = report.chain_won_wal_race,
        partial_uploads_aborted = report.partial_uploads_aborted,
        "WAL recovery done"
    );

    Ok(report)
}

fn abort_txn_no_uploads(store: &LocalStore, txn_id: &str) -> XtableResult<()> {
    use chrono::Utc;
    use xtable_storage::TxnStateRecord;

    let writes = store.iter_write_set(txn_id)?;
    for (_k, entry) in writes {
        if let Some(handle) = &entry.body_handle {
            if let Ok(Some(rec)) = store.get_blob(handle) {
                let _ = std::fs::remove_file(&rec.path);
            }
            let _ = store.delete_blob(handle);
        }
        let _ = store.delete_write_entry(txn_id, &entry.backend_key);
    }
    store.append_wal(&WalRecord::Aborted {
        txn_id: txn_id.to_string(),
        reason: "crash recovery".into(),
    })?;
    // V13 fix: release the snapshot pin so it doesn't accumulate.
    let _ = store.get_txn_state(txn_id).ok().flatten().map(|s| {
        let _ = store.unregister_snapshot(s.snapshot_version);
    });
    if let Some(mut r) = store.get_txn_state(txn_id)? {
        r.status = TxnStatus::Aborted;
        r.last_heartbeat_ms = Utc::now().timestamp_millis();
        store.put_txn_state(txn_id, &r)?;
    } else {
        let mut r = TxnStateRecord::new_active(0, None, Utc::now().timestamp_millis());
        r.status = TxnStatus::Aborted;
        store.put_txn_state(txn_id, &r)?;
    }
    Ok(())
}

impl From<xtable_core::XtableError> for TxnError {
    fn from(_: xtable_core::XtableError) -> Self {
        TxnError::Storage("xerror".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn make() -> (LocalStore, TempDir) {
        let tmp = TempDir::new().unwrap();
        let store = LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap();
        (store, tmp)
    }

    #[tokio::test]
    async fn recover_no_wal_is_noop() {
        let (store, _tmp) = make().await;
        let r = recover(&store).await.unwrap();
        assert_eq!(r.already_committed, 0);
        assert_eq!(r.chain_won_wal_race, 0);
        assert_eq!(r.partial_uploads_aborted, 0);
    }

    #[tokio::test]
    async fn recover_marks_in_progress_txn_as_aborted() {
        let (store, _tmp) = make().await;
        let txn_id = "T1";
        store
            .append_wal(&WalRecord::Begin {
                txn_id: txn_id.into(),
                snapshot_version: 0,
                idempotency_key: None,
            })
            .unwrap();
        let mut r = xtable_storage::TxnStateRecord::new_active(0, None, 0);
        r.status = TxnStatus::Active;
        store.put_txn_state(txn_id, &r).unwrap();
        let rep = recover(&store).await.unwrap();
        assert_eq!(rep.already_committed, 0);
        let post = store.get_txn_state(txn_id).unwrap().unwrap();
        assert_eq!(post.status, TxnStatus::Aborted);
    }
}
