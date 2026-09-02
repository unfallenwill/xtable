//! Periodic GC: abort stale active transactions, drop stale staged blobs,
//! and prune MVCC version chains below the minimum active snapshot.

use std::sync::OnceLock;

use chrono::Utc;
use redb::ReadableTable;
use xtable_core::headers::TxnStatus;
use xtable_core::XtableError;
use xtable_storage::LocalStore;
use xtable_telemetry::metrics::Metrics;
use xtable_telemetry::timed::Timed;
use xtable_telemetry::KeyValue;

/// Lazily-initialised `Metrics` bound to the global OTel meter.
fn metrics() -> &'static Metrics {
    static METRICS: OnceLock<Metrics> = OnceLock::new();
    METRICS.get_or_init(Metrics::default)
}

/// Sweep all active transactions older than `timeout_secs`. Returns the
/// number aborted.
pub fn sweep_stale_txns(store: &LocalStore, timeout_secs: i64) -> Result<usize, XtableError> {
    let now_ms = Utc::now().timestamp_millis();
    let cutoff_ms = now_ms - timeout_secs * 1000;

    let stale: Vec<String> = store.with_read(|txn| {
        use xtable_storage::cf::TBL_TXN_STATE;
        let tbl = txn
            .open_table(TBL_TXN_STATE)
            .map_err(|e| XtableError::Storage(e.to_string()))?;
        let mut out = Vec::new();
        for entry in tbl
            .iter()
            .map_err(|e| XtableError::Storage(e.to_string()))?
        {
            let (k, v) = entry.map_err(|e| XtableError::Storage(e.to_string()))?;
            let rec: xtable_storage::TxnStateRecord =
                bincode::deserialize(v.value()).map_err(XtableError::from)?;
            if rec.status == TxnStatus::Active && rec.last_heartbeat_ms < cutoff_ms {
                out.push(k.value().to_string());
            }
        }
        Ok(out)
    })?;

    let mut aborted = 0;
    for txn_id in stale {
        if abort_txn_local(store, &txn_id)? {
            aborted += 1;
        }
    }
    Ok(aborted)
}

fn abort_txn_local(store: &LocalStore, txn_id: &str) -> Result<bool, XtableError> {
    use xtable_storage::WalRecord;
    let Some(state) = store.get_txn_state(txn_id)? else {
        return Ok(false);
    };
    // Claim the Active state before touching the write set. A commit or a
    // second stale-txn sweeper that wins this CAS owns the transaction now;
    // GC must never clean its writes or overwrite its terminal state.
    if state.status != TxnStatus::Active
        || !store.compare_and_set_txn_status(txn_id, TxnStatus::Active, TxnStatus::Aborted)?
    {
        return Ok(false);
    }
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
        reason: "GC: stale txn timeout".into(),
    })?;
    // V13 fix: release the snapshot pin so GC sweeping doesn't leak pins.
    let _ = store.unregister_snapshot(state.snapshot_version);
    Ok(true)
}

/// Run MVCC chain GC. Returns (chains_visited, entries_removed).
/// Implements invariant I8.
pub fn gc_version_chains(store: &LocalStore) -> Result<(usize, usize), XtableError> {
    // The active-snapshot lookup and chain rewrite must share one redb write
    // transaction. Reading the minimum first leaves a window in which a new
    // reader can register and still lose its visibility anchor.
    store.gc_chains_at_active_snapshot()
}

/// Run a single combined sweep: stale txns + chain GC.
pub fn sweep_all(store: &LocalStore, txn_timeout_secs: i64) -> Result<CombinedSweep, XtableError> {
    let m = metrics();
    let _timed = Timed::new(&m.gc_sweep_duration, vec![KeyValue::new("op", "sweep_all")]);
    let aborted = sweep_stale_txns(store, txn_timeout_secs)?;
    let (chains_pruned, entries_removed) = gc_version_chains(store)?;
    m.gc_entries_removed.add(entries_removed as u64, &[]);
    Ok(CombinedSweep {
        aborted_txns: aborted,
        chains_pruned,
        entries_removed,
    })
}

#[derive(Debug, Clone, Copy)]
pub struct CombinedSweep {
    pub aborted_txns: usize,
    pub chains_pruned: usize,
    pub entries_removed: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use xtable_storage::{TxnStateRecord, VersionEntry};

    #[test]
    fn sweep_aborts_stale_active_txn() {
        let tmp = TempDir::new().unwrap();
        let store = LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap();
        let mut r = TxnStateRecord::new_active(0, None, 0);
        r.last_heartbeat_ms = 0;
        store.put_txn_state("T1", &r).unwrap();
        let n = sweep_stale_txns(&store, 60).unwrap();
        assert_eq!(n, 1);
        let post = store.get_txn_state("T1").unwrap().unwrap();
        assert_eq!(post.status, TxnStatus::Aborted);
    }

    #[test]
    fn sweep_skips_recent_active_txn() {
        let tmp = TempDir::new().unwrap();
        let store = LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap();
        let r = TxnStateRecord::new_active(0, None, Utc::now().timestamp_millis());
        store.put_txn_state("T1", &r).unwrap();
        let n = sweep_stale_txns(&store, 60).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn gc_version_chains_no_active_snapshots_keeps_newest() {
        // With no active snapshots, gc_chains should drop all but newest.
        let tmp = TempDir::new().unwrap();
        let store = LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap();
        store
            .append_chain_entry(
                "k",
                &VersionEntry::new(1, "e1".into(), "k".into(), "T1".into(), 10),
            )
            .unwrap();
        store
            .append_chain_entry(
                "k",
                &VersionEntry::new(2, "e2".into(), "k".into(), "T2".into(), 20),
            )
            .unwrap();
        store
            .append_chain_entry(
                "k",
                &VersionEntry::new(3, "e3".into(), "k".into(), "T3".into(), 30),
            )
            .unwrap();
        let (visited, removed) = gc_version_chains(&store).unwrap();
        assert_eq!(visited, 1);
        assert_eq!(removed, 2);
        let chain = store.read_chain("k").unwrap();
        assert_eq!(chain.entries.len(), 1);
        assert_eq!(chain.entries[0].commit_version, 3);
    }

    #[test]
    fn gc_version_chains_with_active_snapshot_pins_old() {
        // If a snapshot is registered at version 2, GC must keep entries
        // with commit_version >= 2 even though no readers are "actively using it".
        let tmp = TempDir::new().unwrap();
        let store = LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap();
        store
            .append_chain_entry(
                "k",
                &VersionEntry::new(1, "e1".into(), "k".into(), "T1".into(), 10),
            )
            .unwrap();
        store
            .append_chain_entry(
                "k",
                &VersionEntry::new(2, "e2".into(), "k".into(), "T2".into(), 20),
            )
            .unwrap();
        store
            .append_chain_entry(
                "k",
                &VersionEntry::new(3, "e3".into(), "k".into(), "T3".into(), 30),
            )
            .unwrap();
        store.register_snapshot(2).unwrap();
        let (_, removed) = gc_version_chains(&store).unwrap();
        // min_active = 2; only entry with commit_version < 2 (=1) drops.
        assert_eq!(removed, 1);
        let chain = store.read_chain("k").unwrap();
        assert_eq!(chain.entries.len(), 2);
        assert_eq!(chain.entries[0].commit_version, 2);
        assert_eq!(chain.entries[1].commit_version, 3);
    }
}
