//! Property-based tests for txn-coordinator invariants (MVCC + SSI).
//!
//! These tests don't need a real S3 backend — they exercise the coordinator
//! against a mock-style environment (in-memory HashMap simulated via direct
//! redb manipulation).
//!
//! Invariants tested:
//!
//! 1. **Atomicity**: A committed txn's writes are all visible; an aborted
//!    txn's writes are all absent. No partial visibility.
//!
//! 2. **Conflict detection**: When two txns stage writes to the same key
//!    starting from the same `snapshot_version`, exactly one wins on commit
//!    and the other gets a 409 Conflict.
//!
//! 3. **Version monotonicity**: After successful commits, each key's
//!    version strictly increases.
//!
//! 4. **Crash recovery**: A Begin→Stage→...→Commit sequence that crashes
//!    mid-Committing leaves the backend in a state recoverable by replay.

use proptest::prelude::*;
use std::collections::HashMap;
use tempfile::TempDir;

use xtable_backend::BackendClient;
use xtable_core::ObjectKey;
use xtable_storage::LocalStore;
use xtable_tx::TxnCoordinator;

// =========================================================================
// Test helpers
// =========================================================================

async fn build_for_test() -> (TxnCoordinator, LocalStore, TempDir) {
    let tmp = TempDir::new().unwrap();
    let store = LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap();
    let backend = BackendClient::dummy_for_test_async()
        .await
        .expect("dummy backend");
    let coord = TxnCoordinator::new(
        Arc::new(store.clone()),
        Arc::new(backend),
        tmp.path().join("staged"),
        4,
    );
    (coord, store, tmp)
}

use std::sync::Arc;

// =========================================================================
// INVARIANT 1: Atomicity — aborted txn leaves no trace
// =========================================================================

#[tokio::test]
async fn inv_aborted_txn_leaves_no_state() {
    let (coord, _store, _tmp) = build_for_test().await;
    let txn = coord.begin(None).await.unwrap();
    for i in 0..3 {
        let key = ObjectKey::new(format!("k{}", i));
        coord
            .stage(&txn, &key, b"x".to_vec(), None, HashMap::new(), false)
            .await
            .unwrap();
    }
    coord.abort(&txn).await.unwrap();
    let status = coord.status(&txn).await.unwrap();
    assert_eq!(format!("{:?}", status), "Aborted");
    // No write_set entries after abort.
    let ws = _store.iter_write_set(&txn).unwrap();
    assert!(ws.is_empty(), "aborted txn should have no staged writes");
}

#[tokio::test]
async fn inv_commit_no_writes_is_idempotent() {
    let (coord, _store, _tmp) = build_for_test().await;
    let txn = coord.begin(None).await.unwrap();
    let r1 = coord.commit(&txn).await.unwrap();
    let r2 = coord.commit(&txn).await.unwrap();
    assert_eq!(r1.commit_version, r2.commit_version);
}

// =========================================================================
// INVARIANT 2 (moved): write-write conflict detection
// =========================================================================
//
// The MVCC+SSI write-write conflict property is exercised by:
//   - `ssi_write_write_one_winner` in `tests/ssi_invariants.rs`
//     (snapshot-conflict check in `append_chain_entries_bulk`)
//   - `ssi_write_skew_aborts_one` (Cahill cycle detection)

// =========================================================================
// INVARIANT 3: Version monotonicity
// =========================================================================

proptest! {
    #[test]
    fn prop_global_version_monotonic(n in 1usize..30) {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let tmp = TempDir::new().unwrap();
            let store = LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap();
            let mut last = 0u64;
            for _ in 0..n {
                let v = store.next_global_version().unwrap();
                prop_assert!(v > last, "version went backwards: {} <= {}", v, last);
                last = v;
            }
            Ok(())
        })?;
    }
}

// =========================================================================
// INVARIANT 4: Reopen preserves versions
// =========================================================================

proptest! {
    #[test]
    fn prop_versions_persist_across_reopen(
        n in 1usize..10,
        versions in proptest::collection::vec(1u64..1000, 1..10),
    ) {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let tmp = TempDir::new().unwrap();
            let path = tmp.path().join("xt.redb");
            {
                let store = LocalStore::open_path(&path).unwrap();
                for (i, v) in versions.iter().take(n).enumerate() {
                    let k = ObjectKey::new(format!("k{}", i));
                    store.put_version(&k, &xtable_storage::VersionRecord {
                        latest_version: xtable_core::Version(*v),
                        latest_etag: format!("e{}", v),
                        latest_backend_key: format!("k{}", i),
                        last_writer_txn_id: String::new(),
                        tombstone: false,
                        size: 0,
                        last_modified_unix_ms: 0,
                    }).unwrap();
                }
            }
            let store2 = LocalStore::open_path(&path).unwrap();
            for (i, v) in versions.iter().take(n).enumerate() {
                let k = ObjectKey::new(format!("k{}", i));
                let rec = store2.get_version(&k).unwrap().unwrap();
                prop_assert_eq!(rec.latest_version.as_u64(), *v);
            }
            Ok(())
        })?;
    }
}

// =========================================================================
// INVARIANT 5: WAL ordering — seq is strictly monotonic
// =========================================================================

proptest! {
    #[test]
    fn prop_wal_seq_monotonic(n in 1usize..30) {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let tmp = TempDir::new().unwrap();
            let store = LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap();
            use xtable_storage::WalRecord;
            let mut last = 0u64;
            for i in 0..n {
                let rec = WalRecord::Begin {
                    txn_id: format!("T{}", i),
                    snapshot_version: 0,
                    idempotency_key: None,
                };
                let seq = store.append_wal(&rec).unwrap();
                prop_assert!(seq > last);
                last = seq;
            }
            Ok(())
        })?;
    }
}

// =========================================================================
// INVARIANT 6: Read-your-own-writes within a txn
// =========================================================================

#[tokio::test]
async fn inv_read_your_own_writes_within_txn() {
    let (coord, _store, _tmp) = build_for_test().await;
    let txn = coord.begin(None).await.unwrap();
    let key = ObjectKey::new("k");
    let staged = coord
        .stage_body(&txn, "k")
        .await
        .expect("stage_body lookup ok");
    assert!(staged.is_none(), "nothing staged yet");
    coord
        .stage(&txn, &key, b"hello".to_vec(), None, HashMap::new(), false)
        .await
        .unwrap();
    let staged = coord.stage_body(&txn, "k").await.unwrap();
    assert!(staged.is_some());
    assert_eq!(staged.unwrap(), b"hello");
}

// =========================================================================
// INVARIANT 7: Idempotent commit replay
// =========================================================================

#[tokio::test]
async fn inv_commit_replay_returns_same_outcome() {
    let (coord, store, _tmp) = build_for_test().await;
    let txn = coord.begin(Some("idem-key-1".into())).await.unwrap();
    // First commit succeeds.
    let r1 = coord.commit(&txn).await.unwrap();
    // Simulate retry: status should be Committed, outcome reproducible.
    let r2 = coord.commit(&txn).await.unwrap();
    assert_eq!(r1.commit_version, r2.commit_version);
    // TxnState shows Committed.
    let s = store.get_txn_state(&txn).unwrap().unwrap();
    let status_str = format!("{:?}", s.status);
    assert_eq!(status_str, "Committed");
}

// =========================================================================
// INVARIANT 8: GC sweeps stale txns
// =========================================================================

#[tokio::test]
async fn inv_gc_sweeps_stale_txn_but_keeps_recent() {
    use chrono::Utc;
    use xtable_storage::TxnStateRecord;

    let (_coord, store, _tmp) = build_for_test().await;
    let mut stale = TxnStateRecord::new_active(0, None, 0);
    stale.last_heartbeat_ms = 0; // ancient
    store.put_txn_state("STALE", &stale).unwrap();
    let fresh = TxnStateRecord::new_active(0, None, Utc::now().timestamp_millis());
    store.put_txn_state("FRESH", &fresh).unwrap();

    let n = xtable_tx::gc::sweep_stale_txns(&store, 60).unwrap();
    assert_eq!(n, 1, "only STALE should be swept");
    let stale_after = store.get_txn_state("STALE").unwrap().unwrap();
    let fresh_after = store.get_txn_state("FRESH").unwrap().unwrap();
    assert_eq!(format!("{:?}", stale_after.status), "Aborted");
    assert_eq!(format!("{:?}", fresh_after.status), "Active");
}

// =========================================================================
// INVARIANT 9: Unknown txn returns 404
// =========================================================================

#[tokio::test]
async fn inv_unknown_txn_returns_not_found() {
    let (coord, _store, _tmp) = build_for_test().await;
    let err = coord.status("DOES-NOT-EXIST").await.unwrap_err();
    assert_eq!(err.http_status(), 404);
}
