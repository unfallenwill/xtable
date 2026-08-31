//! Regression: the tx-coordinator commit path must NOT issue
//! per-record PUTs to the backend; entries go straight to the
//! MemTable (chunk flush uploads them as a chunk later).
//!
//! Spec §5.1: "the per-record PUT loop in commit_inner is removed;
//! the MemTable publish path is the new single writer of structured
//! data to the backend".
//!
//! The proof is at the backend level: a [`RecordingBackend`] wraps the
//! in-process mock S3 server and counts every put_object call. After
//! a successful commit, the counter must be zero.

use std::sync::Arc;

use tempfile::TempDir;
use xtable_backend::recording::RecordingBackend;
use xtable_core::ObjectKey;
use xtable_storage::{LocalStore, MemTableSet};
use xtable_tx::TxnCoordinator;

#[tokio::test]
async fn commit_does_not_put_object_per_record() {
    let tmp = TempDir::new().unwrap();
    let store = Arc::new(LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap());
    let recording = RecordingBackend::new();
    let (_endpoint, backend) = recording.serve().await.expect("recording backend up");
    let backend = Arc::new(backend);

    let memtable_set = MemTableSet::new(
        xtable_storage::MemTable::new(0),
        xtable_storage::FlushPolicy::default(),
    );
    let coord = TxnCoordinator::with_lock_and_memtable(
        store.clone(),
        backend,
        tmp.path().join("staged"),
        4,
        xtable_tx::si_lock_manager::SiLockManager::new(),
        memtable_set.clone(),
    );

    let txn = coord.begin(None).await.unwrap();
    let key = ObjectKey::new("_xtable/acme/users/u1.json");
    coord
        .stage(
            &txn,
            &key,
            b"{\"id\":\"u1\"}".to_vec(),
            Some("application/json".to_string()),
            Default::default(),
            false,
        )
        .await
        .unwrap();
    let outcome = coord.commit(&txn).await.expect("commit should succeed");
    assert!(outcome.commit_version > 0);

    // Spec §5.1 assertion: no per-record PUT happened.
    let puts = recording.counters.put_object_calls();
    assert_eq!(
        puts, 0,
        "commit must not issue any per-record put_object (spec §5.1); saw {} puts",
        puts
    );

    // No get/delete either — the commit path no longer fetches staging
    // copies or deletes them.
    assert_eq!(recording.counters.get_object_calls(), 0,
        "commit must not issue any get_object");
    assert_eq!(recording.counters.delete_object_calls(), 0,
        "commit must not issue any delete_object");

    // The entry IS in the MemTable — that is the new single writer.
    // PR #4: the memtable key strips the `.json` suffix on the record
    // id (matches the schema engine's `parse_record_key`); earlier the
    // coordinator left it in and reads via the structured engine
    // missed.
    let active = memtable_set.active.read();
    let entry = active
        .get_visible(&("acme".into(), "users".into(), "u1".into()), u64::MAX)
        .expect("entry should be in active MemTable");
    assert!(entry.value.bytes.starts_with(b"{\"id\":\"u1\"}"));
}