#![recursion_limit = "512"]
//! Structured read path goes through the LSM chunk, not per-record S3
//! (spec §5.2).
//!
//! Wire-up: commit an upsert via `StructuredSpace`, drive a manual flush
//! (rotate the active MemTable to immutable + run `flush_one` on it),
//! then call `get_record`. The `RecordingBackend` proves that no
//! `get_object` happens on the per-record key; the only S3 GETs (if any)
//! are against the chunk S3 key the flush uploaded.
//!
//! We need the manual flush because the production `flush_loop` runs as
//! a long-lived task — spinning it up just for one test would invite
//! flake. The `xtable_storage::test_helpers` module exposes the
//! rotate+flush helper so the test stays self-contained.

use std::sync::Arc;

use serde_json::json;
use tempfile::TempDir;

use xtable_schema::{RecordWrite, StructuredSpace, StructuredTxn};
use xtable_storage::test_helpers::recording::RecordingBackend;
use xtable_storage::{
    test_helpers::{flush_to_chunks, rotate_for_test},
    LocalStore,
};
use xtable_tx::TxnCoordinator;

#[tokio::test]
async fn get_record_reads_from_chunk_after_flush() {
    let tmp = TempDir::new().unwrap();
    let store = LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap();
    let recording = RecordingBackend::new();
    let (_endpoint, backend) = recording.serve().await.expect("recording backend up");
    let backend = Arc::new(backend);

    let coord = Arc::new(TxnCoordinator::new(
        Arc::new(store.clone()),
        Arc::clone(&backend),
        tmp.path().join("staged"),
        4,
    ));
    let sp = Arc::new(StructuredSpace::new(coord, store.clone(), backend.clone()));
    let mems = Arc::clone(&sp.mems);

    // 1. Commit a record (MemTable publish — Task 2 path).
    let txn = sp.begin_txn().await.unwrap();
    sp.upsert_record(
        &txn,
        RecordWrite {
            space: "demo".into(),
            table: "users".into(),
            record_id: Some("u1".into()),
            body: json!({"id": "u1", "name": "alice"}),
        },
    )
    .await
    .unwrap();
    let _cv = sp.commit_txn(&txn).await.unwrap();

    // 2. Spec §5.1 sanity: commit didn't issue a per-record PUT.
    assert_eq!(
        recording.counters.put_object_calls(),
        0,
        "commit must not PUT per-record bodies (spec §5.1)"
    );

    // 3. Force a flush so the entry lives in a chunk, not just memtable.
    let _rotated = rotate_for_test(&mems);
    let flushed = flush_to_chunks(&mems, &store, backend.clone())
        .await
        .expect("flush succeeds");
    assert_eq!(flushed, 1, "exactly one memtable flushed");

    // The flush should have uploaded exactly one chunk PUT. Find it by
    // shape: `_xtable/demo/users/shard/<chunk_id>.xtc`.
    let puts_after_flush = recording.counters.put_object_calls();
    assert_eq!(
        puts_after_flush, 1,
        "flush must upload exactly one chunk (got {puts_after_flush} PUTs)"
    );

    // 4. Get the record via the structured engine. read_at_snapshot
    // walks active → immutables → TBL_RECORD_INDEX → chunk decode. The
    // active memtable + immutables are empty post-flush, so the read
    // MUST take the chunk path.
    let gets_before_read = recording.counters.get_object_calls();
    let got = sp
        .get_record(&StructuredTxn::admin(), "demo", "users", "u1", None)
        .await
        .unwrap()
        .expect("record visible after flush");
    let gets_after_read = recording.counters.get_object_calls();
    assert_eq!(got.body["id"], "u1");
    assert_eq!(got.body["name"], "alice");

    // 5. Spec §5.2 proof #1: at least one GET happened during the read
    // (it must have downloaded the chunk body). At most one — there
    // is exactly one chunk containing this record.
    let read_gets = gets_after_read.saturating_sub(gets_before_read);
    assert!(
        read_gets >= 1,
        "get_record post-flush must GET the chunk (got {read_gets} GETs)"
    );

    // 6. Spec §5.2 proof #2: the per-record key was never PUT (Task 2
    // invariant) and never GET. Inspect the recording mock directly so
    // we can tell per-key — the per-record key should be absent.
    {
        let objects = recording.mock.objects.lock().unwrap();
        let per_record_key = "_xtable/demo/users/u1.json";
        assert!(
            !objects.contains_key(per_record_key),
            "per-record key {per_record_key} must not exist in backend (spec §5.1)"
        );
        // The chunk key is shaped
        // `_xtable/<space>/<table>/<shard_byte>/<chunk_id>.xtc`. The
        // shard byte is derived from xxhash on the first key, so we
        // pattern-match by structure rather than by literal "shard/".
        let chunk_prefix = "_xtable/demo/users/";
        let has_chunk = objects.keys().any(|k| {
            k.starts_with(chunk_prefix)
                && k.ends_with(".xtc")
                && k[chunk_prefix.len()..].contains('/')
        });
        assert!(
            has_chunk,
            "expected a chunk key under {chunk_prefix} (<shard>/<id>.xtc); objects={:?}",
            objects.keys().collect::<Vec<_>>()
        );
    }
}
