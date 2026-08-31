//! flush_one emits chunk s3_keys with the structured `(space, table)`
//! prefix, not the legacy empty-segment form (spec §5.4).
//!
//! Wire-up: stage two entries in two different `(space, table)` pairs
//! directly into MemTables, rotate each to its own immutable, then flush
//! with `flush_to_chunks`. The `RecordingBackend` lets us inspect every
//! S3 key that was uploaded, so the assertions can be exact.
//!
//! Both expected paths
//!   `_xtable/acme/users/<shard>/<chunk_id>.xtc`
//!   `_xtable/demo/tasks/<shard>/<chunk_id>.xtc`
//! must be present in the backend, and no key may contain `//`.

#![recursion_limit = "256"]

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use bytes::Bytes;
use tempfile::TempDir;

use xtable_backend::recording::RecordingBackend;
use xtable_storage::{
    test_helpers::{flush_to_chunks, rotate_for_test},
    FlushPolicy, LocalStore, MemEntry, MemTable, MemTableSet, RecordValue,
};

fn make_entry(space: &str, table: &str, rid: &str, wal_seq: u64, cv: u64) -> MemEntry {
    MemEntry {
        key: (space.into(), table.into(), rid.into()),
        value: Arc::new(RecordValue {
            // Tiny JSON body; bytes count doesn't matter for path shape.
            bytes: Bytes::from_static(br#"{"id":"x"}"#),
        }),
        // Set commit_version at construction so the entry is visible
        // when flush_one iterates the memtable (it skips INVISIBLE).
        commit_version: Arc::new(AtomicU64::new(cv)),
        txn_id: format!("txn-{wal_seq}"),
        deleted: false,
        content_type: Some("application/json".into()),
        user_meta: vec![],
        schema_version: 1,
        wal_seq,
        size_bytes: 11,
    }
}

#[tokio::test]
async fn flush_chunk_s3_key_uses_real_space_and_table() {
    let tmp = TempDir::new().unwrap();
    let store = LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap();
    let recording = RecordingBackend::new();
    let (_endpoint, backend) = recording.serve().await.expect("recording backend up");
    let backend = Arc::new(backend);

    let mems = Arc::new(MemTableSet::new(MemTable::new(0), FlushPolicy::default()));

    // 1. Stage entry A in (acme, users) into the active memtable and
    //    rotate it out so it becomes its own immutable. flush_one will
    //    encode it into a single chunk whose s3_key uses A's (space, table).
    mems.active
        .read()
        .put_invisible(make_entry("acme", "users", "u1", 1, 1))
        .expect("put A");
    let _ = rotate_for_test(&mems);

    // 2. Stage entry B in (demo, tasks) and rotate it out separately so
    //    it also gets its own chunk with B's (space, table) prefix.
    mems.active
        .read()
        .put_invisible(make_entry("demo", "tasks", "t1", 2, 2))
        .expect("put B");
    let _ = rotate_for_test(&mems);

    // 3. Flush both immutables → 2 chunk PUTs.
    let flushed = flush_to_chunks(&mems, &store, backend.clone())
        .await
        .expect("flush succeeds");
    assert_eq!(flushed, 2, "two memtables flushed → two chunk PUTs");

    let puts_after_flush = recording.counters.put_object_calls();
    assert_eq!(
        puts_after_flush, 2,
        "flush must upload exactly two chunks (got {puts_after_flush})"
    );

    // 4. Inspect chunk s3_keys via the recording backend's mock store.
    let objects = recording.mock.objects.lock().unwrap();
    let chunk_keys: Vec<String> = objects
        .keys()
        .filter(|k| k.ends_with(".xtc"))
        .cloned()
        .collect();

    // One chunk key per (space, table) pair.
    let has_acme = chunk_keys
        .iter()
        .any(|k| k.starts_with("_xtable/acme/users/"));
    let has_demo = chunk_keys
        .iter()
        .any(|k| k.starts_with("_xtable/demo/tasks/"));

    assert!(
        has_acme,
        "expected a chunk key under `_xtable/acme/users/`; got {:?}",
        chunk_keys
    );
    assert!(
        has_demo,
        "expected a chunk key under `_xtable/demo/tasks/`; got {:?}",
        chunk_keys
    );

    // None of the chunk keys may contain `//`, which would mean an empty
    // space or table segment slipped through (the legacy prefix shape).
    let double_slash: Vec<&String> = chunk_keys.iter().filter(|k| k.contains("//")).collect();
    assert!(
        double_slash.is_empty(),
        "no chunk key may contain `//`; got {:?}",
        double_slash
    );

    // No chunk key may have the legacy `_xtable///shard/...` shape,
    // i.e. a `_xtable/` followed by `/` (an empty segment) right away.
    let legacy = chunk_keys.iter().any(|k| {
        k.starts_with("_xtable//")
            || k.starts_with("_xtable///")
            || k.contains("/__xtable/")
    });
    assert!(
        !legacy,
        "found legacy-shape chunk key; got {:?}",
        chunk_keys
    );
}
