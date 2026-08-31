//! Schema writes go through the MemTable and chunk pipeline, not per-schema
//! S3 PUTs (spec §5.1).
//!
//! Wire-up: commit a `register_schema` and `bind_table_schema` via
//! `StructuredSpace`, drive a manual flush (rotate the active MemTable
//! to immutable + run `flush_to_chunks`), then read the schema body
//! back through `read_at_snapshot` against the schema sub-namespace
//! `(space, "_schema", "{name}/v{N}.json")`. The `RecordingBackend`
//! proves that no per-schema PUT was issued.

#![recursion_limit = "512"]

use std::sync::Arc;

use serde_json::json;
use tempfile::TempDir;

use xtable_schema::StructuredSpace;
use xtable_storage::test_helpers::recording::RecordingBackend;
use xtable_storage::{
    read::read_at_snapshot,
    test_helpers::{flush_to_chunks, rotate_for_test},
    LocalStore,
};
use xtable_tx::TxnCoordinator;

#[tokio::test]
async fn register_schema_lands_in_chunk() {
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

    // 1. Register a schema and bind a table to it in one txn.
    let txn = sp.begin_txn().await.unwrap();
    let v1 = sp
        .register_schema(
            &txn,
            "demo",
            "task",
            json!({
                "type": "object",
                "properties": {"title": {"type": "string"}}
            }),
        )
        .await
        .unwrap();
    assert_eq!(v1, 1, "first registration is version 1");
    sp.bind_table_schema(&txn, "demo", "tasks", json!({"type": "object"}))
        .await
        .unwrap();
    let _cv = sp.commit_txn(&txn).await.unwrap();

    // 2. Spec §5.1 sanity: commit did NOT issue per-schema PUTs.
    // Task 2 removed per-record PUTs and the schema path follows the
    // same pattern.
    assert_eq!(
        recording.counters.put_object_calls(),
        0,
        "commit must not PUT per-schema bodies (spec §5.1)"
    );

    // 3. Force a flush so the entries live in a chunk, not just the
    // memtable.
    let _rotated = rotate_for_test(&mems);
    let flushed = flush_to_chunks(&mems, &store, backend.clone())
        .await
        .expect("flush succeeds");
    assert_eq!(flushed, 1, "exactly one memtable flushed");

    let puts_after_flush = recording.counters.put_object_calls();
    assert_eq!(
        puts_after_flush, 1,
        "flush must upload exactly one chunk (got {puts_after_flush} PUTs)"
    );

    // 4. Read the schema back via read_at_snapshot using the schema
    // sub-namespace key. The MemTable key format produced by
    // `parse_record_key` in the txn coordinator is
    // `(space, "_schema", "{name}/v{N}.json")` — that's what the
    // MemTable (and therefore the chunk index) uses.
    let res = read_at_snapshot(
        &mems,
        &store,
        &backend,
        "demo",
        "_schema",
        "task/v1.json",
        u64::MAX,
    )
    .await
    .expect("read_at_snapshot succeeds")
    .expect("schema body is present after flush");

    // 5. Body round-trips intact.
    let body: serde_json::Value =
        serde_json::from_slice(&res.body).expect("schema body parses as JSON");
    assert_eq!(body["type"], "object");
    assert_eq!(body["properties"]["title"]["type"], "string");

    // 6. Spec §5.1 proof: no per-schema key lives in the backend. The
    // only PUT is the chunk under `_xtable/demo/_schema/<shard>/<id>.xtc`.
    {
        let objects = recording.mock.objects.lock().unwrap();
        let per_schema_key = "_xtable/demo/_schema/task/v1.json";
        assert!(
            !objects.contains_key(per_schema_key),
            "per-schema key {per_schema_key} must not exist in backend (spec §5.1)"
        );
        let chunk_prefix = "_xtable/demo/_schema/";
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

#[tokio::test]
async fn bind_table_schema_lands_in_chunk() {
    // Same shape as the registration test, but for the table-binding
    // alias. The alias name `_table::<table>` is a regular schema
    // namespace entry from the chunk pipeline's perspective.
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

    let txn = sp.begin_txn().await.unwrap();
    sp.bind_table_schema(
        &txn,
        "demo",
        "tasks",
        json!({
            "type": "object",
            "required": ["title"],
            "properties": {"title": {"type": "string"}}
        }),
    )
    .await
    .unwrap();
    let _cv = sp.commit_txn(&txn).await.unwrap();

    assert_eq!(
        recording.counters.put_object_calls(),
        0,
        "commit must not PUT per-schema bodies (spec §5.1)"
    );

    let _rotated = rotate_for_test(&mems);
    let _flushed = flush_to_chunks(&mems, &store, backend.clone())
        .await
        .expect("flush succeeds");

    // The alias name `_table::tasks` becomes part of the record_id
    // inside the MemTable: `_table::tasks/v1.json`.
    let res = read_at_snapshot(
        &mems,
        &store,
        &backend,
        "demo",
        "_schema",
        "_table::tasks/v1.json",
        u64::MAX,
    )
    .await
    .expect("read_at_snapshot succeeds")
    .expect("table-binding body is present after flush");

    let body: serde_json::Value =
        serde_json::from_slice(&res.body).expect("binding body parses as JSON");
    assert_eq!(body["type"], "object");
    assert_eq!(body["required"][0], "title");
}
