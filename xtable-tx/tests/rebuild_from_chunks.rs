//! Cold rebuild reads chunk objects, not per-record JSONs (spec §5.5).
//!
//! Wire-up:
//! 1. Spin up a `LocalStore` + in-process mock S3.
//! 2. Hand-craft two `ChunkWriter` outputs (one entry each) so the test
//!    runs without dragging in the MemTable/flush machinery.
//! 3. PUT each chunk at its expected s3_key
//!    (`_xtable/<space>/<table>/<shard>/<chunk_id>.xtc`, where the shard
//!    byte is `xxh3_64(key_min) as u8`, matching `flush::compute_shard`).
//! 4. Call `rebuild` and assert the per-record index is populated.

use std::collections::HashMap;

use tempfile::TempDir;

use xtable_backend::BackendClient;
use xtable_core::ObjectKey;
use xtable_storage::{
    chunk::{ChunkEntry, ChunkWriter},
    LocalStore,
};
use xtable_tx::rebuild::rebuild;

/// Match the production s3_key format string from `flush_one`:
/// `_xtable/{space}/{table}/{shard}/{chunk_id}.xtc`. Rebuild itself
/// doesn't care about the shard byte — `list_objects` returns the
/// full key and `rebuild` uses it verbatim for TBL_CHUNK_INDEX — so
/// the test can pick any byte.
fn chunk_s3_key(space: &str, table: &str, shard: u8, chunk_id: &str) -> String {
    format!("_xtable/{}/{}/{}/{}.xtc", space, table, shard, chunk_id)
}

fn make_chunk(
    chunk_id: &str,
    space: &str,
    table: &str,
    record_id: &str,
    value: &[u8],
    commit_version: u64,
) -> Vec<u8> {
    let mut w = ChunkWriter::new(chunk_id.into(), space.into(), table.into());
    w.append(ChunkEntry {
        space: space.into(),
        table: table.into(),
        record_id: record_id.into(),
        value: value.to_vec(),
        commit_version,
        txn_id: format!("txn-{commit_version}"),
        deleted: false,
        content_type: Some("application/json".into()),
        user_meta: vec![],
        schema_version: 1,
        wal_seq: commit_version,
    })
    .expect("append");
    let (file, _header, _footer) = w.finalize().expect("finalize");
    file
}

#[tokio::test]
async fn rebuild_reads_records_from_chunks() {
    let tmp = TempDir::new().unwrap();
    let store = LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap();
    let backend = BackendClient::dummy_for_test_async().await.unwrap();

    // Chunk A: (acme, users, u1) at commit_version 1.
    let chunk_a = make_chunk("01HCKZ8X0A", "acme", "users", "u1", br#"{"id":"u1"}"#, 1);
    let s3_key_a = chunk_s3_key("acme", "users", 0, "01HCKZ8X0A");

    // Chunk B: (demo, tasks, t1) at commit_version 2.
    let chunk_b = make_chunk("01HCKZ8X0B", "demo", "tasks", "t1", br#"{"id":"t1"}"#, 2);
    let s3_key_b = chunk_s3_key("demo", "tasks", 0, "01HCKZ8X0B");

    // Upload both chunks to the mock backend.
    backend
        .put_object(
            &ObjectKey::new(&s3_key_a),
            chunk_a,
            Some("zstd"),
            HashMap::new(),
        )
        .await
        .expect("put chunk_a");
    backend
        .put_object(
            &ObjectKey::new(&s3_key_b),
            chunk_b,
            Some("zstd"),
            HashMap::new(),
        )
        .await
        .expect("put chunk_b");

    // Sanity: both keys end in `.xtc` so rebuild picks them up.
    assert!(s3_key_a.ends_with(".xtc"));
    assert!(s3_key_b.ends_with(".xtc"));

    // Run rebuild.
    let report = rebuild(&store, &backend).await.expect("rebuild");

    // The exact rebuilt-count assertion. Two chunks, one entry each,
    // one record per chunk → versions_rebuilt == 2.
    assert_eq!(
        report.versions_rebuilt, 2,
        "expected 2 records rebuilt from 2 chunks; report={:?}",
        report
    );

    // Per-record index must be populated for both (space, table, record_id)
    // triples (spec §5.5 — the rebuild's job is to restore this index).
    let rec_a = store
        .get_record_index("acme", "users", "u1")
        .expect("get_record_index acme/users/u1");
    let rec_b = store
        .get_record_index("demo", "tasks", "t1")
        .expect("get_record_index demo/tasks/t1");

    assert!(
        rec_a.is_some(),
        "record_index for (acme, users, u1) must be populated"
    );
    assert!(
        rec_b.is_some(),
        "record_index for (demo, tasks, t1) must be populated"
    );

    // The commit_version stored on the record index reflects the chunk
    // entry's commit_version (not the chunk's CV range).
    let rec_a = rec_a.unwrap();
    assert_eq!(rec_a.commit_version, 1);
    assert!(!rec_a.deleted);
    // chunk_id on the record index points at the chunk ULID, which
    // lets `read_at_snapshot` follow TBL_RECORD_INDEX → TBL_CHUNK_INDEX.
    assert_eq!(rec_a.chunk_id, "01HCKZ8X0A");

    let rec_b = rec_b.unwrap();
    assert_eq!(rec_b.commit_version, 2);
    assert_eq!(rec_b.chunk_id, "01HCKZ8X0B");

    // TBL_CHUNK_INDEX must also be populated so `read_at_snapshot`'s
    // `lookup_chunk_for_record` can resolve the chunk metadata.
    let chunk_a_idx = store
        .get_chunk_index("01HCKZ8X0A")
        .expect("get_chunk_index A");
    let chunk_b_idx = store
        .get_chunk_index("01HCKZ8X0B")
        .expect("get_chunk_index B");
    assert!(
        chunk_a_idx.is_some(),
        "TBL_CHUNK_INDEX must contain chunk A"
    );
    assert!(
        chunk_b_idx.is_some(),
        "TBL_CHUNK_INDEX must contain chunk B"
    );
}

#[tokio::test]
async fn rebuild_preserves_historical_versions_for_snapshot_reads() {
    let tmp = TempDir::new().unwrap();
    let store = LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap();
    let backend = BackendClient::dummy_for_test_async().await.unwrap();

    let old_chunk = make_chunk("01HCKZ8X10", "acme", "users", "u1", br#"{"name":"old"}"#, 1);
    let new_chunk = make_chunk("01HCKZ8X11", "acme", "users", "u1", br#"{"name":"new"}"#, 2);
    let old_key = chunk_s3_key("acme", "users", 0, "01HCKZ8X10");
    let new_key = chunk_s3_key("acme", "users", 0, "01HCKZ8X11");
    backend
        .put_object(
            &ObjectKey::new(&old_key),
            old_chunk,
            Some("zstd"),
            HashMap::new(),
        )
        .await
        .unwrap();
    backend
        .put_object(
            &ObjectKey::new(&new_key),
            new_chunk,
            Some("zstd"),
            HashMap::new(),
        )
        .await
        .unwrap();

    let report = rebuild(&store, &backend).await.unwrap();
    assert_eq!(report.versions_rebuilt, 2);
    let chain = store.read_chain("_xtable/acme/users/u1").unwrap();
    assert_eq!(
        chain
            .entries
            .iter()
            .map(|entry| entry.commit_version)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    let (entry, body) = store
        .get_record_version_at_snapshot("acme", "users", "u1", 1)
        .unwrap()
        .unwrap();
    assert_eq!(entry.commit_version, 1);
    assert_eq!(body, br#"{"name":"old"}"#);
    assert_eq!(
        store
            .get_record_index("acme", "users", "u1")
            .unwrap()
            .unwrap()
            .commit_version,
        2
    );
}
