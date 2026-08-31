# Chunk-only structured storage

**Date**: 2026-08-31
**Status**: Draft (pending user review)
**Scope**: Structured-data-space layer (xtable-schema, xtable-tx, xtable-storage). BackendClient and keymap unchanged at the trait level.

## 1. Problem

The structured-data-space layer currently stores each record as a
single S3 JSON object at `_xtable/{space}/{table}/{record_id}.json`,
AND publishes a copy to the LSM-tree `MemTable` which gets packed
into an LSM chunk object at `_xtable///shard/{chunk_id}.xtc`.
This is a double-write: every commit pays the S3 PUT cost twice
(once for the per-record JSON, once inside the chunk flush) and
doubles storage cost.

The chunk infrastructure (`xtable-storage/src/chunk.rs`,
`flush.rs`, `read.rs::read_at_snapshot`) is real, working, and
complete. But the structured layer's read path
(`xtable-schema/src/engine.rs::get_record`) calls
`backend.get_object(per_record_key)` and never touches the chunk.
The read-side scaffolding for chunks exists but is never reached.

The user wants the chunk to be the single source of truth and the
per-record JSON path eliminated.

## 2. Goals

1. **Chunk is the only source of truth** for both records and schemas.
2. **No per-record JSON object** is written by the structured layer.
3. **No per-record JSON object** is read by the structured layer.
4. **Cold rebuild** decodes chunks instead of listing per-record
   objects and reading their metadata.
5. **All existing chunk infrastructure** (encode, flush, index,
   decode, bloom) is preserved and connected to the structured
   layer's read and write paths.
6. **Backward compatibility with existing per-record JSON objects
   in the bucket is explicitly NOT a goal.** A migration tool may
   be a follow-up but is not part of this spec.

## 3. Non-goals

- Multi-tenant / multi-bucket keymap (still `IdentityKeyMap`).
- Range GET for chunk reads (PR #4+, mentioned in the existing
  `read.rs` doc-comment as future work; out of scope).
- New chunk format version. Reuse the existing one.
- Wire compression into the chunk encoder (already a no-op per the
  `flush.rs` comments).

## 4. Current state (baseline)

### Write path (today)

```
client.upsert_record(record)
  -> StructuredSpace::upsert_record
    -> self.txn.stage(txn_id, &backend_key, body, ct, meta, false)
       [tx-coordinator stores in TBL_WRITE_SET + MemTable with
        RecordKey = (space="", table="", key)]
  -> StructuredSpace::pending.record(...)  [in-memory pending map]

[client later commits txn]
  tx-coordinator.commit:
    for each write_entry:
      backend.put_object(&backend_key, body, ct, meta)
        [promotes staged body to final S3 key]
      backend.put_object_metadata(...)     [??]
    append TBL_CHUNK_INDEX row per write  [via put_versions_bulk]
    publish each entry to MemTable
      [MemTable key = ("", "", backend_key)]
  flush_loop:
    encode entries -> ChunkWriter.finalize()
    upload to S3 key `_xtable///shard/{chunk_id}.xtc`
    insert TBL_CHUNK_INDEX row pointing at the chunk
```

There are TWO S3 writes per commit:
- One per-record PUT at `_xtable/{space}/{table}/{record_id}.json`
- One part of the chunk PUT (eventually, asynchronously)

### Read path (today)

```
client.get_record(space, table, record_id)
  -> StructuredSpace::get_record -> read_at_snapshot
    1. active MemTable.get_visible(key, snapshot)
    2. flushing MemTables (newest first)
    3. TBL_RECORD_INDEX via store.get_record_index
       [but `backend_key` here is the per-record JSON path, not
        a chunk key — `lookup_chunk_for_record` rsplit('/')
        strip_suffix(".xtc") will fail to parse a JSON path
        correctly, so this branch is effectively dead code]
    4. lookup_chunk_for_record(store, &idx)
       [extracts chunk_id from `idx.backend_key`; the comment
        on line 124 of read.rs admits this is "v1 we use
        `backend_key` as the chunk key (until record index is
        migrated to include chunk_id explicitly in PR #5+)"]
    5. S3 GET chunk body
    6. decompress + decode body entries
    7. find matching entry
```

`engine.rs::get_record` actually does NOT call `read_at_snapshot`;
it calls `backend.get_object(record_key)` directly:

```rust
pub async fn get_record(...) -> ... {
    let r = self.backend.get_record(...).await?;
    // returns GetObjectResult { body, etag, size, user_metadata }
}
```

And `BackendClient::get_record` does:

```rust
self.inner.client.get_object()
    .bucket(&bucket).key(&backend_key)
    .send().await
```

So the chunk-read path (`read_at_snapshot`) is unused by the
structured layer. It exists in `xtable-storage` but is dead code
from the structured layer's perspective.

### Cold rebuild (today)

```rust
let objects = backend.list_objects().await?;   // ListObjectsV2
for lo in objects {
    let meta = backend.head_object(&lo.key).await?...;
    let backend_v = meta.get("x-amz-meta-xtable-version")
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    if backend_v == 0 { continue; }  // stray, skip
    // build per_key index from version
}
```

This assumes per-record JSON objects with `x-xtable-version`
metadata. With chunk-only storage, this is meaningless.

## 5. Target design

### 5.1 Write path (chunk-only)

```
client.upsert_record(record)
  -> StructuredSpace::upsert_record
    -> schema validation (unchanged)
    -> self.txn.stage(txn_id, &backend_key, body, ct, /*no x-xtable-* meta*/, false)
       [tx-coordinator stores in TBL_WRITE_SET and MemTable with
        RecordKey = (real space, real table, key)]
    -> StructuredSpace::pending.record(...)  [unchanged]

[client later commits txn]
  tx-coordinator.commit:
    for each write_entry:
      [DELETE: backend.put_object(&backend_key, ...)]
      [DELETE: promote staged body to final S3 key]
      [DELETE: backend.put_object_metadata(...)]
    append TBL_CHUNK_INDEX row per write  [unchanged]
    publish each entry to MemTable
      [MemTable key = (real space, real table, key)]
  flush_loop:
    encode entries -> ChunkWriter.finalize()
    upload to S3 key `_xtable/{space}/{table}/shard/{chunk_id}.xtc`
    insert TBL_CHUNK_INDEX row pointing at the chunk
```

Key change: `space` and `table` in the MemTable key are no longer
empty strings; they are populated from the staged entry's
`backend_key` (which encodes `record_key(space, table, record_id)`).

Schema writes go through the same path:

```
client.register_schema(space, name, body)
  -> StructuredSpace::register_schema
    -> self.txn.stage(txn_id, &schema_backend_key, body, ct, [], false)
```

where `schema_backend_key = schema_key(space, name, version)` (still
the logical key, never an S3 PUT target). The `tx-coordinator`
publishes this entry to MemTable with `RecordKey = (space, "_schema",
name + "/" + version)`. The chunk flushes group all entries by
their (space, table) prefix.

`TBL_CHUNK_INDEX.s3_key` for the schema's chunk is therefore
`_xtable/{space}/_schema/shard/{chunk_id}.xtc`.

### 5.2 Read path (chunk-only)

```
client.get_record(space, table, record_id)
  -> StructuredSpace::get_record
    -> xtable_storage::read::read_at_snapshot(
         mems, store, backend,
         space, table, record_id, snapshot)
      [active memtable -> immutables -> TBL_RECORD_INDEX ->
       lookup_chunk_for_record -> S3 GET chunk -> decompress +
       decode_body_entries -> find matching entry]
```

`read_at_snapshot` already does the right thing **except** for the
fact that `TBL_RECORD_INDEX.backend_key` currently stores the
per-record JSON path, not a chunk key. We change the field to
`chunk_id: String` (a ULID). `lookup_chunk_for_record` then becomes
`store.get_chunk_index(idx.chunk_id)` — direct, no path parsing.

Schema reads use the same `read_at_snapshot` with `table = "_schema"`.

### 5.3 TBL_RECORD_INDEX schema change

`xtable-storage/src/txn_state.rs::RecordIndexEntry`:

```rust
// before:
pub struct RecordIndexEntry {
    pub commit_version: u64,
    pub deleted: bool,
    pub backend_key: String,    // <- per-record JSON path
    pub schema_version: u32,
    pub txn_id: String,
    pub updated_ms: i64,
}

// after:
pub struct RecordIndexEntry {
    pub commit_version: u64,
    pub deleted: bool,
    pub chunk_id: String,         // <- ULID of the chunk containing the latest version
    pub schema_version: u32,
    pub txn_id: String,
    pub updated_ms: i64,
}
```

`TBL_RECORD_INDEX` key remains `(space, table, record_id)`. Schema
records are keyed `(space, "_schema", "<name>/v<N>")`. The key
format is internal to xtable-storage and not user-facing.

`Store::put_record_index` and `get_record_index` are updated to
the new struct. Existing rows in the redb on disk are NOT migrated
in this spec; the cold rebuild path (which now reads chunks) will
rebuild the index from scratch on first startup, so any pre-existing
per-record JSON objects in the bucket are no longer consulted and
any pre-existing `TBL_RECORD_INDEX` rows are overwritten.

### 5.4 Chunk flush — `space`/`table` no longer empty

`xtable-storage/src/flush.rs::flush_one` currently iterates the
immutable MemTable and calls `ChunkWriter::new(chunk_id, space, table)`
per flush. The `space` and `table` come from
`mem_entry.key.0` and `mem_entry.key.1`. With the change in 5.1,
these are now the real space/table for structured records
(previously empty strings).

The chunk s3_key format changes from `_xtable///shard/{chunk_id}.xtc`
to `_xtable/{space}/{table}/shard/{chunk_id}.xtc` for structured
records. For non-structured records (which still go through the
tx-coordinator with empty space/table), the chunk key remains
`_xtable///shard/{chunk_id}.xtc`. Both flavors coexist; the
cold-rebuild path filters by `endswith(".xtc")` and decodes either.

The `ChunkIndexEntry.key_min` and `key_max` already encode the
record-id range, so chunk-index lookups work without change.

### 5.5 Cold rebuild — decode chunks, not objects

`xtable-tx/src/rebuild.rs::rebuild` is rewritten to:

1. `backend.list_objects().await?` — same as today, returns
   `(key, etag, size)` triples.
2. Filter to keys ending in `.xtc`.
3. For each chunk: `backend.get_object(&chunk_key)` → bytes →
   `decompress_body` → `decode_body_entries`.
4. For each entry, take the one with the highest `commit_version`
   per `(space, table, record_id)`. This is the per-record latest
   version.
5. Build `TBL_VERSIONS` and MVCC chain exactly as today (the
   bulk-write helpers are unchanged).
6. Set `global_version = max(commit_version across all entries)`.

The chunk decode path already exists in `xtable-storage::read` and
is used by `read_at_snapshot`. `rebuild.rs` can use the same
helpers (`decode_body_entries`, `decompress_body`).

### 5.6 Test changes

- `xtable-storage/src/cf.rs`: TBL_RECORD_INDEX redb schema is
  unchanged (same key type, same value type — the value struct
  is updated).
- All test fixtures that mock per-record JSON objects
  (`examples/dummy_for_test_async`, `MockS3`) keep doing the
  per-record JSON PUT (so `backend.get_object` etc. still work in
  unit tests where chunks are not in the picture). Tests that
  exercise the structured read path must be updated to use the
  chunk-based read instead.
- `xtable-schema/src/engine.rs::upsert_record` test fixtures: the
  assertion that the per-record JSON is at a specific S3 key
  must be removed. The test should instead seed the chunk_index
  and memtable directly (or use a helper that does so).

## 6. File-by-file change list

### Modify

- `xtable-tx/src/coordinator.rs` — remove the per-record
  `backend.put_object` loop in `commit_inner`; populate
  `MemTable` `space`/`table` from the staged entry's `backend_key`.
- `xtable-schema/src/engine.rs` — `upsert_record` and
  `register_schema` drop the `x-xtable-*` metadata insertion;
  `get_record` switches to `read_at_snapshot`; `head_object`
  goes through the same chunk path; `delete_record` writes a
  tombstone entry to the chunk (no separate `delete_object`);
  `list_records` / `query` decodes chunk entries that match the
  predicate.
- `xtable-tx/src/rebuild.rs` — rewrite to list `*.xtc` chunks,
  decode each, build the per-record index from the entries.
- `xtable-storage/src/txn_state.rs` — `RecordIndexEntry.backend_key`
  → `chunk_id: String`.
- `xtable-storage/src/store.rs` — `put_record_index` /
  `get_record_index` use the new struct; `put_record_index` takes
  `(space, table, record_id, chunk_id, ...)` instead of
  `(space, table, record_id, backend_key, ...)`.
- `xtable-storage/src/read.rs` — `lookup_chunk_for_record` reads
  `idx.chunk_id` directly (no path parsing). `read_at_snapshot` is
  now reached by the structured layer.
- `xtable-storage/src/flush.rs` — chunk s3_key format now includes
  real `space`/`table` for structured entries. Existing logic for
  non-structured entries (empty `space`/`table`) is unchanged.
- `xtable-storage/src/chunk.rs` — minor: `ChunkWriter::new` already
  accepts `(chunk_id, space, table)`; no signature change.

### Not changed

- `xtable-backend/src/client.rs` — `BackendClient::put_object` /
  `get_object` / `head_object` / `delete_object` / `list_objects`
  remain (chunks still use S3 PUT/GET, and the mock backend still
  serves per-record JSON for unit tests).
- `xtable-backend/src/keymap.rs` — `IdentityKeyMap` is unchanged.
  The `backend_key` returned by `IdentityKeyMap::backend_key` for
  records is the *logical* key (used as the MemTable key), not an
  S3 path.
- `xtable-core` — no changes.
- `xtable-server/src/main.rs` — no changes (the structured read
  path is internal to the schema crate).

## 7. Migration

There is no live migration. The change is **breaking**: any
existing per-record JSON objects in the bucket are no longer read.
On first startup of an upgraded server against a bucket with
existing per-record JSONs:

1. WAL replay runs (`recovery.rs`) — finds nothing (WAL was
   written before this change).
2. Cold rebuild runs (`rebuild.rs`) — lists objects, finds `.xtc`
   files only. Per-record JSONs are ignored.
3. `global_version` is set from chunks.
4. New writes go to chunks only.

If the user wants to preserve existing per-record JSONs, a
follow-up migration tool can: list all `*.json` objects under
`_xtable/`, decode each body, write a single chunk covering them,
delete the per-record objects. This is out of scope here.

## 8. Risks and mitigations

| Risk | Mitigation |
|------|-----------|
| **Crash window**: a commit publishes to MemTable but the chunk flush is async; if the process dies within `flush_interval` (default 60s), those writes are lost from the chunk side. | Drop `FlushPolicy.flush_interval` to 10s for the structured layer. Document the window. |
| **Read amplification**: every `get_record` fetches a full chunk (potentially containing N records). | Existing bloom filter on `ChunkIndexEntry` + `key_min`/`key_max` bounds already short-circuit wrong-chunk fetches. Range GET (PR #4+, out of scope) is the next step. |
| **Cold rebuild time**: scales linearly with chunk count. | Bloom + version range let the rebuild skip chunks older than the max committed version. Per-(space, table) chunk paths let us index by space. |
| **`RecordIndexEntry.chunk_id` change breaks on-disk redb files**: redb data created by old binary is incompatible. | Cold rebuild always re-reads from S3 chunks; the redb table is rebuilt from scratch on first startup. No migration code needed. |
| **`TBL_CHUNK_INDEX` key collision** between structured and non-structured chunks (both write to `TBL_CHUNK_INDEX`, distinguished only by `s3_key` containing `space=""` for non-structured). | None — current code already keys `TBL_CHUNK_INDEX` by `chunk_id` (ULID), not by space/table. Uniqueness holds. |

## 9. Testing strategy

- **Unit** (per crate):
  - `xtable-storage::read`: existing tests pass; add a test that
    `read_at_snapshot` returns the latest version when the entry
    lives only in a chunk (not in active or immutable memtables).
  - `xtable-storage::flush`: add a test that flush emits
    `_xtable/{space}/{table}/shard/{chunk_id}.xtc` for structured
    entries (real `space`/`table`).
  - `xtable-schema::engine`: update `upsert_record` / `get_record` /
    `delete_record` / `head_object` / `list_records` tests to use
    chunk-only paths. Existing tests that mock per-record JSON
    PUTs need replacement with chunk-index seeding.
  - `xtable-tx::coordinator::tests`: add a test that asserts
    commit does **not** call `backend.put_object` for structured
    entries; the entry is published to MemTable instead.
  - `xtable-tx::rebuild::tests`: add a test that rebuilds from a
    bucket containing only chunks.
- **Integration** (`xtable-server/tests/structured_http.rs`):
  - All existing e2e tests should still pass: schema register →
    bind → upsert → get.
  - Add a test that verifies the per-record JSON file is **not**
    written to S3 (only the chunk is).
  - Add a test that the cold-rebuild path produces the same index
    from chunks as from per-record JSONs (snapshot before/after
    refactor).
- **End-to-end manual**:
  - `bash /tmp/run_probe.sh` (in scripts dir) lists the bucket
    and asserts `.xtc` files exist after a series of upserts and
    no per-record `.json` files.
  - `python3 /tmp/xtable_e2e.py` (in scripts dir) still produces
    the same successful 4-step end-to-end sequence.

## 10. Out of scope

- Per-record JSON migration tool (see §7).
- Range GET for chunk body download.
- New chunk format version.
- Compression in chunk encoder.
- Per-table secondary indexes (in addition to chunk index).
- Schema versioning policy changes (current policy is "schema v1 always stored, never compacted").

## 11. Open questions

1. **Should non-structured records (tx-coordinator's raw KV write
   path) also stop writing per-record JSON?** Currently those use
   empty `space`/`table` MemTable keys. They are used by any
   direct `txn.stage` callers that bypass `StructuredSpace`.
   For this spec, the answer is: **leave them as-is** — the
   per-record PUT for those is in the **tx-coordinator's commit
   path**, not in `StructuredSpace`. If/when those callers also
   need chunk-only, that's a separate change.
2. **Should the new chunk s3_key format (`_xtable/{space}/{table}/shard/{chunk_id}.xtc`) be used even for non-structured entries (with empty space/table)?** No — non-structured entries keep their current `_xtable///shard/...` format. Mixed chunks coexist.
