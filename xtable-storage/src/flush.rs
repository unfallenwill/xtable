//! MemTable → S3 chunk flush pipeline.
//!
//! This is the **write path** of the LSM-tree storage layer:
//!
//! ```text
//! Active MemTable
//!       │
//!       │ (size or age threshold)
//!       ▼
//! Immutable MemTable
//!       │
//!       ▼
//! flush_one:
//!   1. Encode all entries → ChunkWriter.finalize() → (file_bytes, header, footer)
//!   2. Upload to S3 (single PUT or multipart based on size)
//!   3. Insert TBL_CHUNK_INDEX row (durable point-of-no-return)
//!   4. Append WAL `MemtableFlushed` record
//!   5. Truncate WAL up to up_to_seq
//!   6. Drop MemTable Arc (last reference releases the buffers)
//! ```
//!
//! The flush task is owned by the storage layer and runs as a long-lived
//! tokio task. It does **not** participate in commit's critical section
//! — commits return after memtable publish, and S3 IO happens entirely
//! out-of-band.

use std::sync::Arc;
use std::time::Instant;

use crate::cf::meta_key;
use crate::chunk::{ChunkEntry, ChunkIndexEntry, ChunkWriter};
use crate::memtable::{MemEntry, MemTable, MemTableSet};
use crate::wal::WalRecord;
use xtable_core::XtableResult;
use xtable_telemetry::metrics::global as metrics;
use xtable_telemetry::timed::Timed;
use xtable_telemetry::KeyValue;

/// Default cap on concurrent flush tasks.
pub const DEFAULT_FLUSH_CONCURRENCY: usize = 4;

/// Run the flush loop indefinitely, picking up immutable memtables as
/// they arrive. Each immutable is flushed in `flush_one`. Caller is
/// responsible for spawning the loop (typically once at server start).
///
/// A 1s wall-clock tick runs `try_rotate_active` alongside the
/// `flush_notify` signal so that *idle* memtables (no incoming
/// commits) still age out and get flushed. `try_rotate_active` is
/// non-blocking — it only acts when it can grab the active write lock
/// without waiting — so the tick is safe to run inside the async
/// runtime even if a commit is concurrently holding the read lock.
///
/// NOTE: This function is intentionally NOT `#[tracing::instrument]`-ed —
/// it is a long-lived task whose single span would live the entire
/// process lifetime, leaking per-span metadata into the tracing
/// subscriber, producing a never-ending trace in trace UIs, and
/// starving short-lived spans in tail-based samplers. Each iteration
/// is captured by the `#[instrument]` on `flush_one`, which produces
/// a discrete `memtable.flush` span per memtable.
pub async fn flush_loop(
    memtables: Arc<MemTableSet>,
    store: crate::store::LocalStore,
    backend: Arc<xtable_backend::BackendClient>,
) -> XtableResult<()> {
    let tick = std::time::Duration::from_secs(1);
    loop {
        tokio::select! {
            _ = memtables.flush_notify.notified() => {}
            _ = tokio::time::sleep(tick) => {
                memtables.try_rotate_active();
            }
        }
        let immutables = memtables.take_immutables().await;
        for mt in immutables {
            if let Err(err) = flush_one(&mt, &store, backend.clone()).await {
                tracing::error!(error=%err, "flush failed; will retry on next rotation");
                // Re-push the memtable so we retry on the next rotation.
                memtables.flushing.lock().push(mt);
            }
        }
    }
}

/// Flush a single immutable memtable. Public so tests can drive it
/// synchronously without running the full loop.
#[tracing::instrument(level = "info", name = "memtable.flush", skip_all, err)]
pub async fn flush_one(
    mt: &Arc<MemTable>,
    store: &crate::store::LocalStore,
    backend: Arc<xtable_backend::BackendClient>,
) -> XtableResult<()> {
    let _timed = Timed::new(
        &metrics().memtable_flush_duration,
        vec![KeyValue::new("op", "flush")],
    );
    let started = Instant::now();

    // 1. Build chunk_id and stats.
    let chunk_id = ulid::Ulid::new().to_string();
    let first_seq = mt.first_wal_seq();
    let last_seq = mt.last_wal_seq();
    let cv_min = mt.commit_version_min();
    let cv_max = mt.commit_version_max();

    if first_seq == u64::MAX || last_seq == 0 {
        // Empty memtable; nothing to flush.
        return Ok(());
    }

    let mut writer: Option<ChunkWriter> = None;

    // Snapshot entries in iteration order. DashMap iter is unsorted;
    // the chunk encoder sorts by `compose_key_bytes` on the fly.
    for entry in mt.map.iter() {
        let e: &MemEntry = entry.value();
        let cv = e.commit_version.load(std::sync::atomic::Ordering::Acquire);
        // Skip invisible entries (commit never happened).
        if cv == crate::memtable::INVISIBLE {
            continue;
        }
        let chunk_entry = ChunkEntry {
            space: e.key.0.clone(),
            table: e.key.1.clone(),
            record_id: e.key.2.clone(),
            value: e.value.bytes.to_vec(),
            commit_version: cv,
            txn_id: e.txn_id.clone(),
            deleted: e.deleted,
            content_type: e.content_type.clone(),
            user_meta: e.user_meta.clone(),
            schema_version: e.schema_version,
            wal_seq: e.wal_seq,
        };
        if writer.is_none() {
            writer = Some(ChunkWriter::new(
                chunk_id.clone(),
                chunk_entry.space.clone(),
                chunk_entry.table.clone(),
            ));
        }
        writer.as_mut().unwrap().append(chunk_entry)?;
    }

    let writer = match writer {
        Some(w) => w,
        // No visible entries — nothing to flush.
        None => return Ok(()),
    };

    let (file_bytes, header, footer) = writer.finalize()?;
    let body_offset = header_bytes_len(&header)?;
    // PR-Fix14.1: the slice is used later for sha256 (currently a no-op
    // reserved column). Bind to `_` so the compiler doesn't complain
    // about an unused binding.
    let _compressed_body =
        &file_bytes[body_offset..body_offset + header.compressed_body_len as usize];

    // 3. Compute shard from the first key.
    let shard = compute_shard(&header.key_min);

    // 4. S3 key.
    let s3_key = format!(
        "_xtable/{}/{}/{}/{}.xtc",
        header.space, header.table, shard, chunk_id
    );

    // 5. Body sha256 (PR-Fix3.4): S3 already etag-verifies the bytes; we
    //    don't need a separate body hash for v1. PR-Fix3.4 dropped the
    //    zstd round-trip; the `sha256_body` field is reserved for a
    //    future read-time re-verify feature.
    let body_sha = String::new();

    // 6. Upload to S3 (single PUT or multipart).
    let etag = upload_chunk(&backend, &s3_key, file_bytes.clone()).await?;

    // 7. Build ChunkIndexEntry.
    let entry = ChunkIndexEntry {
        s3_key: s3_key.clone(),
        space: header.space.clone(),
        table: header.table.clone(),
        shard,
        key_min: header.key_min.clone(),
        key_max: header.key_max.clone(),
        commit_version_min: cv_min,
        commit_version_max: cv_max,
        wal_seq_first: first_seq,
        wal_seq_last: last_seq,
        sha256_body: body_sha,
        size_bytes: file_bytes.len() as u64,
        etag,
        bloom: Some(footer.bloom.clone()),
        flushed_at_ms: chrono::Utc::now().timestamp_millis(),
        status: crate::cf::ChunkStatus::Live,
    };

    // 8. Persist ChunkIndexEntry in redb (durable point of no return).
    store.put_chunk_index(&chunk_id, &entry)?;

    // 8b. Update TBL_RECORD_INDEX chunk_ids for every flushed entry
    // (spec §5.2). The structured layer writes a per-record row at
    // commit time with `chunk_id` placeholdered by the per-record key;
    // without this update, post-flush `read_at_snapshot` lookups fall
    // through to `store.get_chunk_index(<per_record_key>)` and miss.
    // Existing entries preserve their body / commit_version; only
    // `chunk_id` is rewritten.
    if let Err(e) = update_record_index_after_flush(store, mt, &chunk_id) {
        // Best-effort: a stale chunk_id means the read falls through
        // to memtable → immutables, but those are gone post-flush, so
        // log loudly so a chunk-miss bug surfaces.
        tracing::warn!(
            error = %e,
            chunk_id = %chunk_id,
            "TBL_RECORD_INDEX chunk_id update failed; post-flush reads may miss"
        );
    }

    // 9. Append WAL `MemtableFlushed`.
    let _seq = store.append_wal(&WalRecord::MemtableFlushed {
        chunk_id: chunk_id.clone(),
        up_to_seq: last_seq,
        up_to_commit_version: cv_max,
    })?;

    // 10. Update meta key: LAST_FLUSHED_SEQ.
    store.set_flushed_seq(last_seq)?;

    // 11. WAL truncation is best-effort; we don't fail the flush if it
    // errors. The next flush will retry.
    if let Err(e) = store.truncate_wal(last_seq) {
        tracing::warn!(error=%e, "WAL truncation failed; will retry next flush");
    }

    let elapsed = started.elapsed();
    tracing::info!(
        chunk_id = %chunk_id,
        records = header.record_count,
        bytes = file_bytes.len(),
        elapsed_ms = elapsed.as_millis() as u64,
        "chunk flush complete"
    );
    Ok(())
}

/// Walk every MemEntry in the just-flushed memtable and rewrite the
/// matching TBL_RECORD_INDEX row so its `chunk_id` field points at the
/// new chunk ULID. Best-effort: callers should warn on error.
fn update_record_index_after_flush(
    store: &crate::store::LocalStore,
    mt: &Arc<MemTable>,
    new_chunk_id: &str,
) -> XtableResult<()> {
    for entry in mt.map.iter() {
        let e: &MemEntry = entry.value();
        let cv = e.commit_version.load(std::sync::atomic::Ordering::Acquire);
        // Skip entries that never became visible (commit aborted mid-flight
        // or staged but never published). The chunk won't carry them either.
        if cv == crate::memtable::INVISIBLE {
            continue;
        }
        // Update only the exact version emitted by this memtable. An older
        // immutable can flush after a newer commit and must not move the
        // latest-row pointer back to its older chunk.
        store.update_record_index_chunk_id_for_version(
            &e.key.0,
            &e.key.1,
            &e.key.2,
            cv,
            new_chunk_id,
        )?;
    }
    Ok(())
}

/// Compute the S3 key header byte length (mirrors `ChunkHeader::encode`).
fn header_bytes_len(h: &crate::chunk::ChunkHeader) -> XtableResult<usize> {
    let mut buf = bytes::BytesMut::new();
    buf.extend_from_slice(crate::chunk::CHUNK_MAGIC);
    buf.extend_from_slice(&crate::chunk::CHUNK_VERSION.to_le_bytes());
    let id_bytes = h.chunk_id.as_bytes();
    buf.extend_from_slice(&(id_bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(id_bytes);
    buf.extend_from_slice(&h.created_at_ms.to_le_bytes());
    buf.extend_from_slice(&h.compressed_body_len.to_le_bytes());
    buf.extend_from_slice(&h.uncompressed_len.to_le_bytes());
    let space_bytes = h.space.as_bytes();
    buf.extend_from_slice(&(space_bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(space_bytes);
    let table_bytes = h.table.as_bytes();
    buf.extend_from_slice(&(table_bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(table_bytes);
    buf.extend_from_slice(&(h.key_min.len() as u16).to_le_bytes());
    buf.extend_from_slice(&h.key_min);
    buf.extend_from_slice(&(h.key_max.len() as u16).to_le_bytes());
    buf.extend_from_slice(&h.key_max);
    buf.extend_from_slice(&h.record_count.to_le_bytes());
    Ok(buf.len())
}

/// Hash the chunk's first key bytes to derive the shard byte.
fn compute_shard(key_min: &[u8]) -> u8 {
    let h = xxhash_rust::xxh3::xxh3_64(key_min);
    h as u8
}

/// Upload a chunk file to S3. PR-Fix3.2: collapsed to a single
/// `put_object` call; multipart wiring lands when `BackendClient`
/// grows multipart support (see M5 review item).
#[tracing::instrument(
    level = "info",
    name = "chunk.upload",
    skip_all,
    fields(op = "chunk.upload"),
    err
)]
async fn upload_chunk(
    backend: &xtable_backend::BackendClient,
    s3_key: &str,
    file_bytes: Vec<u8>,
) -> XtableResult<String> {
    let _timed = Timed::new(
        &metrics().chunk_upload_duration,
        vec![KeyValue::new("op", "chunk.upload")],
    );
    use std::collections::HashMap;
    use xtable_core::ObjectKey;

    let mut meta = HashMap::new();
    meta.insert("x-amz-meta-xtable-format".into(), "chunk_v1".into());
    backend
        .put_object(&ObjectKey::new(s3_key), file_bytes, Some("zstd"), meta)
        .await?;
    // BackendClient doesn't surface etag yet (see M6 review item).
    Ok(String::new())
}

/// Side-effect free meta helpers (used by tests and gc).
impl crate::store::LocalStore {
    pub fn set_flushed_seq(&self, seq: u64) -> XtableResult<()> {
        self.with_write(|txn| {
            let mut meta = txn
                .open_table(crate::cf::TBL_META)
                .map_err(crate::store::redb_err)?;
            let k = meta_key::LAST_FLUSHED_SEQ.to_string();
            meta.insert(k.as_str(), seq)
                .map_err(crate::store::redb_err)?;
            Ok(())
        })
    }

    pub fn last_flushed_seq(&self) -> XtableResult<u64> {
        self.with_read(|txn| {
            let meta = txn
                .open_table(crate::cf::TBL_META)
                .map_err(crate::store::redb_err)?;
            Ok(meta
                .get(meta_key::LAST_FLUSHED_SEQ)
                .map_err(crate::store::redb_err)?
                .map(|v| v.value())
                .unwrap_or(0))
        })
    }

    /// Truncate WAL rows with `seq <= up_to_seq`. Returns the number of
    /// rows removed. PR-Fix3.1: collapsed `truncate_wal_up_to`,
    /// `truncate_wal_strict`, and `truncate_wal` into one.
    #[tracing::instrument(
        level = "info",
        name = "wal.truncate",
        skip_all,
        fields(op = "wal.truncate"),
        err
    )]
    pub fn truncate_wal(&self, up_to_seq: u64) -> XtableResult<usize> {
        use redb::ReadableTable;
        let mut removed = 0;
        self.with_write(|txn| {
            let mut wal = txn
                .open_table(crate::cf::TBL_WAL)
                .map_err(crate::store::redb_err)?;
            let mut to_remove: Vec<u64> = Vec::new();
            for entry in wal.iter().map_err(crate::store::redb_err)? {
                let (k, _) = entry.map_err(crate::store::redb_err)?;
                let seq = k.value();
                if seq <= up_to_seq {
                    to_remove.push(seq);
                } else {
                    break; // WAL is sorted by key.
                }
            }
            for seq in to_remove {
                wal.remove(seq).map_err(crate::store::redb_err)?;
                removed += 1;
            }
            Ok(())
        })?;
        Ok(removed)
    }
}

// (PR-Fix3.1: old `truncate_wal_up_to`, `truncate_wal_strict`, and
//  `truncate_wal` impls removed; the unified `truncate_wal` lives in
//  the impl block above.)
