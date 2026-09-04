//! Read path orchestration for the LSM-tree storage layer.
//!
//! Lookup sequence (per `read_at_snapshot`):
//!
//! ```text
//! 1. Active memtable.get_visible(key, snapshot)
//! 2. Immutables (newest first)
//! 3. TBL_RECORD_INDEX (per-(space, table, record_id) pointer into a chunk)
//! 4. TBL_CHUNK_INDEX (chunk metadata, with embedded bloom)
//! 5. S3 Range GET (or full GET) for the body
//! 6. zstd decompress + decode entry
//! 7. bloom filter check (skip body download on negative lookup)
//! ```
//!
//! For v1, the orchestration here is intentionally simple. S3
//! Range-GET support lands in PR #4+ via `BackendClient::get_object_range`.

use std::sync::Arc;

use crate::chunk::{bloom_may_contain, decode_body_entries, decompress_body, ChunkIndexEntry};
use crate::memtable::{MemTableSet, RecordKey};
use crate::store::LocalStore;
use xtable_core::{ObjectKey, XtableResult};
use xtable_telemetry::metrics::global as metrics;
use xtable_telemetry::timed::Timed;
use xtable_telemetry::KeyValue;

/// Outcome of a read at a snapshot.
#[derive(Clone)]
pub struct ReadResult {
    pub body: bytes::Bytes,
    pub commit_version: u64,
    pub txn_id: String,
    pub deleted: bool,
    pub content_type: Option<String>,
    pub user_meta: Vec<(String, String)>,
    pub schema_version: u32,
    /// Where the body actually came from. Informational.
    pub source: ReadSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadSource {
    Active,
    Immutable,
    RecordIndex,
    Chunk,
    NotFound,
}

/// Read at snapshot. Walks active memtable → immutables → record index →
/// chunk index → S3 GET, in that order. Honors snapshot isolation: only
/// entries with `commit_version <= snapshot` are visible.
#[tracing::instrument(
    level = "debug",
    name = "chunk.download",
    skip_all,
    fields(op = "chunk.download"),
    err
)]
pub async fn read_at_snapshot(
    mems: &Arc<MemTableSet>,
    store: &LocalStore,
    backend: &Arc<xtable_backend::BackendClient>,
    space: &str,
    table: &str,
    record_id: &str,
    snapshot: u64,
) -> XtableResult<Option<ReadResult>> {
    let _timed = Timed::new(
        &metrics().chunk_download_duration,
        vec![KeyValue::new("op", "chunk.download")],
    );
    let key: RecordKey = (space.to_string(), table.to_string(), record_id.to_string());

    // 1. Active memtable.
    {
        let active = mems.active.read();
        if let Some(e) = active.get_visible(&key, snapshot) {
            let cv = e.commit_version.load(std::sync::atomic::Ordering::Acquire);
            if cv <= snapshot {
                let arg = Arc::clone(&e);
                return Ok(Some(entry_to_result(&arg, cv, ReadSource::Active)));
            }
        }
    }

    // 2. Immutables, newest-first.
    {
        let flushing = mems.flushing.lock();
        for mt in flushing.iter().rev() {
            if let Some(e) = mt.get_visible(&key, snapshot) {
                let cv = e.commit_version.load(std::sync::atomic::Ordering::Acquire);
                if cv <= snapshot {
                    let arg = Arc::clone(&e);
                    return Ok(Some(entry_to_result(&arg, cv, ReadSource::Immutable)));
                }
            }
        }
    }

    // 3. Historical record index. The legacy TBL_RECORD_INDEX row is
    // latest-only and cannot answer an old snapshot; select the newest
    // version <= snapshot before consulting the latest-row fallback.
    if let Some((idx, body)) =
        store.get_record_version_at_snapshot(space, table, record_id, snapshot)?
    {
        if idx.deleted {
            return Ok(Some(tombstone_result(&idx)));
        }
        if !body.is_empty() {
            // Once flush has installed a real chunk pointer, keep the normal
            // chunk read path (and its integrity/bloom behavior). Before the
            // first flush the placeholder pointer is absent, so the durable
            // inline body is the correct source.
            if store.get_chunk_index(&idx.chunk_id)?.is_none() {
                return Ok(Some(ReadResult {
                    body: bytes::Bytes::from(body),
                    commit_version: idx.commit_version,
                    txn_id: idx.txn_id,
                    deleted: false,
                    content_type: None,
                    user_meta: vec![],
                    schema_version: idx.schema_version,
                    source: ReadSource::RecordIndex,
                }));
            }
        }
        // A rebuilt/legacy row may have no inline body. Its exact version
        // still supplies the correct chunk pointer, so continue below.
        return read_from_chunk(store, backend, space, table, record_id, snapshot, &idx).await;
    }

    // 4. Legacy latest-row fallback. New writes always populate the
    // historical table, while this keeps pre-migration stores readable.
    let rec_idx = store.get_record_index(space, table, record_id)?;
    let Some(idx) = rec_idx else {
        return Ok(None);
    };
    if idx.commit_version > snapshot {
        return Ok(None);
    }
    if idx.deleted {
        return Ok(Some(tombstone_result(&idx)));
    }
    read_from_chunk(store, backend, space, table, record_id, snapshot, &idx).await
}

fn tombstone_result(idx: &crate::txn_state::RecordIndexEntry) -> ReadResult {
    ReadResult {
        body: bytes::Bytes::new(),
        commit_version: idx.commit_version,
        txn_id: idx.txn_id.clone(),
        deleted: true,
        content_type: None,
        user_meta: vec![],
        schema_version: idx.schema_version,
        source: ReadSource::NotFound,
    }
}

async fn read_from_chunk(
    store: &LocalStore,
    backend: &Arc<xtable_backend::BackendClient>,
    space: &str,
    table: &str,
    record_id: &str,
    snapshot: u64,
    idx: &crate::txn_state::RecordIndexEntry,
) -> XtableResult<Option<ReadResult>> {
    // 5. Chunk index lookup. The index pointer is version-specific after
    // flush, so an older flush cannot redirect a newer snapshot here.
    let chunk = lookup_chunk_for_record(store, idx)?;
    let Some(chunk) = chunk else {
        return Ok(None);
    };

    // 6. Bloom check.
    let key_bytes = crate::chunk::compose_key_bytes(space, table, record_id);
    if let Some(bloom) = &chunk.bloom {
        if !bloom_may_contain(bloom, &key_bytes) {
            return Ok(None);
        }
    }

    // 7. S3 GET (full body; Range GET lands in PR #4+).
    let file_bytes = backend
        .get_object(&ObjectKey::new(&chunk.s3_key))
        .await
        .map_err(|e| xtable_core::XtableError::Backend(format!("{}", e)))?
        .bytes;

    // 8. Decompress and choose the newest matching entry visible at the
    // requested snapshot. A chunk may contain more than one historical entry
    // for the same record after rebuild/compaction.
    let body = decompress_body(&file_bytes)?;
    let entries = decode_body_entries(
        &body,
        chunk.commit_version_max - chunk.commit_version_min + 1,
    )?;
    let hit = entries
        .into_iter()
        .filter(|e| {
            e.space == space
                && e.table == table
                && e.record_id == record_id
                && e.commit_version <= snapshot
        })
        .max_by_key(|e| e.commit_version);
    let Some(e) = hit else {
        return Ok(None);
    };
    Ok(Some(ReadResult {
        body: bytes::Bytes::from(e.value),
        commit_version: e.commit_version,
        txn_id: e.txn_id,
        deleted: e.deleted,
        content_type: e.content_type,
        user_meta: e.user_meta,
        schema_version: e.schema_version,
        source: if e.deleted {
            ReadSource::NotFound
        } else {
            ReadSource::Chunk
        },
    }))
}

fn entry_to_result(e: &crate::memtable::MemEntry, cv: u64, source: ReadSource) -> ReadResult {
    ReadResult {
        body: bytes::Bytes::clone(&e.value.bytes),
        commit_version: cv,
        txn_id: e.txn_id.clone(),
        deleted: e.deleted,
        content_type: e.content_type.clone(),
        user_meta: e.user_meta.clone(),
        schema_version: e.schema_version,
        source,
    }
}

/// Find the chunk that contains `record_id`'s body, given a record-index
/// entry. The chunk id is carried directly in `idx.chunk_id`.
fn lookup_chunk_for_record(
    store: &LocalStore,
    idx: &crate::txn_state::RecordIndexEntry,
) -> XtableResult<Option<ChunkIndexEntry>> {
    store.get_chunk_index(&idx.chunk_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_source_variants() {
        let _ = ReadSource::Active;
        let _ = ReadSource::Immutable;
        let _ = ReadSource::RecordIndex;
        let _ = ReadSource::Chunk;
        let _ = ReadSource::NotFound;
    }
}
