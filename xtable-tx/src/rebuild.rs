//! Cold rebuild: when redb is missing or corrupt, reconstruct the version
//! index from S3 chunk objects (spec §5.5).
//!
//! ## Wire-up
//!
//! 1. `backend.list_objects()` — discover every chunk.
//! 2. Filter to `*.xtc`; chunks are the only durable surface that holds
//!    structured records now that per-record JSON uploads are gone.
//! 3. For each chunk:
//!    a. `backend.get_object()` to fetch the body.
//!    b. `decompress_body` + `decode_body_entries`.
//!    c. Walk entries to build a per-(space, table, record_id) index
//!       tracking the latest `commit_version` and the chunk that holds
//!       it.
//! 4. Bulk-write `TBL_CHUNK_INDEX`, `TBL_RECORD_INDEX`, `TBL_VERSIONS`,
//!    and `TBL_VERSION_CHAINS`.
//!
//! ## Semantics after this rewrite
//!
//! - `VersionRecord.latest_backend_key` is now the **chunk's** S3 key
//!   that holds the latest version of the record. `TBL_VERSIONS`
//!   becomes a per-record pointer into the chunk world (it stores the
//!   chunk s3_key), so reads via `read_at_snapshot` can still resolve
//!   to a chunk through `TBL_RECORD_INDEX` → `TBL_CHUNK_INDEX`.
//! - `VersionEntry.backend_key` on the chain is the same chunk s3_key.
//! - `TBL_RECORD_INDEX` (the structured path) is rebuilt so that
//!   `read_at_snapshot` can find the chunk via `lookup_chunk_for_record`.
//! - `TBL_CHUNK_INDEX` is rebuilt so the bloom filter and `s3_key`
//!   are available without re-downloading.
//!
//! V14 fix preserved: backend unreachability is a fatal error — an
//! empty `LocalStore` with `global_version=0` would otherwise let
//! subsequent commits overwrite real data at v1+.

use std::collections::HashMap;
use std::sync::OnceLock;

use chrono::Utc;
use tracing::{info, warn};
use xtable_core::ObjectKey;
use xtable_storage::{
    cf::ChunkStatus,
    chunk::{decode_body_entries, decompress_body, ChunkEntry, ChunkHeader, ChunkIndexEntry},
    LocalStore, RecordIndexEntry, VersionEntry, VersionRecord,
};
use xtable_telemetry::metrics::Metrics;
use xtable_telemetry::timed::Timed;
use xtable_telemetry::KeyValue;

/// Lazily-initialised `Metrics` bound to the global OTel meter.
fn metrics() -> &'static Metrics {
    static METRICS: OnceLock<Metrics> = OnceLock::new();
    METRICS.get_or_init(Metrics::default)
}

#[derive(Debug, Default, Clone, Copy)]
pub struct RebuildReport {
    pub objects_scanned: usize,
    pub versions_rebuilt: usize,
    pub orphans_deleted: usize,
}

/// Build a `RecordIndexEntry` from one chunk entry plus the chunk's
/// S3 key and chunk id. Captures every field needed to resolve a
/// post-flush read through `TBL_RECORD_INDEX` → `TBL_CHUNK_INDEX`.
fn record_index_from_entry(e: &ChunkEntry, chunk_id: &str) -> RecordIndexEntry {
    RecordIndexEntry {
        commit_version: e.commit_version,
        deleted: e.deleted,
        chunk_id: chunk_id.to_string(),
        schema_version: e.schema_version,
        txn_id: e.txn_id.clone(),
        updated_ms: Utc::now().timestamp_millis(),
    }
}

/// Build a `VersionRecord` from the latest entry in a chunk. The
/// `latest_backend_key` carries the chunk's S3 key — `TBL_VERSIONS`
/// is now a per-record pointer into the chunk world.
fn version_record_from_entry(
    e: &ChunkEntry,
    chunk_s3_key: &str,
    etag: &str,
    size: u64,
) -> VersionRecord {
    VersionRecord {
        latest_version: xtable_core::Version(e.commit_version),
        latest_etag: etag.to_string(),
        latest_backend_key: chunk_s3_key.to_string(),
        last_writer_txn_id: e.txn_id.clone(),
        tombstone: e.deleted,
        size,
        last_modified_unix_ms: Utc::now().timestamp_millis(),
    }
}

/// Build a `VersionEntry` from the latest entry in a chunk. Same as
/// the legacy version-record semantics, except `backend_key` now
/// carries the chunk's s3_key.
fn version_entry_from_entry(
    e: &ChunkEntry,
    chunk_s3_key: &str,
    etag: &str,
    size: u64,
) -> VersionEntry {
    VersionEntry {
        commit_version: e.commit_version,
        etag: etag.to_string(),
        backend_key: chunk_s3_key.to_string(),
        txn_id: e.txn_id.clone(),
        size,
        content_type: e.content_type.clone(),
        user_meta: e.user_meta.clone(),
        deleted: e.deleted,
        created_ms: Utc::now().timestamp_millis(),
    }
}

/// Construct the per-record key used for both `TBL_VERSIONS` and
/// `TBL_VERSION_CHAINS`. Format: `_xtable/{space}/{table}/{record_id}`.
fn record_key(space: &str, table: &str, record_id: &str) -> String {
    format!("_xtable/{}/{}/{}", space, table, record_id)
}

/// Run cold rebuild into the provided LocalStore (spec §5.5).
#[tracing::instrument(level = "info", name = "tx.rebuild", skip_all, err)]
pub async fn rebuild(
    store: &LocalStore,
    backend: &xtable_backend::BackendClient,
) -> Result<RebuildReport, xtable_core::XtableError> {
    let _timed = Timed::new(
        &metrics().rebuild_cold_duration,
        vec![KeyValue::new("op", "rebuild")],
    );
    info!("starting cold rebuild from chunk objects");

    let objects = backend.list_objects().await.map_err(|e| {
        xtable_core::XtableError::Backend(format!(
            "cold rebuild failed: backend unreachable: {}",
            e
        ))
    })?;

    let mut report = RebuildReport::default();
    report.objects_scanned = objects.len();

    // Per-chunk bookkeeping for TBL_CHUNK_INDEX. Keyed by chunk_id
    // because `TBL_CHUNK_INDEX` is chunk_id → ChunkIndexEntry.
    let mut chunk_entries: Vec<(String, ChunkIndexEntry)> = Vec::new();

    // Per-record bookkeeping. The value carries everything needed
    // to write TBL_RECORD_INDEX, TBL_VERSIONS, and TBL_VERSION_CHAINS.
    struct PerKey {
        commit_version: u64,
        chunk_s3_key: String,
        chunk_id: String,
        entry: ChunkEntry,
        size: u64,
        etag: String,
    }
    struct HistoricalRecord {
        key: (String, String, String),
        entry: ChunkEntry,
        chunk_s3_key: String,
        chunk_id: String,
        etag: String,
    }
    let mut per_key: HashMap<(String, String, String), PerKey> = HashMap::new();
    let mut historical: Vec<HistoricalRecord> = Vec::new();
    let mut max_v: u64 = 0;

    for lo in objects {
        if !lo.key.ends_with(".xtc") {
            // Non-chunk object — schema documents, etc. Skip; the
            // structured rebuild only restores chunks.
            continue;
        }

        let bytes = match backend.get_object(&ObjectKey::new(&lo.key)).await {
            Ok(r) => r.bytes,
            Err(e) => {
                warn!(key = %lo.key, error = %e, "chunk fetch failed; skipping");
                continue;
            }
        };

        let header = match ChunkHeader::decode(&bytes) {
            Ok((h, _)) => h,
            Err(e) => {
                warn!(key = %lo.key, error = %e, "chunk header decode failed; skipping");
                continue;
            }
        };

        let body = match decompress_body(&bytes) {
            Ok(b) => b,
            Err(e) => {
                warn!(key = %lo.key, error = %e, "chunk body decompress failed; skipping");
                continue;
            }
        };

        let entries = match decode_body_entries(&body, header.record_count) {
            Ok(es) => es,
            Err(e) => {
                warn!(key = %lo.key, error = %e, "chunk entry decode failed; skipping");
                continue;
            }
        };

        // cv_min / cv_max come from the entries, not the header. The
        // header's record_count is informational; per-entry CV is the
        // authoritative source for the version range.
        let (cv_min, cv_max) = entries.iter().fold((u64::MAX, 0u64), |(lo, hi), e| {
            (lo.min(e.commit_version), hi.max(e.commit_version))
        });

        // ChunkIndexEntry: S3 key, key min/max, CV range, WAL range,
        // status. We don't have the embedded bloom here without
        // re-parsing the footer; leave it None — rebuild callers
        // always re-fetch on first read anyway, so a missing bloom
        // just means the first read downloads + re-parses the chunk
        // (same as any cold-start).
        let chunk_entry = ChunkIndexEntry {
            s3_key: lo.key.clone(),
            space: header.space.clone(),
            table: header.table.clone(),
            shard: 0, // not authoritative from the header; full read will populate if needed
            key_min: header.key_min.clone(),
            key_max: header.key_max.clone(),
            commit_version_min: cv_min,
            commit_version_max: cv_max,
            wal_seq_first: entries.first().map(|e| e.wal_seq).unwrap_or(0),
            wal_seq_last: entries.last().map(|e| e.wal_seq).unwrap_or(0),
            sha256_body: String::new(),
            size_bytes: lo.size,
            etag: lo.etag.clone(),
            bloom: None,
            flushed_at_ms: header.created_at_ms,
            status: ChunkStatus::Live,
        };
        chunk_entries.push((header.chunk_id.clone(), chunk_entry));

        for e in entries {
            max_v = max_v.max(e.commit_version);
            let key = (e.space.clone(), e.table.clone(), e.record_id.clone());
            let size = e.value.len() as u64;
            historical.push(HistoricalRecord {
                key: key.clone(),
                entry: e.clone(),
                chunk_s3_key: lo.key.clone(),
                chunk_id: header.chunk_id.clone(),
                etag: lo.etag.clone(),
            });
            per_key
                .entry(key)
                .and_modify(|cur| {
                    if e.commit_version > cur.commit_version {
                        cur.commit_version = e.commit_version;
                        cur.chunk_s3_key = lo.key.clone();
                        cur.chunk_id = header.chunk_id.clone();
                        cur.entry = e.clone();
                        cur.size = size;
                        cur.etag = lo.etag.clone();
                    }
                })
                .or_insert(PerKey {
                    commit_version: e.commit_version,
                    chunk_s3_key: lo.key.clone(),
                    chunk_id: header.chunk_id.clone(),
                    entry: e,
                    size,
                    etag: lo.etag.clone(),
                });
        }
    }

    // Build latest-row updates and preserve every historical record version.
    let mut version_updates: Vec<(ObjectKey, VersionRecord)> = Vec::with_capacity(per_key.len());
    let mut record_updates: Vec<((String, String, String), RecordIndexEntry)> =
        Vec::with_capacity(per_key.len());
    let mut chain_entries: Vec<(String, VersionEntry, u64)> = Vec::with_capacity(historical.len());
    let mut historical_updates: Vec<((String, String, String), RecordIndexEntry, Vec<u8>)> =
        Vec::with_capacity(historical.len());

    historical.sort_by(|a, b| {
        a.key
            .cmp(&b.key)
            .then(a.entry.commit_version.cmp(&b.entry.commit_version))
    });
    for h in &historical {
        let index_entry = record_index_from_entry(&h.entry, &h.chunk_id);
        historical_updates.push((h.key.clone(), index_entry, h.entry.value.clone()));
        chain_entries.push((
            record_key(&h.key.0, &h.key.1, &h.key.2),
            version_entry_from_entry(
                &h.entry,
                &h.chunk_s3_key,
                &h.etag,
                h.entry.value.len() as u64,
            ),
            u64::MAX, // cold rebuild: never conflicts with prior state
        ));
    }

    for ((space, table, record_id), k) in per_key {
        let rkey = record_key(&space, &table, &record_id);
        let vrec = version_record_from_entry(&k.entry, &k.chunk_s3_key, &k.etag, k.size);
        version_updates.push((ObjectKey::new(&rkey), vrec));
        let rid = record_index_from_entry(&k.entry, &k.chunk_id);
        record_updates.push(((space, table, record_id), rid));
    }
    report.versions_rebuilt = historical_updates.len();

    // Persist: chunk index, then record index, then versions / chain.
    // Each goes in its own redb txn so a failure in one doesn't poison
    // the others (cold rebuild is best-effort by design).
    for (chunk_id, entry) in &chunk_entries {
        store.put_chunk_index(chunk_id, entry)?;
    }
    for ((space, table, record_id), entry) in &record_updates {
        store.put_record_index(space, table, record_id, entry)?;
    }
    if !version_updates.is_empty() {
        store.put_versions_bulk(&version_updates)?;
    }
    if !historical_updates.is_empty() {
        store.put_record_versions_bulk(&historical_updates)?;
    }
    if !chain_entries.is_empty() {
        store.append_chain_entries_bulk(&chain_entries)?;
    }

    // Set global_version counter so future commits allocate > max_v.
    store.with_write(|txn| {
        use xtable_storage::cf::{meta_key, TBL_META};
        let mut meta = txn
            .open_table(TBL_META)
            .map_err(|e| xtable_core::XtableError::Storage(e.to_string()))?;
        meta.insert(meta_key::GLOBAL_VERSION, max_v)
            .map_err(|e| xtable_core::XtableError::Storage(e.to_string()))?;
        Ok(())
    })?;

    info!(
        scanned = report.objects_scanned,
        rebuilt = report.versions_rebuilt,
        orphans = report.orphans_deleted,
        max_v,
        "cold rebuild done"
    );
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn rebuild_no_objects_is_noop() {
        let tmp = TempDir::new().unwrap();
        let store = LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap();
        let backend = xtable_backend::BackendClient::dummy_for_test_async()
            .await
            .unwrap();
        let r = rebuild(&store, &backend).await.unwrap();
        assert_eq!(r.objects_scanned, 0);
        assert_eq!(r.versions_rebuilt, 0);
        assert_eq!(r.orphans_deleted, 0);
    }
}
