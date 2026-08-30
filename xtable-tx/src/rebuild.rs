//! Cold rebuild: when redb is missing or corrupt, reconstruct the version
//! index from S3 object metadata.
//!
//! V1 fix: the previous logic deleted every object whose `txn_id` was not in
//! a Committed TxnState — but on a fresh redb there are no TxnState records,
//! so ALL objects were treated as orphans and deleted. The README claim
//! "redb loss → no data loss" was false.
//!
//! Correct semantics: each S3 object carries `x-amz-meta-xtable-version`,
//! which is the **single source of truth**. The latest version per key is
//! the new "current". We don't depend on txn_id having a matching TxnState
//! record at all.

use std::collections::HashMap;

use chrono::Utc;
use tracing::{info, warn};

use xtable_core::ObjectKey;
use xtable_storage::{LocalStore, VersionEntry, VersionRecord};

#[derive(Debug, Default, Clone, Copy)]
pub struct RebuildReport {
    pub objects_scanned: usize,
    pub versions_rebuilt: usize,
    pub orphans_deleted: usize,
}

/// Run cold rebuild into the provided LocalStore.
pub async fn rebuild(
    store: &LocalStore,
    backend: &xtable_backend::BackendClient,
) -> Result<RebuildReport, xtable_core::XtableError> {
    info!("starting cold rebuild from backend S3");

    // V14 fix: backend unreachability is now a fatal error. Returning
    // Ok(default) would let an empty store come up with global_version=0,
    // causing subsequent commits to overwrite real data at v1+.
    let objects = backend.list_objects().await.map_err(|e| {
        xtable_core::XtableError::Backend(format!(
            "cold rebuild failed: backend unreachable: {}", e
        ))
    })?;
    // value: (latest_version, latest_etag, size, backend_key)
    let mut per_key: HashMap<String, (u64, String, u64, String)> = HashMap::new();
    let mut report = RebuildReport::default();
    report.objects_scanned = objects.len();

    for lo in objects {
        let meta = match backend.head_object(&ObjectKey::new(&lo.key)).await {
            Ok(h) => h.user_metadata,
            Err(_) => HashMap::new(),
        };
        let backend_v = meta
            .get("x-amz-meta-xtable-version")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);

        if backend_v == 0 {
            // Object lacks xtable metadata — could be a stray. Skip.
            warn!(key = %lo.key, "object lacks xtable metadata; skipping");
            continue;
        }

        // V1 fix: NO txn_is_committed check. The version number alone is
        // sufficient to determine the latest version per key.
        per_key
            .entry(lo.key.clone())
            .and_modify(|e| {
                if backend_v > e.0 {
                    *e = (backend_v, lo.etag.clone(), lo.size, lo.key.clone());
                }
            })
            .or_insert((backend_v, lo.etag.clone(), lo.size, lo.key.clone()));
    }

    // Build version records + max global version.
    let mut max_v: u64 = 0;
    let mut updates: Vec<(ObjectKey, VersionRecord)> = Vec::with_capacity(per_key.len());
    let mut chain_entries: Vec<(String, VersionEntry)> = Vec::with_capacity(per_key.len());
    for (key, (v, etag, size, backend_key)) in per_key {
        max_v = max_v.max(v);
        let now_ms = Utc::now().timestamp_millis();
        let rec = VersionRecord {
            latest_version: xtable_core::Version(v),
            latest_etag: etag.clone(),
            latest_backend_key: backend_key.clone(),
            last_writer_txn_id: String::new(),
            tombstone: false,
            size,
            last_modified_unix_ms: now_ms,
        };
        updates.push((ObjectKey::new(&key), rec));
        // V1 fix: also rebuild the MVCC chain. Reads via read_chain /
        // read_at_snapshot need this; otherwise after a cold rebuild the
        // store knows the latest version but has no entry on the chain,
        // so every read returns None even for committed data.
        chain_entries.push((
            key.clone(),
            VersionEntry::new(v, etag, backend_key, String::new(), size),
        ));
        report.versions_rebuilt += 1;
    }
    if !updates.is_empty() {
        store.put_versions_bulk(&updates)?;
    }
    if !chain_entries.is_empty() {
        // Cold rebuild: use u64::MAX as snapshot so we never conflict
        // with prior state — the rebuilt chains reflect the S3 ground truth.
        let with_snapshot: Vec<(String, VersionEntry, u64)> = chain_entries
            .into_iter()
            .map(|(k, e)| (k, e, u64::MAX))
            .collect();
        store.append_chain_entries_bulk(&with_snapshot)?;
    }

    // Set global_version counter so future commits allocate > max_v.
    store.with_write(|txn| {
        use xtable_storage::cf::{meta_key, TBL_META};
        let mut meta = txn.open_table(TBL_META).map_err(|e| xtable_core::XtableError::Storage(e.to_string()))?;
        meta.insert(meta_key::GLOBAL_VERSION, max_v).map_err(|e| xtable_core::XtableError::Storage(e.to_string()))?;
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
        let backend = xtable_backend::BackendClient::dummy_for_test_async().await.unwrap();
        let r = rebuild(&store, &backend).await.unwrap();
        assert_eq!(r.objects_scanned, 0);
        assert_eq!(r.versions_rebuilt, 0);
        assert_eq!(r.orphans_deleted, 0);
    }
}