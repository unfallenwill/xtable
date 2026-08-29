//! Internal DTOs between S3 handlers and the tx/storage/backend layers.

use chrono::Utc;
use std::collections::HashMap;

use xtable_core::{ObjectKey, Version};
use xtable_storage::VersionRecord;

/// Helpers to construct VersionRecords.
pub fn new_version_record(
    version: Version,
    etag: String,
    backend_key: String,
    writer_txn_id: String,
    size: u64,
) -> VersionRecord {
    VersionRecord {
        latest_version: version,
        latest_etag: etag,
        latest_backend_key: backend_key,
        last_writer_txn_id: writer_txn_id,
        tombstone: false,
        size,
        last_modified_unix_ms: Utc::now().timestamp_millis(),
    }
}

/// Helper: convert xtable metadata map → backend S3 metadata map.
pub fn to_backend_metadata(meta: &HashMap<String, String>) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for (k, v) in meta {
        out.insert(k.clone(), v.clone());
    }
    out
}

/// Helper: parse xtable key from request.
pub fn parse_object_key(s: &str) -> ObjectKey {
    ObjectKey::new(s)
}