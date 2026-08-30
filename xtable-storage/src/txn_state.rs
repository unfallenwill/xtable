//! TxnState and related record types persisted in redb.

use serde::{Deserialize, Serialize};
use xtable_core::headers::TxnStatus;

/// Per-transaction state record (stored in TBL_TXN_STATE).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TxnStateRecord {
    pub status: TxnStatus,
    pub snapshot_version: u64,
    pub started_at_ms: i64,
    pub last_heartbeat_ms: i64,
    pub idempotency_key: Option<String>,
    pub read_keys: Vec<String>,
    pub write_keys: Vec<String>,
    pub alloc_versions: Vec<(String, u64)>,
    pub uploaded_keys: Vec<String>,
}

impl TxnStateRecord {
    pub fn new_active(snapshot_version: u64, idempotency_key: Option<String>, now_ms: i64) -> Self {
        Self {
            status: TxnStatus::Active,
            snapshot_version,
            started_at_ms: now_ms,
            last_heartbeat_ms: now_ms,
            idempotency_key,
            read_keys: Vec::new(),
            write_keys: Vec::new(),
            alloc_versions: Vec::new(),
            uploaded_keys: Vec::new(),
        }
    }
}

/// ReadSet entry: per (txn_id, key) — what version was observed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadSetEntry {
    pub version_observed: u64,
    pub etag_observed: String,
}

/// WriteSet entry: per (txn_id, key) — staged write metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteSetEntry {
    pub backend_key: String,
    pub body_handle: Option<String>,
    pub inline_body: Option<Vec<u8>>,
    pub size: u64,
    pub content_type: Option<String>,
    pub user_meta: Vec<(String, String)>,
    /// True if this staged write is a delete (tombstone). Fix for V10.
    #[serde(default)]
    pub deleted: bool,
}

/// BlobRecord: spill file metadata for staged body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobRecord {
    pub path: String,
    pub size: u64,
    pub sha256: String,
    pub created_at_ms: i64,
}

/// MultipartState: per in-flight multipart upload (Phase 3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultipartState {
    pub upload_id: String,
    pub key: String,
    pub backend_upload_id: String,
    pub parts: Vec<(i32, String, u64)>, // (part_number, etag, size)
    pub txn_id: Option<String>,        // None = non-transactional
}

/// Record index entry — used by structured-data-space to enumerate records
/// without scanning the full object store. Holds enough state to reconstruct
/// a snapshot view at any commit_version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordIndexEntry {
    /// The commit_version at which this index entry became valid (= commit
    /// version of the chain append). Monotonic per (space, table, record).
    pub commit_version: u64,
    /// Tombstone flag — write removed the record.
    pub deleted: bool,
    /// Backend S3 key holding the record's JSON body.
    pub backend_key: String,
    /// Schema version this record conforms to.
    pub schema_version: u32,
    /// Txn that produced this entry.
    pub txn_id: String,
    pub updated_ms: i64,
}

/// Schema index entry — latest known schema document for a (space, name).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaIndexEntry {
    /// Latest schema version registered (starts at 1).
    pub latest_version: u32,
    /// Backend S3 key of the latest schema document. Body is JSON Schema.
    pub latest_backend_key: String,
    pub registered_ms: i64,
}

/// Internal on-disk shape of a `TBL_RECORD_INDEX` row: the index meta plus
/// an inline copy of the record's JSON body (serialized as a string of
/// JSON text — bincode serde does not reliably roundtrip `serde_json::Value`).
/// The body is included so list / query operations don't need to round-trip
/// the backend for every row in v1; once secondary indexes exist the body
/// can be lazily fetched.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredRecord {
    pub entry: RecordIndexEntry,
    /// Pre-serialized JSON text of the body. Empty string = tombstone.
    pub body_json: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn txn_state_record_default() {
        let r = TxnStateRecord::new_active(5, None, 0);
        assert_eq!(r.snapshot_version, 5);
        assert_eq!(r.status, TxnStatus::Active);
        assert!(r.write_keys.is_empty());
    }

    #[test]
    fn txn_state_record_roundtrips_bincode() {
        let r = TxnStateRecord {
            status: TxnStatus::Committed,
            snapshot_version: 10,
            started_at_ms: 1,
            last_heartbeat_ms: 2,
            idempotency_key: Some("k".into()),
            read_keys: vec!["a".into(), "b".into()],
            write_keys: vec!["c".into()],
            alloc_versions: vec![("c".into(), 11)],
            uploaded_keys: vec!["c".into()],
        };
        let bytes = bincode::serialize(&r).unwrap();
        let back: TxnStateRecord = bincode::deserialize(&bytes).unwrap();
        assert_eq!(r, back);
    }
}