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
    pub version_at_read: u64,
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