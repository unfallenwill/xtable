//! WAL record types and (de)serialization.

use serde::{Deserialize, Serialize};

/// A WAL record type. Each state transition in a transaction's lifecycle
/// produces one record. The order of records in `TBL_WAL` is the ground
/// truth for crash recovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalRecord {
    /// Transaction started.
    Begin {
        txn_id: String,
        snapshot_version: u64,
        idempotency_key: Option<String>,
    },
    /// A staged write (within BeginTxn scope, before CommitTxn).
    Stage {
        txn_id: String,
        key: String,
        body_handle: Option<String>,
        version_at_read: u64,
    },
    /// OCC validation passed; about to upload + commit.
    ValidateOk {
        txn_id: String,
        write_keys: Vec<String>,
    },
    /// Backend S3 uploads completed; about to publish versions.
    Committing {
        txn_id: String,
        upload_keys: Vec<String>,
    },
    /// Terminal committed state.
    Committed {
        txn_id: String,
        commit_version: u64,
    },
    /// Terminal result record (covers both Committed and Aborted outcomes).
    CommitResult {
        txn_id: String,
        commit_version: u64,
        success: bool,
    },
    /// Transaction aborted.
    Aborted {
        txn_id: String,
        reason: String,
    },
}

impl WalRecord {
    pub fn txn_id(&self) -> &str {
        match self {
            Self::Begin { txn_id, .. }
            | Self::Stage { txn_id, .. }
            | Self::ValidateOk { txn_id, .. }
            | Self::Committing { txn_id, .. }
            | Self::Committed { txn_id, .. }
            | Self::CommitResult { txn_id, .. }
            | Self::Aborted { txn_id, .. } => txn_id,
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::CommitResult { .. } | Self::Aborted { .. })
    }
}

/// Encode a monotonic sequence number for the WAL key.
pub fn encode_seq(n: u64) -> [u8; 8] {
    n.to_be_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn txn_id_extraction_works_for_all_variants() {
        let records = vec![
            WalRecord::Begin {
                txn_id: "T1".into(),
                snapshot_version: 0,
                idempotency_key: None,
            },
            WalRecord::Stage {
                txn_id: "T1".into(),
                key: "k".into(),
                body_handle: None,
                version_at_read: 0,
            },
            WalRecord::ValidateOk {
                txn_id: "T1".into(),
                write_keys: vec![],
            },
            WalRecord::Committing {
                txn_id: "T1".into(),
                upload_keys: vec![],
            },
            WalRecord::Committed {
                txn_id: "T1".into(),
                commit_version: 5,
            },
            WalRecord::CommitResult {
                txn_id: "T1".into(),
                commit_version: 5,
                success: true,
            },
            WalRecord::Aborted {
                txn_id: "T1".into(),
                reason: "x".into(),
            },
        ];
        for r in &records {
            assert_eq!(r.txn_id(), "T1");
        }
    }

    #[test]
    fn is_terminal_correct() {
        assert!(WalRecord::CommitResult {
            txn_id: "t".into(),
            commit_version: 1,
            success: true
        }
        .is_terminal());
        assert!(WalRecord::Aborted {
            txn_id: "t".into(),
            reason: "r".into()
        }
        .is_terminal());
        assert!(!WalRecord::Begin {
            txn_id: "t".into(),
            snapshot_version: 0,
            idempotency_key: None
        }
        .is_terminal());
    }

    #[test]
    fn wal_record_roundtrips_bincode() {
        let r = WalRecord::CommitResult {
            txn_id: "T1".into(),
            commit_version: 42,
            success: true,
        };
        let bytes = bincode::serialize(&r).unwrap();
        let back: WalRecord = bincode::deserialize(&bytes).unwrap();
        assert_eq!(r, back);
    }
}