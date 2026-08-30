//! WAL record types and (de)serialization.
//!
//! The WAL carries per-transaction lifecycle events plus per-memtable
//! flush events. Each state transition in a transaction's lifecycle
//! produces one record. The order of records in `TBL_WAL` is the ground
//! truth for crash recovery.
//!
//! ## MVCC + SSI era (current)
//!
//! Two variant groups live here:
//! - **Legacy OCC-era** (`Begin` / `Stage` / `Committing` / `Committed` /
//!   `CommitResult` / `Aborted`) — kept for WAL format compatibility
//!   (recovery tests, log-replay tooling).
//! - **New MVCC+SSI-era** (`CahillEdge` / `Commit` / `MemtableFlushed`) —
//!   used by the live commit path.

use serde::{Deserialize, Serialize};

/// A WAL record type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalRecord {
    // ===== Legacy OCC-era variants (kept for WAL format compat; unused by commit path) =====

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

    // ===== New LSM-era variants (used by memtable + chunk flush pipeline) =====

    /// Cahill stage: a write has been added to the txn's in-memory SI
    /// edge set. Used by the LSM commit protocol (PR #4) and as a
    /// recovery audit trail.
    CahillEdge {
        txn_id: String,
        key: String,
        edges: Vec<StoredEdge>,
    },

    /// Transaction committed. Atomic with memtable publish + version
    /// chain append in the same redb write txn (PR #4).
    Commit {
        txn_id: String,
        commit_version: u64,
        write_keys: Vec<String>,
    },

    /// Memtable was flushed to S3. GC truncates WAL up to `up_to_seq`
    /// after this record lands.
    MemtableFlushed {
        chunk_id: String,
        up_to_seq: u64,
        up_to_commit_version: u64,
    },
}

/// Compact edge shadow data for the Cahill WAL audit trail. The
/// in-memory lock manager is the canonical source; this is for recovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredEdge {
    pub peer: String,
    pub key: String,
    pub peer_action: StoredPeerAction,
    pub version: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoredPeerAction {
    Read,
    Wrote,
}

impl WalRecord {
    /// Per-transaction id when this record refers to a specific txn.
    /// Returns `None` for global records like `MemtableFlushed` which
    /// describe a memtable-level event, not a per-txn one.
    pub fn txn_id(&self) -> Option<&str> {
        match self {
            Self::Begin { txn_id, .. }
            | Self::Stage { txn_id, .. }
            | Self::Committing { txn_id, .. }
            | Self::Committed { txn_id, .. }
            | Self::CommitResult { txn_id, .. }
            | Self::Aborted { txn_id, .. }
            | Self::CahillEdge { txn_id, .. }
            | Self::Commit { txn_id, .. } => Some(txn_id),
            Self::MemtableFlushed { .. } => None,
        }
    }

    /// Chunk id when this record carries one. PR-Fix2.3 split from
    /// `txn_id()` so that `MemtableFlushed` doesn't masquerade as a
    /// per-txn record in recovery.
    pub fn chunk_id(&self) -> Option<&str> {
        match self {
            Self::MemtableFlushed { chunk_id, .. } => Some(chunk_id),
            _ => None,
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Committed { .. }
                | Self::CommitResult { .. }
                | Self::Aborted { .. }
                | Self::Commit { .. }
        )
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
            WalRecord::CahillEdge {
                txn_id: "T1".into(),
                key: "k".into(),
                edges: vec![],
            },
            WalRecord::Commit {
                txn_id: "T1".into(),
                commit_version: 5,
                write_keys: vec![],
            },
            WalRecord::MemtableFlushed {
                chunk_id: "C1".into(),
                up_to_seq: 10,
                up_to_commit_version: 5,
            },
        ];
        // Per-txn records: txn_id() returns Some("T1").
        // (MemtableFlushed is a global record and returns None — tested separately below.)
        for (_i, r) in records[..8].iter().enumerate() {
            assert_eq!(r.txn_id(), Some("T1"));
        }
        // PR-Fix2.3: MemtableFlushed is a global record; txn_id returns None
        // and the chunk_id is exposed via the dedicated `chunk_id()` accessor.
        assert_eq!(records[8].txn_id(), None);
        assert_eq!(records[8].chunk_id(), Some("C1"));
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
        assert!(WalRecord::Commit {
            txn_id: "t".into(),
            commit_version: 1,
            write_keys: vec![],
        }
        .is_terminal());
        assert!(!WalRecord::Begin {
            txn_id: "t".into(),
            snapshot_version: 0,
            idempotency_key: None
        }
        .is_terminal());
        assert!(!WalRecord::CahillEdge {
            txn_id: "t".into(),
            key: "k".into(),
            edges: vec![],
        }
        .is_terminal());
        assert!(!WalRecord::MemtableFlushed {
            chunk_id: "c".into(),
            up_to_seq: 1,
            up_to_commit_version: 1,
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

    #[test]
    fn memtable_flushed_roundtrips() {
        let r = WalRecord::MemtableFlushed {
            chunk_id: "C01ABC".into(),
            up_to_seq: 1000,
            up_to_commit_version: 999,
        };
        let bytes = bincode::serialize(&r).unwrap();
        let back: WalRecord = bincode::deserialize(&bytes).unwrap();
        assert_eq!(r, back);
    }
}