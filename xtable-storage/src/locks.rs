//! Pure types for Cahill Serializable Snapshot Isolation (SSI) locks
//! and edges.
//!
//! These types are shared between `xtable-storage` (persists them in
//! redb) and `xtable-tx` (the in-memory lock manager + cycle detection).
//!
//! ## Algorithm reference
//!
//! Cahill, Fekete, Liarokapis, O'Neil, O'Neil,
//! "Serializable Snapshot Isolation in PostgreSQL", VLDB 2008.
//!
//! Each transaction tracks SIRead locks (one per key read, with the
//! version observed) and SIWrite locks (one per key written, with the
//! version to write). Edges capture rw-antidependencies between txns.
//! Cycle detection at commit time finds "dangerous structures" — two
//! txns with both an in-edge and an out-edge between them — and aborts
//! the txn whose `txn_id` is lexicographically larger (the "newer"
//! loser).

use serde::{Deserialize, Serialize};

pub type TxnId = String;
pub type ObjectKey = String;
pub type EdgeKey = String;

/// One SIRead lock: T read key at `version_observed`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SIReadLock {
    pub key: ObjectKey,
    pub version_observed: u64,
    pub etag_observed: String,
}

/// One SIWrite lock: T intends to write key at `version_to_write`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SIWriteLock {
    pub key: ObjectKey,
    pub version_to_write: u64,
}

/// What the peer did — used to disambiguate rw-antidependency direction.
///
///
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeerAction {
    /// Peer read the key at the listed version. This txn wrote the key
    /// later, so the edge is `peer → this` (peer depends on this txn's
    /// future write).
    Read,
    /// Peer wrote the key at the listed version. This txn read the key
    /// earlier, so the edge is `this → peer` (this txn depends on
    /// peer's past write).
    Wrote,
}

/// An rw-antidependency edge.
///
/// One `SIEdge` is stored on **each side** of the relationship: the
/// peer's action is encoded uniformly, and the txn holding the edge
/// infers its own direction from `peer_action` (Read = out-edge,
/// Wrote = in-edge).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SIEdge {
    pub peer: TxnId,
    pub key: ObjectKey,
    pub peer_action: PeerAction,
    pub version: u64,
}

impl SIEdge {
    pub fn contains_in_edge_from(&self, candidate_peer: &str) -> bool {
        self.peer == candidate_peer && matches!(self.peer_action, PeerAction::Wrote)
    }
}

/// Set of edges on one txn (either `in_edges` or `out_edges`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SIEdgeSet {
    pub edges: Vec<SIEdge>,
}

impl SIEdgeSet {
    pub fn contains_in_edge_from(&self, peer: &str) -> bool {
        self.edges
            .iter()
            .any(|e| e.contains_in_edge_from(peer))
    }

    pub fn add(&mut self, e: SIEdge) {
        self.edges.push(e);
    }

    pub fn len(&self) -> usize {
        self.edges.len()
    }

    pub fn is_empty(&self) -> bool {
        self.edges.is_empty()
    }
}

/// Per-txn lock state — held in memory by the lock manager, mirrored
/// to redb for crash safety.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SiTxnLocks {
    pub reads: Vec<SIReadLock>,
    pub writes: Vec<SIWriteLock>,
    pub in_edges: SIEdgeSet,
    pub out_edges: SIEdgeSet,
    pub snapshot_version: u64,
    pub status: SiTxnPhase,
}

/// Lifecycle phase of an SI txn (independent of `TxnStatus` which is the
/// commit-protocol state machine).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum SiTxnPhase {
    #[default]
    Active,
    Committed,
    Aborted,
}

/// One entry in the "recently committed" rolling window. After a txn
/// commits, its locks are kept for `RECENT_WINDOW` commit_versions so
/// concurrent commits can still detect dangerous structures involving
/// this txn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecentlyCommittedTxn {
    pub txn_id: TxnId,
    pub keys: Vec<ObjectKey>,
    pub commit_version: u64,
    pub committed_at_ms: i64,
}

/// One row of `TBL_SI_IN_EDGES_BY_TJ`: index from peer → list of txns that
/// hold an in-edge from this peer. Stored as a small u32 counter for v1
/// (the txn-side rebuild walks `TBL_SI_EDGES` for details).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InEdgeSummary {
    pub count: u32,
}

/// Direction of an `SIEdge` row in `TBL_SI_EDGES` — purely for schema
/// disambiguation when reading back the audit trail.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EdgeDirection {
    In,
    Out,
}

impl EdgeDirection {
    pub fn as_str(self) -> &'static str {
        match self {
            EdgeDirection::In => "in",
            EdgeDirection::Out => "out",
        }
    }
}

/// Rolling window for cycle detection. With 1k commits/sec, this lets
/// us catch write-skew that requires 2 commit steps.
pub const RECENT_WINDOW: u64 = 1024;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edge_in_vs_out_directions() {
        // T1 writes X at v=10; T2 reads X at v=10.
        // From T1: out-edge (peer=T2, peer_action=Read).
        let e1 = SIEdge {
            peer: "T2".into(),
            key: "k1".into(),
            peer_action: PeerAction::Read,
            version: 10,
        };
        // T1's out-edge set contains T2's read; this is an out-edge for T1.
        assert!(!e1.contains_in_edge_from("T2"));
        // T2 holds the mirror: in-edge (peer=T1, peer_action=Wrote).
        let e2 = SIEdge {
            peer: "T1".into(),
            key: "k1".into(),
            peer_action: PeerAction::Wrote,
            version: 10,
        };
        // From T2's view, this is an in-edge from T1.
        assert!(e2.contains_in_edge_from("T1"));
    }

    #[test]
    fn edge_set_contains() {
        let mut s = SIEdgeSet::default();
        assert!(!s.contains_in_edge_from("T1"));
        s.add(SIEdge {
            peer: "T1".into(),
            key: "k".into(),
            peer_action: PeerAction::Wrote,
            version: 5,
        });
        assert!(s.contains_in_edge_from("T1"));
        assert!(!s.contains_in_edge_from("T2"));
    }

    #[test]
    fn structs_roundtrip_bincode() {
        let locks = SiTxnLocks {
            reads: vec![SIReadLock {
                key: "k1".into(),
                version_observed: 7,
                etag_observed: "etag".into(),
            }],
            writes: vec![SIWriteLock {
                key: "k2".into(),
                version_to_write: 8,
            }],
            in_edges: SIEdgeSet::default(),
            out_edges: SIEdgeSet {
                edges: vec![SIEdge {
                    peer: "Tx".into(),
                    key: "k3".into(),
                    peer_action: PeerAction::Read,
                    version: 6,
                }],
            },
            snapshot_version: 6,
            status: SiTxnPhase::Active,
        };
        let bytes = bincode::serialize(&locks).unwrap();
        let back: SiTxnLocks = bincode::deserialize(&bytes).unwrap();
        assert_eq!(locks, back);
    }
}