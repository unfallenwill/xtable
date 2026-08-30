//! Cahill cycle detection — pure function over edges.
//!
//! The SI lock manager does the heavy lifting of edge bookkeeping; this
//! module contains the **standalone** cycle-detection algorithm used
//! by commit-time validation.
//!
//! ## Algorithm
//!
//! A "dangerous structure" exists between txn T and peer P when:
//!
//! 1. T has an in-edge from P (T depends on P's prior write), AND
//! 2. T has an out-edge to P (P depends on T's prior read, or T depends
//!    on P's upcoming write — the dual form).
//!
//! Both must point at the same peer. When this happens, serializing
//! the two txns would require a circular ordering — at least one must
//! abort.
//!
//! ## Tie-breaker
//!
//! The txn with the **lexicographically larger `txn_id`** aborts (ULIDs
//! are monotonic, so this approximates "newer loser"). This is
//! deterministic — tests can pin behavior.

use std::collections::HashSet;

use xtable_storage::{SIEdge, SIEdgeSet};

/// Returns `Some(peer_txn_id)` if a 2-cycle is detected for
/// `self_txn`, else `None`.
pub fn detect_dangerous_structure(
    self_txn: &str,
    in_edges: &SIEdgeSet,
    out_edges: &SIEdgeSet,
) -> Option<String> {
    // 1. Collect peers with in-edges to self.
    let conflictors: HashSet<&str> = in_edges
        .edges
        .iter()
        .filter(|e| e.peer != self_txn)
        .map(|e| e.peer.as_str())
        .collect();

    if conflictors.is_empty() {
        return None;
    }

    // 2. Find any out-edge whose peer is a conflictor.
    let colliding: Vec<&str> = out_edges
        .edges
        .iter()
        .filter(|e| conflictors.contains(e.peer.as_str()))
        .map(|e| e.peer.as_str())
        .collect();

    if colliding.is_empty() {
        return None;
    }

    // 3. Tie-breaker: deterministic abort of the side whose txn_id
    //    is lexicographically larger.
    let max_peer = colliding
        .iter()
        .max_by(|a, b| a.cmp(b))
        .copied()
        .unwrap_or("");
    if self_txn > max_peer {
        // Self is the loser.
        Some(self_txn.to_string())
    } else {
        // Peer is the loser; self proceeds.
        None
    }
}

/// Build an `SIEdgeSet` from raw edges (test convenience).
pub fn edge_set(edges: Vec<SIEdge>) -> SIEdgeSet {
    SIEdgeSet { edges }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtable_storage::PeerAction;

    fn edge(peer: &str, action: PeerAction, key: &str, v: u64) -> SIEdge {
        SIEdge {
            peer: peer.to_string(),
            key: key.to_string(),
            peer_action: action,
            version: v,
        }
    }

    #[test]
    fn no_in_edges_no_cycle() {
        let in_edges = edge_set(vec![]);
        let out_edges = edge_set(vec![edge("T2", PeerAction::Read, "k", 5)]);
        assert_eq!(detect_dangerous_structure("T1", &in_edges, &out_edges), None);
    }

    #[test]
    fn no_out_edges_no_cycle() {
        let in_edges = edge_set(vec![edge("T2", PeerAction::Wrote, "k", 5)]);
        let out_edges = edge_set(vec![]);
        assert_eq!(detect_dangerous_structure("T1", &in_edges, &out_edges), None);
    }

    #[test]
    fn cycle_detected_returns_colliding_peer() {
        // Symmetric cycle detection: T1 detects the 2-cycle and returns
        // T2 (the colliding peer). T2 will detect the same cycle from its
        // own commit pass.
        let in_edges = edge_set(vec![edge("T2", PeerAction::Wrote, "k", 5)]);
        let out_edges = edge_set(vec![edge("T2", PeerAction::Read, "k", 5)]);
        assert_eq!(
            detect_dangerous_structure("T1", &in_edges, &out_edges),
            Some("T2".to_string())
        );
    }

    #[test]
    fn cycle_detected_returns_colliding_peer_when_inverted() {
        // Mirror case — same cycle from T2's POV, should return T1.
        let in_edges = edge_set(vec![edge("T1", PeerAction::Wrote, "k", 5)]);
        let out_edges = edge_set(vec![edge("T1", PeerAction::Read, "k", 5)]);
        assert_eq!(
            detect_dangerous_structure("T2", &in_edges, &out_edges),
            Some("T1".to_string())
        );
    }

    #[test]
    fn multiple_peers_picks_max() {
        let in_edges = edge_set(vec![
            edge("TA", PeerAction::Wrote, "k1", 5),
            edge("TC", PeerAction::Wrote, "k2", 5),
        ]);
        let out_edges = edge_set(vec![
            edge("TB", PeerAction::Read, "k3", 5),
            edge("TC", PeerAction::Read, "k2", 5),
        ]);
        // Self = "T1", conflicting peers = TA, TC. Out has TC. Max = TC.
        // Self "T1" < "TC", so self proceeds → None.
        assert_eq!(detect_dangerous_structure("T1", &in_edges, &out_edges), None);
        // Self = "TZ" (largest), conflicts with TC. Self loses → Some("TZ").
        assert_eq!(
            detect_dangerous_structure("TZ", &in_edges, &out_edges),
            Some("TZ".to_string())
        );
    }
}