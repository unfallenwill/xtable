//! Cahill cycle detection — pure function over edges.
//!
//! Mirrors `SiLockManager::find_dangerous_structure` so the same
//! commit-time call site can be tested without instantiating the full
//! lock manager. Both implementations return the colliding peer (not
//! a tie-broken loser); the caller is expected to treat any `Some(_)`
//! as "abort self on this commit pass".
//!
//! ## Algorithm
//!
//! A "dangerous structure" exists between txn T and peer P when:
//!
//! 1. T has an in-edge from P (T depends on P's prior write), AND
//! 2. T has an out-edge to P (T depends on P's upcoming write — the
//!    dual form).
//!
//! Both must point at the same peer. When this happens, serializing
//! the two txns would require a circular ordering — at least one must
//! abort. The deterministic tie-break between the two is left to the
//! caller.

use std::collections::HashSet;

use xtable_storage::{SIEdge, SIEdgeSet};

/// Returns `Some(peer_txn_id)` if a 2-cycle is detected for
/// `self_txn`, else `None`.
///
/// Symmetric: if T1 has a 2-cycle with T2, both T1 and T2's
/// `detect_dangerous_structure` calls return Some(peer). The caller
/// treats any `Some(_)` as "abort self on commit" — symmetric abort
/// across the cycle is over-cautious but correct (serializability
/// is preserved as long as at least one side aborts).
pub fn detect_dangerous_structure(
    self_txn: &str,
    in_edges: &SIEdgeSet,
    out_edges: &SIEdgeSet,
) -> Option<String> {
    let in_peers: HashSet<&str> = in_edges
        .edges
        .iter()
        .filter(|e| e.peer != self_txn)
        .map(|e| e.peer.as_str())
        .collect();
    if in_peers.is_empty() {
        return None;
    }
    for e in &out_edges.edges {
        if in_peers.contains(e.peer.as_str()) {
            return Some(e.peer.clone());
        }
    }
    None
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
        assert_eq!(
            detect_dangerous_structure("T1", &in_edges, &out_edges),
            None
        );
    }

    #[test]
    fn no_out_edges_no_cycle() {
        let in_edges = edge_set(vec![edge("T2", PeerAction::Wrote, "k", 5)]);
        let out_edges = edge_set(vec![]);
        assert_eq!(
            detect_dangerous_structure("T1", &in_edges, &out_edges),
            None
        );
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
    fn multiple_peers_returns_first_colliding_peer() {
        // In-edges: TA wrote k1, TC wrote k2. Out-edges: TB read k3,
        // TC read k2. The colliding peer set is {TC} (TA and TB do not
        // intersect). The function returns the first matching peer;
        // the caller decides who actually aborts.
        let in_edges = edge_set(vec![
            edge("TA", PeerAction::Wrote, "k1", 5),
            edge("TC", PeerAction::Wrote, "k2", 5),
        ]);
        let out_edges = edge_set(vec![
            edge("TB", PeerAction::Read, "k3", 5),
            edge("TC", PeerAction::Read, "k2", 5),
        ]);
        assert_eq!(
            detect_dangerous_structure("T1", &in_edges, &out_edges),
            Some("TC".to_string())
        );
        assert_eq!(
            detect_dangerous_structure("TZ", &in_edges, &out_edges),
            Some("TC".to_string())
        );
    }
}
