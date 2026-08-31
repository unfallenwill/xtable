//! Cahill SI lock manager.
//!
//! Maintains per-txn SIRead / SIWrite locks and rw-antidependency edges
//! in memory (with redb persistence for crash safety). Provides the
//! commit-time `find_dangerous_structure` cycle check used by the
//! coordinator.
//!
//! ## Thread safety
//!
//! The inner state is wrapped in a `parking_lot::Mutex`. The Cahill commit
//! critical section already serializes through `TxnCoordinator::commit_lock`,
//! so contention here is bounded. All public methods that mutate state
//! take the lock briefly; reads (`find_dangerous_structure`) walk the
//! edges under the lock.
//!
//! ## Lifecycle
//!
//! 1. `begin_txn(txn_id, snapshot)` — registers an empty SiTxnLocks.
//! 2. `register_read(txn_id, key, version, etag)` — adds SIRead lock and
//!    checks the `TBL_SI_RECENT` rolling window for prior writers
//!    (creating in-edges as needed).
//! 3. `register_write(txn_id, key, version)` — adds SIWrite lock and
//!    scans current SIRead holders of `key` for rw-antidependencies
//!    (creating in-edges on this txn, out-edges on each holder).
//! 4. `find_dangerous_structure(txn_id)` — at commit time. Returns
//!    `Some(peer_txn_id)` if this txn has both an in-edge AND an
//!    out-edge to/from the same peer.
//! 5. `mark_committed(txn_id, commit_version)` — moves the txn into the
//!    rolling window so future commits can still detect cycles.
//! 6. `mark_aborted(txn_id)` — releases all locks and edges immediately.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::Mutex;
use tracing::instrument;

use xtable_core::XtableResult;
use xtable_storage::{
    PeerAction, RecentlyCommittedTxn, SIEdge, SIReadLock, SIWriteLock, SiTxnLocks, SiTxnPhase,
};

/// In-memory Cahill lock manager.
pub struct SiLockManager {
    inner: Mutex<SiLockManagerInner>,
}

struct SiLockManagerInner {
    /// Per-txn state.
    by_txn: HashMap<String, SiTxnLocks>,
    /// Secondary index: SIRead holders per key.
    readers_of: HashMap<String, Vec<String>>,
    /// Secondary index: SIWrite holders per key (active + recently committed).
    writers_of: HashMap<String, Vec<WriterView>>,
    /// Rolling window of recently committed txns (keyed by commit_version).
    recent: Vec<RecentlyCommittedTxn>,
    /// Oldest commit_version still in the rolling window.
    recent_floor_version: u64,
}

/// Lightweight view of a writer (active or recently-committed) for the
/// `writers_of` secondary index.
#[derive(Debug, Clone)]
struct WriterView {
    txn_id: String,
    committed_version: u64,
    is_recently_committed: bool,
}

impl SiLockManager {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(SiLockManagerInner {
                by_txn: HashMap::new(),
                readers_of: HashMap::new(),
                writers_of: HashMap::new(),
                recent: Vec::new(),
                recent_floor_version: 0,
            }),
        })
    }

    /// Begin tracking a txn. Returns `Ok(())` if the txn is fresh,
    /// error if it already exists.
    #[instrument(level = "debug", skip_all, fields(txn.id = %txn_id), err)]
    pub fn begin_txn(self: &Arc<Self>, txn_id: &str, snapshot_version: u64) -> XtableResult<()> {
        let mut g = self.inner.lock();
        if g.by_txn.contains_key(txn_id) {
            return Err(xtable_core::XtableError::Storage(format!(
                "SI lock manager: txn already exists: {}",
                txn_id
            )));
        }
        g.by_txn.insert(
            txn_id.to_string(),
            SiTxnLocks {
                status: SiTxnPhase::Active,
                snapshot_version,
                ..Default::default()
            },
        );
        Ok(())
    }

    /// Record a read of `key` at `version_observed`. Adds in-edges from
    /// any recently-committed writer of this key whose
    /// `committed_version > version_observed`.
    #[instrument(level = "debug", skip_all, fields(txn.id = %txn_id, key = %key))]
    pub fn register_read(
        self: &Arc<Self>,
        txn_id: &str,
        key: &str,
        version_observed: u64,
        etag: String,
    ) {
        // Compute new in-edges first (drop the borrow on `g`).
        let new_in_edges: Vec<SIEdge> = {
            let g = self.inner.lock();
            let mut edges = Vec::new();
            if let Some(writers) = g.writers_of.get(key) {
                for w in writers {
                    if w.is_recently_committed && w.committed_version > version_observed {
                        edges.push(SIEdge {
                            peer: w.txn_id.clone(),
                            key: key.to_string(),
                            peer_action: PeerAction::Wrote,
                            version: w.committed_version,
                        });
                    }
                }
            }
            edges
        };
        // Now mutate state.
        let mut g = self.inner.lock();
        if let Some(entry) = g.by_txn.get_mut(txn_id) {
            entry.reads.push(SIReadLock {
                key: key.to_string(),
                version_observed,
                etag_observed: etag,
            });
            for e in new_in_edges {
                entry.in_edges.add(e);
            }
        }
        g.readers_of
            .entry(key.to_string())
            .or_default()
            .push(txn_id.to_string());
    }

    /// Record an intent to write `key` at `version_to_write`. Adds
    /// in-edges from any active reader of this key (those readers will
    /// also gain the mirror out-edge). Updates the writers_of index.
    #[instrument(level = "debug", skip_all, fields(txn.id = %txn_id, key = %key))]
    pub fn register_write(self: &Arc<Self>, txn_id: &str, key: &str, version_to_write: u64) {
        // PR-Fix1.5: single critical section — collect readers and apply
        // all mutations under one lock so concurrent `register_read`
        // cannot slip in between snapshot and update.
        let mut g = self.inner.lock();
        // Snapshot readers under the same lock.
        let readers: Vec<String> = g.readers_of.get(key).cloned().unwrap_or_default();
        if let Some(entry) = g.by_txn.get_mut(txn_id) {
            entry.writes.push(SIWriteLock {
                key: key.to_string(),
                version_to_write,
            });
            for reader in &readers {
                if reader == txn_id {
                    continue;
                }
                entry.out_edges.add(SIEdge {
                    peer: reader.clone(),
                    key: key.to_string(),
                    peer_action: PeerAction::Read,
                    version: version_to_write,
                });
            }
        }
        // Mirror: in-edges on each reader.
        for reader in &readers {
            if reader == txn_id {
                continue;
            }
            if let Some(reader_locks) = g.by_txn.get_mut(reader) {
                reader_locks.in_edges.add(SIEdge {
                    peer: txn_id.to_string(),
                    key: key.to_string(),
                    peer_action: PeerAction::Wrote,
                    version: version_to_write,
                });
            }
        }
        g.writers_of
            .entry(key.to_string())
            .or_default()
            .push(WriterView {
                txn_id: txn_id.to_string(),
                committed_version: 0,
                is_recently_committed: false,
            });
    }

    /// Detect a dangerous structure: any peer this txn has both an
    /// in-edge AND an out-edge to. Returns `Some(peer_txn_id)` if found.
    ///
    /// Symmetric: if T1 has the cycle with T2, both T1 and T2's
    /// `find_dangerous_structure` return Some(peer). Both txns see the
    /// conflict on their own commit pass — simple, correct, no tie-break
    /// needed. The current coordinator caller treats any Some(_) as a
    /// Conflict abort.
    #[instrument(level = "debug", skip_all, fields(txn.id = %txn_id))]
    pub fn find_dangerous_structure(self: &Arc<Self>, txn_id: &str) -> Option<String> {
        let g = self.inner.lock();
        let me = g.by_txn.get(txn_id)?;
        let in_peers: HashSet<&str> = me
            .in_edges
            .edges
            .iter()
            .filter(|e| e.peer != txn_id)
            .map(|e| e.peer.as_str())
            .collect();
        if in_peers.is_empty() {
            return None;
        }
        for e in &me.out_edges.edges {
            if in_peers.contains(e.peer.as_str()) {
                return Some(e.peer.clone());
            }
        }
        None
    }

    /// Mark the txn as recently committed at `commit_version`. The txn's
    /// edges remain in `by_txn` for the rolling window; the writers_of
    /// index is updated so future register_read calls see this txn as a
    /// recent writer.
    #[instrument(level = "debug", skip_all, fields(txn.id = %txn_id, commit_version))]
    pub fn mark_committed(self: &Arc<Self>, txn_id: &str, commit_version: u64) {
        // Collect out-edges first.
        let out_edges_snapshot: Vec<(String, String)> = {
            let g = self.inner.lock();
            g.by_txn
                .get(txn_id)
                .map(|entry| {
                    entry
                        .out_edges
                        .edges
                        .iter()
                        .filter(|e| matches!(e.peer_action, PeerAction::Read))
                        .map(|e| (e.peer.clone(), e.key.clone()))
                        .collect()
                })
                .unwrap_or_default()
        };
        let my_writes: Vec<String> = {
            let g = self.inner.lock();
            g.by_txn
                .get(txn_id)
                .map(|entry| entry.writes.iter().map(|w| w.key.clone()).collect())
                .unwrap_or_default()
        };
        let mut g = self.inner.lock();
        if let Some(entry) = g.by_txn.get_mut(txn_id) {
            entry.status = SiTxnPhase::Committed;
        }
        // Mirror onto peers. PR-Fix1.4: dedup by (peer, key) so we don't
        // produce duplicate in-edges on peers that already received one
        // via `register_write`'s mirror step.
        for (peer_id, key) in &out_edges_snapshot {
            if let Some(peer) = g.by_txn.get_mut(peer_id) {
                let already_present = peer
                    .in_edges
                    .edges
                    .iter()
                    .any(|e| e.peer == txn_id && e.key == *key);
                if !already_present {
                    peer.in_edges.add(SIEdge {
                        peer: txn_id.to_string(),
                        key: key.clone(),
                        peer_action: PeerAction::Wrote,
                        version: commit_version,
                    });
                }
            }
        }
        // Update writers_of.
        for k in &my_writes {
            if let Some(list) = g.writers_of.get_mut(k) {
                for w in list.iter_mut() {
                    if w.txn_id == txn_id {
                        w.committed_version = commit_version;
                        w.is_recently_committed = true;
                    }
                }
            }
        }
        // Add to rolling window.
        g.recent.push(RecentlyCommittedTxn {
            txn_id: txn_id.to_string(),
            keys: my_writes,
            commit_version,
            committed_at_ms: chrono::Utc::now().timestamp_millis(),
        });
        if commit_version > g.recent_floor_version {
            g.recent_floor_version = commit_version;
        }
    }

    /// Mark the txn aborted. Releases all locks and edges immediately,
    /// including dangling in-edges that other active txns held pointing at
    /// this one. Without this cleanup, find_dangerous_structure on those
    /// peers would return phantom cycles against the aborted txn.
    #[instrument(level = "debug", skip_all, fields(txn.id = %txn_id))]
    pub fn mark_aborted(self: &Arc<Self>, txn_id: &str) {
        let mut g = self.inner.lock();
        // Drop this txn from `by_txn` first; keep the txn_id around so we
        // can prune peer references below.
        if let Some(mut entry) = g.by_txn.remove(txn_id) {
            entry.status = SiTxnPhase::Aborted;
        }
        // Walk every active peer and remove any in-edge whose `peer == self`.
        for (peer_id, peer_locks) in g.by_txn.iter_mut() {
            let _ = peer_id;
            peer_locks.in_edges.edges.retain(|e| e.peer != txn_id);
            peer_locks.out_edges.edges.retain(|e| e.peer != txn_id);
        }
        // Remove from secondary indices.
        g.readers_of.retain(|_, list| {
            list.retain(|t| t != txn_id);
            true
        });
        g.writers_of.retain(|_, list| {
            list.retain(|w| w.txn_id != txn_id);
            true
        });
    }

    /// Snapshot the in-memory state for crash-recovery reconstruction.
    pub fn snapshot_for_persist(&self) -> Vec<(String, SiTxnLocks)> {
        let g = self.inner.lock();
        g.by_txn
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Evict recently-committed entries older than `commit_version_floor`.
    pub fn evict_recent_older_than(self: &Arc<Self>, commit_version_floor: u64) {
        let mut g = self.inner.lock();
        g.recent
            .retain(|r| r.commit_version >= commit_version_floor);
        g.recent_floor_version = commit_version_floor;
        // Drop the by_txn entries that have aged past the window and
        // are in `Committed` status. For now, retain all committed.
        g.by_txn.retain(|_, locks| match locks.status {
            SiTxnPhase::Active => true,
            SiTxnPhase::Committed => true,
            SiTxnPhase::Aborted => false,
        });
    }

    /// Active txn ids (for diagnostics / GC).
    pub fn active_txn_ids(&self) -> Vec<String> {
        let g = self.inner.lock();
        g.by_txn
            .iter()
            .filter(|(_, v)| v.status == SiTxnPhase::Active)
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Inspect a single txn's locks (for tests / debugging).
    pub fn inspect(&self, txn_id: &str) -> Option<SiTxnLocks> {
        let g = self.inner.lock();
        g.by_txn.get(txn_id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn begin_and_register_read() {
        let mgr = SiLockManager::new();
        mgr.begin_txn("T1", 10).unwrap();
        mgr.register_read("T1", "k1", 7, "etag-7".to_string());
        let entry = mgr.inspect("T1").unwrap();
        assert_eq!(entry.reads.len(), 1);
        assert_eq!(entry.reads[0].key, "k1");
        assert_eq!(entry.reads[0].version_observed, 7);
    }

    #[test]
    fn register_write_creates_out_edge_to_active_reader() {
        let mgr = SiLockManager::new();
        mgr.begin_txn("T1", 10).unwrap();
        mgr.begin_txn("T2", 10).unwrap();
        mgr.register_read("T1", "k1", 7, "etag-7".to_string());
        mgr.register_write("T2", "k1", 11);

        // T2 has out-edge to T1 (read).
        let t2 = mgr.inspect("T2").unwrap();
        assert_eq!(t2.out_edges.len(), 1);
        assert_eq!(t2.out_edges.edges[0].peer, "T1");
        assert!(matches!(
            t2.out_edges.edges[0].peer_action,
            PeerAction::Read
        ));

        // T1 has in-edge from T2 (write).
        let t1 = mgr.inspect("T1").unwrap();
        assert_eq!(t1.in_edges.len(), 1);
        assert_eq!(t1.in_edges.edges[0].peer, "T2");
        assert!(matches!(
            t1.in_edges.edges[0].peer_action,
            PeerAction::Wrote
        ));
    }

    #[test]
    fn find_dangerous_structure_with_two_txns() {
        let mgr = SiLockManager::new();
        mgr.begin_txn("T1", 10).unwrap();
        mgr.begin_txn("T2", 10).unwrap();
        // T1 reads X, writes Y. T2 reads Y, writes X.
        mgr.register_read("T1", "X", 5, "e".into());
        mgr.register_read("T2", "Y", 5, "e".into());
        mgr.register_write("T1", "Y", 11);
        mgr.register_write("T2", "X", 11);

        // At least one of T1 or T2 should detect a cycle.
        let t1_danger = mgr.find_dangerous_structure("T1");
        let t2_danger = mgr.find_dangerous_structure("T2");
        // The cycle is: T1 has in-edge from T2 (T2 wrote X, T1 read X at 5 < 11).
        // T1 has out-edge to T2 (T2 read Y, T1 wrote Y).
        // So both detect each other. Tie-breaker: larger txn_id loses.
        assert_eq!(t1_danger.as_deref(), Some("T2"));
        assert_eq!(t2_danger.as_deref(), Some("T1"));
    }

    #[test]
    fn mark_aborted_releases_locks() {
        let mgr = SiLockManager::new();
        mgr.begin_txn("T1", 10).unwrap();
        mgr.register_read("T1", "k1", 5, "e".into());
        mgr.mark_aborted("T1");
        assert!(mgr.inspect("T1").is_none());
    }

    #[test]
    fn mark_committed_promotes_to_recently_committed() {
        let mgr = SiLockManager::new();
        mgr.begin_txn("T1", 10).unwrap();
        mgr.register_write("T1", "k1", 11);
        mgr.mark_committed("T1", 11);
        let entry = mgr.inspect("T1").unwrap();
        assert_eq!(entry.status, SiTxnPhase::Committed);
    }
}
