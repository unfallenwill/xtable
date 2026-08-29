//! MVCC version chain.
//!
//! Each object key maps to a chain of `VersionEntry` records, sorted by
//! `commit_version` ascending. Reads at a snapshot walk the chain from newest
//! to oldest, picking the first entry with `commit_version ≤ snapshot`.
//!
//! Invariants:
//! - chain[k].entries is strictly ascending by commit_version
//! - aborted txns leave no entry
//! - read at snapshot S returns entry with `commit_version ≤ S` (newest)
//! - read-your-own-writes within a txn
//! - OCC conflict semantics preserved (uses chain newest ≤ version_at_read)
//! - multi-object commit is atomic
//! - WAL replay recovers equivalent chain state
//! - GC safety

use serde::{Deserialize, Serialize};

/// A single version entry on a key's chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionEntry {
    /// The commit_version when this version was appended (= global_version
    /// allocated at commit time). Monotonic per key.
    pub commit_version: u64,
    /// ETag returned by the backend S3 PutObject. Empty for tombstones.
    pub etag: String,
    /// Backend S3 key (== xtable key in v1).
    pub backend_key: String,
    /// Originating transaction id. Empty for the seeded/initial entries.
    pub txn_id: String,
    pub size: u64,
    pub content_type: Option<String>,
    pub user_meta: Vec<(String, String)>,
    /// True if this version is a delete marker (tombstone).
    pub deleted: bool,
    pub created_ms: i64,
}

impl VersionEntry {
    pub fn new(commit_version: u64, etag: String, backend_key: String, txn_id: String, size: u64) -> Self {
        Self {
            commit_version,
            etag,
            backend_key,
            txn_id,
            size,
            content_type: None,
            user_meta: Vec::new(),
            deleted: false,
            created_ms: chrono::Utc::now().timestamp_millis(),
        }
    }

    pub fn tombstone(commit_version: u64, backend_key: String, txn_id: String) -> Self {
        Self {
            commit_version,
            etag: String::new(),
            backend_key,
            txn_id,
            size: 0,
            content_type: None,
            user_meta: Vec::new(),
            deleted: true,
            created_ms: chrono::Utc::now().timestamp_millis(),
        }
    }
}

/// A key's full version chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionChain {
    pub key: String,
    /// Sorted ascending by `commit_version`. Strictly monotonic.
    pub entries: Vec<VersionEntry>,
}

impl VersionChain {
    pub fn new(key: String) -> Self {
        Self { key, entries: Vec::new() }
    }

    /// Read at snapshot S: walk from newest, pick first entry with
    /// `commit_version ≤ S`. Returns None if no such entry.
    pub fn read_at_snapshot(&self, snapshot: u64) -> Option<&VersionEntry> {
        // entries is sorted ascending; iterate descending to find newest ≤ S.
        for entry in self.entries.iter().rev() {
            if entry.commit_version <= snapshot {
                return Some(entry);
            }
        }
        None
    }

    /// Newest entry's commit_version, or 0 if empty.
    pub fn latest_commit_version(&self) -> u64 {
        self.entries.last().map(|e| e.commit_version).unwrap_or(0)
    }

    /// Append an entry. The caller MUST ensure the entry's commit_version
    /// is strictly greater than the current latest.
    pub fn append(&mut self, entry: VersionEntry) {
        debug_assert!(
            self.entries.last().map(|e| e.commit_version < entry.commit_version).unwrap_or(true),
            "chain append must be strictly increasing"
        );
        self.entries.push(entry);
    }

    /// Prune entries strictly below `min_snapshot` that are not the newest.
/// Returns the number of entries removed.
///
/// Invariant I8: never empty the chain (always keep at least the newest
/// entry). `min_snapshot = u64::MAX` means "no active readers" — drop
/// everything but the newest.
    pub fn prune_below(&mut self, min_snapshot: u64) -> usize {
        let n = self.entries.len();
        if n <= 1 {
            return 0;
        }
        // Drop entries with commit_version < min_snapshot, but always leave
        // at least the newest.
        let count_below = self
            .entries
            .iter()
            .take_while(|e| e.commit_version < min_snapshot)
            .count();
        let drop_count = count_below.min(n - 1);
        if drop_count > 0 {
            self.entries.drain(0..drop_count);
        }
        debug_assert!(!self.entries.is_empty(), "prune_below emptied chain");
        drop_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(v: u64) -> VersionEntry {
        VersionEntry::new(v, format!("e{}", v), "k".into(), format!("T{}", v), 0)
    }

    #[test]
    fn chain_is_strictly_monotonic() {
        let mut c = VersionChain::new("k".into());
        c.append(entry(1));
        c.append(entry(3));
        c.append(entry(5));
        assert_eq!(c.entries.iter().map(|e| e.commit_version).collect::<Vec<_>>(), vec![1, 3, 5]);
    }

    #[test]
    fn read_at_snapshot_picks_newest_le_snapshot() {
        let mut c = VersionChain::new("k".into());
        c.append(entry(1));
        c.append(entry(3));
        c.append(entry(5));
        assert_eq!(c.read_at_snapshot(0).map(|e| e.commit_version), None);
        assert_eq!(c.read_at_snapshot(1).map(|e| e.commit_version), Some(1));
        assert_eq!(c.read_at_snapshot(2).map(|e| e.commit_version), Some(1));
        assert_eq!(c.read_at_snapshot(3).map(|e| e.commit_version), Some(3));
        assert_eq!(c.read_at_snapshot(4).map(|e| e.commit_version), Some(3));
        assert_eq!(c.read_at_snapshot(5).map(|e| e.commit_version), Some(5));
        assert_eq!(c.read_at_snapshot(100).map(|e| e.commit_version), Some(5));
    }

    #[test]
    fn latest_commit_version_is_max() {
        let mut c = VersionChain::new("k".into());
        assert_eq!(c.latest_commit_version(), 0);
        c.append(entry(7));
        assert_eq!(c.latest_commit_version(), 7);
        c.append(entry(11));
        assert_eq!(c.latest_commit_version(), 11);
    }

    #[test]
    fn prune_below_removes_old_but_keeps_newest() {
        let mut c = VersionChain::new("k".into());
        c.append(entry(1));
        c.append(entry(2));
        c.append(entry(3));
        c.append(entry(4));
        c.append(entry(5));
        // Prune below 3 — should keep [3, 4, 5]
        let removed = c.prune_below(3);
        assert_eq!(removed, 2);
        assert_eq!(c.entries.len(), 3);
        assert_eq!(c.entries[0].commit_version, 3);
        assert_eq!(c.entries[2].commit_version, 5);
    }

    #[test]
    fn prune_below_zero_keeps_all_with_active_zero_snapshot() {
        // A min_snapshot of 0 means "active snapshot at version 0" — but
        // since commit_versions start at 1, this means nothing is below 0,
        // so no entries should be dropped.
        let mut c = VersionChain::new("k".into());
        c.append(entry(1));
        c.append(entry(2));
        let removed = c.prune_below(0);
        assert_eq!(removed, 0);
        assert_eq!(c.entries.len(), 2);
    }

    #[test]
    fn prune_below_max_drops_all_but_newest() {
        // u64::MAX = "no active readers" → GC aggressively, keep only newest.
        let mut c = VersionChain::new("k".into());
        c.append(entry(1));
        c.append(entry(2));
        let removed = c.prune_below(u64::MAX);
        assert_eq!(removed, 1);
        assert_eq!(c.entries.len(), 1);
        assert_eq!(c.entries[0].commit_version, 2);
    }

    #[test]
    fn prune_below_does_not_remove_newest_even_if_older_than_min() {
        let mut c = VersionChain::new("k".into());
        c.append(entry(1));
        // Only one entry: prune_below must not remove it.
        let removed = c.prune_below(1000);
        assert_eq!(removed, 0);
        assert_eq!(c.entries.len(), 1);
    }
}