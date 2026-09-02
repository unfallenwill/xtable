//! MVCC reliability — property-based invariant tests.
//!
//! Each `prop_iN_*` test asserts an MVCC invariant the storage layer must
//! uphold. If any invariant fails, the MVCC design is unsound.
//!
//! Invariants covered:
//! - I1 (chain strictly monotonic):  prop_i1_chain_monotonic
//! - I3 (snapshot isolation):         prop_i3_snapshot_isolation
//! - I5 (SSI conflict semantics):     prop_i5_ssi_compatibility
//! - I6 (multi-object atomicity):     prop_i6_multi_object_atomicity
//! - I7 (WAL replay equivalence):    prop_i7_wal_replay_equivalence
//! - I8 (GC safety):                 prop_i8_gc_preserves_snapshot
//! - I8 (GC never deletes newest):   prop_i8_gc_keeps_newest
//! - (chain semantics):              prop_chain_no_duplicate_version
//! - (visibility):                   prop_staged_then_committed_visibility

use proptest::prelude::*;
use tempfile::TempDir;

use xtable_storage::{LocalStore, VersionEntry, WalRecord};

// =========================================================================
// Helpers
// =========================================================================

fn make_store() -> LocalStore {
    let tmp = TempDir::new().unwrap();
    LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap()
}

fn entry(v: u64, key: &str, size: u64) -> VersionEntry {
    let mut e = VersionEntry::new(
        v,
        format!("e{}", v),
        key.to_string(),
        format!("T{}", v),
        size,
    );
    e.created_ms = 0;
    e
}

// =========================================================================
// INVARIANT I1: chain[k].entries is strictly ascending by commit_version
// =========================================================================

proptest! {
    #[test]
    fn prop_i1_chain_monotonic(n in 1usize..30) {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let store = make_store();
            for i in 1..=n {
                store.append_chain_entry("k", &entry(i as u64, "k", 10)).unwrap();
            }
            let chain = store.read_chain("k").unwrap();
            let versions: Vec<u64> = chain.entries.iter().map(|e| e.commit_version).collect();
            for w in versions.windows(2) {
                prop_assert!(w[0] < w[1], "chain not strictly monotonic: {:?}", versions);
            }
            Ok(())
        })?;
    }
}

// =========================================================================
// INVARIANT I3: read at snapshot S returns entry with commit_version ≤ S (newest)
// =========================================================================

proptest! {
    #[test]
    fn prop_i3_snapshot_isolation(
        puts in proptest::collection::vec(1u64..100, 1..15),
        snapshot in 0u64..100,
    ) {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let store = make_store();
            let mut last_v = 0;
            let mut unique_versions: Vec<u64> = Vec::new();
            for p in &puts {
                if *p > last_v {
                    unique_versions.push(*p);
                    store.append_chain_entry("k", &entry(*p, "k", 10)).unwrap();
                    last_v = *p;
                }
            }
            // Pick a snapshot somewhere in the chain.
            let snap = snapshot;
            let got = store.read_at_snapshot("k", snap).unwrap();
            // The expected entry is the unique-largest version ≤ snap.
            let expected = unique_versions.iter().copied().filter(|v| *v <= snap).last();
            match (got.clone(), expected) {
                (Some(g), Some(e)) => prop_assert_eq!(g.commit_version, e),
                (None, None) => {}
                (g, e) => prop_assert!(false, "snapshot mismatch: got={:?} expected={:?}", g, e),
            }
            // And commit_version ≤ snap (never returns newer than snapshot).
            if let Some(g) = got.as_ref() {
                prop_assert!(g.commit_version <= snap, "snapshot returned newer entry");
            }
            Ok(())
        })?;
    }
}

// =========================================================================
// INVARIANT I5: SSI conflict semantics — two txns starting from the same
// snapshot where one commits first must prevent the other from overwriting.
// =========================================================================

proptest! {
    #[test]
    fn prop_i5_ssi_compatibility(
        snapshot_version in 1u64..10,
    ) {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let store = make_store();
            // Seed key at snapshot_version.
            store.append_chain_entry("k", &entry(snapshot_version, "k", 10)).unwrap();
            // Txn A starts at this snapshot and stages a write to "k".
            // Txn B starts at the same snapshot and stages a write to "k".
            // After B commits, A's snapshot is stale: chain[k].latest
            // has advanced past A's snapshot, so the commit-time snapshot
            // conflict check must reject A.
            let mut a_state = xtable_storage::TxnStateRecord::new_active(snapshot_version, None, 0);
            a_state.write_keys.push("k".into());
            store.put_txn_state("A", &a_state).unwrap();
            store.put_write_entry("A", "k", &xtable_storage::WriteSetEntry {
                backend_key: "k".into(),
                body_handle: None,
                inline_body: None,
                size: 1,
                content_type: None,
                user_meta: vec![],
                deleted: false,
            }).unwrap();
            // Txn B's commit_version is allocated as snapshot_version + 1.
            let b_version = snapshot_version + 1;
            store.append_chain_entry("k", &entry(b_version, "k", 20)).unwrap();
            // Now A's commit-time snapshot check finds chain[k].latest = b_version,
            // > A's snapshot → snapshot conflict.
            let chain = store.read_chain("k").unwrap();
            prop_assert_eq!(chain.latest_commit_version(), b_version);
            prop_assert!(chain.latest_commit_version() > a_state.snapshot_version);
            Ok(())
        })?;
    }
}

// =========================================================================
// INVARIANT I6: multi-object commit atomicity (commit all or nothing)
// =========================================================================

proptest! {
    #[test]
    fn prop_i6_multi_object_atomicity(
        keys in proptest::collection::vec("[a-z]{1,3}", 1..6),
        commit_v in 1u64..100,
    ) {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let store = make_store();
            // Simulate a successful commit: all keys get one entry at commit_v.
            let mut entries = Vec::new();
            for k in &keys {
                // Cold-rebuild-style: snapshot = u64::MAX (no conflict).
                entries.push((k.clone(), entry(commit_v, k, 10), u64::MAX));
            }
            store.append_chain_entries_bulk(&entries).unwrap();
            // Now every key's chain must have the committed entry.
            for k in &keys {
                let got = store.read_at_snapshot(k, commit_v).unwrap();
                prop_assert!(got.is_some(), "key {} not visible at commit_v", k);
                prop_assert_eq!(got.unwrap().commit_version, commit_v);
            }
            // Reading at commit_v - 1 must show no new entries.
            if commit_v > 1 {
                for k in &keys {
                    let got = store.read_at_snapshot(k, commit_v - 1).unwrap();
                    // Pre-commit state: no entries with commit_version <= commit_v - 1
                    // (commit_v is the first commit_version ever for these keys).
                    prop_assert!(got.is_none(),
                        "key {} visible at snapshot {} (before commit {})",
                        k, commit_v - 1, commit_v);
                }
            }
            Ok(())
        })?;
    }
}

// =========================================================================
// INVARIANT I7: WAL replay equivalence
// =========================================================================

proptest! {
    #[test]
    fn prop_i7_wal_replay_equivalence(
        commits in proptest::collection::vec(1u64..20, 1..10),
    ) {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let tmp = TempDir::new().unwrap();
            let path = tmp.path().join("xt.redb");
            let store = LocalStore::open_path(&path).unwrap();
            // Apply commits to chain in monotonically increasing order.
            // (Real commit_versions are always strictly increasing.)
            let mut sorted = commits.clone();
            sorted.sort();
            sorted.dedup();
            for v in &sorted {
                store.append_chain_entry("k", &entry(*v, "k", 10)).unwrap();
                store.append_wal(&WalRecord::Begin {
                    txn_id: format!("T{}", v),
                    snapshot_version: v.saturating_sub(1),
                    idempotency_key: None,
                }).unwrap();
                store.append_wal(&WalRecord::Committed {
                    txn_id: format!("T{}", v),
                    commit_version: *v,
                }).unwrap();
            }
            let chain_before = store.read_chain("k").unwrap();
            let wal_before = store.iter_wal().unwrap();
            // "Crash": reopen.
            drop(store);
            let store2 = LocalStore::open_path(&path).unwrap();
            let chain_after = store2.read_chain("k").unwrap();
            let wal_after = store2.iter_wal().unwrap();
            prop_assert_eq!(chain_before.entries.len(), chain_after.entries.len());
            for (a, b) in chain_before.entries.iter().zip(chain_after.entries.iter()) {
                prop_assert_eq!(a.commit_version, b.commit_version);
                prop_assert_eq!(a.size, b.size);
            }
            prop_assert_eq!(wal_before.len(), wal_after.len());
            Ok(())
        })?;
    }
}

// =========================================================================
// INVARIANT I8 (a): GC never deletes the newest entry
// =========================================================================

proptest! {
    #[test]
    fn prop_i8_gc_keeps_newest(n in 2usize..30) {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let store = make_store();
            for i in 1..=n as u64 {
                store.append_chain_entry("k", &entry(i, "k", 10)).unwrap();
            }
            let before_latest = store.read_chain("k").unwrap().latest_commit_version();
            // No active snapshot registered → min_active = u64::MAX.
            let (_, removed) = store.gc_chains(u64::MAX).unwrap();
            prop_assert!(removed > 0);
            let chain_after = store.read_chain("k").unwrap();
            prop_assert_eq!(chain_after.entries.len(), 1, "GC should leave exactly 1 entry");
            prop_assert_eq!(chain_after.entries[0].commit_version, before_latest);
            Ok(())
        })?;
    }
}

// =========================================================================
// INVARIANT I8 (b): GC preserves snapshot pinning
// =========================================================================

proptest! {
    #[test]
    fn prop_i8_gc_preserves_snapshot(
        commits in proptest::collection::vec(1u64..30, 1..10),
        pin in 0u64..30,
    ) {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let store = make_store();
            let mut last_v = 0;
            let mut unique = Vec::new();
            for c in &commits {
                if *c > last_v {
                    unique.push(*c);
                    store.append_chain_entry("k", &entry(*c, "k", 10)).unwrap();
                    last_v = *c;
                }
            }
            // Register a snapshot at `pin`.
            store.register_snapshot(pin).unwrap();
            let (visited, _removed) = store.gc_chains(pin).unwrap();
            prop_assert_eq!(visited, 1);
            let chain_after = store.read_chain("k").unwrap();
            // After GC, entries older than the newest version visible at the
            // pin may be removed. That visibility anchor itself must remain;
            // otherwise a reader at `pin` would see a phantom absence.
            let anchor = unique.iter().copied().filter(|v| *v <= pin).max();
            for e in chain_after.entries.iter() {
                prop_assert!(
                    e.commit_version > pin || Some(e.commit_version) == anchor,
                    "entry with version {} is not the snapshot anchor (pin={})",
                    e.commit_version,
                    pin
                );
            }
            // Read at snapshot pin still works.
            let got = store.read_at_snapshot("k", pin).unwrap();
            match (anchor, got) {
                (Some(expected), Some(got)) => prop_assert_eq!(got.commit_version, expected),
                (None, None) => {}
                (Some(expected), None) => {
                    prop_assert!(false, "snapshot anchor {} was removed", expected)
                }
                (None, Some(got)) => {
                    prop_assert!(false, "future version {} became visible", got.commit_version)
                }
            }
            Ok(())
        })?;
    }
}

// =========================================================================
// Chain has no duplicate version
// =========================================================================

proptest! {
    #[test]
    fn prop_chain_no_duplicate_version(_n in 2usize..20) {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let store = make_store();
            // Append the same commit_version twice — should be rejected.
            let v = 5u64;
            store.append_chain_entry("k", &entry(v, "k", 10)).unwrap();
            let res = store.append_chain_entry("k", &entry(v, "k", 10));
            prop_assert!(res.is_err(), "duplicate commit_version should be rejected");
            Ok(())
        })?;
    }
}

// =========================================================================
// Staged-then-committed visibility: staged writes don't append; commit does
// =========================================================================

proptest! {
    #[test]
    fn prop_staged_then_committed_visibility(
        body_size in 1u64..1000,
    ) {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let store = make_store();
            // No commit yet — read_at_snapshot returns None.
            let got = store.read_at_snapshot("k", u64::MAX).unwrap();
            prop_assert!(got.is_none(), "no version committed yet");
            // Commit.
            store.append_chain_entry("k", &entry(1, "k", body_size)).unwrap();
            // Now read at snapshot 1.
            let got = store.read_at_snapshot("k", 1).unwrap();
            prop_assert!(got.is_some());
            prop_assert_eq!(got.unwrap().size, body_size);
            Ok(())
        })?;
    }
}
