//! SSI (Serializable Snapshot Isolation) property tests.
//!
//! These tests verify Cahill cycle detection prevents write-skew and
//! preserves SSI invariants. They drive the `TxnCoordinator` directly
//! (no HTTP / S3 layer) so failure modes are isolated.
//!
//! ## What each test asserts
//!
//! - `ssi_write_write_one_winner`: two txns writing the same key — one
//!   commits, the other is rejected by `append_chain_entries_bulk`'s
//!   monotonicity check (or by SI cycle if both happen to register writes).
//! - `ssi_write_skew_aborts_one`: the canonical write-skew scenario
//!   (T1 reads X/Y + writes X; T2 reads X/Y + writes Y) — Cahill must
//!   abort one. Without ReadSet capture (PR-Fix8.3) this fails.
//! - `ssi_own_read_write_ok`: reading + writing the same key within one
//!   txn must NOT abort (own-write rule).
//! - `ssi_disjoint_read_write_ok`: non-overlapping read/write txns both
//!   commit cleanly.
//! - `ssi_read_only_txn_never_aborts`: a txn with only reads is always
//!   allowed to commit.

use std::sync::Arc;

use tempfile::TempDir;
use xtable_core::ObjectKey;
use xtable_storage::LocalStore;
use xtable_tx::TxnCoordinator;

async fn setup() -> (Arc<TxnCoordinator>, TempDir) {
    let tmp = TempDir::new().unwrap();
    let store = LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap();
    // The S3 backend is unused in these tests; we never call commit
    // paths that require it (MemTable publish + chain append is local).
    let backend = xtable_backend::BackendClient::dummy_for_test_async()
        .await
        .unwrap();
    let coord = Arc::new(TxnCoordinator::new(
        Arc::new(store),
        Arc::new(backend),
        tmp.path().join("spill"),
        4,
    ));
    (coord, tmp)
}

#[tokio::test]
async fn ssi_write_write_one_winner() {
    // PR-Fix9.1 added a snapshot-conflict check inside
    // `append_chain_entries_bulk`. Two txns at the same snapshot
    // writing the same key now cannot both commit — the second one's
    // append sees chain[K].latest > its snapshot_version and the
    // redb write txn rolls back, returning Conflict.
    let (coord, _tmp) = setup().await;
    let t1 = coord.begin(None).await.unwrap();
    let t2 = coord.begin(None).await.unwrap();
    let key = ObjectKey::new("s/t/k");
    coord
        .stage(&t1, &key, b"v1".to_vec(), None, Default::default(), false)
        .await
        .unwrap();
    coord
        .stage(&t2, &key, b"v2".to_vec(), None, Default::default(), false)
        .await
        .unwrap();
    let r1 = coord.commit(&t1).await;
    let r2 = coord.commit(&t2).await;
    // Strict: not both can succeed.
    assert!(
        !(r1.is_ok() && r2.is_ok()),
        "two concurrent writers on same key must not both commit (got r1={:?} r2={:?})",
        r1.is_ok(),
        r2.is_ok(),
    );
    // At least one succeeds.
    assert!(
        r1.is_ok() || r2.is_ok(),
        "at least one write-write must succeed"
    );
}

#[tokio::test]
async fn ssi_write_skew_aborts_one() {
    let (coord, _tmp) = setup().await;
    let t1 = coord.begin(None).await.unwrap();
    let t2 = coord.begin(None).await.unwrap();

    let x = ObjectKey::new("s/t/x");
    let y = ObjectKey::new("s/t/y");

    // T1 reads X, Y; commits at snapshot S.
    coord
        .read(&t1, &x, xtable_core::Version(0), String::new())
        .await
        .unwrap();
    coord
        .read(&t1, &y, xtable_core::Version(0), String::new())
        .await
        .unwrap();
    // T2 also reads X, Y.
    coord
        .read(&t2, &x, xtable_core::Version(0), String::new())
        .await
        .unwrap();
    coord
        .read(&t2, &y, xtable_core::Version(0), String::new())
        .await
        .unwrap();

    // T1 writes X; T2 writes Y. This is the write-skew pattern.
    coord
        .stage(&t1, &x, b"new".to_vec(), None, Default::default(), false)
        .await
        .unwrap();
    coord
        .stage(&t2, &y, b"new".to_vec(), None, Default::default(), false)
        .await
        .unwrap();

    let r1 = coord.commit(&t1).await;
    let r2 = coord.commit(&t2).await;

    // Cahill cycle detection: at least one must be aborted. The
    // tie-break (lex-larger txn_id loses) means exactly one of them
    // commits and one is rejected with Conflict.
    assert!(
        r1.is_ok() != r2.is_ok(),
        "write skew: exactly one of (T1, T2) must abort (r1={:?} r2={:?})",
        r1.is_ok(),
        r2.is_ok(),
    );
}

#[tokio::test]
async fn ssi_own_read_write_ok() {
    let (coord, _tmp) = setup().await;
    let t = coord.begin(None).await.unwrap();
    let k = ObjectKey::new("s/t/k");
    // Read then write same key — own-write rule should not abort.
    coord
        .read(&t, &k, xtable_core::Version(0), String::new())
        .await
        .unwrap();
    coord
        .stage(&t, &k, b"v".to_vec(), None, Default::default(), false)
        .await
        .unwrap();
    let r = coord.commit(&t).await;
    assert!(r.is_ok(), "own-read-write must succeed: {:?}", r.err());
}

#[tokio::test]
async fn ssi_disjoint_read_write_ok() {
    let (coord, _tmp) = setup().await;
    let t1 = coord.begin(None).await.unwrap();
    let t2 = coord.begin(None).await.unwrap();

    let a = ObjectKey::new("s/t/a");
    let b = ObjectKey::new("s/t/b");

    coord
        .read(&t1, &a, xtable_core::Version(0), String::new())
        .await
        .unwrap();
    coord
        .read(&t2, &b, xtable_core::Version(0), String::new())
        .await
        .unwrap();
    coord
        .stage(&t1, &a, b"x".to_vec(), None, Default::default(), false)
        .await
        .unwrap();
    coord
        .stage(&t2, &b, b"x".to_vec(), None, Default::default(), false)
        .await
        .unwrap();

    let r1 = coord.commit(&t1).await;
    let r2 = coord.commit(&t2).await;
    assert!(r1.is_ok(), "disjoint T1 commit failed: {:?}", r1.err());
    assert!(r2.is_ok(), "disjoint T2 commit failed: {:?}", r2.err());
}

#[tokio::test]
async fn ssi_read_only_txn_never_aborts() {
    let (coord, _tmp) = setup().await;
    let t = coord.begin(None).await.unwrap();
    let k = ObjectKey::new("s/t/k");
    coord
        .read(&t, &k, xtable_core::Version(0), String::new())
        .await
        .unwrap();
    // No writes.
    let r = coord.commit(&t).await;
    assert!(r.is_ok(), "read-only txn must commit: {:?}", r.err());
}
