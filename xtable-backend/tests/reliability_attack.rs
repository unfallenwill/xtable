//! Regression tests for the MVCC reliability findings.
//!
//! Each `pocN_*` test asserts the **correct** invariant the system must
//! satisfy. The test panics if the bug is still present (see each `assert!`
//! message for the original V-number).
//!
//! Mapping (finding → test):
//!   poc1 → V4   OCC must reject write-write conflicts between concurrent txns
//!   poc2 → V2   Recovery must complete a commit whose WAL stalled at
//!               "Committing" — never delete the already-published object
//!   poc3 → V1   Cold rebuild must not classify committed backend objects
//!               as orphans and wipe them
//!   poc4 → V3   A failed commit must not destroy prior committed data on
//!               shared keys
//!   poc5 → V9   Snapshot pins must be reference-counted so GC cannot steal
//!               a pin still held by an in-flight txn
//!   poc6 → V10  Transactional delete must remove the backend object and
//!               leave a tombstone in the version chain
//!   poc7 → V18  HTTP-stage threshold must not reject every transaction
//!               after the first commit advances the global version
//!
//! These tests use the real coordinator / recovery / rebuild paths with a
//! fault-injecting mock backend. They are the e2e companion to the
//! unit-level regression tests in `xtable-tx/tests/regression_vulns.rs`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::extract::{Query, State};
use axum::http::{Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;
use xtable_backend::BackendClient;
use xtable_core::headers::TxnStatus;
use xtable_core::ObjectKey;
use xtable_storage::{LocalStore, VersionEntry, WalRecord};
use xtable_tx::{gc, rebuild, recovery, TxnCoordinator};

// =========================================================================
// 带故障注入的 mock S3 后端
// =========================================================================

#[derive(Clone, Default)]
struct AttackMock {
    objects: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    /// 原始 x-amz-meta-* 头（与真实 S3 一样按对象存储，HEAD/GET 原样返回）。
    meta: Arc<Mutex<HashMap<String, HashMap<String, String>>>>,
    /// 以该前缀开头的 key 的 PUT 一律返回 503（模拟后端局部故障）。
    fail_put_prefix: Arc<Mutex<String>>,
}

impl AttackMock {
    fn set_fail_put_prefix(&self, prefix: &str) {
        *self.fail_put_prefix.lock().unwrap() = prefix.to_string();
    }

    fn contains(&self, key: &str) -> bool {
        self.objects.lock().unwrap().contains_key(key)
    }

    fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.objects.lock().unwrap().get(key).cloned()
    }

    fn keys(&self) -> Vec<String> {
        self.objects.lock().unwrap().keys().cloned().collect()
    }
}

async fn attack_s3_server() -> (String, AttackMock) {
    let mock = AttackMock::default();
    let state = mock.clone();

    async fn root_handler(
        State(s): State<AttackMock>,
        method: Method,
        uri: Uri,
        headers: axum::http::HeaderMap,
        Query(_params): Query<HashMap<String, String>>,
        body: axum::body::Bytes,
    ) -> Response {
        let path = uri.path().to_string();
        let trimmed = path.trim_start_matches('/');
        let (bucket, key) = match trimmed.find('/') {
            Some(i) => (&trimmed[..i], trimmed[i + 1..].to_string()),
            None => (trimmed, String::new()),
        };
        let _ = bucket;

        // GET /bucket → ListObjectsV2
        if key.is_empty() && method == Method::GET {
            let objs = s.objects.lock().unwrap();
            let mut keys: Vec<String> = objs.keys().cloned().collect();
            keys.sort();
            let xml = format!(
                r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><IsTruncated>false</IsTruncated>{}</ListBucketResult>"#,
                keys.iter()
                    .map(|k| format!(
                        "<Contents><Key>{}</Key><Size>{}</Size><ETag>\"e\"</ETag></Contents>",
                        k,
                        objs.get(k).map(|v| v.len()).unwrap_or(0)
                    ))
                    .collect::<Vec<_>>()
                    .join("")
            );
            return (StatusCode::OK, [("content-type", "application/xml")], xml).into_response();
        }

        let mut meta = HashMap::new();
        for (k, v) in headers.iter() {
            let name = k.as_str().to_ascii_lowercase();
            if name.starts_with("x-amz-meta-") {
                meta.insert(name, v.to_str().unwrap_or_default().to_string());
            }
        }

        match method.as_str() {
            "PUT" => {
                let fail = s.fail_put_prefix.lock().unwrap().clone();
                // The coordinator stages uploads at
                // `xtable-txn-staging/{txn_id}/{key}` so a failed-PUT
                // injection keyed on the user-visible key prefix still
                // fires. We strip a recognized staging prefix before the
                // prefix check.
                let logical_key = key
                    .strip_prefix("xtable-txn-staging/")
                    .and_then(|s| s.split_once('/').map(|(_, k)| k))
                    .unwrap_or(&key);
                if !fail.is_empty() && logical_key.starts_with(&fail) {
                    return (StatusCode::SERVICE_UNAVAILABLE, "injected failure").into_response();
                }
                s.objects.lock().unwrap().insert(key.clone(), body.to_vec());
                s.meta.lock().unwrap().insert(key, meta);
                (StatusCode::OK, "").into_response()
            }
            "GET" => {
                let objs = s.objects.lock().unwrap();
                match objs.get(&key) {
                    Some(bytes) => {
                        let mut b = axum::http::Response::builder()
                            .status(200)
                            .header("content-length", bytes.len());
                        for (k, v) in s.meta.lock().unwrap().get(&key).cloned().unwrap_or_default() {
                            b = b.header(k, v);
                        }
                        b.body(axum::body::Body::from(bytes.clone())).unwrap().into_response()
                    }
                    None => (StatusCode::NOT_FOUND, "not found").into_response(),
                }
            }
            "HEAD" => {
                let objs = s.objects.lock().unwrap();
                if !objs.contains_key(&key) {
                    return (StatusCode::NOT_FOUND, "not found").into_response();
                }
                let len = objs.get(&key).map(|v| v.len()).unwrap_or(0);
                let mut b = axum::http::Response::builder().status(200).header("content-length", len);
                for (k, v) in s.meta.lock().unwrap().get(&key).cloned().unwrap_or_default() {
                    b = b.header(k, v);
                }
                b.body(axum::body::Body::empty()).unwrap().into_response()
            }
            "DELETE" => {
                s.objects.lock().unwrap().remove(&key);
                s.meta.lock().unwrap().remove(&key);
                (StatusCode::NO_CONTENT, "").into_response()
            }
            _ => (StatusCode::NOT_FOUND, "unmatched").into_response(),
        }
    }

    let app = Router::new().fallback(any(root_handler)).with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let url = format!("http://{}", addr);
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    (url, mock)
}

async fn build_backend(endpoint: &str) -> BackendClient {
    BackendClient::build(
        endpoint, "us-east-1", "xtable-data",
        "test", "test", true, 5_000,
        16 * 1024 * 1024, 16 * 1024 * 1024,
    ).await.unwrap()
}

async fn setup() -> (TxnCoordinator, LocalStore, Arc<BackendClient>, AttackMock, tempfile::TempDir) {
    let (endpoint, mock) = attack_s3_server().await;
    let backend = Arc::new(build_backend(&endpoint).await);
    let tmp = tempfile::TempDir::new().unwrap();
    let store = LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap();
    let coord = TxnCoordinator::new(
        Arc::new(store.clone()),
        Arc::clone(&backend),
        tmp.path().join("staged"),
        4,
    );
    (coord, store, backend, mock, tmp)
}

/// threshold=0 与 integration_e2e.rs 的做法一致（绕过 HTTP 层真实传参，见 poc7）。
/// V10/V18 fix: the `deleted` flag (last arg) was added and threshold was removed.
async fn stage(coord: &TxnCoordinator, txn: &str, key: &str, body: &[u8]) {
    coord
        .stage(txn, &ObjectKey::new(key), body.to_vec(), None, HashMap::new(), false)
        .await
        .expect("stage");
}

// =========================================================================
// Regression: V4 — OCC must detect write-write conflicts
// =========================================================================

#[tokio::test]
async fn poc1_occ_never_conflicts_between_two_txns() {
    let (coord, store, _backend, mock, _tmp) = setup().await;

    let t1 = coord.begin(None).await.unwrap();
    let t2 = coord.begin(None).await.unwrap();
    stage(&coord, &t1, "k", b"A").await;
    stage(&coord, &t2, "k", b"B").await;

    coord.commit(&t1).await.unwrap();

    // The second commit must return a conflict (lost-update protection).
    // Before the fix it returned Ok because TBL_VERSIONS was never updated
    // on commit, so version_at_read was always 0 and the check always passed.
    let second = coord.commit(&t2).await;
    assert!(second.is_err(),
        "V4: OCC did not detect write-write conflict on the same key");

    // TBL_VERSIONS must reflect the committed version after t1 commits.
    assert!(store.get_version(&ObjectKey::new("k")).unwrap().is_some(),
        "V4: TBL_VERSIONS was not updated on commit — OCC check has no ground truth");

    // t1's value must be preserved; t2's write must not reach the backend.
    assert_eq!(mock.get("k").unwrap(), b"A",
        "V4: lost update — t2 silently overwrote t1's commit");
    let chain = store.read_chain("k").unwrap();
    assert_eq!(chain.entries.len(), 1,
        "V4: only t1's write should be in the version chain (t2 rejected)");
}

// =========================================================================
// Regression: V2 — Recovery must complete a commit whose WAL stalled
// =========================================================================

#[tokio::test]
async fn poc2_recovery_deletes_published_commit() {
    let (coord, store, backend, mock, _tmp) = setup().await;

    let txn = coord.begin(None).await.unwrap();
    stage(&coord, &txn, "k", b"payload").await;

    // Reconstruct the crash window between
    // append_chain_entries_bulk (line ~306) and WAL Committed (line ~311):
    // chain entry + backend object are already published, only the WAL
    // Committed record is missing.
    store.append_wal(&WalRecord::ValidateOk {
        txn_id: txn.clone(),
        write_keys: vec!["k".into()],
    }).unwrap();
    store.append_wal(&WalRecord::Committing {
        txn_id: txn.clone(),
        upload_keys: vec!["k".into()],
    }).unwrap();
    let mut ts = store.get_txn_state(&txn).unwrap().unwrap();
    ts.status = TxnStatus::Committing;
    ts.uploaded_keys = vec!["k".into()];
    ts.alloc_versions = vec![("k".into(), 1)];
    store.put_txn_state(&txn, &ts).unwrap();
    store.append_chain_entry("k", &VersionEntry::new(1, "e1".into(), "k".into(), txn.clone(), 7)).unwrap();
    backend.put_object(&ObjectKey::new("k"), b"payload".to_vec(), None, HashMap::new()).await.unwrap();

    recovery::recover(&store, &*backend).await.unwrap();

    // The chain entry is already published (atomicity point crossed), so
    // recovery must complete the commit: keep the backend object, keep the
    // chain entry, and mark the txn Committed.
    assert!(mock.contains("k"),
        "V2: recovery deleted a backend object whose chain entry was already published");
    let chain = store.read_chain("k").unwrap();
    assert_eq!(chain.entries.len(), 1,
        "V2: chain entry was lost during recovery");
    let post = store.get_txn_state(&txn).unwrap().unwrap();
    assert_eq!(post.status, TxnStatus::Committed,
        "V2: recovered commit left txn in Aborted state (I2/I7 broken)");
}

// =========================================================================
// Regression: V1 — Cold rebuild must preserve committed backend objects
// =========================================================================

#[tokio::test]
async fn poc3_cold_rebuild_annihilates_txn_objects() {
    let (coord, _store, backend, mock, _tmp) = setup().await;

    // Commit three objects via the real path.
    for k in ["a", "b", "c"] {
        let t = coord.begin(None).await.unwrap();
        stage(&coord, &t, k, format!("value-{}", k).as_bytes()).await;
        coord.commit(&t).await.expect("commit");
    }
    assert_eq!(mock.keys().len(), 3);

    // Disaster scenario: redb directory gone, server boots with empty store
    // and runs cold rebuild.
    let tmp2 = tempfile::TempDir::new().unwrap();
    let fresh = LocalStore::open_path(&tmp2.path().join("xt.redb")).unwrap();
    let report = rebuild::rebuild(&fresh, &*backend).await.unwrap();

    // All three objects are committed; they must not be classified as
    // orphans just because the local TxnState was lost with the redb.
    assert_eq!(report.orphans_deleted, 0,
        "V1: cold rebuild wrongly classified committed objects as orphans ({})",
        report.orphans_deleted);
    assert_eq!(mock.keys().len(), 3,
        "V1: cold rebuild deleted committed objects (data loss)");
    // The MVCC chain must be rebuilt from backend state, not left empty.
    assert!(!fresh.read_chain("a").unwrap().entries.is_empty(),
        "V1: cold rebuild left MVCC chain empty for a committed object");
}

// =========================================================================
// Regression: V3 — A failed commit must not destroy prior committed data
// =========================================================================

#[tokio::test]
async fn poc4_failed_commit_destroys_prior_committed_object() {
    let (coord, store, _backend, mock, _tmp) = setup().await;

    // T0: commit k = "old".
    let t0 = coord.begin(None).await.unwrap();
    stage(&coord, &t0, "k", b"old").await;
    coord.commit(&t0).await.unwrap();
    assert_eq!(mock.get("k").unwrap(), b"old");

    // T1: rewrite k plus a key that the mock will fail to PUT.
    mock.set_fail_put_prefix("poison/");
    let t1 = coord.begin(None).await.unwrap();
    stage(&coord, &t1, "k", b"new").await;
    stage(&coord, &t1, "poison/x", b"z").await;

    let r = coord.commit(&t1).await;
    assert!(r.is_err(), "T1 must roll back atomically on upload failure");

    // After T1 rolls back, k must still hold T0's "old" — the compensation
    // path must not run a bare DeleteObject on a key that already had a
    // committed version.
    assert_eq!(mock.get("k").unwrap(), b"old",
        "V3: failed-compensation delete destroyed prior committed data");
    // The chain entry from T0 must still be there.
    let chain = store.read_chain("k").unwrap();
    assert_eq!(chain.entries.len(), 1,
        "V3: T0 chain entry missing after T1 rollback");
}

// =========================================================================
// Regression: V9 — Active snapshots must survive concurrent unregister
// =========================================================================

#[tokio::test]
async fn poc5_shared_snapshot_pin_stolen_by_first_committer() {
    let tmp = tempfile::TempDir::new().unwrap();
    let store = LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap();

    store.append_chain_entry("k", &VersionEntry::new(1, "e1".into(), "k".into(), "T1".into(), 1)).unwrap();
    store.append_chain_entry("k", &VersionEntry::new(6, "e6".into(), "k".into(), "T6".into(), 1)).unwrap();

    // Two txns both pin snapshot 1. register_snapshot must be reference-
    // counted; otherwise the first commit's unregister will steal the pin
    // still held by the second txn.
    store.register_snapshot(1).unwrap();
    store.register_snapshot(1).unwrap();

    // First txn commits and unregisters — the pin must NOT be removed yet.
    store.unregister_snapshot(1).unwrap();

    // GC must not run yet (one pin remains).
    gc::gc_version_chains(&store).unwrap();

    // The still-running second txn must see v1 at snapshot 1 (I3/I8).
    let r = store.read_at_snapshot("k", 1).unwrap();
    assert!(r.is_some(),
        "V9: active snapshot lost data to GC (phantom delete) — pin not refcounted");
    assert_eq!(r.unwrap().commit_version, 1,
        "V9: read_at_snapshot returned the wrong version after GC");
}

// =========================================================================
// Regression: V10 — Transactional delete must remove the backend object
// =========================================================================

#[tokio::test]
async fn poc6_transactional_delete_writes_empty_object() {
    let (coord, store, _backend, mock, _tmp) = setup().await;

    let t0 = coord.begin(None).await.unwrap();
    stage(&coord, &t0, "k", b"real-data").await;
    coord.commit(&t0).await.unwrap();

    // Transactional delete via service.rs (stage with empty body, deleted=true).
    let t1 = coord.begin(None).await.unwrap();
    coord
        .stage(&t1, &ObjectKey::new("k"), Vec::new(), None, HashMap::new(), true)
        .await
        .unwrap();
    coord.commit(&t1).await.unwrap();

    // The backend object must be gone (not replaced by a 0-byte PutObject),
    // and the chain entry must carry a tombstone so reads at this version
    // return None.
    assert!(mock.get("k").is_none(),
        "V10: transactional delete left the backend object in place");
    let last = store.read_chain("k").unwrap().entries.last().cloned().unwrap();
    assert!(last.deleted,
        "V10: chain entry missing tombstone marker after transactional delete");
}

// =========================================================================
// Regression: V18 — HTTP-stage threshold must not reject subsequent txns
// =========================================================================

#[tokio::test]
async fn poc7_http_layer_rejects_every_txn_after_the_first() {
    let (coord, store, _backend, mock, _tmp) = setup().await;

    // First txn (global_version == 0): threshold check passes by accident.
    let t1 = coord.begin(None).await.unwrap();
    let _g1 = store.current_global_version().unwrap(); // = 0
    coord
        .stage(&t1, &ObjectKey::new("k"), b"x".to_vec(), None, HashMap::new(), false)
        .await
        .unwrap();
    coord.commit(&t1).await.unwrap(); // global_version → 1

    // Second txn: the HTTP layer passes current_global_version() as the
    // threshold. If TBL_VERSIONS is never written on commit, get_version
    // of any new key returns 0 < 1 → stage is rejected with InvalidArgument
    // (HTTP 400). Every subsequent txn is dead in the water.
    let t2 = coord.begin(None).await.unwrap();
    let _g2 = store.current_global_version().unwrap(); // = 1
    let r = coord
        .stage(&t2, &ObjectKey::new("fresh"), b"y".to_vec(), None, HashMap::new(), false)
        .await;

    // The stage must succeed — and the write must reach the backend.
    assert!(r.is_ok(),
        "V18: HTTP-stage threshold rejected the second transaction");
    // Commit t2 so the assertion below checks what actually shipped to backend.
    coord.commit(&t2).await.unwrap();
    assert!(mock.keys().contains(&"fresh".to_string()),
        "V18: second transaction's stage succeeded but its write never reached backend");
}
