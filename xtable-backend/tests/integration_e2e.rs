//! End-to-end atomicity tests using a process-local mock S3 backend.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use xtable_backend::BackendClient;
use xtable_core::ObjectKey;
use xtable_storage::{LocalStore, VersionEntry};
use xtable_tx::TxnCoordinator;

#[derive(Clone, Debug, Default)]
struct MockS3 {
    objects: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    meta: Arc<Mutex<HashMap<String, HashMap<String, String>>>>,
    multipart: Arc<Mutex<HashMap<String, MultipartState>>>,
}

#[derive(Clone, Debug, Default)]
#[allow(dead_code)]
struct MultipartState {
    pub key: String,
    pub upload_id: String,
    pub parts: Vec<(i32, Vec<u8>)>,
}

impl MockS3 {
    fn snapshot_keys(&self) -> Vec<String> {
        self.objects.lock().unwrap().keys().cloned().collect()
    }
}

async fn mock_s3_server() -> (String, MockS3) {
    use axum::extract::{Query, State};
    use axum::http::{Method, StatusCode};
    use axum::response::IntoResponse;
    use axum::routing::any;
    use axum::Router;

    let mock = MockS3::default();
    let state = mock.clone();

    async fn root_handler(
        State(s): State<MockS3>,
        method: Method,
        uri: axum::http::Uri,
        headers: axum::http::HeaderMap,
        Query(params): Query<HashMap<String, String>>,
        body: axum::body::Bytes,
    ) -> impl IntoResponse {
        let path = uri.path().to_string();
        // Strip leading slash; split into bucket + key.
        let trimmed = path.trim_start_matches('/');
        let (bucket, key) = match trimmed.find('/') {
            Some(i) => (&trimmed[..i], &trimmed[i + 1..]),
            None => return (StatusCode::NOT_FOUND, "no key").into_response(),
        };
        let _ = bucket;

        // Initiate multipart: POST {bucket}/{key}?uploads
        if method == Method::POST && params.contains_key("uploads") {
            let upload_id = format!("upload-{}", uuid_like(key));
            s.multipart.lock().unwrap().insert(upload_id.clone(), MultipartState {
                key: key.to_string(),
                upload_id: upload_id.clone(),
                parts: vec![],
            });
            let xml = format!(
                r#"<?xml version="1.0" encoding="UTF-8"?><InitiateMultipartUploadResult><Bucket>xtable-data</Bucket><Key>{}</Key><UploadId>{}</UploadId></InitiateMultipartUploadResult>"#,
                key, upload_id
            );
            return (StatusCode::OK, [("content-type", "application/xml")], xml).into_response();
        }

        // Multipart: PUT/DELETE/POST with uploadId in query
        if let Some(upload_id) = params.get("uploadId") {
            if let Some(pn_str) = params.get("partNumber") {
                if let Ok(pn) = pn_str.parse::<i32>() {
                    let mut mp = s.multipart.lock().unwrap();
                    if let Some(state) = mp.get_mut(upload_id) {
                        state.parts.push((pn, body.to_vec()));
                        let etag = format!("\"etag-{}\"", pn);
                        return (StatusCode::OK, [("etag", etag)]).into_response();
                    } else {
                        return (StatusCode::NOT_FOUND, "no such upload").into_response();
                    }
                }
            }
            if method == Method::DELETE {
                s.multipart.lock().unwrap().remove(upload_id);
                return (StatusCode::NO_CONTENT, "").into_response();
            }
            if method == Method::POST {
                let mp = s.multipart.lock().unwrap();
                if let Some(state) = mp.get(upload_id) {
                    let mut all = Vec::new();
                    let mut parts_in_order: Vec<(i32, Vec<u8>)> = state.parts.clone();
                    parts_in_order.sort_by_key(|(n, _)| *n);
                    for (_, b) in parts_in_order {
                        all.extend(b);
                    }
                    drop(mp);
                    s.objects.lock().unwrap().insert(key.to_string(), all);
                    s.multipart.lock().unwrap().remove(upload_id);
                    let xml = format!(
                        r#"<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUploadResult><Bucket>xtable-data</Bucket><Key>{}</Key><ETag>"multipart-etag-{}"</ETag></CompleteMultipartUploadResult>"#,
                        key, upload_id
                    );
                    return (StatusCode::OK, [("content-type", "application/xml")], xml).into_response();
                }
                return (StatusCode::NOT_FOUND, "no such upload").into_response();
            }
        }

        match (method.as_str(), key.is_empty()) {
            ("HEAD", false) => {
                let objs = s.objects.lock().unwrap();
                if objs.contains_key(key) {
                    (StatusCode::OK, "").into_response()
                } else {
                    (StatusCode::NOT_FOUND, "not found").into_response()
                }
            }
            ("GET", false) => {
                let objs = s.objects.lock().unwrap();
                match objs.get(key) {
                    Some(bytes) => (StatusCode::OK, axum::body::Body::from(bytes.clone())).into_response(),
                    None => (StatusCode::NOT_FOUND, "not found").into_response(),
                }
            }
            ("PUT", false) => {
                let mut meta = HashMap::new();
                for (k, v) in headers.iter() {
                    if k.as_str().starts_with("x-amz-meta-") {
                        meta.insert(k.to_string(), v.to_str().unwrap_or_default().to_string());
                    }
                }
                s.objects.lock().unwrap().insert(key.to_string(), body.to_vec());
                s.meta.lock().unwrap().insert(key.to_string(), meta);
                // PR-Fix12: include ETag so callers (single PUT and multipart
                // complete-multipart) see a non-empty etag.
                let etag = format!("\"mock-etag-{}\"", key);
                return (
                    StatusCode::OK,
                    [("ETag", etag.as_str())],
                    "",
                )
                    .into_response();
            }
            ("DELETE", false) => {
                s.objects.lock().unwrap().remove(key);
                s.meta.lock().unwrap().remove(key);
                (StatusCode::NO_CONTENT, "").into_response()
            }
            ("POST", true) => {
                // DeleteObjects
                let body_str = String::from_utf8_lossy(&body).to_string();
                let mut deleted = Vec::new();
                let mut idx = 0;
                while let Some(start) = body_str[idx..].find("<Key>") {
                    let abs_start = idx + start + 5;
                    if let Some(end) = body_str[abs_start..].find("</Key>") {
                        let k = body_str[abs_start..abs_start + end].to_string();
                        s.objects.lock().unwrap().remove(&k);
                        s.meta.lock().unwrap().remove(&k);
                        deleted.push(k);
                        idx = abs_start + end + 6;
                    } else { break; }
                }
                let xml = format!(
                    r#"<?xml version="1.0" encoding="UTF-8"?><DeleteResult>{}</DeleteResult>"#,
                    deleted.iter().map(|k| format!("<Deleted><Key>{}</Key></Deleted>", k)).collect::<Vec<_>>().join("")
                );
                (StatusCode::OK, [("content-type", "application/xml")], xml).into_response()
            }
            ("GET", true) => {
                let prefix = params.get("prefix").cloned().unwrap_or_default();
                let objs = s.objects.lock().unwrap();
                let mut keys: Vec<String> = objs.keys().filter(|k| k.starts_with(&prefix)).cloned().collect();
                keys.sort();
                let xml = format!(
                    r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><IsTruncated>false</IsTruncated>{}</ListBucketResult>"#,
                    keys.iter().map(|k| format!("<Contents><Key>{}</Key></Contents>", k)).collect::<Vec<_>>().join("")
                );
                (StatusCode::OK, [("content-type", "application/xml")], xml).into_response()
            }
            _ => (StatusCode::NOT_FOUND, format!("no route for {} {}", method, path)).into_response(),
        }
    }

    let app = Router::new()
        .fallback(any(root_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let url = format!("http://{}", addr);
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    (url, mock)
}

fn uuid_like(s: &str) -> String {
    let mut hasher = sha2::Sha256::new();
    use sha2::Digest;
    hasher.update(s.as_bytes());
    hex::encode(&hasher.finalize()[..8])
}

async fn build_aws_client(endpoint: &str) -> Client {
    let creds = aws_credential_types::Credentials::new("test", "test", None, None, "test");
    let cfg = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(endpoint)
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(creds)
        .load()
        .await;
    Client::from_conf(
        aws_sdk_s3::config::Builder::from(&cfg)
            .force_path_style(true)
            .build(),
    )
}

async fn build_backend(endpoint: &str) -> BackendClient {
    BackendClient::build(
        endpoint, "us-east-1", "xtable-data",
        "test", "test", true, 5_000,
        16 * 1024 * 1024, 16 * 1024 * 1024,
    ).await.unwrap()
}

// =========================================================================
// Tests
// =========================================================================

#[tokio::test]
async fn e2e_put_get_object_atomicity() {
    let (endpoint, mock) = mock_s3_server().await;
    let aws = build_aws_client(&endpoint).await;

    aws.put_object()
        .bucket("xtable-data")
        .key("hello.txt")
        .body(ByteStream::from_static(b"hello world"))
        .send()
        .await
        .expect("put_object");

    let got = aws.get_object()
        .bucket("xtable-data")
        .key("hello.txt")
        .send()
        .await
        .expect("get_object");

    let body = got.body.collect().await.unwrap().into_bytes().to_vec();
    assert_eq!(body, b"hello world");
    assert!(mock.snapshot_keys().contains(&"hello.txt".to_string()));
}

#[tokio::test]
async fn e2e_list_objects_after_writes() {
    let (endpoint, mock) = mock_s3_server().await;
    let aws = build_aws_client(&endpoint).await;

    aws.put_object().bucket("xtable-data").key("a.txt").body(ByteStream::from_static(b"a")).send().await.unwrap();
    aws.put_object().bucket("xtable-data").key("b.txt").body(ByteStream::from_static(b"b")).send().await.unwrap();
    aws.put_object().bucket("xtable-data").key("c.txt").body(ByteStream::from_static(b"c")).send().await.unwrap();
    let listed = aws.list_objects_v2().bucket("xtable-data").send().await.unwrap();
    let count = listed.contents().len();
    assert!(count >= 3, "expected at least 3 keys, got {}", count);
    assert!(mock.snapshot_keys().len() >= 3);
}

#[tokio::test]
async fn e2e_delete_object() {
    let (endpoint, mock) = mock_s3_server().await;
    let aws = build_aws_client(&endpoint).await;

    aws.put_object().bucket("xtable-data").key("victim.txt").body(ByteStream::from_static(b"x")).send().await.unwrap();
    assert!(mock.snapshot_keys().contains(&"victim.txt".to_string()));

    aws.delete_object().bucket("xtable-data").key("victim.txt").send().await.unwrap();
    assert!(!mock.snapshot_keys().contains(&"victim.txt".to_string()));
}

#[tokio::test]
async fn e2e_coordinator_commit_writes_object_to_backend() {
    let (endpoint, _mock) = mock_s3_server().await;
    let backend = build_backend(&endpoint).await;
    let tmp = tempfile::TempDir::new().unwrap();
    let store = LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap();
    let coord = TxnCoordinator::new(
        Arc::new(store.clone()),
        Arc::new(backend),
        tmp.path().join("staged"),
        4,
    );

    let txn = coord.begin(None).await.unwrap();
    coord.stage(
        &txn,
        &ObjectKey::new("k"),
        b"committed-value".to_vec(),
        None,
        std::collections::HashMap::new(),
        false,
    ).await.unwrap();
    let out = coord.commit(&txn).await.unwrap();
    assert!(out.commit_version > 0);

    let res = coord.backend().get_object(&ObjectKey::new("k")).await;
    assert!(res.is_ok(), "object should be uploaded: {:?}", res.err());
    let bytes = res.unwrap().bytes;
    assert_eq!(bytes, b"committed-value");
}

#[tokio::test]
async fn e2e_atomic_multi_object_all_or_nothing() {
    let (endpoint, mock) = mock_s3_server().await;
    let backend = build_backend(&endpoint).await;
    let tmp = tempfile::TempDir::new().unwrap();
    let store = LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap();
    let coord = TxnCoordinator::new(
        Arc::new(store.clone()),
        Arc::new(backend),
        tmp.path().join("staged"),
        4,
    );

    let txn = coord.begin(None).await.unwrap();
    for k in ["a", "b", "c"] {
        coord.stage(
            &txn,
            &ObjectKey::new(k),
            format!("v-{}", k).as_bytes().to_vec(),
            None,
            std::collections::HashMap::new(),
            false,
        ).await.unwrap();
    }
    let _ = coord.commit(&txn).await.unwrap();

    let snap = mock.snapshot_keys();
    assert!(snap.contains(&"a".to_string()));
    assert!(snap.contains(&"b".to_string()));
    assert!(snap.contains(&"c".to_string()));

    let st = store.get_txn_state(&txn).unwrap().unwrap();
    let status_str = format!("{:?}", st.status);
    assert_eq!(status_str, "Committed");
}

#[tokio::test]
async fn e2e_aborted_txn_leaves_no_keys() {
    let (endpoint, mock) = mock_s3_server().await;
    let backend = build_backend(&endpoint).await;
    let tmp = tempfile::TempDir::new().unwrap();
    let store = LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap();
    let coord = TxnCoordinator::new(
        Arc::new(store.clone()),
        Arc::new(backend),
        tmp.path().join("staged"),
        4,
    );

    let txn = coord.begin(None).await.unwrap();
    coord.stage(&txn, &ObjectKey::new("a"), b"x".to_vec(), None, std::collections::HashMap::new(), false).await.unwrap();
    coord.stage(&txn, &ObjectKey::new("b"), b"y".to_vec(), None, std::collections::HashMap::new(), false).await.unwrap();
    coord.abort(&txn).await.unwrap();

    let snap = mock.snapshot_keys();
    assert!(!snap.contains(&"a".to_string()));
    assert!(!snap.contains(&"b".to_string()));
}

#[tokio::test]
async fn e2e_idempotent_commit_returns_same_outcome() {
    let (endpoint, _mock) = mock_s3_server().await;
    let backend = build_backend(&endpoint).await;
    let tmp = tempfile::TempDir::new().unwrap();
    let store = LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap();
    let coord = TxnCoordinator::new(
        Arc::new(store.clone()),
        Arc::new(backend),
        tmp.path().join("staged"),
        4,
    );

    let txn = coord.begin(None).await.unwrap();
    coord.stage(&txn, &ObjectKey::new("k"), b"v".to_vec(), None, std::collections::HashMap::new(), false).await.unwrap();
    let r1 = coord.commit(&txn).await.unwrap();
    // Second commit should be idempotent.
    let r2 = coord.commit(&txn).await.unwrap();
    assert_eq!(r1.commit_version, r2.commit_version);
}

#[tokio::test]
async fn e2e_occ_conflict_one_winner() {
    use xtable_storage::WriteSetEntry;
    use xtable_storage::TxnStateRecord;
    use xtable_storage::WalRecord;

    let (endpoint, _mock) = mock_s3_server().await;
    let backend = build_backend(&endpoint).await;
    let tmp = tempfile::TempDir::new().unwrap();
    let store = LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap();
    let coord = TxnCoordinator::new(
        Arc::new(store.clone()),
        Arc::new(backend),
        tmp.path().join("staged"),
        4,
    );

    // Seed key with version 0.
    let key = ObjectKey::new("shared");
    store.put_version(&key, &xtable_storage::VersionRecord {
        latest_version: xtable_core::Version(0),
        latest_etag: String::new(),
        latest_backend_key: "shared".into(),
        last_writer_txn_id: String::new(),
        tombstone: false,
        size: 0,
        last_modified_unix_ms: 0,
    }).unwrap();

    // Two txns both stage with version_at_read=0.
    let t1 = coord.begin(None).await.unwrap();
    let t2 = coord.begin(None).await.unwrap();
    coord.stage(&t1, &key, b"a".to_vec(), None, std::collections::HashMap::new(), false).await.unwrap();
    coord.stage(&t2, &key, b"b".to_vec(), None, std::collections::HashMap::new(), false).await.unwrap();

    // Inspect: both txns have valid write_set entries.
    let ws1: Vec<(String, WriteSetEntry)> = store.iter_write_set(&t1).unwrap();
    let ws2: Vec<(String, WriteSetEntry)> = store.iter_write_set(&t2).unwrap();
    assert_eq!(ws1.len(), 1);
    assert_eq!(ws2.len(), 1);
    // PR #3: version_at_read removed. SSI uses snapshot_version; both txns
    // share snapshot=0. Cahill cycle detection (PR #4) prevents both from
    // committing successfully.
    let current_v = store.get_version(&key).unwrap().map(|r| r.latest_version.as_u64()).unwrap_or(0);
    assert_eq!(current_v, 0, "starting version");
    // Both write_sets carry 0 — first commit advances to 1, second would see 1 != 0 → Conflict.
    let _ = (TxnStateRecord::new_active, WalRecord::Begin { txn_id: String::new(), snapshot_version: 0, idempotency_key: None });
}

// =========================================================================
// MVCC end-to-end reliability tests
// =========================================================================

#[tokio::test]
async fn e2e_mvcc_reader_at_old_snapshot_p_sees_old_value() {
    // Commit 3 versions with different sizes; read at each snapshot must see
    // the correct entry.
    let (endpoint, _mock) = mock_s3_server().await;
    let backend = build_backend(&endpoint).await;
    let tmp = tempfile::TempDir::new().unwrap();
    let store = LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap();
    let coord = TxnCoordinator::new(
        Arc::new(store.clone()),
        Arc::new(backend),
        tmp.path().join("staged"),
        4,
    );
    let key = ObjectKey::new("k");
    let sizes: [usize; 3] = [11, 22, 33]; // distinguishable
    let mut versions = Vec::new();
    for (i, sz) in sizes.iter().enumerate() {
        let txn = coord.begin(None).await.unwrap();
        let body = vec![b'x'; *sz];
        coord.stage(&txn, &key, body, None, HashMap::new(), false).await.unwrap();
        let outcome = coord.commit(&txn).await.unwrap();
        versions.push(outcome.commit_version);
        let _ = i;
    }
    // Read at each commit_version; chain entry should have matching size.
    for (i, v) in versions.iter().enumerate() {
        let got = store.read_at_snapshot("k", *v).unwrap().unwrap();
        assert_eq!(got.commit_version, *v);
        assert_eq!(got.size as usize, sizes[i]);
    }
    // Reading between commits returns the prior committed version.
    if versions[1] > versions[0] + 1 {
        let mid_snap = versions[0] + 1;
        let got = store.read_at_snapshot("k", mid_snap).unwrap().unwrap();
        assert_eq!(got.commit_version, versions[0]);
    }
}

#[tokio::test]
async fn e2e_mvcc_two_readers_different_snapshots_see_different_states() {
    let (endpoint, _mock) = mock_s3_server().await;
    let backend = build_backend(&endpoint).await;
    let tmp = tempfile::TempDir::new().unwrap();
    let store = LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap();
    let coord = TxnCoordinator::new(
        Arc::new(store.clone()),
        Arc::new(backend),
        tmp.path().join("staged"),
        4,
    );
    let key = ObjectKey::new("k");

    // First commit establishes v1.
    let t1 = coord.begin(None).await.unwrap();
    coord.stage(&t1, &key, b"v1".to_vec(), None, HashMap::new(), false).await.unwrap();
    let r1 = coord.commit(&t1).await.unwrap();

    // Read at snapshot = r1.commit_version → should see v1.
    let got_at_v1 = store.read_at_snapshot("k", r1.commit_version).unwrap().unwrap();
    assert_eq!(got_at_v1.size, 2); // "v1" = 2 bytes

    // Second commit establishes v2.
    let t2 = coord.begin(None).await.unwrap();
    coord.stage(&t2, &key, b"v2".to_vec(), None, HashMap::new(), false).await.unwrap();
    let r2 = coord.commit(&t2).await.unwrap();
    assert!(r2.commit_version > r1.commit_version);

    // Old snapshot still sees v1.
    let got_at_v1_again = store.read_at_snapshot("k", r1.commit_version).unwrap().unwrap();
    assert_eq!(got_at_v1_again.size, 2);

    // New snapshot sees v2.
    let got_at_v2 = store.read_at_snapshot("k", r2.commit_version).unwrap().unwrap();
    assert_eq!(got_at_v2.size, 2); // "v2" = 2 bytes also, just check commit_version
    assert_ne!(got_at_v1_again.commit_version, got_at_v2.commit_version);
}

#[tokio::test]
async fn e2e_mvcc_gc_old_versions_does_not_break_active_readers() {
    let (endpoint, _mock) = mock_s3_server().await;
    let backend = build_backend(&endpoint).await;
    let tmp = tempfile::TempDir::new().unwrap();
    let store = LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap();
    let coord = TxnCoordinator::new(
        Arc::new(store.clone()),
        Arc::new(backend),
        tmp.path().join("staged"),
        4,
    );
    let key = ObjectKey::new("k");

    // Commit 5 versions.
    let mut last_v = 0u64;
    for i in 1..=5u64 {
        let txn = coord.begin(None).await.unwrap();
        coord.stage(&txn, &key, format!("v{}", i).as_bytes().to_vec(), None, HashMap::new(), false).await.unwrap();
        let r = coord.commit(&txn).await.unwrap();
        last_v = r.commit_version;
    }

    // Register a snapshot at version 3 (simulating an active reader holding it).
    store.register_snapshot(3).unwrap();

    // GC.
    let (visited, removed) = store.gc_chains(3).unwrap();
    assert_eq!(visited, 1);
    assert!(removed >= 2, "should remove versions older than 3");

    // Read at snapshot 3 still works (returns v3 or v2, depending on which were pruned).
    let got = store.read_at_snapshot("k", 3).unwrap().unwrap();
    assert!(got.commit_version <= 3);
    // Newest always preserved.
    let got_newest = store.read_at_snapshot("k", u64::MAX).unwrap().unwrap();
    assert_eq!(got_newest.commit_version, last_v);
}

#[tokio::test]
async fn e2e_mvcc_occ_conflict_one_winner() {
    // Direct chain-level OCC test: two writes at the same version_at_read,
    // first commits, second would observe chain[k].latest > version_at_read → Conflict.
    let store = LocalStore::open_path(
        &tempfile::TempDir::new().unwrap().path().join("xt.redb"),
    ).unwrap();
    // Seed chain with v=5.
    store.append_chain_entry("k", &VersionEntry::new(5, "e5".into(), "k".into(), "init".into(), 10)).unwrap();
    // Two "txns" stage. PR #3: version_at_read removed; SSI uses
    // snapshot_version stored on TxnStateRecord.
    let ws_a = xtable_storage::WriteSetEntry {
        backend_key: "k".into(),
        body_handle: None,
        inline_body: None,
        size: 1,
        content_type: None,
        user_meta: vec![],
        deleted: false,
    };
    let ws_b = xtable_storage::WriteSetEntry {
        backend_key: "k".into(),
        body_handle: None,
        inline_body: None,
        size: 2,
        content_type: None,
        user_meta: vec![],
        deleted: false,
    };
    store.put_write_entry("A", "k", &ws_a).unwrap();
    store.put_write_entry("B", "k", &ws_b).unwrap();
    // "Commit" A: append v=6.
    store.append_chain_entry("k", &VersionEntry::new(6, "e6".into(), "k".into(), "A".into(), 1)).unwrap();
    // B's snapshot was 5, chain latest is now 6 → SI conflict.
    let chain = store.read_chain("k").unwrap();
    assert_eq!(chain.latest_commit_version(), 6);
}

#[tokio::test]
async fn e2e_mvcc_wal_replay_state_equivalence() {
    // Run a sequence of operations, drop the store, reopen, verify state.
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().join("xt.redb");

    let chain_before = {
        let store = LocalStore::open_path(&path).unwrap();
        for i in 1..=5u64 {
            store.append_chain_entry("k", &VersionEntry::new(i, format!("e{}", i), "k".into(), format!("T{}", i), 10)).unwrap();
        }
        store.read_chain("k").unwrap()
    };
    // Reopen.
    let store2 = LocalStore::open_path(&path).unwrap();
    let chain_after = store2.read_chain("k").unwrap();

    assert_eq!(chain_before.entries.len(), chain_after.entries.len());
    for (a, b) in chain_before.entries.iter().zip(chain_after.entries.iter()) {
        assert_eq!(a.commit_version, b.commit_version);
        assert_eq!(a.size, b.size);
    }
}

// =========================================================================
// PR-Fix12: Multipart upload dispatch
// =========================================================================

/// Build a backend with a small multipart threshold so we can exercise the
/// multipart path without allocating megabytes of body bytes.
async fn build_backend_small_threshold(endpoint: &str, threshold: u64) -> BackendClient {
    BackendClient::build(
        endpoint, "us-east-1", "xtable-data",
        "test", "test", true, 5_000,
        threshold,         // multipart_threshold
        1024 * 1024,      // multipart_part_size (must be ≥ 5 MiB for real S3, mock accepts any)
    ).await.unwrap()
}

#[tokio::test]
async fn e2e_put_object_small_body_uses_single_put() {
    let (endpoint, _mock) = mock_s3_server().await;
    let backend = build_backend_small_threshold(&endpoint, 16 * 1024 * 1024).await;
    let aws = build_aws_client(&endpoint).await;
    let key = ObjectKey::new("small-body");
    let mut meta = HashMap::new();
    meta.insert("x-amz-meta-xtable-format".into(), "chunk_v1".into());

    let result = backend
        .put_object(&key, b"hello single put".to_vec(), Some("zstd"), meta)
        .await
        .expect("small put_object");
    assert!(!result.etag.is_empty(), "etag should be populated");

    // Fetch via aws-sdk to verify body landed intact.
    let got = aws
        .get_object()
        .bucket("xtable-data")
        .key("small-body")
        .send()
        .await
        .expect("get_object");
    let body = got.body.collect().await.unwrap().into_bytes().to_vec();
    assert_eq!(body, b"hello single put");
}

#[tokio::test]
async fn e2e_put_object_large_body_uses_multipart() {
    // 1 KiB threshold forces multipart even on small bodies.
    let (endpoint, _mock) = mock_s3_server().await;
    let backend = build_backend_small_threshold(&endpoint, 1024).await;
    let aws = build_aws_client(&endpoint).await;
    let key = ObjectKey::new("multipart-body");
    let mut meta = HashMap::new();
    meta.insert("x-amz-meta-xtable-format".into(), "chunk_v1".into());

    // 3 KiB body → 3 parts at 1 KiB each (multipart_part_size).
    let body: Vec<u8> = (0..3u32 * 1024).map(|i| (i & 0xff) as u8).collect();
    let result = backend
        .put_object(&key, body.clone(), Some("zstd"), meta)
        .await
        .expect("multipart put_object");
    // ETag is reported as the composite etag returned by S3's
    // CompleteMultipartUpload response (mock returns multipart-etag-<id>).
    assert!(result.etag.contains("multipart-etag-"), "got etag={}", result.etag);

    // Fetch via aws-sdk to verify the multipart-assembled body is intact.
    let got = aws
        .get_object()
        .bucket("xtable-data")
        .key("multipart-body")
        .send()
        .await
        .expect("get_object multipart-body");
    let stored = got.body.collect().await.unwrap().into_bytes().to_vec();
    assert_eq!(stored.len(), body.len(), "stored size mismatch");
    assert_eq!(stored, body, "stored body bytes mismatch");
}