//! End-to-end smoke for the structured-data-space HTTP layer.
//!
//! Each step is a real HTTP request/response through axum. The test prints
//! PASS/FAIL per step; exits non-zero if any step fails.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use serde_json::{json, Value};
use tempfile::TempDir;
use tower::ServiceExt;

use xtable_auth::{CredentialStore, StaticCredential};
use xtable_backend::BackendClient;
use xtable_core::config::Config;
use xtable_server::app::AppState;
use xtable_storage::LocalStore;

async fn build_app() -> (Router, TempDir) {
    let tmp = TempDir::new().unwrap();
    let mut cfg = Config::default();
    cfg.server.data_dir = tmp.path().to_path_buf();
    cfg.storage.redb_dir = tmp.path().join("redb");
    cfg.storage.staged_body_spill_dir = tmp.path().join("staged");
    std::fs::create_dir_all(&cfg.storage.redb_dir).unwrap();
    std::fs::create_dir_all(&cfg.storage.staged_body_spill_dir).unwrap();

    let store = LocalStore::open(&cfg.storage.redb_dir).unwrap();
    let backend = BackendClient::dummy_for_test_async().await.unwrap();
    let creds = Arc::new(CredentialStore::new());
    creds.put(
        StaticCredential {
            access_key_id: "test".into(),
            secret_access_key: "test".into(),
        }
        .into_entry(),
    );
    let state = AppState::new(
        cfg,
        store,
        backend,
        creds,
        xtable_telemetry::metrics::Metrics::default(),
    );
    let app = xtable_server::structured::router().with_state(Arc::new(state));
    (app, tmp)
}

fn body(v: Value) -> Body {
    Body::from(serde_json::to_vec(&v).unwrap())
}

async fn read_json(resp: axum::response::Response) -> Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap_or(Value::Null)
}

async fn assert_status(
    label: &str,
    app: &mut Router,
    req: Request<Body>,
    want: StatusCode,
) -> bool {
    let resp = app.clone().oneshot(req).await.unwrap();
    let got = resp.status();
    if got == want {
        println!("[PASS] {label}: {got}");
        true
    } else {
        eprintln!("[FAIL] {label}: expected {want}, got {got}");
        let body = read_json(resp).await;
        eprintln!("       body = {body}");
        false
    }
}

#[tokio::test]
#[ignore = "spec §5.1 removed per-record PUTs; structured-data-space reads must walk MemTable (re-enable in Task 4)"]
async fn smoke_structured_data_space() {
    println!("── xtable structured-data-space smoke ──");
    let (mut app, _tmp) = build_app().await;
    let mut failures = 0;

    failures += (!assert_status(
        "register schema v1",
        &mut app,
        Request::builder()
            .method("POST")
            .uri("/v1/spaces/acme/schemas")
            .header("content-type", "application/json")
            .body(body(json!({
                "name": "task",
                "body": {
                    "type": "object",
                    "required": ["title", "status"],
                    "properties": {
                        "title":  {"type": "string", "minLength": 1},
                        "status": {"enum": ["open", "done"]},
                        "n":      {"type": "integer"}
                    }
                }
            })))
            .unwrap(),
        StatusCode::CREATED,
    )
    .await) as usize;

    failures += (!assert_status(
        "bind table-schema",
        &mut app,
        Request::builder()
            .method("POST")
            .uri("/v1/spaces/acme/tables/tasks/bind")
            .header("content-type", "application/json")
            .body(body(json!({
                "body": {
                    "type": "object",
                    "required": ["title", "status"],
                    "properties": {
                        "title":  {"type": "string", "minLength": 1},
                        "status": {"enum": ["open", "done"]},
                        "n":      {"type": "integer"}
                    }
                }
            })))
            .unwrap(),
        StatusCode::NO_CONTENT,
    )
    .await) as usize;

    failures += (!assert_status(
        "upsert valid record (t1)",
        &mut app,
        Request::builder()
            .method("POST")
            .uri("/v1/spaces/acme/tables/tasks/records")
            .header("content-type", "application/json")
            .body(body(json!({
                "record_id": "t1",
                "body": {"title": "alpha", "status": "open", "n": 1}
            })))
            .unwrap(),
        StatusCode::CREATED,
    )
    .await) as usize;

    failures += (!assert_status(
        "upsert invalid body rejected",
        &mut app,
        Request::builder()
            .method("POST")
            .uri("/v1/spaces/acme/tables/tasks/records")
            .header("content-type", "application/json")
            .body(body(json!({
                "record_id": "t2",
                "body": {"title": "x", "status": "weird"}
            })))
            .unwrap(),
        StatusCode::BAD_REQUEST,
    )
    .await) as usize;

    for (id, n, status) in [("t3", 5, "open"), ("t4", 3, "done"), ("t5", 9, "open")] {
        failures += (!assert_status(
            &format!("upsert {id}"),
            &mut app,
            Request::builder()
                .method("POST")
                .uri("/v1/spaces/acme/tables/tasks/records")
                .header("content-type", "application/json")
                .body(body(json!({
                    "record_id": id,
                    "body": {"title": format!("task {id}"), "status": status, "n": n}
                })))
                .unwrap(),
            StatusCode::CREATED,
        )
        .await) as usize;
    }

    failures += (!assert_status(
        "get record t1",
        &mut app,
        Request::builder()
            .method("GET")
            .uri("/v1/spaces/acme/tables/tasks/records/t1")
            .body(Body::empty())
            .unwrap(),
        StatusCode::OK,
    )
    .await) as usize;

    // Query: filter status=open + sort by n asc.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/spaces/acme/tables/tasks/records?filter_field=status&filter_op=eq&filter_value=open&sort=n&dir=asc")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let resp_body = read_json(resp).await;
    let ids: Vec<String> = resp_body["records"]
        .as_array()
        .unwrap_or(&Vec::new())
        .iter()
        .map(|r| r["record_id"].as_str().unwrap_or("?").to_string())
        .collect();
    if ids == vec!["t1", "t3", "t5"] {
        println!("[PASS] query open asc by n: {ids:?}");
    } else {
        eprintln!("[FAIL] query open asc by n: got {ids:?}");
        failures += 1;
    }

    failures += (!assert_status(
        "delete t1",
        &mut app,
        Request::builder()
            .method("DELETE")
            .uri("/v1/spaces/acme/tables/tasks/records/t1")
            .body(Body::empty())
            .unwrap(),
        StatusCode::OK,
    )
    .await) as usize;

    failures += (!assert_status(
        "get t1 after delete → 404",
        &mut app,
        Request::builder()
            .method("GET")
            .uri("/v1/spaces/acme/tables/tasks/records/t1")
            .body(Body::empty())
            .unwrap(),
        StatusCode::NOT_FOUND,
    )
    .await) as usize;

    // Snapshots and diff.
    let s1 = read_json(
        app.clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/spaces/acme/snapshot")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap(),
    )
    .await["snapshot_version"]
        .as_u64()
        .unwrap_or(0);

    failures += (!assert_status(
        "upsert after snapshot",
        &mut app,
        Request::builder()
            .method("POST")
            .uri("/v1/spaces/acme/tables/tasks/records")
            .header("content-type", "application/json")
            .body(body(json!({
                "record_id": "after_snap",
                "body": {"title": "later", "status": "open", "n": 100}
            })))
            .unwrap(),
        StatusCode::CREATED,
    )
    .await) as usize;

    let s2 = read_json(
        app.clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/spaces/acme/snapshot")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap(),
    )
    .await["snapshot_version"]
        .as_u64()
        .unwrap_or(0);
    if s2 > s1 {
        println!("[PASS] snapshot advanced: {s1} → {s2}");
    } else {
        eprintln!("[FAIL] snapshot did not advance: {s1} → {s2}");
        failures += 1;
    }

    let uri = format!("/v1/spaces/acme/tables/tasks/diff?s1={s1}&s2={s2}");
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let diff_body = read_json(resp).await;
    let count = diff_body["changes"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    if count >= 1 {
        println!("[PASS] diff surfaces {count} change(s)");
    } else {
        eprintln!("[FAIL] diff returned no changes (want ≥1)");
        failures += 1;
    }

    failures += (!assert_status(
        "record at S1 not yet visible",
        &mut app,
        Request::builder()
            .method("GET")
            .uri(&format!(
                "/v1/spaces/acme/tables/tasks/records/after_snap?snapshot={s1}"
            ))
            .body(Body::empty())
            .unwrap(),
        StatusCode::NOT_FOUND,
    )
    .await) as usize;

    failures += (!assert_status(
        "record at S2 visible",
        &mut app,
        Request::builder()
            .method("GET")
            .uri(&format!(
                "/v1/spaces/acme/tables/tasks/records/after_snap?snapshot={s2}"
            ))
            .body(Body::empty())
            .unwrap(),
        StatusCode::OK,
    )
    .await) as usize;

    println!("── smoke done; {failures} failure(s) ──");
    assert_eq!(failures, 0, "smoke had {failures} failure(s)");
}
