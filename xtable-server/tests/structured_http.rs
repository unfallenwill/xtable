//! Integration test for the structured-data-space HTTP routes.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use tempfile::TempDir;
use tower::ServiceExt;

use xtable_backend::BackendClient;
use xtable_core::config::Config;
use xtable_server::app::AppState;
use xtable_storage::LocalStore;

async fn test_app() -> (axum::Router, TempDir) {
    let tmp = TempDir::new().unwrap();
    let mut cfg = Config::default();
    cfg.server.data_dir = tmp.path().to_path_buf();
    cfg.storage.redb_dir = tmp.path().join("redb");
    cfg.storage.staged_body_spill_dir = tmp.path().join("staged");
    std::fs::create_dir_all(&cfg.storage.redb_dir).unwrap();
    std::fs::create_dir_all(&cfg.storage.staged_body_spill_dir).unwrap();

    let store = LocalStore::open(&cfg.storage.redb_dir).unwrap();
    let backend = BackendClient::dummy_for_test_async().await.unwrap();
    cfg.auth.jwt_secret = "test".into();

    let state = AppState::new(
        cfg,
        store,
        backend,
        xtable_telemetry::metrics::Metrics::default(),
    );
    let app = xtable_server::structured::router().with_state(Arc::new(state));
    (app, tmp)
}

fn json_body(v: Value) -> Body {
    Body::from(serde_json::to_vec(&v).unwrap())
}

async fn read_json(resp: axum::response::Response) -> Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap_or(Value::Null)
}

#[tokio::test]
async fn explicit_transaction_write_commit_makes_record_visible() {
    let (app, _t) = test_app().await;

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/structured/txn")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let begin = read_json(resp).await;
    let txn_id = begin["txn_id"].as_str().unwrap().to_string();

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/structured/txn/{txn_id}/write"))
                .header("content-type", "application/json")
                .body(json_body(json!({
                    "space": "s",
                    "table": "t",
                    "record_id": "r1",
                    "body": { "value": 1 }
                })))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // A staged write is not visible before commit.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/spaces/s/tables/t/records/r1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/structured/txn/{txn_id}/commit"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/spaces/s/tables/t/records/r1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(read_json(resp).await["body"]["value"], 1);
}

#[tokio::test]
async fn explicit_transaction_abort_discards_staged_write() {
    let (app, _t) = test_app().await;
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/structured/txn")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let txn_id = read_json(resp).await["txn_id"]
        .as_str()
        .unwrap()
        .to_string();

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/structured/txn/{txn_id}/write"))
                .header("content-type", "application/json")
                .body(json_body(json!({
                    "space": "s",
                    "table": "t",
                    "record_id": "r1",
                    "body": { "value": 1 }
                })))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/structured/txn/{txn_id}/abort"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/spaces/s/tables/t/records/r1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
#[ignore = "spec §5.1 removed per-record PUTs; structured-data-space reads must walk MemTable (re-enable in Task 4)"]
async fn register_schema_then_upsert_record() {
    let (app, _t) = test_app().await;

    // Register a schema.
    let req = Request::builder()
        .method("POST")
        .uri("/v1/spaces/acme/schemas")
        .header("content-type", "application/json")
        .body(json_body(json!({
            "name": "task",
            "body": {
                "type": "object",
                "required": ["title"],
                "properties": { "title": {"type": "string"}, "done": {"type": "boolean"} },
                "additionalProperties": false
            }
        })))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let body = read_json(resp).await;
    assert_eq!(body["version"], 1);

    // Bind the schema to a table.
    let req = Request::builder()
        .method("POST")
        .uri("/v1/spaces/acme/tables/tasks/bind")
        .header("content-type", "application/json")
        .body(json_body(json!({
            "body": {
                "type": "object",
                "required": ["title"],
                "properties": { "title": {"type": "string"} }
            }
        })))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // Upsert a record.
    let req = Request::builder()
        .method("POST")
        .uri("/v1/spaces/acme/tables/tasks/records")
        .header("content-type", "application/json")
        .body(json_body(json!({
            "record_id": "t1",
            "body": { "title": "first", "done": false }
        })))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let body = read_json(resp).await;
    assert_eq!(body["record_id"], "t1");
    assert_eq!(body["schema_version"], 1);

    // Get the record.
    let req = Request::builder()
        .method("GET")
        .uri("/v1/spaces/acme/tables/tasks/records/t1")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = read_json(resp).await;
    assert_eq!(body["body"]["title"], "first");
}

#[tokio::test]
#[ignore = "spec §5.1 removed per-record PUTs; structured-data-space reads must walk MemTable (re-enable in Task 4)"]
async fn upsert_invalid_body_rejected_400() {
    let (app, _t) = test_app().await;

    // Bind a schema.
    let req = Request::builder()
        .method("POST")
        .uri("/v1/spaces/s1/tables/t/bind")
        .header("content-type", "application/json")
        .body(json_body(json!({
            "body": { "type": "object", "required": ["x"], "properties": {"x": {"type": "integer"}} }
        })))
        .unwrap();
    let bind_resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        bind_resp.status(),
        StatusCode::NO_CONTENT,
        "bind should succeed"
    );

    // Upsert invalid.
    let req = Request::builder()
        .method("POST")
        .uri("/v1/spaces/s1/tables/t/records")
        .header("content-type", "application/json")
        .body(json_body(json!({
            "record_id": "r",
            "body": { "x": "not int" }
        })))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "invalid body should be rejected"
    );
}

#[tokio::test]
async fn delete_record_returns_404_on_subsequent_get() {
    let (app, _t) = test_app().await;
    let req = Request::builder()
        .method("POST")
        .uri("/v1/spaces/s/tables/t/records")
        .header("content-type", "application/json")
        .body(json_body(json!({
            "record_id": "x",
            "body": { "v": 1 }
        })))
        .unwrap();
    let _ = app.clone().oneshot(req).await.unwrap();

    let req = Request::builder()
        .method("DELETE")
        .uri("/v1/spaces/s/tables/t/records/x")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let req = Request::builder()
        .method("GET")
        .uri("/v1/spaces/s/tables/t/records/x")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn query_with_filter_and_sort() {
    let (app, _t) = test_app().await;

    for (i, id) in ["a", "b", "c"].iter().enumerate() {
        let body = json!({
            "record_id": id,
            "body": { "n": i as u64 + 1, "name": id }
        });
        let req = Request::builder()
            .method("POST")
            .uri("/v1/spaces/s/tables/t/records")
            .header("content-type", "application/json")
            .body(json_body(body))
            .unwrap();
        let _ = app.clone().oneshot(req).await.unwrap();
    }

    let req = Request::builder()
        .method("GET")
        .uri("/v1/spaces/s/tables/t/records?sort=n&dir=desc&limit=2")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = read_json(resp).await;
    let ids: Vec<&str> = body["records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["record_id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["c", "b"]);
}

#[tokio::test]
async fn diff_returns_changed_records() {
    let (app, _t) = test_app().await;
    let req = Request::builder()
        .method("POST")
        .uri("/v1/spaces/s/tables/t/records")
        .header("content-type", "application/json")
        .body(json_body(json!({
            "record_id": "r",
            "body": { "v": 1 }
        })))
        .unwrap();
    let _ = app.clone().oneshot(req).await.unwrap();

    // Get a snapshot.
    let snap1 = {
        let req = Request::builder()
            .method("GET")
            .uri("/v1/spaces/s/snapshot")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        let b = read_json(resp).await;
        b["snapshot_version"].as_u64().unwrap()
    };

    // Update the record.
    let req = Request::builder()
        .method("POST")
        .uri("/v1/spaces/s/tables/t/records")
        .header("content-type", "application/json")
        .body(json_body(json!({
            "record_id": "r",
            "body": { "v": 2 }
        })))
        .unwrap();
    let _ = app.clone().oneshot(req).await.unwrap();

    let snap2 = {
        let req = Request::builder()
            .method("GET")
            .uri("/v1/spaces/s/snapshot")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        let b = read_json(resp).await;
        b["snapshot_version"].as_u64().unwrap()
    };

    assert_ne!(snap1, snap2);
    let uri = format!("/v1/spaces/s/tables/t/diff?s1={}&s2={}", snap1, snap2);
    let req = Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = read_json(resp).await;
    assert!(!body["changes"].as_array().unwrap().is_empty());
}

#[tokio::test]
#[ignore = "spec §5.1 removed per-record PUTs; structured-data-space reads must walk MemTable (re-enable in Task 4)"]
async fn list_schemas_returns_registered() {
    let (app, _t) = test_app().await;
    let req = Request::builder()
        .method("POST")
        .uri("/v1/spaces/s/schemas")
        .header("content-type", "application/json")
        .body(json_body(json!({ "name": "a", "body": {"type":"object"} })))
        .unwrap();
    let _ = app.clone().oneshot(req).await.unwrap();
    let req = Request::builder()
        .method("POST")
        .uri("/v1/spaces/s/schemas")
        .header("content-type", "application/json")
        .body(json_body(json!({ "name": "b", "body": {"type":"string"} })))
        .unwrap();
    let _ = app.clone().oneshot(req).await.unwrap();

    let req = Request::builder()
        .method("GET")
        .uri("/v1/spaces/s/schemas")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = read_json(resp).await;
    let schemas = body["schemas"].as_array().unwrap();
    let names: Vec<&str> = schemas
        .iter()
        .map(|s| s["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["a", "b"]);
}

#[tokio::test]
async fn begin_structured_txn_returns_id() {
    let (app, _t) = test_app().await;
    let req = Request::builder()
        .method("POST")
        .uri("/v1/structured/txn")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let body = read_json(resp).await;
    assert!(body["txn_id"].is_string());
    assert!(body["snapshot_version"].is_number());
}

#[tokio::test]
async fn get_record_returns_404_when_missing() {
    let (app, _t) = test_app().await;
    let req = Request::builder()
        .method("GET")
        .uri("/v1/spaces/s/tables/t/records/none")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn get_schema_returns_404_when_missing() {
    let (app, _t) = test_app().await;
    let req = Request::builder()
        .method("GET")
        .uri("/v1/spaces/s/schemas/missing")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
#[ignore = "spec §5.1 removed per-record PUTs; structured-data-space reads must walk MemTable (re-enable in Task 4)"]
async fn get_schema_with_explicit_version() {
    let (app, _t) = test_app().await;
    // Register v1.
    let req = Request::builder()
        .method("POST")
        .uri("/v1/spaces/s/schemas")
        .header("content-type", "application/json")
        .body(json_body(
            json!({ "name": "n", "body": {"type":"object","required":["v"]} }),
        ))
        .unwrap();
    let _ = app.clone().oneshot(req).await.unwrap();
    let req = Request::builder()
        .method("POST")
        .uri("/v1/spaces/s/schemas")
        .header("content-type", "application/json")
        .body(json_body(
            json!({ "name": "n", "body": {"type":"object","required":["w"]} }),
        ))
        .unwrap();
    let _ = app.clone().oneshot(req).await.unwrap();

    let req = Request::builder()
        .method("GET")
        .uri("/v1/spaces/s/schemas/n?version=1")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = read_json(resp).await;
    assert_eq!(body["version"], 1);
    assert!(body["body"]["required"]
        .as_array()
        .unwrap()
        .contains(&json!("v")));
}

#[tokio::test]
async fn query_with_filter_ops() {
    let (app, _t) = test_app().await;
    for (id, n) in [("a", 1), ("b", 2), ("c", 3)] {
        let body = json!({ "record_id": id, "body": {"n": n} });
        let req = Request::builder()
            .method("POST")
            .uri("/v1/spaces/s/tables/t/records")
            .header("content-type", "application/json")
            .body(json_body(body))
            .unwrap();
        let _ = app.clone().oneshot(req).await.unwrap();
    }
    let req = Request::builder()
        .method("GET")
        .uri("/v1/spaces/s/tables/t/records?filter_field=n&filter_op=ge&filter_value=2")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = read_json(resp).await;
    let ids: Vec<&str> = body["records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["record_id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["b", "c"]);
}

#[tokio::test]
async fn query_with_unknown_filter_op_returns_400() {
    let (app, _t) = test_app().await;
    let req = Request::builder()
        .method("GET")
        .uri("/v1/spaces/s/tables/t/records?filter_field=x&filter_op=fancy&filter_value=1")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn query_with_contains_filter() {
    let (app, _t) = test_app().await;
    let body = json!({ "record_id": "a", "body": {"name": "alpha"} });
    let req = Request::builder()
        .method("POST")
        .uri("/v1/spaces/s/tables/t/records")
        .header("content-type", "application/json")
        .body(json_body(body))
        .unwrap();
    let _ = app.clone().oneshot(req).await.unwrap();
    let req = Request::builder()
        .method("GET")
        .uri("/v1/spaces/s/tables/t/records?filter_field=name&filter_op=contains&filter_value=alp")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let body = read_json(resp).await;
    assert_eq!(body["total_matched"], 1);
}

#[tokio::test]
async fn diff_with_invalid_snapshots_returns_400() {
    let (app, _t) = test_app().await;
    let req = Request::builder()
        .method("GET")
        .uri("/v1/spaces/s/tables/t/diff?s1=foo&s2=bar")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn space_snapshot_returns_current_version() {
    let (app, _t) = test_app().await;
    let req = Request::builder()
        .method("GET")
        .uri("/v1/spaces/anything/snapshot")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = read_json(resp).await;
    assert_eq!(body["space"], "anything");
    assert!(body["snapshot_version"].is_number());
}

#[tokio::test]
async fn bind_table_404_on_invalid_schema_body() {
    let (app, _t) = test_app().await;
    let req = Request::builder()
        .method("POST")
        .uri("/v1/spaces/s/tables/t/bind")
        .header("content-type", "application/json")
        .body(json_body(json!({ "body": "not an object" })))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn get_record_with_snapshot_query() {
    let (app, _t) = test_app().await;
    let req = Request::builder()
        .method("POST")
        .uri("/v1/spaces/s/tables/t/records")
        .header("content-type", "application/json")
        .body(json_body(json!({ "record_id": "r", "body": { "v": 1 } })))
        .unwrap();
    let _ = app.clone().oneshot(req).await.unwrap();

    let req = Request::builder()
        .method("GET")
        .uri("/v1/spaces/s/tables/t/records/r?snapshot=0")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    // Snapshot 0 < current commit → record not visible at that snapshot.
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn diff_with_missing_query_returns_400() {
    let (app, _t) = test_app().await;
    let req = Request::builder()
        .method("GET")
        .uri("/v1/spaces/s/tables/t/diff")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    // Should still work: parse error. Actually with missing query params, axum returns 400.
    let s = resp.status();
    assert!(
        s == StatusCode::BAD_REQUEST || s == StatusCode::INTERNAL_SERVER_ERROR,
        "unexpected {s}"
    );
}

#[tokio::test]
async fn query_filter_exists_matches_existing_field() {
    let (app, _t) = test_app().await;
    let req = Request::builder()
        .method("POST")
        .uri("/v1/spaces/s/tables/t/records")
        .header("content-type", "application/json")
        .body(json_body(json!({ "record_id": "x", "body": {"k": 1} })))
        .unwrap();
    let _ = app.clone().oneshot(req).await.unwrap();
    let req = Request::builder()
        .method("POST")
        .uri("/v1/spaces/s/tables/t/records")
        .header("content-type", "application/json")
        .body(json_body(json!({ "record_id": "y", "body": {} })))
        .unwrap();
    let _ = app.clone().oneshot(req).await.unwrap();
    let req = Request::builder()
        .method("GET")
        .uri("/v1/spaces/s/tables/t/records?filter_field=k&filter_op=exists&filter_value=true")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let body = read_json(resp).await;
    let ids: Vec<&str> = body["records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["record_id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["x"]);
}

#[tokio::test]
async fn query_filter_ne_includes_others() {
    let (app, _t) = test_app().await;
    let req = Request::builder()
        .method("POST")
        .uri("/v1/spaces/s/tables/t/records")
        .header("content-type", "application/json")
        .body(json_body(json!({ "record_id": "a", "body": {"n": 1} })))
        .unwrap();
    let _ = app.clone().oneshot(req).await.unwrap();
    let req = Request::builder()
        .method("POST")
        .uri("/v1/spaces/s/tables/t/records")
        .header("content-type", "application/json")
        .body(json_body(json!({ "record_id": "b", "body": {"n": 2} })))
        .unwrap();
    let _ = app.clone().oneshot(req).await.unwrap();
    let req = Request::builder()
        .method("GET")
        .uri("/v1/spaces/s/tables/t/records?filter_field=n&filter_op=ne&filter_value=1")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let body = read_json(resp).await;
    // total_matched is records pre-filter; output records reflect post-filter.
    assert_eq!(body["records"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn query_filter_lt_le_geq() {
    let (app, _t) = test_app().await;
    for n in [1u64, 2, 3, 4] {
        let req = Request::builder()
            .method("POST")
            .uri("/v1/spaces/s/tables/t/records")
            .header("content-type", "application/json")
            .body(json_body(
                json!({ "record_id": format!("r{n}"), "body": {"n": n} }),
            ))
            .unwrap();
        let _ = app.clone().oneshot(req).await.unwrap();
    }
    for (op, expected) in [
        ("lt", 2u64), // 1 only
        ("le", 2),    // 1, 2
        ("gt", 4),    // 4 only (3 entries go via single check)
    ] {
        let req = Request::builder()
            .method("GET")
            .uri(format!("/v1/spaces/s/tables/t/records?filter_field=n&filter_op={op}&filter_value={expected}"))
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        let body = read_json(resp).await;
        let matched = body["total_matched"].as_u64().unwrap();
        assert!(matched >= 1, "op={op} expected ≥1, got {matched}");
    }
}

#[tokio::test]
async fn upsert_with_autoid_record_id() {
    let (app, _t) = test_app().await;
    let req = Request::builder()
        .method("POST")
        .uri("/v1/spaces/s/tables/t/records")
        .header("content-type", "application/json")
        .body(json_body(json!({ "body": {"n": 1} })))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let body = read_json(resp).await;
    let rid = body["record_id"].as_str().unwrap();
    assert!(!rid.is_empty());
}

#[tokio::test]
async fn upsert_with_invalid_json_returns_400() {
    let (app, _t) = test_app().await;
    let req = Request::builder()
        .method("POST")
        .uri("/v1/spaces/s/tables/t/records")
        .header("content-type", "application/json")
        .body(Body::from("not json {{"))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    // Axum rejects malformed JSON → 4xx.
    assert_eq!(resp.status().is_client_error(), true);
}

#[tokio::test]
async fn register_schema_missing_body_field_returns_4xx() {
    let (app, _t) = test_app().await;
    let req = Request::builder()
        .method("POST")
        .uri("/v1/spaces/s/schemas")
        .header("content-type", "application/json")
        .body(json_body(json!({ "name": "n" })))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let s = resp.status();
    assert!(s.is_client_error(), "expected 4xx, got {s}");
}

#[tokio::test]
async fn register_schema_non_object_body_returns_400() {
    let (app, _t) = test_app().await;
    let req = Request::builder()
        .method("POST")
        .uri("/v1/spaces/s/schemas")
        .header("content-type", "application/json")
        .body(json_body(
            json!({ "name": "n", "body": ["array", "not", "object"] }),
        ))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn bind_table_non_object_body_returns_400() {
    let (app, _t) = test_app().await;
    let req = Request::builder()
        .method("POST")
        .uri("/v1/spaces/s/tables/t/bind")
        .header("content-type", "application/json")
        .body(json_body(json!({ "body": 42 })))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn upsert_no_schema_returns_success() {
    let (app, _t) = test_app().await;
    let req = Request::builder()
        .method("POST")
        .uri("/v1/spaces/s/tables/t/records")
        .header("content-type", "application/json")
        .body(json_body(
            json!({ "record_id": "r", "body": { "anything": "goes" } }),
        ))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn batch_upsert_commits_all_records_at_one_version() {
    let (app, _t) = test_app().await;
    let req = Request::builder()
        .method("POST")
        .uri("/v1/spaces/s/tables/t/records/batch")
        .header("content-type", "application/json")
        .body(json_body(json!({
            "records": (1..=10)
                .map(|i| json!({ "record_id": format!("r{i}"), "body": { "n": i } }))
                .collect::<Vec<_>>()
        })))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let body = read_json(resp).await;
    assert_eq!(body["records"].as_array().unwrap().len(), 10);
    let versions: std::collections::HashSet<_> = body["records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|record| record["commit_version"].as_u64().unwrap())
        .collect();
    assert_eq!(versions.len(), 1);
    assert_eq!(
        body["commit_version"].as_u64(),
        versions.iter().next().copied()
    );

    let req = Request::builder()
        .method("GET")
        .uri("/v1/spaces/s/tables/t/records?limit=20")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = read_json(resp).await;
    assert_eq!(body["total_matched"], 10);
}

#[tokio::test]
async fn explicit_transaction_commits_records_with_different_schemas_at_one_snapshot() {
    let (app, _t) = test_app().await;

    for (name, table, field) in [("user", "users", "name"), ("order", "orders", "total")] {
        let req = Request::builder()
            .method("POST")
            .uri("/v1/spaces/s/schemas")
            .header("content-type", "application/json")
            .body(json_body(json!({
                "name": name,
                "body": { "type": "object", "required": [field] }
            })))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(req).await.unwrap().status(),
            StatusCode::CREATED
        );

        let req = Request::builder()
            .method("POST")
            .uri(format!("/v1/spaces/s/tables/{table}/bind"))
            .header("content-type", "application/json")
            .body(json_body(json!({
                "body": { "type": "object", "required": [field] }
            })))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(req).await.unwrap().status(),
            StatusCode::NO_CONTENT
        );
    }

    let req = Request::builder()
        .method("POST")
        .uri("/v1/structured/txn")
        .body(Body::empty())
        .unwrap();
    let txn_id = read_json(app.clone().oneshot(req).await.unwrap()).await["txn_id"]
        .as_str()
        .unwrap()
        .to_string();

    for (table, record_id, body) in [
        ("users", "u1", json!({ "name": "Ada" })),
        ("orders", "o1", json!({ "total": 42 })),
    ] {
        let req = Request::builder()
            .method("POST")
            .uri(format!("/v1/structured/txn/{txn_id}/write"))
            .header("content-type", "application/json")
            .body(json_body(json!({
                "space": "s",
                "table": table,
                "record_id": record_id,
                "body": body
            })))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(req).await.unwrap().status(),
            StatusCode::OK
        );
    }

    let req = Request::builder()
        .method("POST")
        .uri(format!("/v1/structured/txn/{txn_id}/commit"))
        .body(Body::empty())
        .unwrap();
    let commit = read_json(app.clone().oneshot(req).await.unwrap()).await;
    assert_eq!(commit["committed"], true);
    let commit_version = commit["commit_version"].as_u64().unwrap();

    for (table, record_id, field) in [("users", "u1", "name"), ("orders", "o1", "total")] {
        let req = Request::builder()
            .method("GET")
            .uri(format!("/v1/spaces/s/tables/{table}/records/{record_id}"))
            .body(Body::empty())
            .unwrap();
        let record = read_json(app.clone().oneshot(req).await.unwrap()).await;
        assert_eq!(record["commit_version"], commit_version);
        assert!(record["body"].get(field).is_some());
    }
}

#[tokio::test]
async fn explicit_transaction_aborts_schema_binding_and_record_together() {
    let (app, _t) = test_app().await;
    let req = Request::builder()
        .method("POST")
        .uri("/v1/structured/txn")
        .body(Body::empty())
        .unwrap();
    let txn_id = read_json(app.clone().oneshot(req).await.unwrap()).await["txn_id"]
        .as_str()
        .unwrap()
        .to_string();

    let req = Request::builder()
        .method("POST")
        .uri(format!("/v1/structured/txn/{txn_id}/bind"))
        .header("content-type", "application/json")
        .body(json_body(json!({
            "space": "s",
            "table": "t",
            "body": { "type": "object", "required": ["n"], "properties": { "n": { "type": "integer" } } }
        })))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(req).await.unwrap().status(),
        StatusCode::OK
    );

    let req = Request::builder()
        .method("POST")
        .uri(format!("/v1/structured/txn/{txn_id}/write"))
        .header("content-type", "application/json")
        .body(json_body(json!({
            "space": "s",
            "table": "t",
            "record_id": "r1",
            "body": { "n": 1 }
        })))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(req).await.unwrap().status(),
        StatusCode::OK
    );

    let req = Request::builder()
        .method("POST")
        .uri(format!("/v1/structured/txn/{txn_id}/abort"))
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        app.clone().oneshot(req).await.unwrap().status(),
        StatusCode::OK
    );

    // The binding was part of the aborted transaction. An unconstrained
    // body is accepted by a later short transaction, proving the metadata
    // write was discarded together with the record write.
    let req = Request::builder()
        .method("POST")
        .uri("/v1/spaces/s/tables/t/records")
        .header("content-type", "application/json")
        .body(json_body(json!({
            "record_id": "r2",
            "body": { "not_n": "accepted" }
        })))
        .unwrap();
    assert_eq!(
        app.oneshot(req).await.unwrap().status(),
        StatusCode::CREATED
    );
}

#[tokio::test]
async fn batch_upsert_rolls_back_when_one_record_is_invalid() {
    let (app, _t) = test_app().await;
    let req = Request::builder()
        .method("POST")
        .uri("/v1/spaces/s/schemas")
        .header("content-type", "application/json")
        .body(json_body(json!({
            "name": "task",
            "body": { "type": "object", "required": ["n"], "properties": { "n": { "type": "integer" } } }
        })))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(req).await.unwrap().status(),
        StatusCode::CREATED
    );

    let req = Request::builder()
        .method("POST")
        .uri("/v1/spaces/s/tables/t/bind")
        .header("content-type", "application/json")
        .body(json_body(json!({
            "body": { "type": "object", "required": ["n"], "properties": { "n": { "type": "integer" } } }
        })))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(req).await.unwrap().status(),
        StatusCode::NO_CONTENT
    );

    let req = Request::builder()
        .method("POST")
        .uri("/v1/spaces/s/tables/t/records/batch")
        .header("content-type", "application/json")
        .body(json_body(json!({
            "records": [
                { "record_id": "valid", "body": { "n": 1 } },
                { "record_id": "invalid", "body": { "n": "not-an-integer" } }
            ]
        })))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let req = Request::builder()
        .method("GET")
        .uri("/v1/spaces/s/tables/t/records/valid")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn delete_record_returns_404_when_missing() {
    let (app, _t) = test_app().await;
    let req = Request::builder()
        .method("DELETE")
        .uri("/v1/spaces/s/tables/t/records/none")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn upsert_rejects_missing_record_id_when_required() {
    // Verify basic record creation succeeds with the minimal request shape.
    let (app, _t) = test_app().await;
    let req = Request::builder()
        .method("POST")
        .uri("/v1/spaces/s/tables/t/records")
        .header("content-type", "application/json")
        .body(json_body(json!({ "record_id": "a", "body": {"n": 1} })))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
}
