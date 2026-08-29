//! Structured-data-space HTTP routes.
//!
//! All routes are mounted under `/v1/spaces/:space/...`. Each write request
//! runs in its own short-lived transaction so multi-record atomicity per
//! request is guaranteed. For cross-request atomicity, use the explicit
//! `/v1/structured/txn` endpoints (or compose with the lower-level
//! `/?transactional=...` routes).
//!
//! Error responses are JSON: `{"error": "<msg>", "code": "<s3-style>"}`.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Json;
use axum::Router;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use xtable_core::{XtableError, XtableResult};
use xtable_schema::{
    Filter, OrderDir, Query as StructuredQuery, RecordWrite, WriteOutcome,
};

use crate::app::AppState;

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/v1/spaces/:space/schemas", post(register_schema).get(list_schemas))
        .route("/v1/spaces/:space/schemas/:name", get(get_schema))
        .route("/v1/spaces/:space/tables/:table/bind", post(bind_table))
        .route(
            "/v1/spaces/:space/tables/:table/records",
            post(upsert_record).get(query_records),
        )
        .route(
            "/v1/spaces/:space/tables/:table/records/:record_id",
            get(get_record).delete(delete_record),
        )
        .route("/v1/spaces/:space/tables/:table/diff", get(diff_records))
        .route("/v1/structured/txn", post(begin_structured_txn))
        .route("/v1/spaces/:space/snapshot", get(space_snapshot))
}

#[derive(Debug, Deserialize)]
struct RegisterSchemaReq {
    name: String,
    body: Value,
}

async fn register_schema(
    State(state): State<Arc<AppState>>,
    Path(space): Path<String>,
    Json(req): Json<RegisterSchemaReq>,
) -> Response {
    let space_in = space.clone();
    let name_in = req.name.clone();
    let body_in = req.body.clone();
    let result: XtableResult<u32> = async {
        let t = state.structured.begin_txn().await?;
        let v = state
            .structured
            .register_schema(&t, &space_in, &name_in, body_in)
            .await?;
        let _ = state.structured.commit_txn(&t).await?;
        Ok(v)
    }
    .await;
    match result {
        Ok(version) => (
            StatusCode::CREATED,
            Json(json!({ "version": version, "name": req.name })),
        )
            .into_response(),
        Err(e) => error_response(e),
    }
}

#[derive(Debug, Deserialize)]
struct SchemaQuery {
    version: Option<String>,
    snapshot: Option<String>,
}

async fn get_schema(
    State(state): State<Arc<AppState>>,
    Path((space, name)): Path<(String, String)>,
    Query(params): Query<SchemaQuery>,
) -> Response {
    let snap = params.snapshot.as_deref().and_then(|s| s.parse::<u64>().ok());
    let version = params.version.as_deref().and_then(|s| s.parse::<u32>().ok());
    match state.structured.get_schema(&space, &name, version, snap).await {
        Ok(Some(info)) => (
            StatusCode::OK,
            Json(json!({
                "space": info.space,
                "name": info.name,
                "version": info.version,
                "body": info.body,
            })),
        )
            .into_response(),
        Ok(None) => not_found(),
        Err(e) => error_response(e),
    }
}

async fn list_schemas(
    State(state): State<Arc<AppState>>,
    Path(space): Path<String>,
) -> Response {
    match state.structured.list_schemas(&space).await {
        Ok(items) => (
            StatusCode::OK,
            Json(json!({
                "space": space,
                "schemas": items.iter().map(|s| json!({
                    "name": s.name,
                    "version": s.version,
                })).collect::<Vec<_>>(),
            })),
        )
            .into_response(),
        Err(e) => error_response(e),
    }
}

#[derive(Debug, Deserialize)]
struct BindReq {
    body: Value,
}

async fn bind_table(
    State(state): State<Arc<AppState>>,
    Path((space, table)): Path<(String, String)>,
    Json(req): Json<BindReq>,
) -> Response {
    let space_in = space.clone();
    let table_in = table.clone();
    let body_in = req.body.clone();
    let result: XtableResult<()> = async {
        let t = state.structured.begin_txn().await?;
        state
            .structured
            .bind_table_schema(&t, &space_in, &table_in, body_in)
            .await?;
        let _ = state.structured.commit_txn(&t).await?;
        Ok(())
    }
    .await;
    match result {
        Ok(()) => (StatusCode::NO_CONTENT, "").into_response(),
        Err(e) => error_response(e),
    }
}

#[derive(Debug, Deserialize)]
struct UpsertRecordReq {
    record_id: Option<String>,
    body: Value,
    expected_schema_version: Option<u32>,
}

#[derive(Debug, Serialize)]
struct UpsertRecordResp {
    record_id: String,
    schema_version: u32,
    backend_key: String,
    commit_version: u64,
}

async fn upsert_record(
    State(state): State<Arc<AppState>>,
    Path((space, table)): Path<(String, String)>,
    Json(req): Json<UpsertRecordReq>,
) -> Response {
    let space_in = space.clone();
    let table_in = table.clone();
    let result: XtableResult<(WriteOutcome, u64)> = async {
        let t = state.structured.begin_txn().await?;
        let outcome = state
            .structured
            .upsert_record(
                &t,
                RecordWrite {
                    space: space_in,
                    table: table_in,
                    record_id: req.record_id.clone(),
                    body: req.body.clone(),
                    expected_schema_version: req.expected_schema_version,
                },
            )
            .await?;
        let commit = state.structured.commit_txn(&t).await?;
        Ok((outcome, commit))
    }
    .await;
    match result {
        Ok((o, c)) => (
            StatusCode::CREATED,
            Json(UpsertRecordResp {
                record_id: o.record_id,
                schema_version: o.schema_version,
                backend_key: o.backend_key,
                commit_version: c,
            }),
        )
            .into_response(),
        Err(e) => error_response(e),
    }
}

#[derive(Debug, Deserialize)]
struct GetRecordQuery {
    snapshot: Option<String>,
}

async fn get_record(
    State(state): State<Arc<AppState>>,
    Path((space, table, record_id)): Path<(String, String, String)>,
    Query(params): Query<GetRecordQuery>,
) -> Response {
    let snap = params.snapshot.as_deref().and_then(|s| s.parse::<u64>().ok());
    match state.structured.get_record(&space, &table, &record_id, snap) {
        Ok(Some(r)) => (
            StatusCode::OK,
            Json(json!({
                "space": r.space,
                "table": r.table,
                "record_id": r.record_id,
                "body": r.body,
                "schema_version": r.schema_version,
                "commit_version": r.commit_version,
                "deleted": r.deleted,
            })),
        )
            .into_response(),
        Ok(None) => not_found(),
        Err(e) => error_response(e),
    }
}

async fn delete_record(
    State(state): State<Arc<AppState>>,
    Path((space, table, record_id)): Path<(String, String, String)>,
) -> Response {
    let space_in = space.clone();
    let table_in = table.clone();
    let record_in = record_id.clone();
    let result: XtableResult<u64> = async {
        let t = state.structured.begin_txn().await?;
        state
            .structured
            .delete_record(&t, &space_in, &table_in, &record_in)
            .await?;
        let cv = state.structured.commit_txn(&t).await?;
        Ok(cv)
    }
    .await;
    match result {
        Ok(cv) => (
            StatusCode::OK,
            Json(json!({ "deleted": true, "commit_version": cv })),
        )
            .into_response(),
        Err(e) => error_response(e),
    }
}

#[derive(Debug, Deserialize)]
struct QueryParams {
    snapshot: Option<String>,
    limit: Option<String>,
    offset: Option<String>,
    sort: Option<String>,
    dir: Option<String>,
    filter_field: Option<String>,
    filter_op: Option<String>,
    filter_value: Option<String>,
}

async fn query_records(
    State(state): State<Arc<AppState>>,
    Path((space, table)): Path<(String, String)>,
    Query(params): Query<QueryParams>,
) -> Response {
    let snap = params.snapshot.as_deref().and_then(|s| s.parse::<u64>().ok());
    let mut q = StructuredQuery::new();
    if let (Some(f), Some(op), Some(v)) = (&params.filter_field, &params.filter_op, &params.filter_value) {
        match build_filter(f, op, v) {
            Ok(filter) => q = q.filter(filter),
            Err(e) => return error_response(e),
        }
    }
    if let Some(field) = &params.sort {
        let dir = match params.dir.as_deref() {
            Some("desc") | Some("DESC") => OrderDir::Desc,
            _ => OrderDir::Asc,
        };
        q = q.order(field, dir);
    }
    if let Some(n) = params.limit.as_deref().and_then(|s| s.parse::<usize>().ok()) {
        q = q.limit(n);
    }
    if let Some(o) = params.offset.as_deref().and_then(|s| s.parse::<usize>().ok()) {
        q = q.offset(o);
    }
    match state.structured.query_records(&space, &table, q, snap) {
        Ok(res) => (
            StatusCode::OK,
            Json(json!({
                "snapshot_version": res.snapshot_version,
                "total_matched": res.total_matched,
                "records": res.records.iter().map(|r| json!({
                    "space": r.space,
                    "table": r.table,
                    "record_id": r.record_id,
                    "body": r.body,
                    "schema_version": r.schema_version,
                    "commit_version": r.commit_version,
                    "deleted": r.deleted,
                })).collect::<Vec<_>>(),
            })),
        )
            .into_response(),
        Err(e) => error_response(e),
    }
}

fn build_filter(field: &str, op: &str, value: &str) -> XtableResult<Filter> {
    let v: Value = serde_json::from_str(value)
        .or_else(|_| Ok::<Value, serde_json::Error>(Value::String(value.to_string())))?;
    match op {
        "eq" => Ok(Filter::Eq { field: field.to_string(), value: v }),
        "ne" => Ok(Filter::Ne { field: field.to_string(), value: v }),
        "gt" => Ok(Filter::Gt { field: field.to_string(), value: v }),
        "ge" => Ok(Filter::Ge { field: field.to_string(), value: v }),
        "lt" => Ok(Filter::Lt { field: field.to_string(), value: v }),
        "le" => Ok(Filter::Le { field: field.to_string(), value: v }),
        "contains" => {
            let s = v
                .as_str()
                .ok_or_else(|| XtableError::invalid("contains requires string value"))?
                .to_string();
            Ok(Filter::Contains { field: field.to_string(), value: s })
        }
        "exists" => Ok(Filter::Exists { field: field.to_string() }),
        _ => Err(XtableError::invalid(format!("unknown filter op: {op}"))),
    }
}

#[derive(Debug, Deserialize)]
struct DiffQuery {
    s1: String,
    s2: String,
}

async fn diff_records(
    State(state): State<Arc<AppState>>,
    Path((space, table)): Path<(String, String)>,
    Query(params): Query<DiffQuery>,
) -> Response {
    let (s1, s2) = match (params.s1.parse::<u64>(), params.s2.parse::<u64>()) {
        (Ok(a), Ok(b)) => (a, b),
        _ => return error_response(XtableError::invalid("s1 and s2 must be u64")),
    };
    match state.structured.diff(&space, &table, s1, s2) {
        Ok(items) => (
            StatusCode::OK,
            Json(json!({
                "from": s1,
                "to": s2,
                "changes": items.iter().map(|(id, a, b)| json!({
                    "record_id": id,
                    "before": a,
                    "after": b,
                })).collect::<Vec<_>>(),
            })),
        )
            .into_response(),
        Err(e) => error_response(e),
    }
}

async fn begin_structured_txn(
    State(state): State<Arc<AppState>>,
) -> Response {
    match state.structured.begin_txn().await {
        Ok(t) => (
            StatusCode::CREATED,
            Json(json!({
                "txn_id": t.txn_id,
                "snapshot_version": t.snapshot_version,
            })),
        )
            .into_response(),
        Err(e) => error_response(e),
    }
}

async fn space_snapshot(
    State(state): State<Arc<AppState>>,
    Path(space): Path<String>,
) -> Response {
    let snap = state.store.current_global_version().unwrap_or(0);
    (
        StatusCode::OK,
        Json(json!({
            "space": space,
            "snapshot_version": snap,
        })),
    )
        .into_response()
}

fn error_response(e: XtableError) -> Response {
    // All XtableError variants map to a valid HTTP status; panic on impossible.
    let status = StatusCode::from_u16(e.http_status())
        .expect("XtableError returned an invalid HTTP status code");
    (
        status,
        Json(json!({
            "error": format!("{}", e),
            "code": e.s3_code(),
        })),
    )
        .into_response()
}

fn not_found() -> Response {
    (StatusCode::NOT_FOUND, Json(json!({ "error": "not found" }))).into_response()
}
