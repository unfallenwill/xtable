//! Structured-data-space HTTP routes.
//!
//! All routes are mounted under `/v1/spaces/:space/...`. Each write request
//! runs in its own short-lived transaction so multi-record atomicity per
//! request is guaranteed. Batch writes use one transaction for all records.
//!
//! Error responses are JSON: `{"error": "<msg>", "code": "<s3-style>"}`.
//!
//! Observability (Phase 6, Task 6.3):
//! - Every handler is wrapped in `#[tracing::instrument]` so it emits a
//!   span with `space` / `table` / `record_id` attributes as appropriate.
//! - Transaction commit metrics are recorded at the coordinator boundary;
//!   handlers only provide request spans and HTTP RED metrics.

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
use xtable_schema::{Filter, OrderDir, Query as StructuredQuery, RecordWrite, WriteOutcome};

use crate::app::AppState;

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/v1/spaces/:space/schemas",
            post(register_schema).get(list_schemas),
        )
        .route("/v1/spaces/:space/schemas/:name", get(get_schema))
        .route("/v1/spaces/:space/tables/:table/bind", post(bind_table))
        .route(
            "/v1/spaces/:space/tables/:table/records",
            post(upsert_record).get(query_records),
        )
        .route(
            "/v1/spaces/:space/tables/:table/records/batch",
            post(batch_upsert_records),
        )
        .route(
            "/v1/spaces/:space/tables/:table/records/:record_id",
            get(get_record).delete(delete_record),
        )
        .route("/v1/spaces/:space/tables/:table/diff", get(diff_records))
        .route("/v1/structured/txn", post(begin_structured_txn))
        .route(
            "/v1/structured/txn/:txn_id/write",
            post(write_structured_txn),
        )
        .route(
            "/v1/structured/txn/:txn_id/schema",
            post(register_schema_in_txn),
        )
        .route("/v1/structured/txn/:txn_id/bind", post(bind_table_in_txn))
        .route(
            "/v1/structured/txn/:txn_id/commit",
            post(commit_structured_txn),
        )
        .route(
            "/v1/structured/txn/:txn_id/abort",
            post(abort_structured_txn),
        )
        .route("/v1/spaces/:space/snapshot", get(space_snapshot))
}

#[derive(Debug, Deserialize)]
struct RegisterSchemaReq {
    name: String,
    body: Value,
}

#[tracing::instrument(
    level = "info",
    name = "http.schema.register",
    skip_all,
    fields(space = %space, op = "register_schema"),
)]
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

#[tracing::instrument(
    level = "info",
    name = "http.schema.get",
    skip_all,
    fields(space = %space, name = %name, op = "get_schema"),
)]
async fn get_schema(
    State(state): State<Arc<AppState>>,
    Path((space, name)): Path<(String, String)>,
    Query(params): Query<SchemaQuery>,
) -> Response {
    let snap = params
        .snapshot
        .as_deref()
        .and_then(|s| s.parse::<u64>().ok());
    let version = params
        .version
        .as_deref()
        .and_then(|s| s.parse::<u32>().ok());
    // PR #6: wrap read in a short-lived txn for SSI ReadSet capture.
    let txn = match state.structured.begin_txn().await {
        Ok(t) => t,
        Err(e) => return error_response(e),
    };
    let result = state
        .structured
        .get_schema(&txn, &space, &name, version, snap)
        .await;
    let _ = state.structured.abort_txn(&txn).await;
    match result {
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

#[tracing::instrument(
    level = "info",
    name = "http.schema.list",
    skip_all,
    fields(space = %space, op = "list_schemas"),
)]
async fn list_schemas(State(state): State<Arc<AppState>>, Path(space): Path<String>) -> Response {
    let txn = match state.structured.begin_txn().await {
        Ok(t) => t,
        Err(e) => return error_response(e),
    };
    let result = state.structured.list_schemas(&txn, &space).await;
    let _ = state.structured.abort_txn(&txn).await;
    match result {
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

#[tracing::instrument(
    level = "info",
    name = "http.table.bind",
    skip_all,
    fields(space = %space, table = %table, op = "bind_table"),
)]
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
}

#[derive(Debug, Serialize)]
struct UpsertRecordResp {
    record_id: String,
    schema_version: u32,
    backend_key: String,
    commit_version: u64,
}

#[derive(Debug, Deserialize)]
struct BatchUpsertRecordsReq {
    records: Vec<UpsertRecordReq>,
}

#[derive(Debug, Serialize)]
struct BatchUpsertRecordsResp {
    records: Vec<UpsertRecordResp>,
    commit_version: u64,
}

#[tracing::instrument(
    level = "info",
    name = "http.record.upsert",
    skip_all,
    fields(space = %space, table = %table, op = "upsert_record"),
)]
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

#[tracing::instrument(
    level = "info",
    name = "http.record.batch_upsert",
    skip_all,
    fields(space = %space, table = %table, op = "batch_upsert_records"),
)]
async fn batch_upsert_records(
    State(state): State<Arc<AppState>>,
    Path((space, table)): Path<(String, String)>,
    Json(req): Json<BatchUpsertRecordsReq>,
) -> Response {
    if req.records.is_empty() {
        return error_response(XtableError::invalid("records must not be empty"));
    }

    let result: XtableResult<(Vec<WriteOutcome>, u64)> = async {
        let t = state.structured.begin_txn().await?;
        let mut outcomes = Vec::with_capacity(req.records.len());
        for record in req.records {
            let outcome = state
                .structured
                .upsert_record(
                    &t,
                    RecordWrite {
                        space: space.clone(),
                        table: table.clone(),
                        record_id: record.record_id,
                        body: record.body,
                    },
                )
                .await;
            match outcome {
                Ok(outcome) => outcomes.push(outcome),
                Err(err) => {
                    let _ = state.structured.abort_txn(&t).await;
                    return Err(err);
                }
            }
        }
        let commit = state.structured.commit_txn(&t).await?;
        Ok((outcomes, commit))
    }
    .await;

    match result {
        Ok((outcomes, commit_version)) => (
            StatusCode::CREATED,
            Json(BatchUpsertRecordsResp {
                records: outcomes
                    .into_iter()
                    .map(|outcome| UpsertRecordResp {
                        record_id: outcome.record_id,
                        schema_version: outcome.schema_version,
                        backend_key: outcome.backend_key,
                        commit_version,
                    })
                    .collect(),
                commit_version,
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

#[tracing::instrument(
    level = "info",
    name = "http.record.get",
    skip_all,
    fields(
        space = %space,
        table = %table,
        record_id = %record_id,
        op = "get_record"
    ),
)]
async fn get_record(
    State(state): State<Arc<AppState>>,
    Path((space, table, record_id)): Path<(String, String, String)>,
    Query(params): Query<GetRecordQuery>,
) -> Response {
    let snap = params
        .snapshot
        .as_deref()
        .and_then(|s| s.parse::<u64>().ok());
    let txn = match state.structured.begin_txn().await {
        Ok(t) => t,
        Err(e) => return error_response(e),
    };
    let result = state
        .structured
        .get_record(&txn, &space, &table, &record_id, snap)
        .await;
    let _ = state.structured.abort_txn(&txn).await;
    match result {
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

#[tracing::instrument(
    level = "info",
    name = "http.record.delete",
    skip_all,
    fields(
        space = %space,
        table = %table,
        record_id = %record_id,
        op = "delete_record"
    ),
)]
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

#[tracing::instrument(
    level = "info",
    name = "http.record.query",
    skip_all,
    fields(space = %space, table = %table, op = "query_records"),
)]
async fn query_records(
    State(state): State<Arc<AppState>>,
    Path((space, table)): Path<(String, String)>,
    Query(params): Query<QueryParams>,
) -> Response {
    let snap = params
        .snapshot
        .as_deref()
        .and_then(|s| s.parse::<u64>().ok());
    let mut q = StructuredQuery::new();
    if let (Some(f), Some(op), Some(v)) = (
        &params.filter_field,
        &params.filter_op,
        &params.filter_value,
    ) {
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
    if let Some(n) = params
        .limit
        .as_deref()
        .and_then(|s| s.parse::<usize>().ok())
    {
        q = q.limit(n);
    }
    if let Some(o) = params
        .offset
        .as_deref()
        .and_then(|s| s.parse::<usize>().ok())
    {
        q = q.offset(o);
    }
    let txn = match state.structured.begin_txn().await {
        Ok(t) => t,
        Err(e) => return error_response(e),
    };
    let result = state
        .structured
        .query_records(&txn, &space, &table, q, snap);
    let _ = state.structured.abort_txn(&txn).await;
    match result {
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
        "eq" => Ok(Filter::Eq {
            field: field.to_string(),
            value: v,
        }),
        "ne" => Ok(Filter::Ne {
            field: field.to_string(),
            value: v,
        }),
        "gt" => Ok(Filter::Gt {
            field: field.to_string(),
            value: v,
        }),
        "ge" => Ok(Filter::Ge {
            field: field.to_string(),
            value: v,
        }),
        "lt" => Ok(Filter::Lt {
            field: field.to_string(),
            value: v,
        }),
        "le" => Ok(Filter::Le {
            field: field.to_string(),
            value: v,
        }),
        "contains" => {
            let s = v
                .as_str()
                .ok_or_else(|| XtableError::invalid("contains requires string value"))?
                .to_string();
            Ok(Filter::Contains {
                field: field.to_string(),
                value: s,
            })
        }
        "exists" => Ok(Filter::Exists {
            field: field.to_string(),
        }),
        _ => Err(XtableError::invalid(format!("unknown filter op: {op}"))),
    }
}

#[derive(Debug, Deserialize)]
struct DiffQuery {
    s1: String,
    s2: String,
}

#[tracing::instrument(
    level = "info",
    name = "http.record.diff",
    skip_all,
    fields(space = %space, table = %table, op = "diff_records"),
)]
async fn diff_records(
    State(state): State<Arc<AppState>>,
    Path((space, table)): Path<(String, String)>,
    Query(params): Query<DiffQuery>,
) -> Response {
    let (s1, s2) = match (params.s1.parse::<u64>(), params.s2.parse::<u64>()) {
        (Ok(a), Ok(b)) => (a, b),
        _ => return error_response(XtableError::invalid("s1 and s2 must be u64")),
    };
    let txn = match state.structured.begin_txn().await {
        Ok(t) => t,
        Err(e) => return error_response(e),
    };
    let result = state.structured.diff(&txn, &space, &table, s1, s2);
    let _ = state.structured.abort_txn(&txn).await;
    match result {
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

#[tracing::instrument(
    level = "info",
    name = "http.txn.begin",
    skip_all,
    fields(op = "begin_structured_txn")
)]
async fn begin_structured_txn(State(state): State<Arc<AppState>>) -> Response {
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

#[derive(Debug, Deserialize)]
struct StructuredTxnWriteReq {
    space: String,
    table: String,
    record_id: Option<String>,
    body: Value,
}

#[derive(Debug, Deserialize)]
struct StructuredTxnSchemaReq {
    space: String,
    name: String,
    body: Value,
}

#[derive(Debug, Deserialize)]
struct StructuredTxnBindReq {
    space: String,
    table: String,
    body: Value,
}

/// Register a schema version inside an explicit transaction. The schema is
/// visible to readers only after the transaction commits.
#[tracing::instrument(
    level = "info",
    name = "http.txn.schema",
    skip_all,
    fields(txn_id = %txn_id, op = "register_schema")
)]
async fn register_schema_in_txn(
    State(state): State<Arc<AppState>>,
    Path(txn_id): Path<String>,
    Json(req): Json<StructuredTxnSchemaReq>,
) -> Response {
    let txn = match state.structured.txn_handle(txn_id.clone()) {
        Ok(t) => t,
        Err(e) => return error_response(e),
    };
    match state
        .structured
        .register_schema(&txn, &req.space, &req.name, req.body)
        .await
    {
        Ok(version) => (
            StatusCode::OK,
            Json(json!({
                "txn_id": txn_id,
                "space": req.space,
                "name": req.name,
                "version": version,
            })),
        )
            .into_response(),
        Err(e) => error_response(e),
    }
}

/// Bind a table to a schema body inside an explicit transaction. This is a
/// metadata write in the same commit set as record writes.
#[tracing::instrument(
    level = "info",
    name = "http.txn.bind",
    skip_all,
    fields(txn_id = %txn_id, op = "bind_table")
)]
async fn bind_table_in_txn(
    State(state): State<Arc<AppState>>,
    Path(txn_id): Path<String>,
    Json(req): Json<StructuredTxnBindReq>,
) -> Response {
    let txn = match state.structured.txn_handle(txn_id.clone()) {
        Ok(t) => t,
        Err(e) => return error_response(e),
    };
    match state
        .structured
        .bind_table_schema(&txn, &req.space, &req.table, req.body)
        .await
    {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({
                "txn_id": txn_id,
                "space": req.space,
                "table": req.table,
                "bound": true,
            })),
        )
            .into_response(),
        Err(e) => error_response(e),
    }
}

/// Stage one structured record write in an already-open transaction. The
/// write is not visible until the matching commit request succeeds.
#[tracing::instrument(
    level = "info",
    name = "http.txn.write",
    skip_all,
    fields(txn_id = %txn_id, op = "write")
)]
async fn write_structured_txn(
    State(state): State<Arc<AppState>>,
    Path(txn_id): Path<String>,
    Json(req): Json<StructuredTxnWriteReq>,
) -> Response {
    let txn = match state.structured.txn_handle(txn_id.clone()) {
        Ok(t) => t,
        Err(e) => return error_response(e),
    };
    match state
        .structured
        .upsert_record(
            &txn,
            RecordWrite {
                space: req.space,
                table: req.table,
                record_id: req.record_id,
                body: req.body,
            },
        )
        .await
    {
        Ok(outcome) => (
            StatusCode::OK,
            Json(json!({
                "txn_id": txn.txn_id,
                "snapshot_version": txn.snapshot_version,
                "record_id": outcome.record_id,
                "schema_version": outcome.schema_version,
                "backend_key": outcome.backend_key,
            })),
        )
            .into_response(),
        Err(e) => error_response(e),
    }
}

#[tracing::instrument(
    level = "info",
    name = "http.txn.commit",
    skip_all,
    fields(txn_id = %txn_id, op = "commit")
)]
async fn commit_structured_txn(
    State(state): State<Arc<AppState>>,
    Path(txn_id): Path<String>,
) -> Response {
    let txn = match state.structured.txn_handle(txn_id) {
        Ok(t) => t,
        Err(e) => return error_response(e),
    };
    match state.structured.commit_txn(&txn).await {
        Ok(commit_version) => (
            StatusCode::OK,
            Json(json!({
                "txn_id": txn.txn_id,
                "commit_version": commit_version,
                "committed": true,
            })),
        )
            .into_response(),
        Err(e) => error_response(e),
    }
}

#[tracing::instrument(
    level = "info",
    name = "http.txn.abort",
    skip_all,
    fields(txn_id = %txn_id, op = "abort")
)]
async fn abort_structured_txn(
    State(state): State<Arc<AppState>>,
    Path(txn_id): Path<String>,
) -> Response {
    let txn = match state.structured.txn_handle(txn_id) {
        Ok(t) => t,
        Err(e) => return error_response(e),
    };
    match state.structured.abort_txn(&txn).await {
        Ok(()) => (StatusCode::OK, Json(json!({ "aborted": true }))).into_response(),
        Err(e) => error_response(e),
    }
}

#[tracing::instrument(
    level = "info",
    name = "http.space.snapshot",
    skip_all,
    fields(space = %space, op = "space_snapshot"),
)]
async fn space_snapshot(State(state): State<Arc<AppState>>, Path(space): Path<String>) -> Response {
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
