//! xtable server binary entry point.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use axum::extract::State;
use axum::http::{HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::Router;
use clap::Parser;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use xtable_auth::verify_request as xtable_verify_request;

use xtable_auth::StaticCredential;
use xtable_backend::BackendClient;
use xtable_core::config::Config;
use xtable_core::headers::{
    XTABLE_COMMIT_VERSION, XTABLE_CONFLICT_KEYS, XTABLE_SNAPSHOT_VERSION, XTABLE_TXN_ID,
    XTABLE_TXN_STATUS,
};
use xtable_server::shutdown::wait_for_shutdown;
use xtable_storage::LocalStore;

#[derive(Parser, Debug)]
#[command(name = "xtable", about = "xtable server")]
struct Args {
    /// Path to config TOML. Defaults to ./xtable.toml
    #[arg(long, env = "XTABLE_CONFIG")]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let args = Args::parse();
    let config_path = args
        .config
        .unwrap_or_else(|| PathBuf::from("./xtable.toml"));
    let config = Config::load(&config_path).context("loading config")?;

    info!(
        listen = %config.server.listen,
        backend = %config.backend.endpoint,
        bucket = %config.backend.bucket,
        data_dir = %config.storage.redb_dir.display(),
        "starting xtable"
    );

    // Storage
    std::fs::create_dir_all(&config.storage.redb_dir)?;
    std::fs::create_dir_all(&config.storage.staged_body_spill_dir)?;
    let store = LocalStore::open(&config.storage.redb_dir)?;

    // Crash recovery + cold-rebuild on startup.
    let backend = BackendClient::build(
        &config.backend.endpoint,
        &config.backend.region,
        &config.backend.bucket,
        &config.backend.access_key_id,
        &config.backend.secret_access_key,
        config.backend.force_path_style,
        config.backend.request_timeout_ms,
        config.backend.multipart_threshold_bytes,
        config.backend.multipart_part_size_bytes,
    )
    .await
    .context("building backend client")?;

    {
        // Crash WAL replay.
        match xtable_tx::recovery::recover(&store, &backend).await {
            Ok(report) => info!(?report, "recovery complete"),
            Err(e) => warn!(err = %e, "recovery failed; attempting cold rebuild"),
        }
        // Cold rebuild: only if recovery surfaced 0 commits and redb is fresh.
        if store.current_global_version().unwrap_or(0) == 0 && store.iter_wal().unwrap().is_empty() {
            match xtable_tx::rebuild::rebuild(&store, &backend).await {
                Ok(report) => info!(?report, "cold rebuild done"),
                Err(e) => {
                    // V14 fix: refuse to start with an empty index when the
                    // backend is unreachable. Otherwise we'd accept any
                    // subsequent commit as if it were a fresh global_version=0
                    // and risk overwriting real data at v1+.
                    return Err(anyhow::anyhow!(
                        "cold rebuild failed; refusing to start with empty index: {}", e
                    ));
                }
            }
        }
    }

    // Credential store
    let creds = Arc::new(xtable_auth::CredentialStore::new());
    creds.put(
        StaticCredential {
            access_key_id: config.auth.edge_access_key_id.clone(),
            secret_access_key: config.auth.edge_secret_access_key.clone(),
        }
        .into_entry(),
    );

    let state = xtable_server::app::AppState::new(config.clone(), store, backend, creds);

    // Spawn GC task.
    spawn_gc(state.clone());

    let app = build_router(state);

    let listener = tokio::net::TcpListener::bind(&config.server.listen)
        .await
        .with_context(|| format!("bind {}", &config.server.listen))?;
    info!(addr = ?listener.local_addr(), "listening");

    let shutdown_grace = config.server.shutdown_grace_secs;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            wait_for_shutdown().await;
            info!("graceful shutdown starting ({}s grace)", shutdown_grace);
        })
        .await?;

    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
}

fn spawn_gc(state: xtable_server::app::AppState) {
    let store = state.store.clone();
    let interval_secs = state.config.txn.gc_interval_secs;
    let timeout_secs = state.config.txn.default_timeout_secs as i64;
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(interval_secs.max(5)));
        loop {
            tick.tick().await;
            match xtable_tx::gc::sweep_all(&store, timeout_secs) {
                Ok(report) if report.aborted_txns > 0 || report.entries_removed > 0 => {
                    info!(?report, "GC sweep");
                }
                Ok(_) => {}
                Err(e) => warn!(err = %e, "GC sweep failed"),
            }
        }
    });
}

fn build_router(state: xtable_server::app::AppState) -> axum::Router {
    use axum::error_handling::HandleError;
    use axum::routing::get;

    // V15 fix: SigV4 authentication middleware. All S3 + transactional
    // routes pass through this layer. /healthz and /readyz are public.
    let auth_layer = axum::middleware::from_fn_with_state(
        state.clone(),
        auth_middleware,
    );

    let admin = axum::Router::new()
        .route("/healthz", get(|| async { "ok\n" }))
        .route("/readyz", get(|| async { "ok\n" }));

    // Transactional extension routes (Phase 2.3). axum matches on path
    // only, not query string, so we register a single dispatcher at `/`
    // that branches on `transactional=begin|commit|abort` (POST) and
    // `transactional=status` (GET).
    let txn_routes = Router::new()
        .route("/", post(txn_dispatch).get(status_dispatch))
        .with_state(state.clone());

    // S3 routing — bypass s3s entirely (its second SigV4 verifier disagrees
    // with xtable's hand-rolled verifier). Direct dispatch through our own
    // router calls XtableS3Service methods straight from axum.
    let s3_svc = std::sync::Arc::new(xtable_s3::service::XtableS3Service::new(
        state.backend.clone(),
        state.store.clone(),
        state.txn.clone(),
        state.auth.creds.clone(),
    ));
    let s3_direct = xtable_s3::direct_router::build_direct_router(state.auth.clone())
        .with_state(xtable_s3::direct_router::DirectRouterState(s3_svc));

    axum::Router::new()
        .merge(admin)
        .merge(txn_routes)
        .merge(s3_direct)
        .with_state(state)
        .layer(auth_layer)
}

/// V15: SigV4 auth middleware. Rejects unauthenticated requests with 401
/// before they reach the S3 / transactional handlers.
async fn auth_middleware(
    axum::extract::State(state): axum::extract::State<xtable_server::app::AppState>,
    req: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use xtable_auth::verify_request;
    let is_read = matches!(*req.method(), axum::http::Method::GET | axum::http::Method::HEAD);
    if let Err(e) = verify_request(&state.auth, &req, is_read) {
        let status = e.http_status();
        return (axum::http::StatusCode::from_u16(status).unwrap_or(axum::http::StatusCode::UNAUTHORIZED),
                format!("{}", e)).into_response();
    }
    next.run(req).await
}

async fn handle_s3_error(err: s3s::HttpError) -> http::Response<s3s::Body> {
    tracing::error!(?err, "s3 service error");
    http::Response::builder()
        .status(http::StatusCode::INTERNAL_SERVER_ERROR)
        .body(s3s::Body::from("Internal Server Error".to_string()))
        .unwrap()
}

// ----- Transactional routes -----

async fn begin_txn(
    State(state): State<xtable_server::app::AppState>,
) -> impl IntoResponse {
    match state.txn.begin(None).await {
        Ok(txn_id) => {
            let snapshot = state.store.current_global_version().unwrap_or(0);
            let mut resp = (StatusCode::OK, "ok").into_response();
            if let Ok(hv) = HeaderValue::from_str(&txn_id) {
                resp.headers_mut().insert(XTABLE_TXN_ID, hv);
            }
            if let Ok(hv) = HeaderValue::from_str(&snapshot.to_string()) {
                resp.headers_mut().insert(XTABLE_SNAPSHOT_VERSION, hv);
            }
            resp
        }
        Err(e) => error_to_response(e),
    }
}

async fn commit_txn(
    State(state): State<xtable_server::app::AppState>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    let txn_id = match headers.get(XTABLE_TXN_ID).and_then(|v| v.to_str().ok()) {
        Some(s) => s.to_string(),
        None => return error_to_response(xtable_core::XtableError::invalid("missing x-xtable-txn-id header")),
    };
    match state.txn.commit(&txn_id).await {
        Ok(outcome) => {
            let mut resp = (StatusCode::OK, "committed").into_response();
            if let Ok(hv) = HeaderValue::from_str(&outcome.commit_version.to_string()) {
                resp.headers_mut().insert(XTABLE_COMMIT_VERSION, hv);
            }
            resp
        }
        Err(e) => error_to_response(e),
    }
}

async fn abort_txn(
    State(state): State<xtable_server::app::AppState>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    let txn_id = match headers.get(XTABLE_TXN_ID).and_then(|v| v.to_str().ok()) {
        Some(s) => s.to_string(),
        None => return error_to_response(xtable_core::XtableError::invalid("missing x-xtable-txn-id header")),
    };
    match state.txn.abort(&txn_id).await {
        Ok(()) => (StatusCode::NO_CONTENT, "").into_response(),
        Err(e) => error_to_response(e),
    }
}

async fn status_txn(
    State(state): State<xtable_server::app::AppState>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    let txn_id = match headers.get(XTABLE_TXN_ID).and_then(|v| v.to_str().ok()) {
        Some(s) => s.to_string(),
        None => return error_to_response(xtable_core::XtableError::invalid("missing x-xtable-txn-id header")),
    };
    match state.txn.status(&txn_id).await {
        Ok(s) => {
            let mut resp = (StatusCode::OK, s.to_string()).into_response();
            if let Ok(hv) = HeaderValue::from_str(s.as_str()) {
                resp.headers_mut().insert(XTABLE_TXN_STATUS, hv);
            }
            resp
        }
        Err(e) => error_to_response(e),
    }
}

fn error_to_response(e: xtable_core::XtableError) -> axum::response::Response {
    let status = StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut resp = (status, format!("{}", e)).into_response();
    // 409 → include conflict keys header (placeholder; specific keys handled by CommitTxn upstream).
    if status == StatusCode::CONFLICT {
        if let Some(keys) = extract_conflict_keys(&e) {
            if let Ok(hv) = HeaderValue::from_str(&keys) {
                resp.headers_mut().insert(XTABLE_CONFLICT_KEYS, hv);
            }
        }
    }
    resp
}

/// Dispatch POST `/` on the `transactional` query parameter.
async fn txn_dispatch(
    State(state): State<xtable_server::app::AppState>,
    headers: axum::http::HeaderMap,
    query: axum::extract::RawQuery,
) -> axum::response::Response {
    let q = query.0.unwrap_or_default();
    let op = q
        .split('&')
        .find_map(|kv| kv.strip_prefix("transactional="))
        .unwrap_or("");
    match op {
        "begin" => begin_txn(State(state)).await.into_response(),
        "commit" => commit_txn(State(state), headers).await.into_response(),
        "abort" => abort_txn(State(state), headers).await.into_response(),
        _ => error_to_response(xtable_core::XtableError::invalid(format!(
            "unknown transactional op: {}",
            op
        ))),
    }
}

/// Dispatch GET `/` on the `transactional` query parameter.
async fn status_dispatch(
    State(state): State<xtable_server::app::AppState>,
    headers: axum::http::HeaderMap,
    query: axum::extract::RawQuery,
) -> axum::response::Response {
    let q = query.0.unwrap_or_default();
    let op = q
        .split('&')
        .find_map(|kv| kv.strip_prefix("transactional="))
        .unwrap_or("");
    match op {
        "status" => status_txn(State(state), headers).await.into_response(),
        _ => error_to_response(xtable_core::XtableError::invalid(format!(
            "unknown transactional op (GET): {}",
            op
        ))),
    }
}

fn extract_conflict_keys(e: &xtable_core::XtableError) -> Option<String> {
    let s = format!("{}", e);
    s.strip_prefix("conflict: ")
        .map(|s| s.to_string())
        .or_else(|| s.strip_prefix("aborted: ").map(|s| s.to_string()))
}