//! xtable server binary entry point.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use axum::response::IntoResponse;
use clap::Parser;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;


use xtable_auth::StaticCredential;
use xtable_backend::BackendClient;
use xtable_core::config::Config;
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
    use axum::routing::get;

    // SigV4 authentication middleware. The structured-data-space routes under
    // /v1 require authentication; /healthz and /readyz are public.
    let auth_layer = axum::middleware::from_fn_with_state(
        state.clone(),
        auth_middleware,
    );

    let admin = axum::Router::new()
        .route("/healthz", get(|| async { "ok\n" }))
        .route("/readyz", get(|| async { "ok\n" }));

    // Structured-data-space routes under /v1.
    let state_arc = std::sync::Arc::new(state.clone());
    let structured_routes = xtable_server::structured::router().with_state(state_arc);

    axum::Router::new()
        .merge(admin)
        .merge(structured_routes)
        .with_state(state)
        .layer(auth_layer)
}

/// SigV4 auth middleware. Rejects unauthenticated requests with 401 before
/// they reach the structured-data-space handlers.
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