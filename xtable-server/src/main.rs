//! xtable server binary entry point.

use std::path::PathBuf;

use anyhow::Context;
use axum::middleware;
use axum::response::IntoResponse;
use clap::Parser;
use tracing::{info, warn};

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
    let args = Args::parse();
    let config_path = args
        .config
        .unwrap_or_else(|| PathBuf::from("./xtable.toml"));
    let config = Config::load(&config_path).context("loading config")?;

    // Hold the telemetry guard for the entire program lifetime. MUST be a
    // named binding so its `Drop` drains OTel providers on shutdown. MUST
    // run before `Metrics::new` so the meter handles bind to the live
    // `SdkMeterProvider` (OTel 0.27 instruments are permanently bound to
    // the `Meter` that created them).
    // Environment variables provide the OTLP endpoint, transport, and
    // profile, while the TOML observability block supplies service/resource
    // defaults. A plain
    // `into()` would intentionally leave endpoint unset and silently disable
    // export, even when OTEL_EXPORTER_OTLP_ENDPOINT was configured.
    let telemetry_cfg = match xtable_telemetry::config::load_from_env() {
        Some(env_cfg) => {
            xtable_telemetry::config::merge_with_toml(Some(env_cfg), &config.observability)
                .expect("environment telemetry config must be present")
        }
        None => config.observability.clone().into(),
    };
    let _guard = xtable_telemetry::init::init(&telemetry_cfg)?;

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
        match xtable_tx::recovery::recover(&store).await {
            Ok(report) => info!(?report, "recovery complete"),
            Err(e) => warn!(err = %e, "recovery failed; attempting cold rebuild"),
        }
        // Cold rebuild: only if recovery surfaced 0 commits and redb is fresh.
        if store.current_global_version().unwrap_or(0) == 0 && store.iter_wal().unwrap().is_empty()
        {
            match xtable_tx::rebuild::rebuild(&store, &backend).await {
                Ok(report) => info!(?report, "cold rebuild done"),
                Err(e) => {
                    // V14 fix: refuse to start with an empty index when the
                    // backend is unreachable. Otherwise we'd accept any
                    // subsequent commit as if it were a fresh global_version=0
                    // and risk overwriting real data at v1+.
                    return Err(anyhow::anyhow!(
                        "cold rebuild failed; refusing to start with empty index: {}",
                        e
                    ));
                }
            }
        }
    }

    // Bind metric handles AFTER telemetry init. `Metrics::default()` would
    // bind to the OTel no-op provider and silently drop every recording
    // for the lifetime of the process; `Metrics::new(&global::meter("xtable"))`
    // binds to whatever meter provider `init` just installed.
    let metrics =
        xtable_telemetry::metrics::Metrics::new(&xtable_telemetry::global::meter("xtable"));

    let state = xtable_server::app::AppState::new(config.clone(), store, backend.clone(), metrics);

    // Spawn GC task.
    spawn_gc(state.clone());

    // Spawn memtable flush loop (PR #4). Drains immutable memtables
    // into S3 chunks via multipart upload.
    spawn_flush_loop(state.clone());

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

/// Spawn the memtable-to-S3 chunk flush loop (PR #4). This task
/// picks up immutable memtables produced by the active memtable and
/// uploads them as chunks.
fn spawn_flush_loop(state: xtable_server::app::AppState) {
    use std::sync::Arc;
    use xtable_storage::flush::flush_loop;

    let backend = state.backend.clone();
    let store = state.store.clone();
    let memtable_set = Arc::clone(state.coordinator.memtable_set());
    tokio::spawn(async move {
        if let Err(err) = flush_loop(memtable_set, store, backend).await {
            tracing::error!(error = %err, "flush task exited");
        }
    });
}

fn build_router(state: xtable_server::app::AppState) -> axum::Router {
    use axum::routing::get;
    use tower_http::trace::TraceLayer;
    use xtable_server::red_middleware::red_metrics_middleware;
    use xtable_telemetry::extract_route::extract_matched_path;
    use xtable_telemetry::http_semconv::{SemConvMakeSpan, SemConvOnFailure, SemConvOnResponse};

    // JWT authentication middleware. The structured-data-space routes under
    // /v1 require authentication; /healthz and /readyz are public.
    let auth_layer = axum::middleware::from_fn_with_state(state.clone(), auth_middleware);

    let admin = axum::Router::new()
        .route("/healthz", get(|| async { "ok\n" }))
        .route("/readyz", get(|| async { "ok\n" }));

    // Structured-data-space routes under /v1.
    let state_arc = std::sync::Arc::new(state.clone());
    let structured_routes = xtable_server::structured::router().with_state(state_arc);

    let protected_routes = structured_routes.layer(auth_layer);

    axum::Router::new()
        .merge(admin)
        .merge(protected_routes)
        // Axum middleware is LIFO: the LAST `.layer()` call is the OUTERMOST
        // wrapper, so a request traverses layers top-down (outer → inner) and
        // responses bubble bottom-up. TraceLayer must be outermost so every
        // request — including auth-rejected 401s — produces a span and feeds
        // the RED middleware; `auth_layer` is innermost so unauthenticated
        // requests short-circuit at the bottom and their 401 responses still
        // bubble up through TraceLayer and red_metrics_middleware on the way
        // out (per spec §9.1 "every request including auth-rejected 401
        // produces a span").
        .layer(middleware::from_fn(extract_matched_path))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            red_metrics_middleware,
        ))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(SemConvMakeSpan)
                .on_response(SemConvOnResponse)
                .on_failure(SemConvOnFailure),
        )
        .with_state(state)
}

/// JWT auth middleware. Rejects unauthenticated requests with 401 before
/// they reach the structured-data-space handlers.
async fn auth_middleware(
    axum::extract::State(state): axum::extract::State<xtable_server::app::AppState>,
    req: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use xtable_auth::verify_request;
    let is_read = matches!(
        *req.method(),
        axum::http::Method::GET | axum::http::Method::HEAD
    );
    let anonymous = !req
        .headers()
        .contains_key(axum::http::header::AUTHORIZATION);
    let auth_span = tracing::info_span!(
        "auth.verify",
        "http.request.method" = %req.method(),
        outcome = tracing::field::Empty,
    );
    let result = auth_span.in_scope(|| verify_request(&state.auth, &req, is_read));
    if let Err(e) = result {
        auth_span.record("outcome", "invalid");
        let status = e.http_status();
        return (
            axum::http::StatusCode::from_u16(status)
                .unwrap_or(axum::http::StatusCode::UNAUTHORIZED),
            format!("{}", e),
        )
            .into_response();
    }
    auth_span.record(
        "outcome",
        if is_read && state.auth.allow_anonymous_read && anonymous {
            "anonymous_allowed"
        } else {
            "valid"
        },
    );
    next.run(req).await
}
