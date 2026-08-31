//! `AppState`: shared state passed to all handlers.

use std::sync::Arc;

use xtable_auth::{CredentialStore, EdgeAuth};
use xtable_backend::BackendClient;
use xtable_schema::StructuredSpace;
use xtable_storage::LocalStore;
use xtable_telemetry::metrics::Metrics;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<xtable_core::config::Config>,
    pub store: LocalStore,
    pub backend: Arc<BackendClient>,
    pub auth: Arc<EdgeAuth>,
    pub structured: Arc<StructuredSpace>,
    /// PR #4: expose the txn coordinator so the background flush loop
    /// can share its memtable set. The structured layer holds a clone.
    pub coordinator: Arc<xtable_tx::TxnCoordinator>,
    /// OpenTelemetry RED metric handles. Phase 6 binds these to the live
    /// `SdkMeterProvider` in `main.rs` *after* `telemetry::init` runs; the
    /// caller is responsible for sequencing — see the comment on
    /// `Metrics::default()` in `xtable-telemetry/src/metrics.rs`.
    pub metrics: Metrics,
}

impl std::fmt::Debug for AppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppState").finish_non_exhaustive()
    }
}

impl AppState {
    pub fn new(
        config: xtable_core::config::Config,
        store: LocalStore,
        backend: BackendClient,
        creds: Arc<CredentialStore>,
        metrics: Metrics,
    ) -> Self {
        let cfg = Arc::new(config.clone());
        let auth = Arc::new(EdgeAuth {
            creds,
            allow_anonymous_read: config.auth.allow_anonymous_read,
            // Region participates in the SigV4 HMAC chain, so the verifier
            // must use the same region the client signed with. We pin it to
            // the backend bucket region — non-AWS providers like volcengine
            // TOS reject signatures built with any other region.
            region: config.backend.region.clone(),
        });
        let backend_arc = Arc::new(backend);
        // The structured-data-space layer owns the only transaction coordinator
        // it talks to. We never expose it on `AppState` because no HTTP handler
        // needs it directly — the structured routes always go through
        // `state.structured.{begin,commit}_txn`.
        let txn = Arc::new(xtable_tx::TxnCoordinator::new(
            Arc::new(store.clone()),
            Arc::clone(&backend_arc),
            config.storage.staged_body_spill_dir.clone(),
            config.txn.commit_upload_concurrency,
        ));
        let structured = Arc::new(StructuredSpace::new(
            Arc::clone(&txn),
            store.clone(),
            Arc::clone(&backend_arc),
        ));
        Self {
            config: cfg,
            store,
            backend: backend_arc,
            auth,
            structured,
            coordinator: Arc::clone(&txn),
            metrics,
        }
    }
}
