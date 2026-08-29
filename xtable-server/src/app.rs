//! `AppState`: shared state passed to all handlers.

use std::sync::Arc;

use xtable_auth::{CredentialStore, EdgeAuth};
use xtable_backend::BackendClient;
use xtable_schema::StructuredSpace;
use xtable_storage::LocalStore;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<xtable_core::config::Config>,
    pub store: LocalStore,
    pub backend: Arc<BackendClient>,
    pub auth: Arc<EdgeAuth>,
    pub structured: Arc<StructuredSpace>,
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
    ) -> Self {
        let cfg = Arc::new(config.clone());
        let auth = Arc::new(EdgeAuth {
            creds,
            allow_anonymous_read: config.auth.allow_anonymous_read,
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
            txn,
            store.clone(),
            Arc::clone(&backend_arc),
        ));
        Self {
            config: cfg,
            store,
            backend: backend_arc,
            auth,
            structured,
        }
    }
}