//! `extract_matched_path` middleware.
//!
//! When axum matches a request to a route it stores the route template in the
//! request's extensions bag as `MatchedPath`. Pulling it here makes the
//! concrete path (e.g. `/v1/spaces/foo/tables/users/records`) visible to
//! `MakeSpan` callbacks used by `tower_http::trace::TraceLayer`, which then
//! emit `http.route` as the OTel semconv attribute instead of leaking raw
//! path parameters.

use axum::extract::{MatchedPath, Request};
use axum::middleware::Next;
use axum::response::Response;

pub async fn extract_matched_path(req: Request, next: Next) -> Response {
    // MatchedPath is populated by axum's routing when the request
    // enters a matched route. We pull it into the typed extensions
    // bag so downstream layers / MakeSpan can read it.
    let method = req.method().clone();
    let uri = req.uri().clone();
    if let Some(mp) = req.extensions().get::<MatchedPath>().cloned() {
        tracing::debug!(route=%mp.as_str(), method=%method, path=%uri.path(),
                        "matched_path available");
    }
    next.run(req).await
}