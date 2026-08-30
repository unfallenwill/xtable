//! RED (Rate / Errors / Duration) middleware for `xtable-server`.
//!
//! Per the design plan §6.1 the middleware stamps the canonical HTTP
//! attributes (`http.route`, `http.request.method`,
//! `http.response.status_code`) onto the OTel RED instruments held on
//! [`crate::app::AppState`]:
//!
//! - `http_request_duration`  — wall-clock latency in seconds
//! - `http_requests_total`    — count of requests
//! - `http_active_requests`   — in-flight gauge
//!
//! Originally specified to live in `xtable-telemetry`, but the middleware
//! takes `xtable_server::app::AppState` — placing it there would form a
//! dependency cycle (`xtable-telemetry` would import `xtable-server`,
//! while `xtable-server` already imports `xtable-telemetry`). It is
//! therefore relocated here in Task 5.3-fix.

use axum::body::Body;
use axum::extract::{MatchedPath, Request, State};
use axum::middleware::Next;
use axum::response::Response;
use opentelemetry::KeyValue;
use std::time::Instant;

use crate::app::AppState;

pub async fn red_metrics_middleware(
    State(state): State<AppState>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map(|m| m.as_str().to_string())
        .unwrap_or_default();
    let method = req.method().clone();
    let started = Instant::now();

    state.metrics.http_active_requests.add(1, &[]);
    let response = next.run(req).await;
    state.metrics.http_active_requests.add(-1, &[]);

    let status = response.status().as_u16();
    let attrs = [
        KeyValue::new("http.route", route),
        KeyValue::new("http.request.method", method.as_str().to_owned()),
        KeyValue::new("http.response.status_code", i64::from(status)),
    ];
    state
        .metrics
        .http_request_duration
        .record(started.elapsed().as_secs_f64(), &attrs);
    state.metrics.http_requests_total.add(1, &attrs);
    response
}