//! Smoke test for `SemConvMakeSpan`.
//!
//! The test only exercises the `MakeSpan` half of the semconv pair because
//! that side has no temporal dependencies — `OnResponse` / `OnFailure` need a
//! running tower `Service` to drive them, which would pull in a full
//! `tower_http::trace::TraceLayer` harness. Phase 6 wires the full layer
//! against `xtable-server`'s router; here we only assert that `make_span`
//! produces a span named `HTTP`.

use axum::http::Request;
use tower_http::trace::MakeSpan;
use xtable_telemetry::http_semconv::SemConvMakeSpan;

#[test]
fn make_span_creates_with_required_attrs() {
    let mut mk = SemConvMakeSpan;
    let req = Request::builder()
        .method("POST")
        .uri("/v1/spaces/foo/tables/users/records")
        .body(())
        .unwrap();
    let span = mk.make_span(&req);
    let ext = span.metadata().map(|m| m.name()).unwrap_or("");
    assert_eq!(ext, "HTTP");
}
