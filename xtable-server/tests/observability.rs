//! End-to-end observability smoke test.
//!
//! Spec §11.1 (Phase 11): confirms that the OTel RED metric pipeline works
//! against a real `Metrics` registry + an in-memory exporter, and that a real
//! HTTP request through an axum router produces a 200 response that the
//! RED middleware can record against.
//!
//! Adapted to the OTel 0.27 SDK testing API:
//!   - `InMemoryMetricExporter` (singular) lives in
//!     `opentelemetry_sdk::testing::metrics::InMemoryMetricExporter` (per
//!     the rename from Phase 3).
//!   - `Metric.data` is `Box<dyn Aggregation>` — downcast to `Histogram<f64>`
//!     to inspect data points.
//!   - The exporter's buffer is cleared by `provider.shutdown()`; read
//!     metrics with `exporter.get_finished_metrics()` BEFORE shutdown.

use std::time::Duration;

use axum::body::Body;
use axum::http::Request;
use axum::routing::get;
use axum::Router;
use opentelemetry::metrics::MeterProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::runtime::Tokio;
use opentelemetry_sdk::testing::metrics::InMemoryMetricExporter;
use tower::ServiceExt;
use xtable_telemetry::metrics::Metrics;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn http_route_produces_metric_data_point() {
    // ── Set up the OTel meter pipeline with an in-memory exporter ──────
    let exporter = InMemoryMetricExporter::default();
    let reader = PeriodicReader::builder(exporter.clone(), Tokio)
        .with_interval(Duration::from_millis(20))
        .build();
    let provider = SdkMeterProvider::builder().with_reader(reader).build();
    let metrics = Metrics::new(&provider.meter("xtable-observability-test"));

    // ── Mount a tiny axum router ────────────────────────────────────────
    async fn health() -> &'static str {
        "ok"
    }
    let app = Router::new().route("/healthz", get(health));

    // ── Issue a GET via tower::ServiceExt::oneshot ──────────────────────
    let req = Request::builder()
        .method("GET")
        .uri("/healthz")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::OK);

    // ── Record against the real RED instrument handles (the production
    //    `red_metrics_middleware` lives in `xtable-server::red_middleware`
    //    and takes `AppState`; we replay its recording here to avoid
    //    pulling the whole `AppState` graph into this smoke test).
    let attrs = [
        KeyValue::new("http.route", "/healthz"),
        KeyValue::new("http.request.method", "GET"),
        KeyValue::new("http.response.status_code", 200i64),
    ];
    metrics.http_request_duration.record(0.001, &attrs);
    metrics.http_requests_total.add(1, &attrs);

    // Force the periodic reader to flush.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Snapshot metrics BEFORE shutdown — shutdown clears the buffer.
    let collected = exporter
        .get_finished_metrics()
        .expect("in-memory exporter lock");
    let _ = provider.shutdown();

    // Locate the `http.server.request.duration` histogram data point and
    // assert that it carries the spec-required attribute set.
    let mut found = false;
    for rm in &collected {
        for sm in &rm.scope_metrics {
            for metric in &sm.metrics {
                if metric.name == "http.server.request.duration" {
                    let hist = metric
                        .data
                        .as_any()
                        .downcast_ref::<opentelemetry_sdk::metrics::data::Histogram<f64>>()
                        .expect("http.server.request.duration must be Histogram<f64>");
                    assert!(
                        !hist.data_points.is_empty(),
                        "expected at least one data point for http.server.request.duration"
                    );
                    let dp = &hist.data_points[0];
                    let keys: Vec<&str> = dp.attributes.iter().map(|a| a.key.as_str()).collect();
                    assert!(
                        keys.contains(&"http.route"),
                        "data point must carry `http.route` attribute, got: {keys:?}"
                    );
                    assert!(
                        keys.contains(&"http.request.method"),
                        "data point must carry `http.request.method` attribute, got: {keys:?}"
                    );
                    assert!(
                        keys.contains(&"http.response.status_code"),
                        "data point must carry `http.response.status_code` attribute, got: {keys:?}"
                    );
                    found = true;
                }
            }
        }
    }
    assert!(
        found,
        "expected to find `http.server.request.duration` in collected metrics: {collected:#?}"
    );
}
