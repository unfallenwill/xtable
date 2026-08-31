//! Verifies that `Metrics::new` registers every instrument declared in
//! `metrics.rs` against a real `Meter` and that recording against the handles
//! surfaces the corresponding metric names in the in-memory exporter.
//!
//! Adapted to the OTel 0.27 SDK testing API:
//!   - `InMemoryMetricExporter` (singular) lives in
//!     `opentelemetry_sdk::testing::metrics`.
//!   - Metrics are read with `exporter.get_finished_metrics()` BEFORE calling
//!     `provider.shutdown()` (which clears the in-memory buffer).

use std::time::Duration;

use opentelemetry::metrics::MeterProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::runtime::Tokio;
use opentelemetry_sdk::testing::metrics::InMemoryMetricExporter;
use xtable_telemetry::metrics::Metrics;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn all_instruments_registered_and_recording() {
    let exporter = InMemoryMetricExporter::default();
    let reader = PeriodicReader::builder(exporter.clone(), Tokio)
        .with_interval(Duration::from_millis(20))
        .build();
    let provider = SdkMeterProvider::builder().with_reader(reader).build();
    let meter = provider.meter("xtable-test");
    let m = Metrics::new(&meter);

    // Record a single sample against each handle we care about. The exporter
    // receives the full set on the next periodic tick; what we assert is that
    // every instrument name appears at least once.
    m.http_request_duration
        .record(0.123, &[KeyValue::new("http.route", "/v1/x")]);
    m.http_requests_total
        .add(1, &[KeyValue::new("http.route", "/v1/x")]);
    m.http_active_requests.add(1, &[]);
    m.txn_commit_total.add(1, &[KeyValue::new("outcome", "ok")]);
    m.txn_ssi_conflict_total.add(1, &[]);
    m.process_runtime_uptime.add(42, &[]);

    tokio::time::sleep(Duration::from_millis(150)).await;
    let names: std::collections::HashSet<String> = exporter
        .get_finished_metrics()
        .unwrap_or_default()
        .iter()
        .flat_map(|rm| rm.scope_metrics.iter())
        .flat_map(|sm| sm.metrics.iter())
        .map(|m| m.name.to_string())
        .collect();
    let _ = provider.shutdown();

    for expected in [
        "http.server.request.duration",
        "http.server.requests",
        "http.server.active_requests",
        "txn.commit.count",
        "txn.ssi.conflict.count",
        "process.runtime.uptime",
    ] {
        assert!(names.contains(expected), "missing instrument {expected}");
    }
}
