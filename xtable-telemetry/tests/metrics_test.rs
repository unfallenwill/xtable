//! Smoke test for the OTel meter pipeline using the in-memory exporter.
//!
//! Adapted to the 0.27 SDK API:
//!   - `InMemoryMetricsExporter` → `opentelemetry_sdk::testing::metrics::InMemoryMetricExporter`
//!   - `exporter.finish()` → `exporter.get_finished_metrics()` → `Vec<ResourceMetrics>`
//!   - flattened through `scope_metrics[].metrics[].name` to find the instrument.
//!   - The in-memory exporter's `shutdown()` clears its buffer, so we read metrics
//!     after waiting for the periodic reader to tick — but BEFORE calling
//!     `provider.shutdown()` (which would clear the buffer).
//!   - The test runs on a single-threaded multi-thread tokio runtime to drive the
//!     PeriodicReader's worker task.

use opentelemetry::metrics::MeterProvider as _;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::runtime::Tokio;
use opentelemetry_sdk::testing::metrics::InMemoryMetricExporter;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn meter_provider_registers_instruments() {
    let exporter = InMemoryMetricExporter::default();
    let reader = PeriodicReader::builder(exporter.clone(), Tokio)
        .with_interval(Duration::from_millis(50))
        .build();
    let provider = SdkMeterProvider::builder().with_reader(reader).build();
    let meter = provider.meter("xtable-test");
    let h = meter.f64_histogram("test.duration").build();
    h.record(1.23, &[]);
    // give the periodic worker time to tick at least once and push to the exporter
    tokio::time::sleep(Duration::from_millis(150)).await;
    // Snapshot BEFORE shutdown — shutdown() clears the in-memory buffer.
    let collected = exporter.get_finished_metrics().unwrap();
    let _ = provider.shutdown();
    assert!(
        collected
            .iter()
            .flat_map(|rm| rm.scope_metrics.iter())
            .flat_map(|sm| sm.metrics.iter())
            .any(|m| m.name == "test.duration"),
        "expected to find `test.duration` in collected metrics: {collected:#?}"
    );
}