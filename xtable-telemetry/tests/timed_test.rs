//! Verifies that the `Timed` guard records an elapsed-time measurement to its
//! histogram when dropped, both on normal return and on early `?` return.
//!
//! Adapted to the OTel 0.27 SDK testing API:
//!   - `InMemoryMetricExporter` (singular) lives in
//!     `opentelemetry_sdk::testing::metrics`.
//!   - Metrics are read with `exporter.get_finished_metrics()` BEFORE calling
//!     `provider.shutdown()` (which clears the in-memory buffer).
//!   - The flattened path is `resource_metrics[].scope_metrics[].metrics[]`.

use std::time::Duration;

use opentelemetry::metrics::MeterProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::PeriodicReader;
use opentelemetry_sdk::runtime::Tokio;
use opentelemetry_sdk::testing::metrics::InMemoryMetricExporter;
use xtable_telemetry::timed::Timed;

fn setup_provider_and_histogram(
    name: &'static str,
) -> (
    InMemoryMetricExporter,
    opentelemetry_sdk::metrics::SdkMeterProvider,
    opentelemetry::metrics::Histogram<f64>,
) {
    let exporter = InMemoryMetricExporter::default();
    // Use a short periodic interval so the test can observe the metric before
    // shutdown clears the in-memory buffer.
    let reader = PeriodicReader::builder(exporter.clone(), Tokio)
        .with_interval(Duration::from_millis(20))
        .build();
    let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_reader(reader)
        .build();
    let meter = provider.meter("xtable-test");
    let h = meter.f64_histogram(name).build();
    (exporter, provider, h)
}

fn collect_names(
    exporter: &InMemoryMetricExporter,
) -> std::collections::HashSet<String> {
    exporter
        .get_finished_metrics()
        .unwrap_or_default()
        .iter()
        .flat_map(|rm| rm.scope_metrics.iter())
        .flat_map(|sm| sm.metrics.iter())
        .map(|m| m.name.to_string())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn timed_records_on_drop() {
    let (exporter, provider, h) = setup_provider_and_histogram("t");
    {
        let _t = Timed::new(&h, vec![KeyValue::new("k", "v")]);
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    } // _t drops here → records

    // Wait briefly so the periodic reader can flush into the exporter.
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    let names = collect_names(&exporter);
    let _ = provider.shutdown();
    assert!(
        names.contains("t"),
        "expected `t` in collected metrics, got: {names:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn timed_records_on_early_return_path() {
    let (exporter, provider, h) = setup_provider_and_histogram("early");

    fn inner(h: &opentelemetry::metrics::Histogram<f64>) -> Result<(), ()> {
        let _t = Timed::new(h, vec![]);
        Err(()) // early return path — _t drops on function unwind
    }
    let _ = inner(&h);

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    let names = collect_names(&exporter);
    let _ = provider.shutdown();
    assert!(
        names.contains("early"),
        "expected `early` in collected metrics, got: {names:?}"
    );
}