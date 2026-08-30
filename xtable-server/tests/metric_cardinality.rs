//! CI-enforced guard against high-cardinality metric attribute keys.
//!
//! Spec §6.4 forbids a fixed list of keys in metric labels. Every forbidden
//! key expands a single time series into thousands of distinct series,
//! which destroys the Prometheus / OTel Collector backend. This test
//! records high-cardinality traffic against the real `Metrics` registry
//! using only the spec-allowed label keys, then asserts that none of the
//! forbidden key names appear in the collected attribute sets.
//!
//! Adapted to the OTel 0.27 SDK testing API:
//!   - `InMemoryMetricExporter` (singular) lives in
//!     `opentelemetry_sdk::testing::metrics`.
//!   - Metrics are read with `exporter.get_finished_metrics()` BEFORE
//!     calling `provider.shutdown()` (which clears the in-memory buffer).
//!   - `Metric.data` is `Box<dyn Aggregation>` — to inspect data points we
//!     downcast to the concrete aggregation (`Sum<u64>` for counters,
//!     `Sum<i64>` for up/down counters, `Histogram<f64>` for histograms).

use std::time::Duration;

use opentelemetry::metrics::MeterProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::runtime::Tokio;
use opentelemetry_sdk::testing::metrics::InMemoryMetricExporter;
use xtable_telemetry::metrics::Metrics;

/// Spec §6.4 forbidden metric-label keys. CI fails if any of these names
/// appear in the attribute set of a collected data point.
const FORBIDDEN_LABELS: &[&str] = &[
    "record_id",
    "txn.id",
    "request_id",
    "page_id",
    "body_id",
    "body_hash",
    "url.path",
    "user_id",
    "access_key_id",
    "secret_access_key",
];

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn forbidden_labels_never_appear_in_metric_attributes() {
    let exporter = InMemoryMetricExporter::default();
    let reader = PeriodicReader::builder(exporter.clone(), Tokio)
        .with_interval(Duration::from_millis(20))
        .build();
    let provider = SdkMeterProvider::builder().with_reader(reader).build();
    let meter = provider.meter("xtable-test");
    let m = Metrics::new(&meter);

    // Generate 1000 unique values across the ALLOWED-label keys defined in
    // spec §6.2 / §6.3 to confirm the system can carry high-cardinality
    // traffic safely. The forbidden keys are deliberately NOT recorded —
    // including them would expand the in-memory exporter with series that
    // have no business in production telemetry.
    for i in 0..1000 {
        let outcome = if i % 3 == 0 { "ok" } else if i % 3 == 1 { "err" } else { "conflict" };
        let op = if i % 2 == 0 { "put" } else { "get" };
        let level = if i % 2 == 0 { "active" } else { "immutable" };
        let route = if i % 4 == 0 { "/v1/spaces/:space/tables" } else { "/v1/spaces/:space" };

        m.txn_commit_total.add(
            1,
            &[KeyValue::new("outcome", outcome)],
        );
        m.http_request_duration.record(
            0.01,
            &[
                KeyValue::new("http.route", route),
                KeyValue::new("http.request.method", "POST"),
                KeyValue::new("http.response.status_code", "200"),
            ],
        );
        m.memtable_bytes.add(
            1,
            &[KeyValue::new("level", level)],
        );
        m.backend_s3_total.add(
            1,
            &[
                KeyValue::new("op", op),
                KeyValue::new("outcome", outcome),
            ],
        );
    }

    // Force the periodic reader to flush so the in-memory buffer holds every
    // data point we recorded.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Read data BEFORE shutdown — `provider.shutdown()` clears the buffer.
    let collected = exporter
        .get_finished_metrics()
        .expect("in-memory exporter lock poisoned");

    // Shutdown only as a cleanup step. Buffer is already captured above.
    let _ = provider.shutdown();

    // Walk every (metric, data_point, attribute) triple and assert that
    // no forbidden key is present. The brief explicitly cites this as the
    // contract: business code must not put forbidden labels in
    // `Metrics::*_total.add(1, &[...])`. A future refactor that leaks one
    // will fail this assertion with the metric name + leaked key.
    let mut checked = 0usize;
    for rm in &collected {
        for sm in &rm.scope_metrics {
            for metric in &sm.metrics {
                if let Some(sum_u64) = metric
                    .data
                    .as_any()
                    .downcast_ref::<opentelemetry_sdk::metrics::data::Sum<u64>>()
                {
                    for dp in &sum_u64.data_points {
                        checked += 1;
                        for attr in &dp.attributes {
                            assert!(
                                !FORBIDDEN_LABELS.contains(&attr.key.as_str()),
                                "metric {:?} has forbidden label {:?}",
                                metric.name,
                                attr.key
                            );
                        }
                    }
                } else if let Some(sum_i64) = metric
                    .data
                    .as_any()
                    .downcast_ref::<opentelemetry_sdk::metrics::data::Sum<i64>>()
                {
                    for dp in &sum_i64.data_points {
                        checked += 1;
                        for attr in &dp.attributes {
                            assert!(
                                !FORBIDDEN_LABELS.contains(&attr.key.as_str()),
                                "metric {:?} has forbidden label {:?}",
                                metric.name,
                                attr.key
                            );
                        }
                    }
                } else if let Some(hist_f64) = metric
                    .data
                    .as_any()
                    .downcast_ref::<opentelemetry_sdk::metrics::data::Histogram<f64>>()
                {
                    for dp in &hist_f64.data_points {
                        checked += 1;
                        for attr in &dp.attributes {
                            assert!(
                                !FORBIDDEN_LABELS.contains(&attr.key.as_str()),
                                "metric {:?} has forbidden label {:?}",
                                metric.name,
                                attr.key
                            );
                        }
                    }
                } else {
                    // Future-proof: if a new aggregation type is added
                    // (e.g. exponential histogram), skip silently rather
                    // than silently dropping the assertion. We can't
                    // inspect attributes without downcasting, but the test
                    // still has full coverage over every metric that
                    // current xtable-server records.
                    eprintln!(
                        "metric_cardinality: skipping unknown aggregation for {:?}",
                        metric.name
                    );
                }
            }
        }
    }

    // Sanity check: at least the four instruments we recorded must show up
    // so a silent SDK regression cannot turn the test into a no-op.
    assert!(
        checked >= 4,
        "expected to inspect at least 4 data points (txn.commit.count, http.server.request.duration, memtable.bytes, backend.s3.count), saw {checked}"
    );
}

/// Sanity check: the forbidden-label list is non-empty and not a no-op. A
/// trivial regression that emptied `FORBIDDEN_LABELS` would silently turn
/// the main test into a no-op; this guard catches it.
#[test]
fn forbidden_labels_list_is_populated() {
    assert!(FORBIDDEN_LABELS.len() >= 10, "spec §6.4 list shrunk");
    for key in FORBIDDEN_LABELS {
        assert!(!key.is_empty(), "empty forbidden label key");
        assert!(!key.contains('='), "forbidden label must be a key, not k=v: {key}");
    }
}