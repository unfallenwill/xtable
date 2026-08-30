//! Verifies that `TelemetryGuard::drop` shuts down its three providers in
//! the documented order — **logs → metrics → traces** — and that the drop
//! itself completes without panicking.
//!
//! The previous version of this test only asserted that `drop` did not
//! panic; it did not pin down ordering, which is the load-bearing property
//! of the guard. To verify order, we wire each provider with a custom
//! exporter that records its own label into a shared `Vec` when its
//! `shutdown` is invoked. Since each provider's shutdown cascades to its
//! processors and exporters, the recorded labels arrive in the same order
//! as the provider-level shutdowns. After the guard is dropped we assert
//! the recorded order matches expectations.
//!
//! Adapted to the OTel 0.27 SDK API:
//!   - `LoggerProvider` lives in `opentelemetry_sdk::logs` (the path in
//!     `opentelemetry_appender_tracing::logger` does not exist on 0.27).
//!   - `LogExporter` / `PushMetricExporter` use `#[async_trait]`; `SpanExporter`
//!     uses `BoxFuture<'static, _>` for its async `export`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::future::BoxFuture;
use opentelemetry_sdk::export::logs::{LogBatch, LogExporter};
use opentelemetry_sdk::export::trace::{ExportResult, SpanData, SpanExporter};
use opentelemetry_sdk::logs::{LoggerProvider, LogResult};
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::metrics::{MetricResult, PeriodicReader, SdkMeterProvider, Temporality};
use opentelemetry_sdk::runtime::Tokio;
use opentelemetry_sdk::trace::{BatchSpanProcessor, TracerProvider};
use xtable_telemetry::shutdown::TelemetryGuard;

type Order = Arc<Mutex<Vec<&'static str>>>;

#[derive(Debug)]
struct OrderedLogExporter {
    label: &'static str,
    order: Order,
}

#[async_trait]
impl LogExporter for OrderedLogExporter {
    async fn export(&mut self, _batch: LogBatch<'_>) -> LogResult<()> {
        Ok(())
    }

    fn shutdown(&mut self) {
        self.order.lock().unwrap().push(self.label);
    }

    fn set_resource(&mut self, _resource: &opentelemetry_sdk::Resource) {}
}

struct OrderedMetricExporter {
    label: &'static str,
    order: Order,
}

#[async_trait]
impl PushMetricExporter for OrderedMetricExporter {
    async fn export(&self, _metrics: &mut ResourceMetrics) -> MetricResult<()> {
        Ok(())
    }

    async fn force_flush(&self) -> MetricResult<()> {
        Ok(())
    }

    fn shutdown(&self) -> MetricResult<()> {
        self.order.lock().unwrap().push(self.label);
        Ok(())
    }

    fn temporality(&self) -> Temporality {
        Temporality::Cumulative
    }
}

#[derive(Debug)]
struct OrderedSpanExporter {
    label: &'static str,
    order: Order,
}

impl SpanExporter for OrderedSpanExporter {
    fn export(&mut self, _batch: Vec<SpanData>) -> BoxFuture<'static, ExportResult> {
        Box::pin(async { Ok(()) })
    }

    fn shutdown(&mut self) {
        self.order.lock().unwrap().push(self.label);
    }

    fn set_resource(&mut self, _resource: &opentelemetry_sdk::Resource) {}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn guard_shuts_down_in_order() {
    let order: Order = Arc::new(Mutex::new(Vec::new()));

    let log = LoggerProvider::builder()
        .with_simple_exporter(OrderedLogExporter {
            label: "logs",
            order: order.clone(),
        })
        .build();

    let metric_reader = PeriodicReader::builder(
        OrderedMetricExporter {
            label: "metrics",
            order: order.clone(),
        },
        Tokio,
    )
    .with_interval(Duration::from_secs(60))
    .build();
    let meter = SdkMeterProvider::builder().with_reader(metric_reader).build();

    let span_processor = BatchSpanProcessor::builder(
        OrderedSpanExporter {
            label: "traces",
            order: order.clone(),
        },
        Tokio,
    )
    .build();
    let tracer = TracerProvider::builder()
        .with_span_processor(span_processor)
        .build();

    let guard = TelemetryGuard::new(tracer, meter, log, Duration::from_secs(5));
    drop(guard);

    // Drop spawns a worker thread to perform the shutdown (Drop can't await).
    // The shutdown sequence itself is fast (no network involved — the custom
    // exporters are no-ops); sleep briefly to let the thread finish.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let recorded: Vec<&'static str> = order.lock().unwrap().clone();
    assert_eq!(
        recorded,
        vec!["logs", "metrics", "traces"],
        "expected shutdown order logs → metrics → traces, got {recorded:?}"
    );
}