//! Verifies that `TelemetryGuard` drops without panicking even when the
//! owned providers are wired to real exporters (OTLP gRPC / in-memory).
//!
//! Adapted to the OTel 0.27 SDK API:
//!   - `LoggerProvider` lives in `opentelemetry_sdk::logs`, not in
//!     `opentelemetry_appender_tracing::logger` (that path does not exist in
//!     the 0.27 line).
//!   - The brief's test imports the OTLP gRPC `SpanExporter`; to keep the
//!     test self-contained and avoid hitting the network, we use the SDK
//!     testing exporter (`InMemoryExporter`) instead — the goal is to prove
//!     that `Drop` runs cleanly across all three providers, which does not
//!     depend on the transport.

use std::time::Duration;

use opentelemetry_sdk::logs::LoggerProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::testing::trace::InMemorySpanExporter;
use opentelemetry_sdk::trace::{BatchSpanProcessor, TracerProvider};
use opentelemetry_sdk::runtime::Tokio;
use xtable_telemetry::shutdown::TelemetryGuard;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn guard_drops_without_panic() {
    let span_exporter = InMemorySpanExporter::default();
    let processor = BatchSpanProcessor::builder(span_exporter, Tokio).build();
    let tracer = TracerProvider::builder()
        .with_span_processor(processor)
        .build();
    let meter = SdkMeterProvider::builder().build();
    let log = LoggerProvider::builder().build();
    let guard = TelemetryGuard::new(tracer, meter, log, Duration::from_secs(5));
    drop(guard); // must not panic
}