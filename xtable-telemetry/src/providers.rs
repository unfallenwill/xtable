//! OTel provider factories: tracer, meter, and log.
//!
//! Each function assembles an SDK provider configured for OTLP export using the
//! shared `TelemetryConfig` and `Resource`.

use std::time::Duration;

use anyhow::Context;
use opentelemetry_otlp::{MetricExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::trace::{BatchConfigBuilder, BatchSpanProcessor, TracerProvider as SdkTracerProvider};
use opentelemetry_sdk::Resource;

use crate::config::{OtlpProtocol, TelemetryConfig};
use crate::profiles::sampler_for;

/// Build an OTLP `SdkTracerProvider` with a `BatchSpanProcessor` and the profile sampler.
pub fn build_tracer_provider(
    cfg: &TelemetryConfig,
    resource: Resource,
) -> anyhow::Result<SdkTracerProvider> {
    let endpoint = cfg
        .endpoint
        .as_deref()
        .context("endpoint missing — telemetry is disabled")?;

    let exporter = build_span_exporter(cfg, endpoint)?;

    let batch_config = BatchConfigBuilder::default()
        .with_max_queue_size(2048)
        .with_max_export_batch_size(512)
        .with_scheduled_delay(Duration::from_secs(5))
        .build();

    let processor = BatchSpanProcessor::builder(exporter, opentelemetry_sdk::runtime::Tokio)
        .with_batch_config(batch_config)
        .build();

    let sampler = sampler_for(cfg.profile, cfg.trace_sample_ratio);
    let provider = SdkTracerProvider::builder()
        .with_resource(resource)
        .with_sampler(sampler)
        .with_span_processor(processor)
        .build();
    Ok(provider)
}

/// Build an OTLP `SdkMeterProvider` with a `PeriodicReader` driven by
/// `cfg.metric_export_interval_secs`.
pub fn build_meter_provider(
    cfg: &TelemetryConfig,
    resource: Resource,
) -> anyhow::Result<SdkMeterProvider> {
    let endpoint = cfg
        .endpoint
        .as_deref()
        .context("endpoint missing — telemetry is disabled")?;

    let exporter = build_metric_exporter(cfg, endpoint)?;

    let reader = PeriodicReader::builder(exporter, opentelemetry_sdk::runtime::Tokio)
        .with_interval(cfg.metric_export_interval_secs)
        .with_timeout(Duration::from_secs(10))
        .build();

    Ok(SdkMeterProvider::builder()
        .with_resource(resource)
        .with_reader(reader)
        .build())
}

fn build_span_exporter(cfg: &TelemetryConfig, endpoint: &str) -> anyhow::Result<SpanExporter> {
    match cfg.protocol {
        OtlpProtocol::Grpc => SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .with_timeout(Duration::from_secs(5))
            .build()
            .map_err(|e| anyhow::anyhow!("OTLP span exporter build: {e}")),
        OtlpProtocol::HttpProtobuf => SpanExporter::builder()
            .with_http()
            .with_endpoint(endpoint)
            .with_timeout(Duration::from_secs(5))
            .build()
            .map_err(|e| anyhow::anyhow!("OTLP span exporter build: {e}")),
    }
}

fn build_metric_exporter(cfg: &TelemetryConfig, endpoint: &str) -> anyhow::Result<MetricExporter> {
    match cfg.protocol {
        OtlpProtocol::Grpc => MetricExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .with_timeout(Duration::from_secs(5))
            .build()
            .map_err(|e| anyhow::anyhow!("OTLP metric exporter build: {e}")),
        OtlpProtocol::HttpProtobuf => MetricExporter::builder()
            .with_http()
            .with_endpoint(endpoint)
            .with_timeout(Duration::from_secs(5))
            .build()
            .map_err(|e| anyhow::anyhow!("OTLP metric exporter build: {e}")),
    }
}