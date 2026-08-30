//! Telemetry init: layered subscriber + propagator + provider glue.
//!
//! `install_subscriber` installs the layered `tracing_subscriber`
//! registry with an OTLP trace layer (one filter per layer), a stdout
//! JSON layer (another filter), and a default `RUST_LOG`-aware
//! EnvFilter as the base filter. `install_log_provider` installs the
//! OTel appender bridge so `tracing` events also become OTel
//! `LogRecord`s. The orchestrating `init()` is added in a follow-up
//! task (Phase 4.7).

use anyhow::Context;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::TracerProvider as SdkTracerProvider;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer as _;

use crate::config::TelemetryConfig;
use crate::profiles::{env_filter_for_layer, Layer};

/// Install the layered `tracing_subscriber` registry.
///
/// The subscriber is composed of:
///   * a default `EnvFilter` (honours `RUST_LOG`) for the base registry
///   * an OTLP trace layer (its own `EnvFilter`) that ships spans via
///     `tracing_opentelemetry`
///   * a JSON stdout layer (its own `EnvFilter`) for human inspection
///
/// Every layer carries its own filter, per spec §5.3 — this lets ops
/// tune the OTLP volume independently of stdout volume without a
/// global filter getting in the way.
pub fn install_subscriber(
    cfg: &TelemetryConfig,
    tracer_provider: &SdkTracerProvider,
) -> anyhow::Result<()> {
    // `cfg` is part of the public signature so future versions can tune
    // layers per profile; currently each layer reads its own filter
    // independently, so cfg is unused here. Silence the unused warning.
    let _ = cfg;
    let tracer = tracer_provider.tracer("xtable");
    opentelemetry::global::set_tracer_provider(tracer_provider.clone());

    let otel_layer = tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_filter(env_filter_for_layer(Layer::OtlpTrace));
    let fmt_layer = tracing_subscriber::fmt::layer()
        .json()
        .with_current_span(true)
        .with_span_list(false)
        .with_filter(env_filter_for_layer(Layer::Stdout));

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with(otel_layer)
        .with(fmt_layer)
        .try_init()
        .context("install tracing subscriber")?;
    Ok(())
}

/// Install the OTel log appender bridge so `tracing` events also surface
/// as OTel `LogRecord`s.
///
/// Built on top of
/// `opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge`,
/// which is the 0.27 name for the bridge (the brief's
/// `OpenTelemetryLogLayer` is from an earlier API).
pub fn install_log_provider(
    log_provider: &opentelemetry_sdk::logs::LoggerProvider,
) -> anyhow::Result<()> {
    let bridge =
        opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(log_provider);
    tracing_subscriber::registry()
        .with(bridge.with_filter(env_filter_for_layer(Layer::OtlpLog)))
        .try_init()
        .context("install log bridge")?;
    Ok(())
}
