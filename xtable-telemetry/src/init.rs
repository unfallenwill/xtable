//! Telemetry init: layered subscriber + propagator + provider glue.
//!
//! Two subscriber shapes:
//! * `install_otel_subscriber` — OTLP trace layer only (no stdout).
//!   Used when OTel export is enabled.
//! * `install_stdout_subscriber` — JSON stdout layer only. Used when
//!   OTel is disabled so logs remain visible to operators.
//!
//! Both build a `tracing_subscriber` but neither calls `try_init` —
//! the caller decides when to install the subscriber globally.
//!
//! `install_log_provider` accepts an existing subscriber and adds the
//! OTel appender bridge as a new layer; it also does **not** call
//! `try_init`.
//!
//! `init` orchestrates everything: builds the resource, the three
//! providers (tracer / meter / log) when OTel is enabled, chains the
//! layers via the two builders above, installs them and the W3C
//! TraceContext + W3C Baggage propagators (composed together), then
//! calls `try_init` exactly once on the combined subscriber and
//! returns a `TelemetryGuard` that owns the providers and drains
//! them on `Drop`.
//!
//! Returns `Ok(None)` when no OTLP endpoint is configured — a
//! stdout-only JSON subscriber is installed so logs remain visible.
//! Callers can wire it in unconditionally with
//! `let _guard = telemetry::init(&cfg)?;`.

use anyhow::Context;
use opentelemetry::propagation::TextMapCompositePropagator;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::propagation::{BaggagePropagator, TraceContextPropagator};
use opentelemetry_sdk::trace::TracerProvider as SdkTracerProvider;
use tracing::Subscriber;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer as _;

use crate::config::TelemetryConfig;
use crate::profiles::{env_filter_for_layer, Layer};
use crate::providers::{build_meter_provider, build_tracer_provider, install_log_appender};
use crate::resource::build_resource;
use crate::shutdown::TelemetryGuard;

/// Build the OTel-only `tracing_subscriber` stack.
///
/// Ships spans via `tracing_opentelemetry` to the provided
/// `SdkTracerProvider`. Stdout is intentionally silent — production
/// log volume stays bounded by collector ingest.
///
/// Every layer carries its own filter, per spec §5.3 — this lets ops
/// tune the OTLP volume independently of stdout volume without a
/// global filter getting in the way.
///
/// The function does **not** call `try_init`; the caller chooses when
/// (and whether) to install it as the global default.
pub fn install_otel_subscriber(
    cfg: &TelemetryConfig,
    tracer_provider: &SdkTracerProvider,
) -> impl Subscriber + for<'a> LookupSpan<'a> {
    // `cfg` is part of the public signature so future versions can tune
    // layers per profile; currently each layer reads its own filter
    // independently, so cfg is unused here. Silence the unused warning.
    let _ = cfg;
    let tracer = tracer_provider.tracer("xtable");
    opentelemetry::global::set_tracer_provider(tracer_provider.clone());

    let otel_layer = tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_filter(env_filter_for_layer(Layer::OtlpTrace));

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with(otel_layer)
}

/// Build the stdout-only `tracing_subscriber` stack.
///
/// Used when OTel export is disabled. Writes JSON to stdout with the
/// `Layer::Stdout` filter.
///
/// The function does **not** call `try_init`; the caller chooses when
/// (and whether) to install it as the global default.
pub fn install_stdout_subscriber(
    cfg: &TelemetryConfig,
) -> impl Subscriber + for<'a> LookupSpan<'a> {
    let _ = cfg;
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
        .with(fmt_layer)
}

/// Append the OTel log appender bridge onto an existing subscriber.
///
/// Built on top of
/// `opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge`,
/// which is the 0.27 name for the bridge (the brief's
/// `OpenTelemetryLogLayer` is from an earlier API).
///
/// Like `install_subscriber`, this function does **not** call
/// `try_init` — the caller continues to compose layers and then
/// installs the combined subscriber with a single `try_init`.
pub fn install_log_provider<S>(
    log_provider: &opentelemetry_sdk::logs::LoggerProvider,
    inner: S,
) -> impl Subscriber + for<'a> LookupSpan<'a>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let bridge =
        opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(log_provider);
    inner.with(bridge.with_filter(env_filter_for_layer(Layer::OtlpLog)))
}

/// One-shot orchestrator: build providers, install subscribers and
/// propagators, return a `TelemetryGuard`.
///
/// When `cfg.endpoint` is unset or empty the function returns `Ok(None)`
/// — telemetry is treated as opt-in. The caller still holds a regular
/// `Result` so wiring can be unconditional:
/// `let _guard = telemetry::init(&cfg)?;` and ignore the option.
///
/// Propagators: per spec §12 (Baggage), both W3C TraceContext and W3C
/// Baggage must be active. OTel 0.27 only supports a single global
/// propagator at a time, so we compose them via the API-level
/// `TextMapCompositePropagator` (the 0.27 type lives in
/// `opentelemetry::propagation`) before handing it to
/// `global::set_text_map_propagator`.
pub fn init(cfg: &TelemetryConfig) -> anyhow::Result<Option<TelemetryGuard>> {
    // W3C TraceContext + W3C Baggage, composed into a single global
    // propagator. Installed unconditionally so the HTTP layer can
    // extract incoming `traceparent` headers for correlation even
    // when OTel export is disabled.
    let composite = TextMapCompositePropagator::new(vec![
        Box::new(TraceContextPropagator::new()),
        Box::new(BaggagePropagator::new()),
    ]);
    opentelemetry::global::set_text_map_propagator(composite);

    // Telemetry is opt-in: when no endpoint is configured, install a
    // stdout-only JSON subscriber so logs are visible. The
    // stdout/OTel paths are mutually exclusive — when OTel is enabled
    // stdout stays silent by design (production log volume stays
    // bounded by collector ingest).
    let endpoint = match cfg.endpoint.as_deref() {
        Some(e) if !e.trim().is_empty() => e,
        _ => {
            install_stdout_subscriber(cfg)
                .try_init()
                .context("install stdout subscriber")?;
            tracing::info!("OpenTelemetry disabled — logs to stdout");
            return Ok(None);
        }
    };

    let resource = build_resource(cfg);
    let tracer = build_tracer_provider(cfg, resource.clone())?;
    let meter = build_meter_provider(cfg, resource.clone())?;
    let log = install_log_appender(cfg, resource)?;

    // Install the live `SdkMeterProvider` as the process-wide meter
    // provider. Without this, `Metrics::default()` (used by
    // the shared global metrics cache) and the
    // `Metrics::new(&global::meter("xtable"))`
    // call in xtable-server main bind their instruments to whatever
    // global provider existed at first call — the no-op default —
    // and every recording is silently dropped. Mirrors the
    // `set_tracer_provider` call in `install_subscriber`.
    //
    // Sequencing contract: instruments constructed AFTER this call
    // (i.e. any `Metrics::new(...)` or `Metrics::default()` executed
    // by later initialization) will bind to the live `SdkMeterProvider`
    // and forward recordings to the OTLP exporter. Instruments
    // constructed BEFORE this call (e.g. from a test that built
    // Metrics before init()) bind to whatever provider existed at
    // construction time and are unaffected by this re-bind.
    opentelemetry::global::set_meter_provider(meter.clone());

    // Build the layered subscriber (without calling try_init) and chain
    // the OTel log bridge onto it. A single try_init at the end means
    // the layers stay composed — calling try_init twice would race the
    // global default and discard the first subscriber entirely.
    let subscriber = install_log_provider(&log, install_otel_subscriber(cfg, &tracer));
    subscriber
        .try_init()
        .context("install tracing subscriber")?;

    tracing::info!(
        otel_endpoint = %endpoint,
        profile = ?cfg.profile,
        "OpenTelemetry enabled"
    );

    Ok(Some(TelemetryGuard::new(
        tracer,
        meter,
        log,
        cfg.shutdown_flush_timeout_secs,
    )))
}
