//! Drop guard that flushes and shuts down the three OpenTelemetry providers
//! (logs → metrics → traces) when dropped.
//!
//! Order matters: log records often carry trace_id/span_id references that
//! downstream consumers may want to correlate, so we flush the log provider
//! first; metrics second; traces last so any in-flight log/metric exporters
//! that reference trace context see the closed spans.
//!
//! Shutdown is performed on a dedicated thread because `Drop` cannot `await`.
//! The per-exporter timeout (5s in `providers::build_*`) bounds the work —
//! the `timeout` field on this guard is advisory and reserved for future use.

use std::time::Duration;

use opentelemetry_sdk::logs::LoggerProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::TracerProvider as SdkTracerProvider;

/// Drop guard owning the three OTel providers.
///
/// Constructed by `init()` (Phase 4.7) and held for the lifetime of the
/// server; on drop, all providers are flushed and shut down in the correct
/// order.
pub struct TelemetryGuard {
    pub(crate) tracer: SdkTracerProvider,
    pub(crate) meter: SdkMeterProvider,
    pub(crate) log: LoggerProvider,
    pub(crate) timeout: Duration,
}

impl TelemetryGuard {
    /// Build a new guard owning the three providers plus an advisory
    /// shutdown timeout.
    pub fn new(
        tracer: SdkTracerProvider,
        meter: SdkMeterProvider,
        log: LoggerProvider,
        timeout: Duration,
    ) -> Self {
        Self {
            tracer,
            meter,
            log,
            timeout,
        }
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        let timeout = self.timeout;
        // Each provider is cheap to clone (Arc-backed), so we move clones
        // into the worker thread rather than fighting the borrow checker on
        // `&mut self`.
        let log = self.log.clone();
        let meter = self.meter.clone();
        let tracer = self.tracer.clone();
        let result = std::thread::Builder::new()
            .name("xtable-otel-shutdown".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build();
                if let Ok(rt) = rt {
                    rt.block_on(async move {
                        // Order: logs → metrics → traces.
                        let _ = log.shutdown();
                        let _ = meter.shutdown();
                        let _ = tracer.shutdown();
                    });
                }
            });
        // We can't easily join with a timeout inside Drop; rely on the
        // per-exporter timeouts (set to 5s in `providers`) to bound work.
        if let Err(e) = result {
            tracing::warn!(error=%e, "OTel shutdown thread spawn failed");
        }
        let _ = timeout; // currently advisory — exporter timeouts bound work
    }
}