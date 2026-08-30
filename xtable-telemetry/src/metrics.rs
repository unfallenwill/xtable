//! Pre-registered OTel metric instruments.
//!
//! All instruments are declared once here so that downstream code can record
//! against well-known handles. The registry is built per-`Meter` via
//! [`Metrics::new`]; the same `Meter` is shared across the lifetime of the
//! process so that instrument handles remain stable for the duration of
//! telemetry collection.
//!
//! HTTP RED (Rate / Errors / Duration) is captured by the three `http.*`
//! instruments. The transaction (`txn.*`) subset covers the commit / abort /
//! begin lifecycle; additional storage, chunk, backend, and resource
//! instruments will be added in a follow-up task.

use opentelemetry::metrics::{Counter, Histogram, Meter, UpDownCounter};

/// Pre-built OTel metric instruments.
///
/// Cloning is cheap — each handle is an `Arc`-backed reference to the SDK
/// instrument implementation.
#[derive(Clone)]
pub struct Metrics {
    // ── HTTP server (RED) ──────────────────────────────────────────────────
    /// Wall-clock duration of HTTP server requests, in seconds.
    pub http_request_duration: Histogram<f64>,
    /// Total count of HTTP server requests received.
    pub http_requests_total: Counter<u64>,
    /// Number of HTTP server requests currently in-flight.
    pub http_active_requests: UpDownCounter<i64>,

    // ── Transaction commit ────────────────────────────────────────────────
    /// Wall-clock duration of transaction commit attempts, in seconds.
    pub txn_commit_duration: Histogram<f64>,
    /// Total count of transaction commit attempts.
    pub txn_commit_total: Counter<u64>,
    /// Number of transaction commit operations currently in-flight.
    pub txn_commit_active: UpDownCounter<i64>,

    // ── Transaction abort ─────────────────────────────────────────────────
    /// Wall-clock duration of transaction aborts, in seconds.
    pub txn_abort_duration: Histogram<f64>,
    /// Total count of transaction aborts.
    pub txn_abort_total: Counter<u64>,

    // ── Transaction begin ─────────────────────────────────────────────────
    /// Wall-clock duration of transaction begin operations, in seconds.
    pub txn_begin_duration: Histogram<f64>,
    /// Total count of transaction begin operations.
    pub txn_begin_total: Counter<u64>,

    // ── SSI conflicts ─────────────────────────────────────────────────────
    /// Total count of SSI write-write conflicts detected.
    pub txn_ssi_conflict_total: Counter<u64>,
}

impl Metrics {
    /// Build all instruments against the supplied `Meter`.
    pub fn new(meter: &Meter) -> Self {
        Self {
            http_request_duration: meter
                .f64_histogram("http.server.request.duration")
                .with_description("Duration of HTTP server requests")
                .with_unit("s")
                .build(),
            http_requests_total: meter
                .u64_counter("http.server.requests")
                .with_description("Total HTTP server requests")
                .build(),
            http_active_requests: meter
                .i64_up_down_counter("http.server.active_requests")
                .with_description("Active HTTP server requests")
                .build(),

            txn_commit_duration: meter
                .f64_histogram("txn.commit.duration")
                .with_description("Duration of transaction commits")
                .with_unit("s")
                .build(),
            txn_commit_total: meter
                .u64_counter("txn.commit.count")
                .with_description("Total transaction commit attempts")
                .build(),
            txn_commit_active: meter
                .i64_up_down_counter("txn.commit.active")
                .with_description("In-flight commit operations")
                .build(),

            txn_abort_duration: meter
                .f64_histogram("txn.abort.duration")
                .with_description("Duration of transaction aborts")
                .with_unit("s")
                .build(),
            txn_abort_total: meter
                .u64_counter("txn.abort.count")
                .with_description("Total transaction aborts")
                .build(),

            txn_begin_duration: meter
                .f64_histogram("txn.begin.duration")
                .with_description("Duration of transaction begins")
                .with_unit("s")
                .build(),
            txn_begin_total: meter
                .u64_counter("txn.begin.count")
                .with_description("Total transaction begins")
                .build(),

            txn_ssi_conflict_total: meter
                .u64_counter("txn.ssi.conflict.count")
                .with_description("SSI conflict count")
                .build(),
        }
    }
}