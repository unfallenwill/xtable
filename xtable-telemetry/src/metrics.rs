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
//! begin lifecycle. Storage (`memtable.*` / `chunk.*` / `wal.*` / `gc.*` /
//! `recovery.*` / `rebuild.*`), backend (`backend.s3.*`), and process-runtime
//! (`process.runtime.*`) instruments round out the §6.2 / §6.3 surface.

use opentelemetry::metrics::{Counter, Histogram, Meter, UpDownCounter};
use std::sync::OnceLock;

/// Lazily initialized metrics bound to the process-wide global meter.
pub fn global() -> &'static Metrics {
    static METRICS: OnceLock<Metrics> = OnceLock::new();
    METRICS.get_or_init(Metrics::default)
}

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

    // ── MemTable ──────────────────────────────────────────────────────────
    /// Wall-clock duration of memtable write operations, in seconds.
    pub memtable_write_duration: Histogram<f64>,
    /// Wall-clock duration of memtable flush operations, in seconds.
    pub memtable_flush_duration: Histogram<f64>,
    /// Current bytes occupied by memtables (active + immutable).
    pub memtable_bytes: UpDownCounter<i64>,
    /// Current number of entries across all memtables.
    pub memtable_entries: UpDownCounter<i64>,

    // ── Chunk storage ─────────────────────────────────────────────────────
    /// Wall-clock duration of chunk uploads, in seconds.
    pub chunk_upload_duration: Histogram<f64>,
    /// Wall-clock duration of chunk downloads, in seconds.
    pub chunk_download_duration: Histogram<f64>,
    /// Total count of chunk cache hits.
    pub chunk_cache_hits: Counter<u64>,
    /// Total count of chunk cache misses.
    pub chunk_cache_misses: Counter<u64>,
    /// Current bytes held in the chunk cache.
    pub chunk_cache_bytes: UpDownCounter<i64>,

    // ── WAL ───────────────────────────────────────────────────────────────
    /// Wall-clock duration of WAL append operations, in seconds.
    pub wal_append_duration: Histogram<f64>,

    // ── GC / recovery / rebuild ───────────────────────────────────────────
    /// Wall-clock duration of GC sweeps, in seconds.
    pub gc_sweep_duration: Histogram<f64>,
    /// Total entries removed by GC sweeps.
    pub gc_entries_removed: Counter<u64>,
    /// Wall-clock duration of WAL replay during recovery, in seconds.
    pub recovery_replay_duration: Histogram<f64>,
    /// Wall-clock duration of cold rebuild operations, in seconds.
    pub rebuild_cold_duration: Histogram<f64>,

    // ── S3 backend ────────────────────────────────────────────────────────
    /// Wall-clock duration of S3 backend operations, in seconds.
    pub backend_s3_duration: Histogram<f64>,
    /// Total count of S3 backend operations.
    pub backend_s3_total: Counter<u64>,

    // ── Process runtime (USE) ─────────────────────────────────────────────
    /// Process heap memory usage in bytes (sampled gauge via UpDownCounter).
    pub process_runtime_memory_heap: UpDownCounter<i64>,
    /// Number of CPU cores available to the process.
    pub process_runtime_cpu_count: UpDownCounter<i64>,
    /// Process uptime since boot, in seconds (OTel semconv counter).
    pub process_runtime_uptime: Counter<u64>,
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

            // ── MemTable ──────────────────────────────────────────────────
            memtable_write_duration: meter
                .f64_histogram("memtable.write.duration")
                .with_description("Duration of memtable write operations")
                .with_unit("s")
                .build(),
            memtable_flush_duration: meter
                .f64_histogram("memtable.flush.duration")
                .with_description("Duration of memtable flush operations")
                .with_unit("s")
                .build(),
            memtable_bytes: meter
                .i64_up_down_counter("memtable.bytes")
                .with_description("Current memtable bytes (active + immutable)")
                .with_unit("By")
                .build(),
            memtable_entries: meter
                .i64_up_down_counter("memtable.entries")
                .with_description("Current memtable entry count")
                .build(),

            // ── Chunk ─────────────────────────────────────────────────────
            chunk_upload_duration: meter
                .f64_histogram("chunk.upload.duration")
                .with_description("Duration of chunk uploads")
                .with_unit("s")
                .build(),
            chunk_download_duration: meter
                .f64_histogram("chunk.download.duration")
                .with_description("Duration of chunk downloads")
                .with_unit("s")
                .build(),
            chunk_cache_hits: meter
                .u64_counter("chunk.cache.hits")
                .with_description("Chunk cache hits")
                .build(),
            chunk_cache_misses: meter
                .u64_counter("chunk.cache.misses")
                .with_description("Chunk cache misses")
                .build(),
            chunk_cache_bytes: meter
                .i64_up_down_counter("chunk.cache.bytes")
                .with_description("Current chunk cache size in bytes")
                .with_unit("By")
                .build(),

            // ── WAL ───────────────────────────────────────────────────────
            wal_append_duration: meter
                .f64_histogram("wal.append.duration")
                .with_description("Duration of WAL append operations")
                .with_unit("s")
                .build(),

            // ── GC / recovery / rebuild ───────────────────────────────────
            gc_sweep_duration: meter
                .f64_histogram("gc.sweep.duration")
                .with_description("Duration of GC sweeps")
                .with_unit("s")
                .build(),
            gc_entries_removed: meter
                .u64_counter("gc.entries.removed")
                .with_description("Entries removed by GC sweeps")
                .build(),
            recovery_replay_duration: meter
                .f64_histogram("recovery.replay.duration")
                .with_description("Duration of WAL replay during recovery")
                .with_unit("s")
                .build(),
            rebuild_cold_duration: meter
                .f64_histogram("rebuild.cold.duration")
                .with_description("Duration of cold rebuild operations")
                .with_unit("s")
                .build(),

            // ── S3 backend ────────────────────────────────────────────────
            backend_s3_duration: meter
                .f64_histogram("backend.s3.duration")
                .with_description("Duration of S3 backend operations")
                .with_unit("s")
                .build(),
            backend_s3_total: meter
                .u64_counter("backend.s3.count")
                .with_description("Total S3 backend operations")
                .build(),

            // ── Process runtime (USE) ─────────────────────────────────────
            process_runtime_memory_heap: meter
                .i64_up_down_counter("process.runtime.memory.heap")
                .with_description("Process heap memory usage in bytes")
                .with_unit("By")
                .build(),
            process_runtime_cpu_count: meter
                .i64_up_down_counter("process.runtime.cpu.count")
                .with_description("Number of CPU cores available to the process")
                .build(),
            process_runtime_uptime: meter
                .u64_counter("process.runtime.uptime")
                .with_description("Process uptime since boot")
                .with_unit("s")
                .build(),
        }
    }
}

impl Default for Metrics {
    /// Fallback handle set when telemetry has not been initialised.
    ///
    /// The `Default` impl uses the process-wide global meter, which OTel 0.27
    /// supplies with a no-op provider until `global::set_meter_provider(...)`
    /// is called. Every instrument handle still type-checks, so callers like
    /// `xtable-server`'s RED middleware can record against `state.metrics`
    /// unconditionally — recordings are silently dropped when no exporter is
    /// attached, and start flowing as soon as `telemetry::init()` wires a
    /// real `SdkMeterProvider`.
    ///
    /// **Sequencing contract:** `Metrics::default()` binds instruments to the
    /// `Meter` returned by `global::meter("xtable")` at the moment of
    /// construction. OTel 0.27 instruments are permanently bound to their
    /// creating `Meter` — calling `global::set_meter_provider(...)` AFTER
    /// construction does not redirect already-built instruments. Callers
    /// MUST construct `Metrics` AFTER `xtable_telemetry::init::init(cfg)`
    /// has run (which sets the global provider), so the instruments bind
    /// to the live `SdkMeterProvider`.
    fn default() -> Self {
        let meter = opentelemetry::global::meter("xtable");
        Self::new(&meter)
    }
}
