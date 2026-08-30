# Metric naming — xtable-server

Authoritative reference for the names, types, labels, units, and cardinality
policy of every metric exported by `xtable-server` and its sibling crates.
Mirrors spec `docs/superpowers/specs/2026-08-30-otel-server-design.md` §6.
All instruments are declared once in
`xtable-telemetry/src/metrics.rs` (`Metrics::new`) and cloned into
`AppState` for downstream use.

> CI guard: `xtable-server/tests/metric_cardinality.rs` walks the collected
> attribute sets of every instrument and fails the build if any forbidden
> label leaks. Any change to this list must keep the test green.

## 1. HTTP entry — RED method (spec §6.1)

| Metric                          | Type             | Labels                                          | Unit | Description                                          |
|---------------------------------|------------------|-------------------------------------------------|------|------------------------------------------------------|
| `http.server.request.duration`  | Histogram        | `http.route`, `http.request.method`, `http.response.status_code` | `s`  | Wall-clock duration of HTTP server requests.         |
| `http.server.requests`          | Counter          | `http.route`, `http.request.method`, `http.response.status_code` | —    | Total HTTP server requests.                          |
| `http.server.active_requests`   | UpDownCounter    | —                                               | —    | HTTP server requests currently in-flight.            |

All histograms carry exemplars linking to the sampled trace.

## 2. Internal operations — Latency + Errors + Saturation (spec §6.2)

For each async fn entry point: `{op}.duration` (Histogram) /
`{op}.count` (Counter, label `outcome`) / `{op}.active` (UpDownCounter).

| Metric                          | Type             | Labels                                  | Unit | Description                                                  |
|---------------------------------|------------------|-----------------------------------------|------|--------------------------------------------------------------|
| `txn.commit.duration`           | Histogram        | `space`, `table`                        | `s`  | Wall-clock duration of transaction commit attempts.          |
| `txn.commit.count`              | Counter          | `outcome` (`ok` / `err` / `conflict`)   | —    | Total transaction commit attempts.                           |
| `txn.commit.active`             | UpDownCounter    | —                                       | —    | Transaction commit operations currently in-flight.           |
| `txn.abort.duration`            | Histogram        | —                                       | `s`  | Wall-clock duration of transaction aborts.                   |
| `txn.abort.count`               | Counter          | `outcome`                               | —    | Total transaction aborts.                                    |
| `txn.begin.duration`            | Histogram        | —                                       | `s`  | Wall-clock duration of transaction begin operations.         |
| `txn.begin.count`               | Counter          | `outcome`                               | —    | Total transaction begin operations.                          |
| `txn.ssi.conflict.count`        | Counter          | —                                       | —    | SSI write-write conflicts detected.                          |
| `memtable.write.duration`       | Histogram        | `op`                                    | `s`  | Duration of memtable write operations.                       |
| `memtable.flush.duration`       | Histogram        | `op`                                    | `s`  | Duration of memtable flush operations.                       |
| `memtable.bytes`                | UpDownCounter    | `level` (`active` / `immutable`)         | `By` | Current memtable bytes (active + immutable).                 |
| `memtable.entries`              | UpDownCounter    | `level`                                 | —    | Current memtable entry count.                                |
| `chunk.upload.duration`         | Histogram        | `op`                                    | `s`  | Duration of chunk uploads.                                   |
| `chunk.download.duration`       | Histogram        | `op`                                    | `s`  | Duration of chunk downloads.                                 |
| `chunk.cache.hits`              | Counter          | —                                       | —    | Total chunk cache hits.                                      |
| `chunk.cache.misses`            | Counter          | —                                       | —    | Total chunk cache misses.                                    |
| `chunk.cache.bytes`             | UpDownCounter    | —                                       | `By` | Current chunk cache size in bytes.                           |
| `wal.append.duration`           | Histogram        | `op`                                    | `s`  | Duration of WAL append operations.                           |
| `gc.sweep.duration`             | Histogram        | `op`                                    | `s`  | Duration of GC sweeps.                                        |
| `gc.entries.removed`            | Counter          | —                                       | —    | Total entries removed by GC sweeps.                          |
| `recovery.replay.duration`      | Histogram        | `op`                                    | `s`  | Duration of WAL replay during recovery.                      |
| `rebuild.cold.duration`         | Histogram        | `op`                                    | `s`  | Duration of cold rebuild operations.                         |
| `backend.s3.duration`           | Histogram        | `op` (`put` / `get` / `multipart`), `outcome` | `s` | Duration of S3 backend operations.                       |
| `backend.s3.count`              | Counter          | `op`, `outcome`                         | —    | Total S3 backend operations.                                 |

## 3. Resource utilization — USE method (spec §6.3)

| Metric                          | Type             | Unit | Description                                                  |
|---------------------------------|------------------|------|--------------------------------------------------------------|
| `process.runtime.memory.heap`   | UpDownCounter    | `By` | Process heap memory usage in bytes (sampled gauge).          |
| `process.runtime.cpu.count`     | UpDownCounter    | —    | Number of CPU cores available to the process (static).       |
| `process.runtime.uptime`        | Counter          | `s`  | Process uptime since boot (OTel semconv counter).            |

## 4. Cardinality policy (spec §6.4)

### 4.1 Whitelisted metric-label keys

- OTel semconv enums: `http.request.method`, `http.response.status_code`,
  `outcome`, `op`, `level`.
- Config-time enums: `environment`.
- `space` and `table` **only** when
  `[observability].enable_per_table_metrics = true` AND the space/table
  count is bounded by config (future-proofed; off by default).

### 4.2 Forbidden metric-label keys (CI-enforced)

The following keys MUST NEVER appear in any metric attribute set. Each is
high-cardinality by construction and would destroy the time-series backend.

| Key                  | Why it's forbidden                                                  |
|----------------------|---------------------------------------------------------------------|
| `record_id`          | ULID per record — unique per write.                                 |
| `txn.id`             | ULID per transaction — unique per transaction.                      |
| `request_id`         | ULID per HTTP request — unique per request.                         |
| `page_id`            | Per memtable page id.                                               |
| `body_id`            | Per record body id.                                                 |
| `body_hash`          | Per record body hash (sha256).                                      |
| `url.path`           | Raw path (use `http.route` — `MatchedPath` is cardinality-safe).    |
| `user_id`            | User identifier.                                                    |
| `access_key_id`      | Auth credential.                                                    |
| `secret_access_key`  | Auth credential (should never appear anywhere outside the secret store). |

In addition:

- Any user-controlled string.
- Any timestamp or unbounded number.

### 4.3 What to do instead

- For high-cardinality identifiers, prefer span attributes or log fields
  (where cardinality is a per-event cost, not a per-second cost) or the
  request's `baggage` (per-trace, not per-metric).
- For raw URL paths, use `axum::extract::MatchedPath` and emit
  `http.route` (e.g. `/v1/spaces/:space/tables/:table/records`) instead of
  the raw `url.path`.