# Log conventions — xtable-server

Authoritative reference for log format, severity, attribute policy, and the
canonical `info!(...)` template. Mirrors spec
`docs/superpowers/specs/2026-08-30-otel-server-design.md` §8.

## 1. Output format (spec §8.1)

All logs are emitted as **structured JSON** to stdout. The OTel log record
is exported via `opentelemetry-appender-tracing` for OTLP ingest.

```
tracing_subscriber::fmt::layer().json()
```

Example record:

```json
{
  "timestamp": "2026-08-30T19:42:01.123Z",
  "level": "INFO",
  "target": "xtable_tx::coordinator",
  "message": "txn committed",
  "txn.id": "01JC9EZ8T7M0RJX9Y8P3Q5W4V0",
  "space": "acme",
  "table": "tasks",
  "op": "upsert",
  "duration_ms": 18,
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "span_id": "00f067aa0ba902b7",
  "resource": {
    "service.name": "xtable",
    "service.version": "0.1.0",
    "deployment.environment.name": "production"
  }
}
```

## 2. Every log entry carries (spec §8.2)

| Field         | Source                                                                 |
|---------------|------------------------------------------------------------------------|
| `timestamp`   | RFC 3339 wall-clock.                                                   |
| `level`       | One of `ERROR`, `WARN`, `INFO`, `DEBUG`, `TRACE` (see §3 mapping).     |
| `target`      | `tracing` module path (e.g. `xtable_tx::coordinator`).                 |
| `message`     | Short human-readable summary; first positional arg to `info!`.        |
| `attributes`  | Structured key-value payload — only whitelisted keys (see §4).         |
| `trace_id`    | Hex-encoded W3C trace id from current span (auto-attached).            |
| `span_id`     | Hex-encoded W3C span id from current span (auto-attached).             |
| `resource`    | Shared with tracer / meter (service.name, version, env, host, …).      |

## 3. Severity mapping (spec §8.3)

| `tracing` | OTel `severity_text` | OTel `severity_number` |
|-----------|----------------------|------------------------|
| `ERROR`    | `ERROR`              | 17                     |
| `WARN`     | `WARN`               | 13                     |
| `INFO`     | `INFO`               | 9                      |
| `DEBUG`    | `DEBUG`              | 5                      |
| `TRACE`    | `TRACE`              | 1                      |

The mapping is enforced by `tracing-opentelemetry` + the JSON layer's
default behavior; do not set `severity_text` by hand.

## 4. Attribute policy

### 4.1 Whitelisted attribute keys

These keys are safe to emit as structured fields on log records:

- OTel semconv attributes already used in spans (e.g. `http.route`,
  `http.request.method`, `http.response.status_code`).
- OTel resource attributes (`service.*`, `host.*`, `process.*`, `os.*`).
- Business enumerators with a closed set of values:
  `op` (`put` / `get` / `upsert` / `delete` / `commit` / `abort` / `begin`),
  `outcome` (`ok` / `err` / `conflict`),
  `level` (`active` / `immutable`).
- `space` / `table` — bounded by configuration.
- `duration_ms` — measured latency, fixed-width integer.
- `request.id` — when explicitly set by the trace layer.

### 4.2 Forbidden attribute keys

These MUST NEVER appear in log attributes — same list and rationale as
the metric-cardinality policy (see `metric-naming.md`):

- `record_id`, `txn.id`, `request_id`, `page_id`, `body_id`, `body_hash`
- `url.path`
- `user_id`, `access_key_id`, `secret_access_key`
- Any user-controlled string, any timestamp, any unbounded number

If you need to log a per-record id for debugging, attach it as a **span
attribute** (visible only when the trace is sampled) or include it in a
**separate debug log** at `TRACE` level with an explicit `target` filter
so production's `INFO`-default `EnvFilter` drops it.

## 5. Canonical `info!(...)` template

Use this template for every business-level `info!` site. Stable field
order keeps JSON diffs reviewable; trailing comma-free message at the end.

```rust
info!(
    txn.id = %txn_id,
    space = %space,
    table = %table,
    op = "upsert",
    outcome = "ok",
    duration_ms = elapsed.as_millis() as u64,
    "txn committed"
);
```

Rules:

1. Use `%format_arg` for owned `String` / `&str` / `Display` values so the
   field is rendered into the JSON record.
2. Use `?format_arg` for non-`Display` types (`Debug`).
3. Use a string literal for `op` and `outcome` — they must be a closed set
   to avoid silent label drift.
4. The trailing positional argument is the human-readable `message`. It is
   short (≤ 80 chars), present tense, no PII, no full request body.
5. Never interpolate user-controlled content into the message: log
   attributes carry the data, the message summarises it.

### 5.1 Anti-patterns (must fail review)

```rust
// ❌ user-controlled body content in the message
info!("record submitted: {}", body);

// ❌ unbounded user-controlled label
info!(user_id = %user.id, "request");

// ❌ logging an entire blob
info!(payload = ?body, "received");
```

### 5.2 Correct alternatives

```rust
// ✅ summary in the message, details as attributes
info!(
    body.size = body.len(),
    content.type = body.content_type(),
    "record body received"
);
```

## 6. Severity by call site

| Site                                                | Default level |
|-----------------------------------------------------|--------------|
| HTTP entry / handler return                         | `info`       |
| tx begin / commit / abort                           | `info`       |
| storage put / get / scan / delete                   | `info`       |
| memtable / chunk / version_chain internals          | `debug`      |
| lock manager / GC / flush / recovery / rebuild      | `info`       |
| helpers (encode / decode / serialize)               | `trace`      |

A site that needs a different default level is an exception and should be
called out in the PR.

---

> The forbidden list in §4.2 mirrors
> `xtable-server/tests/metric_cardinality.rs::FORBIDDEN_LABELS` and
> `docs/observability/metric-naming.md` §4.2. If you change one, change
> all three.