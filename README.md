# xtable

> A single-node structured data service that adds schemas, snapshots, and
> multi-record transactions to S3-compatible storage.

xtable is a Rust service for applications that need database-like semantics
over object storage. Clients write JSON records through an HTTP API; xtable
keeps transaction and version metadata locally and stores immutable data
chunks in S3-compatible storage.

## Start here

### Why it exists

S3 gives you durable objects, but it does not provide a transaction boundary
across several objects, a table schema, or a consistent application snapshot.
xtable provides those semantics at the service layer:

- records addressed by `(space, table, record_id)`;
- optional JSON-Schema validation per table;
- multi-record transactions with MVCC and Serializable Snapshot Isolation;
- snapshot reads, queries, and snapshot diff;
- local WAL/redb state with recovery and cold-rebuild support;
- batched, zstd-compressed chunks uploaded to S3.

The current design is intentionally single-node and single-bucket. It is a
transactional structured-data layer, not a general-purpose SQL database or a
distributed database.

### Current status

xtable is an early v0.1 implementation. The core transaction, MVCC, chunk,
recovery, schema, and HTTP flows are implemented and tested. Production
hardening is still needed around multi-node deployment, local-state backup,
compaction, and operational guarantees.

## Quick start

### Requirements

- Rust stable and Cargo
- An S3-compatible endpoint, such as MinIO, AWS S3, Ceph, OSS, or COS
- A writable local directory for redb, WAL, and staged bodies

Build and test the workspace:

```bash
cargo build --workspace --all-features
cargo test --workspace --all-features
```

Create a local configuration from `xtable.local.toml` or provide equivalent
environment variables. The important settings are:

```toml
[server]
listen = "127.0.0.1:9000"
public_endpoint = "http://127.0.0.1:9000"
data_dir = "/tmp/xtable"

[auth]
jwt_secret = "replace-me"
allow_anonymous_read = false

[backend]
endpoint = "http://127.0.0.1:9001"
region = "us-east-1"
bucket = "xtable-data"
access_key_id = "minioadmin"
secret_access_key = "minioadmin"
force_path_style = true

[storage]
redb_dir = "/tmp/xtable/redb"
staged_body_spill_dir = "/tmp/xtable/staged"
```

Run the server:

```bash
cargo run -p xtable-server -- --config xtable.local.toml
```

Health endpoints are public:

```bash
curl http://127.0.0.1:9000/healthz
curl http://127.0.0.1:9000/readyz
```

All data endpoints require an HS256 Bearer JWT unless anonymous reads are
enabled. In production, terminate HTTPS at a reverse proxy or configure TLS
at the deployment edge.

## A first structured-data flow

The API is mounted under `/v1`. The following example registers a schema,
binds a table, writes a record, and reads it back. Replace `$TOKEN` with a
JWT signed by the configured secret.

```bash
curl -X POST http://127.0.0.1:9000/v1/spaces/acme/schemas \
  -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d '{"name":"task","body":{"type":"object","required":["title"],"properties":{"title":{"type":"string"}}}}'

curl -X POST http://127.0.0.1:9000/v1/spaces/acme/tables/tasks/bind \
  -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d '{"body":{"type":"object","required":["title"],"properties":{"title":{"type":"string"}}}}'

curl -X POST http://127.0.0.1:9000/v1/spaces/acme/tables/tasks/records \
  -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d '{"record_id":"t1","body":{"title":"alpha"}}'

curl http://127.0.0.1:9000/v1/spaces/acme/tables/tasks/records/t1 \
  -H "authorization: Bearer $TOKEN"
```

For several writes that must become visible together, use an explicit
transaction:

```bash
# 1. Begin once and retain txn_id.
curl -X POST http://127.0.0.1:9000/v1/structured/txn \
  -H "authorization: Bearer $TOKEN"

# 2. Stage writes, schema registration, or table binding using that txn_id.
curl -X POST http://127.0.0.1:9000/v1/structured/txn/$TXN_ID/write \
  -H "authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d '{"space":"acme","table":"tasks","record_id":"t2","body":{"title":"beta"}}'

# 3. Commit or abort explicitly.
curl -X POST http://127.0.0.1:9000/v1/structured/txn/$TXN_ID/commit \
  -H "authorization: Bearer $TOKEN"
```

The transaction snapshot is captured at `begin`. Writes are read-your-own-
writes inside that transaction and become visible to other readers only after
commit. A transaction can include records from different schemas and tables;
they receive one shared commit version.

## API reference

All routes are mounted under `/v1`. Health probes are public; data routes
require JWT authentication.

### Schemas and tables

| Method | Route | Purpose |
|---|---|---|
| `POST` | `/v1/spaces/:space/schemas` | Register a schema; returns its version. |
| `GET` | `/v1/spaces/:space/schemas` | List schemas visible in the current state. |
| `GET` | `/v1/spaces/:space/schemas/:name` | Read a schema, optionally at `version` or `snapshot`. |
| `POST` | `/v1/spaces/:space/tables/:table/bind` | Bind a table to a schema body. |

### Records and snapshots

| Method | Route | Purpose |
|---|---|---|
| `POST` | `/v1/spaces/:space/tables/:table/records` | Upsert one record; `record_id` may be auto-generated. |
| `POST` | `/v1/spaces/:space/tables/:table/records/batch` | Atomically upsert several records. |
| `GET` | `/v1/spaces/:space/tables/:table/records` | Query with pagination, sort, filters, and `snapshot`. |
| `GET` | `/v1/spaces/:space/tables/:table/records/:rid` | Read one record, optionally at `snapshot`. |
| `DELETE` | `/v1/spaces/:space/tables/:table/records/:rid` | Write a tombstone for one record. |
| `GET` | `/v1/spaces/:space/tables/:table/diff` | Compare snapshots with `s1` and `s2`. |
| `GET` | `/v1/spaces/:space/snapshot` | Return the current space snapshot version. |

Supported query operators are `eq`, `ne`, `gt`, `ge`, `lt`, `le`, `contains`,
and `exists`. Errors use the shape:

```json
{"error":"description","code":"TransactionConflict"}
```

### Explicit transactions

| Method | Route | Purpose |
|---|---|---|
| `POST` | `/v1/structured/txn` | Begin; returns `txn_id` and `snapshot_version`. |
| `POST` | `/v1/structured/txn/:txn_id/write` | Stage a record upsert. |
| `POST` | `/v1/structured/txn/:txn_id/schema` | Register a schema in the transaction. |
| `POST` | `/v1/structured/txn/:txn_id/bind` | Bind a table schema in the transaction. |
| `POST` | `/v1/structured/txn/:txn_id/commit` | Publish all staged changes at one commit version. |
| `POST` | `/v1/structured/txn/:txn_id/abort` | Discard all staged changes. |

The HTTP transaction is intentionally explicit: `begin`, one or more
`write`/`schema`/`bind` calls, then `commit` or `abort`. The client owns the
transaction ID and should abort abandoned transactions or rely on the
configured transaction timeout and GC.

## Consistency and durability model

### MVCC and SSI

Each transaction captures a snapshot version at begin. Reads use that fixed
snapshot, while a staged write set provides read-your-own-writes. At commit,
xtable checks write conflicts and SSI read/write dependencies; dangerous
structures are detected with Cahill cycle detection. A conflicting
transaction is aborted rather than partially committed.

The logical state machine is:

```text
Active -> Committing -> Committed
                    \\-> Aborted
```

Commit is idempotent: a retry after a durable commit returns the original
commit outcome. Abort is also safe to repeat.

### Local state and S3 data

The local `redb` store contains the WAL, transaction state, version chains,
schema metadata, SI lock metadata, and chunk index. Committed records first
publish to an in-memory MemTable. A background flush encodes immutable
MemTables as zstd chunks and uploads them to S3; reads can use the MemTable or
the chunk index.

The WAL is replayed on restart. A transaction left before commit is aborted;
a transaction with a `Committing` record is recovered according to the
recorded state. The chunk index can be rebuilt by scanning S3 metadata, but
this is a recovery path, not a substitute for backing up local state during
normal operation.

### Important boundary

xtable coordinates one logical transaction across its configured local store
and S3 bucket. It does not currently provide distributed transactions across
multiple xtable nodes, buckets, or independent databases. The local redb/WAL
directory must therefore live on durable storage and be included in the
operator's backup and recovery plan.

## Architecture

```text
HTTP client + JWT
       |
       v
xtable-server / axum
       |
       +--> xtable-schema: schemas, tables, records, queries, diff
       |
       +--> xtable-tx: MVCC, SSI, transaction lifecycle, recovery, GC
       |          |
       |          +--> xtable-storage: redb, WAL, MemTable, chunks, index
       |          +--> xtable-backend: aws-sdk-s3 / S3-compatible endpoint
       |
       +--> xtable-auth and xtable-telemetry
```

| Crate | Responsibility |
|---|---|
| `xtable-core` | Pure IDs, errors, headers, and configuration types. |
| `xtable-auth` | HS256 JWT verification and read/write authorization. |
| `xtable-backend` | S3 client, object operations, multipart upload, and key mapping. |
| `xtable-storage` | redb state, WAL, MemTables, chunk encoding, indexes, and locks. |
| `xtable-tx` | Transaction coordinator, MVCC, SSI, recovery, rebuild, and GC. |
| `xtable-schema` | Structured data-space semantics and JSON-Schema validation. |
| `xtable-telemetry` | OpenTelemetry metrics, traces, resource attributes, and timing helpers. |
| `xtable-server` | The `xtable` HTTP server and lifecycle wiring. |
| `xtable-cli` | The `xtctl` operator CLI (`serve`, `doctor`). |

## Development and verification

Run the complete local pipeline:

```bash
./scripts/ci.sh
```

It checks formatting, builds all targets, runs Clippy with warnings denied,
runs workspace tests, and executes the structured HTTP smoke tests. To run
the coverage gate:

```bash
cargo install cargo-llvm-cov
./scripts/coverage.sh
```

The coverage gate is 90% line coverage for production code that is
unit-testable. Process entrypoints, external SDK/HTTP adapters, and test-only
support are excluded from this unit-test gate and exercised by integration or
smoke tests.

The test suite includes unit tests, property-based MVCC/SSI invariants,
storage recovery tests, S3-client end-to-end tests against a mock server, and
HTTP tests for schema, record, snapshot, and explicit transaction flows.

## Observability

Telemetry is opt-in. Set `OTEL_EXPORTER_OTLP_ENDPOINT` to enable OTLP export;
without it, the server does not export telemetry.

Useful environment variables:

| Variable | Values / default |
|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Required to enable export. |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `grpc` (default) or `http/protobuf`. |
| `OTEL_PROFILE` | `dev`, `staging`, `production` (default), or `debug`. |
| `OTEL_TRACES_SAMPLER_ARG` | Optional trace ratio from `0.0` to `1.0`. |
| `OTEL_SERVICE_NAME` | Service name; defaults to `xtable`. |
| `OTEL_SERVICE_INSTANCE_ID` | Optional instance identifier. |

See the [metric naming](docs/observability/metric-naming.md), [log
conventions](docs/observability/log-conventions.md), and [collector
example](docs/observability/collector-tail-sampling.example.yaml).

## Roadmap

- Multi-tenant isolation and per-tenant credentials
- Durable local-state snapshots to S3
- Chunk GC and multi-level compaction
- Range reads and larger-object support
- Replication and read replicas
- Cross-bucket or multi-node transaction coordination

## Security notes

- Use a strong JWT secret and rotate it through deployment configuration.
- Put the HTTP server behind HTTPS in production.
- Restrict S3 credentials to the configured bucket and required object
  operations.
- Enable the backend's server-side encryption where required.
- Treat the local redb/WAL directory as durable application state.

## License

Apache-2.0. See [LICENSE](LICENSE).
