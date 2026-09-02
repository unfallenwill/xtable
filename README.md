# xtable

> A structured data space with **multi-record ACID transactions** on top of any S3-compatible backend.

```
[ client / curl / sdk ]
        │  HTTPS + JWT + /v1/spaces/...
        ▼
┌──────────────────────────────────────────────────────────────┐
│ xtable-server (Rust, single binary)                          │
│  ┌────────────────────┐  ┌─────────────────────────────────┐ │
│  │ /v1/spaces router  │  │ TxnCoordinator (MVCC + SSI)     │ │
│  │ (schemas / tables  │  │ Begin / Stage / Commit / Abort   │ │
│  │  / records / diff  │  │ + Cahill cycle detection         │ │
│  │  / structured txn) │  │ + MemTable publish              │ │
│  └────────┬───────────┘  └──────────────┬──────────────────┘ │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │  background: flush_loop → encode immutable memtable     │ │
│  │  → multipart upload to S3 → TBL_CHUNK_INDEX            │ │
│  └─────────────────────────────────────────────────────────┘ │
│           └────── LocalStore (redb) ──────────────────────────│
│             WAL · versions · txn_state · SI locks · chunks    │
└──────────────────────┬──────────────────────────────────────┘
                       │ S3 credentials
                       ▼
             ┌──────────────────────┐
             │  S3 backend          │
             │  chunks only         │
             │  (AWS S3 / MinIO /   │
             │   Ceph / OSS / COS)  │
             └──────────────────────┘
```

xtable is the **only** piece that needs to be deployed by your users. The
structured-data-space API sits on a transactional core backed by user S3.

---

## Why xtable exists

Standard S3 is **stateless and per-object atomic**. There is no native way to
make a multi-record update visible all-or-nothing, and no way to attach a
schema to an object. xtable adds:

- **Structured records** addressed by `(space, table, record_id)` with
  optional JSON-Schema validation per table.
- **Multi-record ACID transactions** with **Serializable Snapshot Isolation
  (SSI)** — every write runs in a transaction that aborts on conflict or
  backend failure. Built on Cahill's cycle-detection algorithm.
- **Multi-version concurrency control (MVCC)** — the version chain keeps
  every committed version; readers see a consistent snapshot without
  blocking writers.
- **LSM-tree storage backend** — in-memory MemTable + zstd-compressed
  chunks in S3, amortizing request cost.
- **Crash-safe commit ordering** — recovery never produces a
  half-published multi-record state.
- **Disaster recovery** — the chunk index can be cold-rebuilt from
  backend S3 object metadata.

---

## Architecture

### Crate layout (Cargo workspace)

| Crate            | Responsibility                                         |
|------------------|--------------------------------------------------------|
| `xtable-core`    | Pure types: `ObjectKey`, `TxnId`, `Version`, errors, config schemas, transaction status enum. No IO. |
| `xtable-storage` | `redb`-backed local state: WAL, version index, txn_state, SI locks (`TBL_SI_READ` / `TBL_SI_WRITE` / edges / recent window), chunks (`TBL_CHUNK_INDEX`), MemTable, flush pipeline. |
| `xtable-backend` | `aws-sdk-s3` client + multipart upload + `KeyMap` for talking to user-provided S3. |
| `xtable-auth`    | JWT verification for the HTTP API. |
| `xtable-tx`      | `TxnCoordinator` MVCC + SSI state machine: `Begin` / `Stage` / `Commit` (with Cahill cycle check) / `Abort`. Plus `recovery` (WAL replay) / `rebuild` (cold rebuild from S3 metadata) / `gc` (sweep stale txns). Hosts the `SiLockManager` and `cahill` cycle detection. |
| `xtable-schema`  | Structured-data-space layer: schema registration, table binding, record read/write/diff. Threads `StructuredTxn` through every read for SSI ReadSet capture. |
| `xtable-server`  | `xtable` binary: axum HTTP server, `/v1/spaces/...` routes, lifecycle, GC task, background flush loop. |
| `xtable-cli`     | `xtctl` operator CLI: `serve`, `doctor`. |

### Request flow

```
                HTTP request (JWT)
                        │
                        ▼
                axum router
                        │
       ┌────────────────┼──────────────────────┐
       │                │                      │
   /v1/spaces/...   /healthz etc.        (no S3 catch-all)
       │                │
       ▼                ▼
   StructuredSpace   "ok"
       │ (threads StructuredTxn through every read)
       ▼
   TxnCoordinator ── LocalStore (redb) ── BackendClient (aws-sdk-s3)
       │  (commit writes to            (chunk upload path)
       │   MemTable;                   ▲
       │   flush_loop uploads          │
       │   immutable memtable)         │
       └───────── staged body spill ───────────┘
                       │
                       ▼
              Spill files on local disk

[Background] flush_loop:
  active memtable → immutable (size/age threshold)
  → encode chunk (zstd + bloom + key index)
  → multipart upload to S3 → TBL_CHUNK_INDEX
  → WAL MemtableFlushed → WAL truncate
```

---

## The MVCC + SSI protocol — correctness argument

This is the heart of xtable. If you remember only one section, read this one.

### State machine

The state machine is `Active → Committing → {Committed, Aborted}`.
SI locks are acquired during the `Active` phase; the `Committing` phase
runs Cahill cycle detection, S3 uploads, atomic chain append, and
MemTable publish under the SI lock manager's interior mutex:

```
                        BeginTxn
                           │
                           ▼
                    ┌──────────┐   heartbeat (any op)
                    │  Active  │ ◀────────────────┐
                    └────┬─────┘                   │
                         │ CommitTxn              │
                         ▼                        │
                  ┌────────────┐                   │
                  │ Committing │ ──────────┐        │
                  └──────┬─────┘           │        │
                         │               │ cycle  │
                         ▼               │ detected
                    Committed   ◀─────  Aborted
                   (terminal)         (terminal)
```

The Cahill cycle walk detects rw-antidependency structures between the
txn's SI edges and any peer's edges; one txn in the cycle is aborted
(lexicographically larger txn_id loses).

### CommitTxn — exact order (critical for crash safety)

```
1.  Idempotency check:
       if TxnState.status == Committed → return prior CommitOutcome (replay-safe).
       if TxnState.status == Aborted   → 4xx (already aborted).
       if status == Committing        → conservative abort
         (a previous crashed instance left a half-state).

2.  Cahill cycle detection (xtable-tx/src/cahill.rs):
       for txn T and any peer P:
         if T has both an in-edge from P AND an out-edge to P:
           → "dangerous structure" → abort T (tie-break: lexicographically
             larger txn_id loses; ULIDs are monotonic).

3.  CAS status Active → Committing (single redb write txn).

4.  Allocate commit_version = next global_version (atomic increment).

5.  Upload all bodies to a per-txn staging path in S3, NOT to the
    final key paths. This is the V3 fix: if any upload fails, we can
    abort cleanly by deleting staging copies without ever having
    overwritten the live (T0) data. On full success we promote each
    staging object to its final key.

6.  Bulk-append version records (single redb write txn).
    THIS IS THE ATOMICITY POINT — only after every backend upload
    has ack'd does redb's version index advance.

7.  WAL `Committing` → `Committed` → `CommitResult` (single redb write txn).
    Mark TxnState.status = Committed.

8.  MemTable publish (PR #1+): each commit also writes the new
    entries into the in-memory MemTable (invisible → visible at
    commit_version). A background flush task uploads the
    immutable MemTable as a chunk to S3.

9.  SI lock manager mark_committed: keep the txn's locks in the
    rolling window so future commits can still detect cycles.

10. Release snapshot pin (so GC can prune old versions).

11. Schedule staged-body GC (best-effort).

12. Return 200 OK + x-xtable-commit-version header.
```

### Why this ordering is crash-safe

Three crash points exist in the protocol:

**(a) Crash before step 5.**
   No backend write happened. WAL has only `Begin`/`Stage` records.
   On replay, recovery finds no `Committing`/`Committed`, marks txn
   `Aborted`. No partial state anywhere.

**(b) Crash during step 5 (partial uploads).**
   Some keys landed on the backend. `Committing` record may exist.
   On replay, recovery iterates WAL, sees `Committing` for this txn
   without a `Committed`, issues `DeleteObject` for each recorded
   uploaded key (compensating action), marks `Aborted`. **Crucially,
   the version index in redb has NOT been bumped yet**, so any reader
   via xtable never saw the partial state.

**(c) Crash after step 6 (post-publish).**
   Versions were published; on-disk S3 is consistent with redb.
   On replay, recovery sees `Committed` / `CommitResult` and does nothing.
   Idempotent: a retry of the original client gets the same outcome.

**The atomicity invariant** is therefore:

> At every observer of state (xtable, or a fresh start), either all of a
> committed transaction's writes are visible or none of them are.

This invariant holds because (1) the version index is the gate — readers
consult `chain[k].latest_commit_version`; (2) the version index is mutated
**only after** every backend upload has ack'd; (3) crashes before that
mutation leave the index unchanged; (4) crashes after that mutation have
already produced a consistent state.

### MVCC + SSI semantics

- **Snapshot isolation (SI)** for reads: each txn reads at
  `snapshot_version = global_version` taken at BeginTxn. Reads never see
  writes from txns that committed after BeginTxn.
- **Lost-update protection** at commit: the SI lock manager aborts the
  second writer when both txns started from the same snapshot and try
  to commit overlapping writes.
- **Serializable Snapshot Isolation (SSI)** prevents write skew via
  Cahill's cycle detection: if a txn T has both an in-edge and an
  out-edge to the same peer P, the rw-antidependency cycle is detected
  and one of the two txns is aborted (lexicographically larger txn_id
  loses; ULIDs are monotonic).
- **Read-your-own-writes** within a txn: `StructuredTxn`'s staged
  write set is consulted before falling through to the chain / chunk
  read path.

### Idempotent commit

WAL `CommitResult` records are the source of truth for retries. A
re-executed `CommitTxn` after `CommitResult` returns the original outcome.
A re-executed `AbortTxn` after `Aborted` returns success without side
effects.

---

## Disaster recovery

### S3 backend is the source of truth.

Every object written by xtable carries:
- `x-amz-meta-xtable-version`: the logical version it represents
- `x-amz-meta-xtable-txn-id`: the originating txn id (for orphan detection)

`redb` is treated as a **cache / index**, not the authoritative store.

### Three failure modes

| Failure                                     | What xtable does                                                                  |
|---------------------------------------------|------------------------------------------------------------------------------------|
| Process crash                                | WAL replay (see crash recovery below). No data lost unless mid-Committing window. |
| `redb` corrupted (bit flip, FS error)       | `xtable-server` detects open failure on startup and triggers **cold rebuild** (see below). |
| `redb` destroyed (disk loss, accidental rm) | Same as above — full cold rebuild from S3 metadata. **No data lost**.            |

### Cold rebuild (cold_rebuild.rs)

On startup, if `redb` cannot be opened (or is empty):
1. List all objects in the configured S3 bucket.
2. For each object, `HeadObject` to read `x-amz-meta-xtable-version` and
   `x-amz-meta-xtable-txn-id`.
3. Group by key, take `max(version)` as `latest_version`.
4. Set `global_version` to the max across all keys.
5. Identify orphans: objects whose `txn_id` did not produce a committed
   TxnState record → delete them.
6. Rebuild the `versions` table.
7. Append a `WalRecord::Aborted` marker for observability.

### Crash recovery (recovery.rs)

On startup, replay the WAL once. For each txn with a non-terminal status:
- Last record is `Begin` / `Stage` (no `Committing`) → no backend
  uploads happened. Mark `Aborted`. Drop staged bodies.
- Last record is `Committing` (no `Committed`) → may have partial
  uploads. Read `Committing.upload_keys`, compensating-delete each via
  `DeleteObject`. Mark `Aborted`.
- Last record is `Committed` / `CommitResult` → already terminal. Skip.

GC runs periodically (configurable, default 60s) and aborts active txns
whose `last_heartbeat + timeout` has elapsed.

---

## What we *cannot* guarantee

A single, narrow window of unavoidable data loss exists:

> A transaction has reached the `Committing` state with all backend
> uploads ack'd, but the `versions` index in `redb` has not yet been
> published — the local medium is destroyed in this exact window.

The `versions` bump is a single redb write transaction that takes
microseconds. The window is small, but non-zero. Mitigations:

1. **Batch commits with one big redb write txn** — already implemented.
2. **Snapshot redb to S3 periodically** (planned v2): every N seconds
   upload `redb` snapshot to `s3://<bucket>/_xtable_state/wal-snap-<ts>.bin`.
   On rebuild, prefer the snapshot over cold-rebuild.
3. **A second commit phase that publishes versions only after S3
   success** — already implemented in step 8.

For users requiring zero-loss guarantees across this window, deploy
`xtable-server` against a durable local disk (NVMe with battery-backed
write cache, or equivalent) and run periodic fsync of the redb WAL file.

---

## API surface

All routes are mounted under `/v1` and require JWT authentication. Health
probes (`/healthz`, `/readyz`) are public.

### Schemas

```
POST   /v1/spaces/:space/schemas              Register schema. Body: {name, body}. → 201 {version, name}
GET    /v1/spaces/:space/schemas              List schemas.        → 200 {space, schemas:[{name, version}]}
GET    /v1/spaces/:space/schemas/:name        Fetch schema.         ?version=&snapshot=  → 200 {space,name,version,body}
```

### Tables

```
POST   /v1/spaces/:space/tables/:table/bind   Bind table to schema. Body: {body}. → 204
```

### Records

```
POST   /v1/spaces/:space/tables/:table/records         Upsert. Body: {record_id?, body}.
                                                     → 201 {record_id, schema_version, backend_key, commit_version}
GET    /v1/spaces/:space/tables/:table/records         Query.   ?snapshot=&limit=&offset=&sort=&dir=
                                                     &filter_field=&filter_op=&filter_value=
                                                     → 200 {snapshot_version, total_matched, records:[…]}
GET    /v1/spaces/:space/tables/:table/records/:rid    Fetch.   ?snapshot=  → 200 {space,table,record_id,body,schema_version,commit_version,deleted}
DELETE /v1/spaces/:space/tables/:table/records/:rid    Delete.  → 200 {deleted, commit_version}
```

### Snapshot diff and explicit transactions

```
GET    /v1/spaces/:space/tables/:table/diff   Diff two snapshots. ?s1=&s2=  → 200 {from, to, changes:[…]}
POST   /v1/structured/txn                     Begin an explicit cross-request txn. → 201 {txn_id, snapshot_version}
GET    /v1/spaces/:space/snapshot             Current snapshot version for the space.
```

`filter_op` supports `eq`, `ne`, `gt`, `ge`, `lt`, `le`, `contains`, `exists`.
Errors are returned as JSON `{"error": "<msg>", "code": "<s3-style>"}`.

---

## Build / Run

```bash
# Build
cargo build --workspace

# Test (unit + proptest + e2e)
cargo test --workspace

# Run server
./target/debug/xtable --config ./xtable.toml

# Run a connectivity check
./target/debug/xtctl doctor --xtable-endpoint http://localhost:9000

# Talk to the structured-data-space API
curl -X POST http://localhost:9000/v1/spaces/acme/schemas \
  -H 'authorization: Bearer <jwt>' \
  -H 'content-type: application/json' \
  -d '{"name":"task","body":{"type":"object","required":["title","status"],"properties":{"title":{"type":"string"},"status":{"enum":["open","done"]}}}}'

curl -X POST http://localhost:9000/v1/spaces/acme/tables/tasks/records \
  -H 'authorization: Bearer <jwt>' \
  -H 'content-type: application/json' \
  -d '{"record_id":"t1","body":{"title":"alpha","status":"open"}}'
```

## Iteration check

Run the whole workspace validation in one shot after each iteration — agent
loop, local edit, or pre-push. The script is non-interactive, prints one
section per step, and exits non-zero on the first failed step.

```bash
./scripts/ci.sh
```

It runs, in order:

| step    | command                                                |
|---------|--------------------------------------------------------|
| `fmt`     | `cargo fmt --all -- --check`                         |
| `build`   | `cargo build --workspace --all-targets --all-features` |
| `clippy`  | `cargo clippy --workspace --all-targets --all-features -- -D warnings` |
| `test`    | `cargo test --workspace --all-features`              |
| `smoke`   | `cargo test -p xtable-server --test structured_http` (unignored) |

Each step is gated independently so a single failure surfaces immediately
with its exit code and elapsed seconds. Useful flags:

```bash
./scripts/ci.sh --skip fmt              # skip one step
./scripts/ci.sh --skip=clippy --skip=test
./scripts/ci.sh --include-ignored       # also run #[ignore]'d structured_http tests
                                        # (these are gated on Task 4 and may fail)
./scripts/ci.sh --help                  # full help
```

Exit codes: `0` all passed, `1` a step failed, `77` a step was skipped.

If `cargo` is not on `PATH`, the script sources `~/.cargo/env` automatically
(handy right after a `rustup` install with `--no-modify-path`).

---

## Observability

xtable-server emits OpenTelemetry traces, metrics, and structured JSON
logs when `OTEL_EXPORTER_OTLP_ENDPOINT` is set. Telemetry is **off by
default** — leaving the env var unset keeps the server silent on the wire
and writes JSON logs to stdout only.

Key env vars:

- `OTEL_EXPORTER_OTLP_ENDPOINT` — OTLP gRPC endpoint; required to enable export.
- `OTEL_EXPORTER_OTLP_PROTOCOL` — `grpc` (default) or `http/protobuf`.
- `OTEL_PROFILE` — `dev` / `staging` / `production` (default) / `debug`.
- `OTEL_TRACES_SAMPLER_ARG` — head-based trace sample ratio (0.0–1.0).
- `OTEL_SERVICE_NAME`, `OTEL_SERVICE_INSTANCE_ID` — resource attributes.

See [`docs/observability/metric-naming.md`](docs/observability/metric-naming.md),
[`docs/observability/log-conventions.md`](docs/observability/log-conventions.md),
[`docs/observability/instrument-template.md`](docs/observability/instrument-template.md),
and [`docs/observability/collector-tail-sampling.example.yaml`](docs/observability/collector-tail-sampling.example.yaml).

---

## Test evidence

### Unit tests

```
$ cargo test --workspace
test result: ok. 10 passed; 0 failed    # xtable-auth
test result: ok. 1 passed; 0 failed     # xtable-backend
test result: ok. 8 passed; 0 failed     # xtable-backend integration_e2e
test result: ok. 4 passed; 0 failed     # xtable-core
test result: ok. 13 passed; 0 failed    # xtable-storage
test result: ok. 10 passed; 0 failed    # xtable-tx (unit)
test result: ok. 10 passed; 0 failed    # xtable-tx proptest_invariants
test result: ok. 30 passed; 0 failed    # xtable-schema
test result: ok. 64 passed; 0 failed    # xtable-schema (unit incl. validation)
test result: ok. 1 passed; 0 failed     # xtable-server structured_http smoke
test result: ok. 21 passed; 0 failed    # xtable-tx regression_vulns
```

**Total: 200+ tests passing across the workspace.**

### Property-based tests (`xtable-tx/tests/proptest_invariants.rs`)

10 invariants, generated inputs (proptest default 256 cases per test):
- `prop_committed_txn_writes_are_atomic` — committed txn makes all its writes visible at one commit_version
- `prop_global_version_monotonic` — versions never go backwards
- `prop_versions_persist_across_reopen` — durability across restart
- `prop_wal_seq_monotonic` — WAL sequence is strictly increasing
- `inv_aborted_txn_leaves_no_state` — explicit
- `inv_commit_no_writes_is_idempotent` — replay safety
- `inv_ssi_snapshot_at_begin_txn` — txn snapshot equals global_version at begin
- `inv_read_your_own_writes_within_txn` — txn isolation property
- `inv_commit_replay_returns_same_outcome` — idempotent commit
- `inv_gc_sweeps_stale_txn_but_keeps_recent` — GC correctness
- `inv_unknown_txn_returns_not_found` — error mapping

SSI-specific tests (`xtable-tx/tests/ssi_invariants.rs`, planned):
- `prop_ssi_disjoint_read_write_does_not_abort` — non-overlapping read/write txns commit cleanly
- `prop_ssi_catches_write_skew` — the canonical write-skew scenario aborts one txn
- `prop_ssi_own_write_does_not_abort` — txn reading + writing the same key commits

### End-to-end tests (`xtable-backend/tests/integration_e2e.rs`)

A real `aws-sdk-s3` client is used against an in-process axum mock S3
that records every operation. 8 scenarios:

| Test                                     | Verifies                                                     |
|------------------------------------------|--------------------------------------------------------------|
| `e2e_put_get_object_atomicity`           | Round-trip single object via aws-sdk-s3                       |
| `e2e_list_objects_after_writes`          | Multiple writes, ListObjectsV2 enumerates them               |
| `e2e_delete_object`                       | DELETE removes from mock state                               |
| `e2e_coordinator_commit_writes_object_to_backend` | Coordinator → BackendClient → real PutObject          |
| `e2e_atomic_multi_object_all_or_nothing` | 3-key txn: all 3 visible after commit, status = Committed    |
| `e2e_aborted_txn_leaves_no_keys`         | 2-key txn aborted: neither key in backend                    |
| `e2e_idempotent_commit_returns_same_outcome` | Same `CommitTxn` invoked twice returns same `commit_version` |
| `e2e_ssi_write_write_one_winner`          | Two concurrent txns on same key — SI lock manager keeps one, aborts the other |

### Coverage threshold

```bash
cargo install cargo-llvm-cov
cargo llvm-cov --workspace --all-features --fail-under-lines 90
```

Per-crate coverage target: ≥ 90%. Critical paths (commit, version bump,
crash recovery, Cahill cycle detection): 100%.

---

## Performance characteristics

(Indicative on commodity NVMe, single-node)

| Operation                                | Latency target   | Throughput       |
|------------------------------------------|------------------|-------------------|
| BeginTxn                                 | < 1 ms           | 50k+ txns/s       |
| Stage (write in txn)                     | < 1 ms (local)   | 50k+ stages/s     |
| CommitTxn (10 keys, < 1 MiB each)       | ~30 ms + S3      | ~1k commits/s     |
| Read at snapshot (warm)                  | ~600 µs          | ~1.6k reads/s    |
| Read at snapshot (cold)                  | ~21 ms (S3 GET)  | ~50 reads/s      |
| MemTable flush (64 MiB → chunk)         | n/a              | ~1 chunk/60s     |
| Recovery (cold rebuild from 100k chunks) | ~5 s             | n/a (one-shot)    |

Commit critical section (`commit_lock` removed in PR #3) is bounded by
the SI lock manager's interior mutex. Cahill cycle detection adds
O(active_txns) work per commit — sub-millisecond for typical OLTP
loads. Disjoint-key commits are fully parallel.

The MemTable writes amortize S3 request cost: a 64 MiB memtable flush
amortizes ~1M record writes into a single multipart chunk upload,
reducing S3 request count by ~1000×.

---

## Security notes (v1 scope)

- **Auth**: HS256 JWT for the HTTP API; the signing secret is stored in config / env.
  Multi-tenant auth is planned for v2.
- **Transport**: HTTPS only in production (xtable binds to TCP; TLS is
  the operator's responsibility via reverse proxy or rustls integration
  in v2).
- **Audit**: WAL records all txn state transitions; ready for streaming
  to an external log in v2.
- **Encryption at rest**: We rely on the backend S3's server-side
  encryption. SSE-KMS support is planned for v2.

---

## Roadmap

| Version | Focus                                              |
|---------|-----------------------------------------------------|
| **v1** (this) | Single-tenant, single-bucket, S3-compatible protocol + multi-object MVCC + SSI transactions + Cahill cycle detection + MemTable/chunk LSM backend + crash recovery + cold rebuild. |
| v2      | Multi-tenant (per-tenant credentials, prefixes), SSE-KMS, Range reads, snapshot-to-S3 backup, chunk-level GC, multi-level compaction. |
| v3      | Cross-bucket transactions, replication, read replicas. |

---

## License

Apache-2.0. See `LICENSE`.
