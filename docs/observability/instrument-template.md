# `#[instrument]` template — xtable-server

Authoritative reference for the seven rules every `#[tracing::instrument]`
call site in xtable must follow. Mirrors spec
`docs/superpowers/specs/2026-08-30-otel-server-design.md` §10.1.

> Reviewers will flag any violation. New code is rejected until the
> `#[instrument]` attribute complies with all seven rules.

## The 7 rules

### 1. `skip_all` — always

```rust
#[tracing::instrument(level = "info", name = "txn.commit", skip_all, ...)]
```

`skip_all` is mandatory. Without it, every function argument becomes a
span field, which leaks:

- High-cardinality identifiers (record ids, body bytes, S3 keys) into
  every span and every sampling exporter.
- Types that don't implement `Serialize` / `Display` (compile failure).

If a specific argument SHOULD appear in the span, name it explicitly in
`fields(...)` (see rule 4).

### 2. `name = "<stable.dot.delimited>"` — never a Rust fn path

```rust
#[tracing::instrument(name = "txn.commit", ...)]
```

The default span name is the fully-qualified function path
(`xtable_tx::coordinator::commit_txn`). That string contains the type
generic parameters (see rule 7) and drifts when modules are renamed.

Required form:

- Stable across refactors.
- Lowercase, dot-delimited, ≤ 64 chars.
- Matches the corresponding metric name where one exists
  (`txn.commit` ↔ `txn.commit.duration` / `txn.commit.count`).

### 3. `level = "..."` — chosen from the level table

| Site                                                | `level`     |
|-----------------------------------------------------|-------------|
| HTTP handler entry                                  | `"info"`    |
| tx begin / commit / abort                           | `"info"`    |
| storage put / get / scan / delete                   | `"info"`    |
| memtable / chunk / version_chain internals          | `"debug"`   |
| lock manager / GC / flush / recovery / rebuild      | `"info"`    |
| helpers (encode / decode / serialize)               | `"trace"`   |

The level is sampled by the production `EnvFilter` (`debug,xtable=trace`).
Putting `info` on a hot helper floods production; putting `trace` on an
HTTP handler hides it.

### 4. `fields(...)` — every identifier that earns a span attribute

```rust
#[tracing::instrument(
    level = "info",
    name = "txn.commit",
    skip_all,
    fields(
        txn.id = %txn_id,
        space = %space,
        table = %table,
        op = "commit",
    ),
    err,
    ret(Display),
)]
```

Rules:

- Use `%format_arg` for `Display` types (e.g. `String`, `&str`,
  `Ulid`).
- Use `?format_arg` for `Debug`-only types.
- Use a string literal for closed-set values (`op`, `outcome`).
- DO NOT include high-cardinality identifiers (`record_id`, `body_hash`,
  full URLs, full S3 keys) — those belong on a child span at `debug`
  level, not the entry-point span.

### 5. `err` — always

```rust
#[tracing::instrument(..., err)]
```

`err` records the error as a span event (`level = ERROR`,
`message = "error"`) and sets the OTel span status to `Error`. Required
on every fallible function so error rates in Tempo / Jaeger line up with
`txn.commit.count` with `outcome = "err"`.

### 6. `ret(Display)` (when the return type implements `Display`)

```rust
#[tracing::instrument(..., ret(Display))]
```

Records the function's return value as a span field so a sampled trace
shows the outcome without having to grep for the next log line. Use
`ret(Debug)` if the return type doesn't `impl Display`.

> Known limitation: `xtable_tx::CommitOutcome` and `xtable_core::XtableError`
> don't currently `impl Display`. We use `ret(Debug)` for those sites
> until the Display impls land (see plan §7-9 deferred items).

### 7. No generics in the span name

The `name` MUST NOT include type generic parameters. The
`#[tracing::instrument]` proc-macro records the span name verbatim and
`tracing-subscriber` / `tracing-opentelemetry` treat it as a literal
attribute — generic names like `commit::<xtable_tx::Ctx>` create
high-cardinality span names when the same fn is monomorphized.

If you have a generic helper that needs different names per monomorphization, write explicit non-generic wrappers instead.

## Full template

```rust
#[tracing::instrument(
    level = "info",
    name = "txn.commit",
    skip_all,
    fields(
        txn.id = %txn_id,
        space = %space,
        table = %table,
        op = "commit",
    ),
    err,
    ret(Display),
)]
pub async fn commit_txn(
    txn_id: Ulid,
    space: &str,
    table: &str,
) -> XtableResult<CommitOutcome> {
    // ...
}
```

## Review checklist

| Question                                                                  | Required |
|---------------------------------------------------------------------------|----------|
| Is `skip_all` present?                                                    | yes      |
| Is `name = "..."` a stable dot-delimited literal, not the fn path?        | yes      |
| Is the `level` from the table above?                                      | yes      |
| Are all fields needed for debugging explicitly listed (no high-card)?     | yes      |
| Is `err` present on every fallible function?                              | yes      |
| Is `ret(Display)` (or `ret(Debug)` if no Display) on the return value?    | yes      |
| Does the span name avoid type generic parameters?                          | yes      |
| Is the instrumented fn `async`? (sync fns use `info_span!().in_scope()`) | n/a      |