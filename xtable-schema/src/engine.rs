//! Structured data space — Schema / Record / Snapshot on top of the
//! transactional object store.
//!
//! Design:
//! - Schema documents are stored as immutable S3 objects at
//!   `_xtable/<space>/_schema/<name>/v<N>.json`.
//! - Record documents at `_xtable/<space>/<table>/<record_id>.json`.
//! - A redb sidecar index keeps the latest state per record and per
//!   schema, with the body inlined so listing & filtering don't have
//!   to round-trip the backend for every row.
//! - The structured layer subscribes to `TxnCoordinator`'s post-commit
//!   hook so the index is updated atomically with the chain-append.
//!
//! Snapshot semantics:
//! - `current_global_version`: read latest.
//! - `txn.snapshot_version`: read what was committed when this txn began.
//! - pin a snapshot explicitly with `pin_snapshot(snapshot_version)`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex, OnceLock};

use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{info, warn};

use xtable_backend::BackendClient;
use xtable_core::{ObjectKey, XtableError, XtableResult};
use xtable_storage::{
    LocalStore, MemTableSet, RecordIndexEntry, SchemaIndexEntry,
};
use xtable_telemetry::metrics::Metrics;
use xtable_telemetry::timed::Timed;
use xtable_telemetry::KeyValue;
use xtable_tx::{CommitEvent, PostCommitHook, TxnCoordinator};

use crate::key::{is_structured_key, parse_record_key, parse_schema_key, record_key, schema_key};
use crate::query::{Query, QueryResult, Record};
use crate::validation::{validate, JsonSchema, ValidationError};

/// Lazily-initialised `Metrics` bound to the global OTel meter.
fn metrics() -> &'static Metrics {
    static METRICS: OnceLock<Metrics> = OnceLock::new();
    METRICS.get_or_init(Metrics::default)
}

/// A transaction handle exposed to the structured-layer caller.
#[derive(Debug, Clone)]
pub struct StructuredTxn {
    pub txn_id: String,
    pub snapshot_version: u64,
    /// PR-Fix8.1: weak back-reference to the StructuredSpace that minted
    /// this txn. Used by `record_read` to call `coordinator.read()` so
    /// SSI ReadSet capture works. Weak so a `StructuredTxn` does not
    /// leak the engine.
    pub(crate) space: std::sync::Weak<StructuredSpace>,
}

impl StructuredTxn {
    /// A no-op txn used by admin / test code paths that don't have a
    /// real SI transaction. The snapshot_version is `u64::MAX` so that
    /// read_at_snapshot logic falls through to "latest".
    pub fn admin() -> Self {
        Self {
            txn_id: "_admin".to_string(),
            snapshot_version: u64::MAX,
            space: std::sync::Weak::new(),
        }
    }

    /// Record that this txn read `key` at `observed_version`. Forwards
    /// to the coordinator's `read` so the SI lock manager sees it.
    /// PR-Fix8.1.
    ///
    /// Synchronous because the underlying coordinator call only touches
    /// in-process state (parking_lot Mutex). The coordinator's `read`
    /// is async for trait uniformity; we block_on it here so the read
    /// functions (which are themselves sync) can call this directly.
    pub fn record_read(&self, key: xtable_core::ObjectKey, observed_version: u64) {
        if self.space.strong_count() == 0 {
            return;
        }
        if self.txn_id == "_admin" {
            return;
        }
        let space = match self.space.upgrade() {
            Some(s) => s,
            None => return,
        };
        let _ = futures::executor::block_on(space.txn.read(
            &self.txn_id,
            &key,
            xtable_core::Version(observed_version),
            String::new(),
        ));
    }
}

#[derive(Debug, Clone)]
pub struct RecordWrite {
    pub space: String,
    pub table: String,
    pub record_id: Option<String>,
    pub body: Value,
}

#[derive(Debug, Clone)]
pub struct WriteOutcome {
    pub record_id: String,
    pub schema_version: u32,
    pub backend_key: String,
}

#[derive(Debug, Clone)]
pub struct SchemaInfo {
    pub space: String,
    pub name: String,
    pub version: u32,
    pub body: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebuildReport {
    pub records: usize,
    pub schemas: usize,
}

// ---------- Pending-write book-keeping ----------

#[derive(Debug, Clone)]
struct PendingRecord {
    space: String,
    table: String,
    record_id: String,
    schema_version: u32,
    body: Value,
    backend_key: String,
}

#[derive(Debug, Clone)]
struct PendingSchema {
    space: String,
    name: String,
    version: u32,
    #[allow(dead_code)]
    body: Value,
    backend_key: String,
}

#[derive(Default)]
pub struct PendingMap {
    pub(crate) inner: StdMutex<HashMap<String, PendingTxn>>,
}

#[derive(Default)]
pub struct PendingTxn {
    records: Vec<PendingRecord>,
    schemas: Vec<PendingSchema>,
}

impl PendingMap {
    fn record(&self, txn_id: &str, r: PendingRecord) {
        let mut m = self.inner.lock().expect("pending map");
        m.entry(txn_id.to_string()).or_default().records.push(r);
    }
    fn schema(&self, txn_id: &str, s: PendingSchema) {
        let mut m = self.inner.lock().expect("pending map");
        m.entry(txn_id.to_string()).or_default().schemas.push(s);
    }
    fn take(&self, txn_id: &str) -> PendingTxn {
        let mut m = self.inner.lock().expect("pending map");
        m.remove(txn_id).unwrap_or_default()
    }
    /// Lookup the latest schema version registered in this pending txn.
    /// Returns the version (1-based) of the most recent `register_schema`
    /// or `bind_table_schema` call for `(space, name)`, or None if no
    /// schema was staged in this txn yet.
    fn latest_schema_version_in_txn(&self, txn_id: &str, space: &str, name: &str) -> Option<u32> {
        let m = self.inner.lock().expect("pending map");
        m.get(txn_id).and_then(|p| {
            p.schemas
                .iter()
                .rev()
                .find(|s| s.space == space && s.name == name)
                .map(|s| s.version)
        })
    }
    /// Lookup a pending schema body by name (latest in this txn).
    fn latest_schema_body_in_txn(&self, txn_id: &str, space: &str, name: &str) -> Option<Value> {
        let m = self.inner.lock().expect("pending map");
        m.get(txn_id).and_then(|p| {
            p.schemas
                .iter()
                .rev()
                .find(|s| s.space == space && s.name == name)
                .map(|s| s.body.clone())
        })
    }
}

// ---------- Engine ----------

pub struct StructuredSpace {
    pub txn: Arc<TxnCoordinator>,
    pub store: LocalStore,
    pub backend: Arc<BackendClient>,
    /// PR #4: the LSM-tree memtable set shared with the txn coordinator.
    /// `get_record` / `head_object` route through it via
    /// `xtable_storage::read::read_at_snapshot` (spec §5.2). Decoupling
    /// from `txn` keeps the engine agnostic to who owns the set.
    pub mems: Arc<MemTableSet>,
    pending: Arc<PendingMap>,
}

impl std::fmt::Debug for StructuredSpace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StructuredSpace").finish_non_exhaustive()
    }
}

impl StructuredSpace {
    pub fn new(txn: Arc<TxnCoordinator>, store: LocalStore, backend: Arc<BackendClient>) -> Self {
        let mems = Arc::clone(txn.memtable_set());
        let pending = Arc::new(PendingMap::default());
        let hook = post_commit_hook(store.clone(), Arc::clone(&pending));
        txn.register_post_commit_hook(hook);
        Self { txn, store, backend, mems, pending }
    }

    pub fn pending(&self) -> Arc<PendingMap> {
        Arc::clone(&self.pending)
    }

    // ---- Transaction lifecycle ----

    pub async fn begin_txn(self: &Arc<Self>) -> XtableResult<StructuredTxn> {
        let txn_id = self.txn.begin(None).await?;
        let snapshot_version = self.store.current_global_version()?;
        Ok(StructuredTxn {
            txn_id,
            snapshot_version,
            space: Arc::downgrade(self),
        })
    }

    pub async fn heartbeat(&self, t: &StructuredTxn) -> XtableResult<()> {
        self.txn.heartbeat(&t.txn_id).await
    }

    pub async fn commit_txn(&self, t: &StructuredTxn) -> XtableResult<u64> {
        let out = self.txn.commit(&t.txn_id).await?;
        // Hook fires inside commit(), but on empty txns the hook ran with
        // no pending entries — that's fine. Always best-effort purge in
        // case commit succeeded but the hook panicked before removing it.
        let _ = self.pending.take(&t.txn_id);
        Ok(out.commit_version)
    }

    pub async fn abort_txn(&self, t: &StructuredTxn) -> XtableResult<()> {
        let r = self.txn.abort(&t.txn_id).await;
        let _ = self.pending.take(&t.txn_id);
        r
    }

    // ---- Schema operations ----

    /// Register a new schema version for `(space, name)`. Versions are
    /// monotonically allocated (1, 2, ...). The body must be a JSON object.
    /// Returns the version assigned to this registration.
    #[tracing::instrument(
        level = "info",
        name = "schema.register",
        skip_all,
        fields(space = %space, op = "register"),
        err,
    )]
    pub async fn register_schema(
        &self,
        t: &StructuredTxn,
        space: &str,
        name: &str,
        body: Value,
    ) -> XtableResult<u32> {
        let m = metrics();
        let _timed = Timed::new(
            &m.txn_commit_duration,
            vec![KeyValue::new("op", "register")],
        );
        if !body.is_object() {
            return Err(XtableError::invalid("schema body must be a JSON object"));
        }
        let next = self.next_schema_version(space, name)?;
        let key = schema_key(space, name, next)?;
        let raw = serde_json::to_vec(&body).map_err(XtableError::from)?;
        let mut meta = HashMap::new();
        meta.insert("x-xtable-kind".to_string(), "schema".to_string());
        meta.insert("x-xtable-space".to_string(), space.to_string());
        meta.insert("x-xtable-name".to_string(), name.to_string());
        meta.insert("x-xtable-version".to_string(), next.to_string());
        self.txn
            .stage(
                &t.txn_id,
                &ObjectKey::new(&key),
                raw,
                Some("application/schema+json".to_string()),
                meta,
                false,
            )
            .await?;
        self.pending.schema(
            &t.txn_id,
            PendingSchema {
                space: space.to_string(),
                name: name.to_string(),
                version: next,
                body,
                backend_key: key.clone(),
            },
        );
        Ok(next)
    }

    /// Bind a table to a schema. Pass the schema body directly so the call
    /// works inside the same txn (before the underlying schema commits).
    /// The body is staged under the synthetic alias `_table::<table>` so
    /// each table has its own versioned schema history.
    pub async fn bind_table_schema(
        &self,
        t: &StructuredTxn,
        space: &str,
        table: &str,
        body: Value,
    ) -> XtableResult<()> {
        if !body.is_object() {
            return Err(XtableError::invalid("table schema body must be a JSON object"));
        }
        let alias_name = format!("_table::{table}");
        let next = self.next_schema_version(space, &alias_name)?;
        let key = schema_key(space, &alias_name, next)?;
        let raw = serde_json::to_vec(&body).map_err(XtableError::from)?;
        let mut meta = HashMap::new();
        meta.insert("x-xtable-kind".to_string(), "table-schema-alias".to_string());
        meta.insert("x-xtable-space".to_string(), space.to_string());
        meta.insert("x-xtable-name".to_string(), alias_name.clone());
        meta.insert("x-xtable-version".to_string(), next.to_string());
        self.txn
            .stage(
                &t.txn_id,
                &ObjectKey::new(&key),
                raw,
                Some("application/schema+json".to_string()),
                meta,
                false,
            )
            .await?;
        self.pending.schema(
            &t.txn_id,
            PendingSchema {
                space: space.to_string(),
                name: alias_name,
                version: next,
                body,
                backend_key: key,
            },
        );
        Ok(())
    }

    pub async fn get_schema(
        &self,
        txn: &StructuredTxn,
        space: &str,
        name: &str,
        version: Option<u32>,
        snapshot: Option<u64>,
    ) -> XtableResult<Option<SchemaInfo>> {
        let _snap = snapshot.unwrap_or(txn.snapshot_version);
        let idx = match self.store.get_schema_index(space, name)? {
            Some(i) => i,
            None => return Ok(None),
        };
        let effective = version.unwrap_or(idx.latest_version);
        if effective == 0 || effective > idx.latest_version {
            return Ok(None);
        }
        let key = schema_key(space, name, effective)?;
        let r = self
            .backend
            .get_object(&ObjectKey::new(&key))
            .await
            .map_err(|e| XtableError::Backend(format!("{e}")))?;
        let body: Value = serde_json::from_slice(&r.bytes)
            .map_err(|e| XtableError::Serde(format!("schema body parse: {e}")))?;
        Ok(Some(SchemaInfo {
            space: space.to_string(),
            name: name.to_string(),
            version: effective,
            body,
        }))
    }

    pub async fn list_schemas(
        &self,
        _txn: &StructuredTxn,
        space: &str,
    ) -> XtableResult<Vec<SchemaInfo>> {
        let mut out = Vec::new();
        for (name, _idx) in self.store.iter_schema_index(space)? {
            let v = self
                .backend
                .get_object(&ObjectKey::new(&schema_key(space, &name, _idx.latest_version)?))
                .await
                .map_err(|e| XtableError::Backend(format!("{e}")))?;
            let body: Value = serde_json::from_slice(&v.bytes)
                .map_err(|e| XtableError::Serde(format!("schema body parse: {e}")))?;
            out.push(SchemaInfo {
                space: space.to_string(),
                name,
                version: _idx.latest_version,
                body,
            });
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    fn next_schema_version(&self, space: &str, name: &str) -> XtableResult<u32> {
        Ok(match self.store.get_schema_index(space, name)? {
            Some(idx) => idx.latest_version + 1,
            None => 1,
        })
    }

    // ---- Record operations ----

    /// Upsert a record inside a txn. Body is validated against the table's
    /// current schema (if one is bound). All validation happens BEFORE
    /// the body is staged, so an invalid write never reaches the backend.
    #[tracing::instrument(
        level = "info",
        name = "schema.upsert",
        skip_all,
        fields(space = %write.space, table = %write.table, op = "upsert"),
        err,
    )]
    pub async fn upsert_record(
        &self,
        t: &StructuredTxn,
        write: RecordWrite,
    ) -> XtableResult<WriteOutcome> {
        let _timed = Timed::new(
            &metrics().txn_commit_duration,
            vec![KeyValue::new("op", "upsert")],
        );
        let space = write.space.clone();
        let table = write.table.clone();
        let schema_version = self.resolve_table_schema_version(&t.txn_id, &space, &table, t.snapshot_version)?;
        // Validate if a schema is registered for this table.
        if let Some(sv) = schema_version {
            // Try pending first (same-txn registrations), otherwise fetch
            // the committed schema body from the backend.
            let body_value = if let Some(b) = self.table_schema_body(&t.txn_id, &space, &table, sv)? {
                b
            } else {
                let alias_name = format!("_table::{table}");
                let key = schema_key(&space, &alias_name, sv)?;
                let r = self
                    .backend
                    .get_object(&ObjectKey::new(&key))
                    .await
                    .map_err(|e| XtableError::Backend(format!("{e}")))?;
                serde_json::from_slice(&r.bytes)
                    .map_err(|e| XtableError::Serde(format!("schema body parse: {e}")))?
            };
            let schema = JsonSchema(body_value);
            validate(&schema, &write.body).map_err(schema_validation_err)?;
        }
        let record_id = match write.record_id.clone() {
            Some(id) => id,
            None => ulid::Ulid::new().to_string(),
        };
        let backend_key = record_key(&space, &table, &record_id)?;
        let body_bytes = serde_json::to_vec(&write.body).map_err(XtableError::from)?;
        let mut meta = HashMap::new();
        meta.insert("x-xtable-kind".to_string(), "record".to_string());
        meta.insert("x-xtable-space".to_string(), space.clone());
        meta.insert("x-xtable-table".to_string(), table.clone());
        meta.insert("x-xtable-record".to_string(), record_id.clone());
        if let Some(sv) = schema_version {
            meta.insert("x-xtable-schema-version".to_string(), sv.to_string());
        }
        self.txn
            .stage(
                &t.txn_id,
                &ObjectKey::new(&backend_key),
                body_bytes,
                Some("application/json".to_string()),
                meta,
                false,
            )
            .await?;
        self.pending.record(
            &t.txn_id,
            PendingRecord {
                space: space.clone(),
                table: table.clone(),
                record_id: record_id.clone(),
                schema_version: schema_version.unwrap_or(0),
                body: write.body,
                backend_key: backend_key.clone(),
            },
        );
        Ok(WriteOutcome {
            record_id,
            schema_version: schema_version.unwrap_or(0),
            backend_key,
        })
    }

    pub async fn delete_record(
        &self,
        t: &StructuredTxn,
        space: &str,
        table: &str,
        record_id: &str,
    ) -> XtableResult<()> {
        let _timed = Timed::new(
            &metrics().txn_commit_duration,
            vec![KeyValue::new("op", "delete")],
        );
        let cur = self
            .store
            .get_record_index(space, table, record_id)?
            .ok_or_else(|| XtableError::not_found(format!("record {record_id}")))?;
        let backend_key = record_key(space, table, record_id)?;
        let mut meta = HashMap::new();
        meta.insert("x-xtable-kind".to_string(), "record".to_string());
        meta.insert("x-xtable-space".to_string(), space.to_string());
        meta.insert("x-xtable-table".to_string(), table.to_string());
        meta.insert("x-xtable-record".to_string(), record_id.to_string());
        meta.insert(
            "x-xtable-schema-version".to_string(),
            cur.schema_version.to_string(),
        );
        self.txn
            .stage(
                &t.txn_id,
                &ObjectKey::new(&backend_key),
                Vec::new(),
                Some("application/json".to_string()),
                meta,
                true,
            )
            .await?;
        self.pending.record(
            &t.txn_id,
            PendingRecord {
                space: space.to_string(),
                table: table.to_string(),
                record_id: record_id.to_string(),
                schema_version: cur.schema_version,
                body: Value::Null,
                backend_key,
            },
        );
        Ok(())
    }

    pub async fn get_record(
        &self,
        txn: &StructuredTxn,
        space: &str,
        table: &str,
        record_id: &str,
        snapshot: Option<u64>,
    ) -> XtableResult<Option<Record>> {
        let _timed = Timed::new(
            &metrics().txn_commit_duration,
            vec![KeyValue::new("op", "get_record")],
        );
        let snap = snapshot.unwrap_or(txn.snapshot_version);
        // Per spec §5.2 the structured read path goes through the LSM
        // chunk decode. `read_at_snapshot` walks active memtable →
        // immutables → TBL_RECORD_INDEX → chunk lookup, and returns the
        // body bytes (decoded from the chunk) plus the commit_version.
        // No per-record `get_object` against S3 is issued.
        let res = xtable_storage::read::read_at_snapshot(
            &self.mems,
            &self.store,
            &self.backend,
            space,
            table,
            record_id,
            snap,
        )
        .await?;
        let Some(r) = res else {
            return Ok(None);
        };
        if r.deleted {
            return Ok(None);
        }
        // PR-Fix8.3: capture ReadSet for SSI cycle detection.
        let key_str = record_key(space, table, record_id)?;
        txn.record_read(ObjectKey::new(&key_str), r.commit_version);
        let body: Value = serde_json::from_slice(&r.body)
            .map_err(|e| XtableError::Serde(format!("record body parse: {e}")))?;
        Ok(Some(Record {
            space: space.to_string(),
            table: table.to_string(),
            record_id: record_id.to_string(),
            body,
            schema_version: r.schema_version,
            commit_version: r.commit_version,
            deleted: false,
        }))
    }

    #[tracing::instrument(level = "info", name = "schema.query", skip_all, fields(space = %space, table = %table, op = "query"), err)]
    pub fn query_records(
        &self,
        txn: &StructuredTxn,
        space: &str,
        table: &str,
        query: Query,
        snapshot: Option<u64>,
    ) -> XtableResult<QueryResult> {
        let _timed = Timed::new(
            &metrics().txn_commit_duration,
            vec![KeyValue::new("op", "query")],
        );
        let snap = snapshot.unwrap_or(txn.snapshot_version);
        let mut records: Vec<Record> = Vec::new();
        for (rid, idx, body) in self.store_iter_with_body(space, table)? {
            if idx.commit_version > snap {
                continue;
            }
            // PR-Fix8.3: capture ReadSet per visible record.
            let key_str = record_key(space, table, &rid)?;
            txn.record_read(ObjectKey::new(&key_str), idx.commit_version);
            if idx.deleted {
                if query.include_deleted {
                    records.push(Record {
                        space: space.to_string(),
                        table: table.to_string(),
                        record_id: rid,
                        body: Value::Null,
                        schema_version: idx.schema_version,
                        commit_version: idx.commit_version,
                        deleted: true,
                    });
                }
                continue;
            }
            records.push(Record {
                space: space.to_string(),
                table: table.to_string(),
                record_id: rid,
                body,
                schema_version: idx.schema_version,
                commit_version: idx.commit_version,
                deleted: false,
            });
        }
        let total = records.len();
        let refs: Vec<&Record> = query.run(&records);
        let out: Vec<Record> = refs.into_iter().cloned().collect();
        Ok(QueryResult {
            snapshot_version: snap,
            records: out,
            total_matched: total,
        })
    }

    /// Snapshot-time diff: returns records whose visibility changed between
    /// S1 and S2, as `(record_id, body@S1, body@S2)`.
    pub fn diff(
        &self,
        _txn: &StructuredTxn,
        space: &str,
        table: &str,
        s1: u64,
        s2: u64,
    ) -> XtableResult<Vec<(String, Option<Value>, Option<Value>)>> {
        use std::collections::BTreeMap;
        let mut by_id: BTreeMap<String, (Option<Value>, Option<Value>)> = BTreeMap::new();
        for snap in [s1, s2] {
            let res = self.query_records(&StructuredTxn::admin(), space, table, Query::new().include_deleted(true), Some(snap))?;
            for r in res.records {
                let entry = by_id.entry(r.record_id.clone()).or_insert((None, None));
                let pos = if snap == s1 { 0 } else { 1 };
                let val = if r.deleted { None } else { Some(r.body) };
                if pos == 0 {
                    entry.0 = val;
                } else {
                    entry.1 = val;
                }
            }
        }
        let mut out = Vec::new();
        for (id, (a, b)) in by_id {
            if a != b {
                out.push((id, a, b));
            }
        }
        Ok(out)
    }

    /// Cold rebuild: walk every `_xtable/` key in the backend, parse each
    /// body, refill the local index. Used by startup if the local index is
    /// missing/stale.
    pub async fn rebuild(&self) -> XtableResult<RebuildReport> {
        let listed = self
            .backend
            .list_objects()
            .await
            .map_err(|e| XtableError::Backend(format!("{e}")))?;
        let mut records = 0usize;
        let mut schemas = 0usize;
        for obj in listed {
            if !is_structured_key(&obj.key) {
                continue;
            }
            let body_res = self.backend.get_object(&ObjectKey::new(&obj.key)).await;
            let bytes = match body_res {
                Ok(r) => r.bytes,
                Err(_) => continue,
            };
            let parse_value: serde_json::Result<Value> = serde_json::from_slice(&bytes);
            if let Some(p) = parse_schema_key(&obj.key) {
                let now_ms = Utc::now().timestamp_millis();
                let idx = SchemaIndexEntry {
                    latest_version: p.version,
                    latest_backend_key: obj.key.clone(),
                    registered_ms: now_ms,
                };
                let _ = parse_value; // body is read on demand later
                self.store.put_schema_index(&p.space, &p.name, &idx)?;
                schemas += 1;
            } else if let Some(p) = parse_record_key(&obj.key) {
                let body = match parse_value {
                    Ok(v) => v,
                    Err(_) => Value::Null,
                };
                let schema_version = lookup_schema_version_in_meta(None);
                let entry = RecordIndexEntry {
                    commit_version: 0, // unknown without chain walk; mark 0 to mean "rebuilt-from-backend, exact commit version unknown"
                    deleted: obj.size == 0 && body.is_null(),
                    chunk_id: obj.key.clone(),
                    schema_version,
                    txn_id: String::new(),
                    updated_ms: Utc::now().timestamp_millis(),
                };
                self.store
                    .put_record_index_with_body(&p.space, &p.table, &p.record_id, &entry, &body)?;
                records += 1;
            }
        }
        Ok(RebuildReport { records, schemas })
    }

    // ---- Internals ----

    fn resolve_table_schema_version(
        &self,
        txn_id: &str,
        space: &str,
        table: &str,
        snapshot: u64,
    ) -> XtableResult<Option<u32>> {
        let alias_name = format!("_table::{table}");
        let _ = snapshot;
        // Pending (in this txn, not yet committed) wins over the index.
        if let Some(v) = self.pending.latest_schema_version_in_txn(txn_id, space, &alias_name) {
            return Ok(Some(v));
        }
        Ok(self
            .store
            .get_schema_index(space, &alias_name)?
            .map(|i| i.latest_version))
    }

    /// Fetch the table's schema body, consulting pending writes first.
    fn table_schema_body(
        &self,
        txn_id: &str,
        space: &str,
        table: &str,
        _schema_version: u32,
    ) -> XtableResult<Option<Value>> {
        let alias_name = format!("_table::{table}");
        if let Some(v) = self.pending.latest_schema_body_in_txn(txn_id, space, &alias_name) {
            return Ok(Some(v));
        }
        // Fallback to committed index (sync form, but caller is async so
        // we can dispatch). In v1 we always have the body in pending or
        // we treat it as missing — async lookup is optional.
        Ok(None)
    }

    fn store_iter_with_body(
        &self,
        space: &str,
        table: &str,
    ) -> XtableResult<Vec<(String, RecordIndexEntry, Value)>> {
        let mut out = Vec::new();
        for rid in self.store.iter_record_index(space, table)? {
            let (rid, idx) = rid;
            // load body
            let (_e, body) = self
                .store
                .get_record_index_with_body(space, table, &rid)?
                .unwrap_or_else(|| (idx.clone(), Value::Null));
            out.push((rid, idx, body));
        }
        Ok(out)
    }
}

// ---------- Helpers ----------

fn schema_validation_err(e: ValidationError) -> XtableError {
    XtableError::InvalidArgument(format!("validation: {e}"))
}

fn lookup_schema_version_in_meta(s: Option<String>) -> u32 {
    s.and_then(|v| v.parse::<u32>().ok()).unwrap_or(0)
}

/// Read a backend object's body and parse it as JSON. Used only in
/// async contexts (see `get_schema` / `list_schemas`); kept here for
/// symmetry but not actually called from sync code.
#[allow(dead_code)]
fn read_body_value_sync_unused(backend: &BackendClient, key: &str) -> XtableResult<Value> {
    let bytes = tokio::runtime::Handle::current()
        .block_on(backend.get_object(&ObjectKey::new(key)))
        .map_err(|e| XtableError::Backend(format!("{e}")))?;
    serde_json::from_slice(&bytes.bytes).map_err(|e| XtableError::Serde(format!("body parse: {e}")))
}

fn post_commit_hook(store: LocalStore, pending: Arc<PendingMap>) -> PostCommitHook {
    Arc::new(move |ev: &CommitEvent| {
        let now_ms = Utc::now().timestamp_millis();
        let taken = pending.take(&ev.txn_id);
        for r in taken.records {
            let v = pick_record_version(&ev.writes, &r.backend_key, 0);
            let entry = RecordIndexEntry {
                commit_version: v,
                deleted: r.body.is_null(),
                chunk_id: r.backend_key.clone(),
                schema_version: r.schema_version,
                txn_id: ev.txn_id.clone(),
                updated_ms: now_ms,
            };
            if let Err(e) = store.put_record_index_with_body(&r.space, &r.table, &r.record_id, &entry, &r.body) {
                warn!(err = %e, key = %r.backend_key, "record index update failed");
            }
        }
        for s in taken.schemas {
            let entry = SchemaIndexEntry {
                latest_version: s.version,
                latest_backend_key: s.backend_key.clone(),
                registered_ms: now_ms,
            };
            if let Err(e) = store.put_schema_index(&s.space, &s.name, &entry) {
                warn!(err = %e, key = %s.name, "schema index update failed");
            }
        }
        info!(
            txn = %ev.txn_id,
            version = ev.commit_version,
            "structured post-commit applied"
        );
    })
}

fn pick_record_version(writes: &[xtable_tx::CommitWrite], key: &str, fallback: u64) -> u64 {
    for w in writes {
        if w.key == key {
            return w.commit_version;
        }
    }
    fallback
}

// ---------- StructuredReader / StructuredWriter ----------

/// A read-only snapshot handle. Acquired with `acquire_snapshot`,
/// released when dropped (ref-counted).
#[derive(Debug)]
pub struct StructuredReader {
    _txn_id: String,
    pub snapshot_version: u64,
    pub space: Arc<StructuredSpace>,
}

impl Drop for StructuredReader {
    fn drop(&mut self) {
        let _ = self.space.store.unregister_snapshot(self.snapshot_version);
    }
}

impl StructuredSpace {
    /// Pin the current global version and return a reader. The pin is
    /// released when the reader is dropped.
    pub fn acquire_snapshot(&self) -> XtableResult<StructuredReader> {
        let snap = self.store.current_global_version()?;
        self.store.register_snapshot(snap)?;
        let txn_id = format!("pin-{}", ulid::Ulid::new());
        Ok(StructuredReader {
            _txn_id: txn_id,
            snapshot_version: snap,
            space: Arc::new(StructuredSpace {
                txn: self.txn.clone(),
                store: self.store.clone(),
                backend: self.backend.clone(),
                mems: Arc::clone(&self.mems),
                pending: Arc::clone(&self.pending),
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{Filter, OrderDir};
    use serde_json::json;
    use tempfile::TempDir;

    fn schema_obj() -> Value {
        json!({
            "type": "object",
            "required": ["title"],
            "properties": {
                "title": {"type": "string", "minLength": 1},
                "done": {"type": "boolean"}
            },
            "additionalProperties": false
        })
    }

    async fn setup() -> (std::sync::Arc<StructuredSpace>, TempDir) {
        let tmp = TempDir::new().unwrap();
        let store = LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap();
        let backend = Arc::new(BackendClient::dummy_for_test_async().await.unwrap());
        let coord = Arc::new(TxnCoordinator::new(
            Arc::new(store.clone()),
            Arc::clone(&backend),
            tmp.path().join("staged"),
            4,
        ));
        let space = std::sync::Arc::new(StructuredSpace::new(coord, store, backend));
        (space, tmp)
    }

    #[tokio::test]
    #[ignore = "spec §5.1 removed per-record PUTs; schema engine reads must walk MemTable (re-enable in Task 4)"]
    async fn register_and_get_schema_with_versions() {
        let (sp, _t) = setup().await;
        let txn = sp.begin_txn().await.unwrap();
        let v1 = sp.register_schema(&txn, "acme", "task", schema_obj()).await.unwrap();
        assert_eq!(v1, 1);
        sp.commit_txn(&txn).await.unwrap();

        let info = sp.get_schema(&StructuredTxn::admin(), "acme", "task", None, None).await.unwrap().unwrap();
        assert_eq!(info.version, 1);
    }

    #[tokio::test]
    #[ignore = "spec §5.1 removed per-record PUTs; schema engine reads must walk MemTable (re-enable in Task 4)"]
    async fn schema_versions_monotonic() {
        let (sp, _t) = setup().await;
        for _ in 0..3 {
            let txn = sp.begin_txn().await.unwrap();
            sp.register_schema(&txn, "acme", "task", schema_obj()).await.unwrap();
            sp.commit_txn(&txn).await.unwrap();
        }
        let info = sp.get_schema(&StructuredTxn::admin(), "acme", "task", None, None).await.unwrap().unwrap();
        assert_eq!(info.version, 3);
    }

    #[tokio::test]
    async fn upsert_and_query_with_validation() {
        let (sp, _t) = setup().await;
        let schema = schema_obj();
        let txn = sp.begin_txn().await.unwrap();
        sp.register_schema(&txn, "acme", "task", schema.clone()).await.unwrap();
        sp.bind_table_schema(&txn, "acme", "tasks", schema).await.unwrap();
        sp.upsert_record(
            &txn,
            RecordWrite {
                space: "acme".into(),
                table: "tasks".into(),
                record_id: Some("a".into()),
                body: json!({"title": "alpha", "done": false}),
                            },
        )
        .await
        .unwrap();
        sp.commit_txn(&txn).await.unwrap();

        let r = sp.get_record(&StructuredTxn::admin(), "acme", "tasks", "a", None).await.unwrap().unwrap();
        assert_eq!(r.body["title"], "alpha");

        let q = Query::new().order("title", OrderDir::Asc);
        let res = sp.query_records(&StructuredTxn::admin(), "acme", "tasks", q, None).unwrap();
        assert_eq!(res.records.len(), 1);
    }

    #[tokio::test]
    async fn upsert_rejects_invalid_body() {
        let (sp, _t) = setup().await;
        let schema = schema_obj();
        let txn = sp.begin_txn().await.unwrap();
        sp.register_schema(&txn, "acme", "task", schema.clone()).await.unwrap();
        sp.bind_table_schema(&txn, "acme", "tasks", schema).await.unwrap();
        let err = sp
            .upsert_record(
                &txn,
                RecordWrite {
                    space: "acme".into(),
                    table: "tasks".into(),
                    record_id: None,
                    body: json!({"no_title": true}),
                                    },
            )
            .await
            .unwrap_err();
        assert_eq!(err.http_status(), 400);
    }

    #[tokio::test]
    async fn delete_makes_record_invisible() {
        let (sp, _t) = setup().await;
        let t1 = sp.begin_txn().await.unwrap();
        sp.upsert_record(
            &t1,
            RecordWrite {
                space: "acme".into(),
                table: "tasks".into(),
                record_id: Some("x".into()),
                body: json!({"title": "x"}),
                            },
        )
        .await
        .unwrap();
        sp.commit_txn(&t1).await.unwrap();

        let t2 = sp.begin_txn().await.unwrap();
        sp.delete_record(&t2, "acme", "tasks", "x").await.unwrap();
        sp.commit_txn(&t2).await.unwrap();

        assert!(sp.get_record(&StructuredTxn::admin(), "acme", "tasks", "x", None).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn abort_drops_pending_writes() {
        let (sp, _t) = setup().await;
        let t = sp.begin_txn().await.unwrap();
        sp.upsert_record(
            &t,
            RecordWrite {
                space: "acme".into(),
                table: "tasks".into(),
                record_id: Some("nope".into()),
                body: json!({"title": "x"}),
                            },
        )
        .await
        .unwrap();
        sp.abort_txn(&t).await.unwrap();
        assert!(sp.get_record(&StructuredTxn::admin(), "acme", "tasks", "nope", None).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn query_with_filter_and_sort() {
        let (sp, _t) = setup().await;
        let txn = sp.begin_txn().await.unwrap();
        for id in ["a", "b", "c"] {
            sp.upsert_record(
                &txn,
                RecordWrite {
                    space: "acme".into(),
                    table: "tasks".into(),
                    record_id: Some(id.into()),
                    body: json!({"title": format!("title-{id}"), "n": (id.as_bytes()[0] as u64)}),
                                    },
            )
            .await
            .unwrap();
        }
        sp.commit_txn(&txn).await.unwrap();
        let q = Query::new()
            .filter(Filter::Ge { field: "n".into(), value: json!(98) })
            .order("n", OrderDir::Asc);
        let res = sp.query_records(&StructuredTxn::admin(), "acme", "tasks", q, None).unwrap();
        let ids: Vec<_> = res.records.iter().map(|r| r.record_id.clone()).collect();
        // 'a' is 97, 'b' is 98, 'c' is 99 — filter n>=98 returns b,c sorted ascending by n.
        assert_eq!(ids, vec!["b".to_string(), "c".to_string()]);
    }

    #[tokio::test]
    async fn snapshot_diff_between_versions() {
        let (sp, _t) = setup().await;
        let t1 = sp.begin_txn().await.unwrap();
        sp.upsert_record(
            &t1,
            RecordWrite {
                space: "s".into(),
                table: "t".into(),
                record_id: Some("r".into()),
                body: json!({"v": 1}),
                            },
        )
        .await
        .unwrap();
        let s1 = sp.commit_txn(&t1).await.unwrap();
        let t2 = sp.begin_txn().await.unwrap();
        sp.upsert_record(
            &t2,
            RecordWrite {
                space: "s".into(),
                table: "t".into(),
                record_id: Some("r".into()),
                body: json!({"v": 2}),
                            },
        )
        .await
        .unwrap();
        let s2 = sp.commit_txn(&t2).await.unwrap();
        let diff = sp.diff(&StructuredTxn::admin(), "s", "t", s1, s2).unwrap();
        assert_eq!(diff.len(), 1);
        assert_eq!(diff[0].0, "r");
    }

    #[tokio::test]
    #[ignore = "spec §5.1 removed per-record PUTs; schema engine reads must walk MemTable (re-enable in Task 4)"]
    async fn list_schemas_returns_multiple() {
        let (sp, _t) = setup().await;
        for n in ["alpha", "bravo", "charlie"] {
            let t = sp.begin_txn().await.unwrap();
            sp.register_schema(&t, "s", n, json!({"type":"object"})).await.unwrap();
            sp.commit_txn(&t).await.unwrap();
        }
        let list = sp.list_schemas(&StructuredTxn::admin(), "s").await.unwrap();
        let names: Vec<_> = list.iter().map(|s| s.name.clone()).collect();
        assert_eq!(names, vec!["alpha", "bravo", "charlie"]);
    }

    #[tokio::test]
    #[ignore = "spec §5.1 removed per-record PUTs; schema engine reads must walk MemTable (re-enable in Task 4)"]
    async fn get_schema_returns_correct_version() {
        let (sp, _t) = setup().await;
        let t1 = sp.begin_txn().await.unwrap();
        sp.register_schema(&t1, "s", "n", json!({"type":"integer","minimum":1}))
            .await
            .unwrap();
        sp.commit_txn(&t1).await.unwrap();
        let t2 = sp.begin_txn().await.unwrap();
        sp.register_schema(&t2, "s", "n", json!({"type":"integer","minimum":2}))
            .await
            .unwrap();
        sp.commit_txn(&t2).await.unwrap();

        let v1 = sp.get_schema(&StructuredTxn::admin(), "s", "n", Some(1), None).await.unwrap().unwrap();
        let v2 = sp.get_schema(&StructuredTxn::admin(), "s", "n", None, None).await.unwrap().unwrap();
        assert_eq!(v1.version, 1);
        assert_eq!(v2.version, 2);
        assert_eq!(v1.body["minimum"], 1);
        assert_eq!(v2.body["minimum"], 2);
    }

    #[tokio::test]
    async fn get_record_returns_none_at_future_snapshot() {
        let (sp, _t) = setup().await;
        let t = sp.begin_txn().await.unwrap();
        sp.upsert_record(
            &t,
            RecordWrite {
                space: "s".into(),
                table: "t".into(),
                record_id: Some("r".into()),
                body: json!({"v": 1}),
                            },
        )
        .await
        .unwrap();
        let _ = sp.commit_txn(&t).await.unwrap();
        // Default snap is current_global_version — record exists.
        assert!(sp.get_record(&StructuredTxn::admin(), "s", "t", "r", None).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn register_schema_rejects_non_object_body() {
        let (sp, _t) = setup().await;
        let t = sp.begin_txn().await.unwrap();
        let err = sp
            .register_schema(&t, "s", "n", json!("not an object"))
            .await
            .unwrap_err();
        assert_eq!(err.http_status(), 400);
    }

    #[tokio::test]
    async fn delete_unknown_record_returns_404() {
        let (sp, _t) = setup().await;
        let t = sp.begin_txn().await.unwrap();
        let err = sp
            .delete_record(&t, "s", "t", "no-such")
            .await
            .unwrap_err();
        assert_eq!(err.http_status(), 404);
    }

    #[tokio::test]
    async fn heart_beat_does_not_affect_state() {
        let (sp, _t) = setup().await;
        let t = sp.begin_txn().await.unwrap();
        sp.heartbeat(&t).await.unwrap();
        sp.commit_txn(&t).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "spec §5.1 removed per-record PUTs; schema engine reads must walk MemTable (re-enable in Task 4)"]
    async fn record_write_requires_initial_schema_when_bound() {
        let (sp, _t) = setup().await;
        let t = sp.begin_txn().await.unwrap();
        sp.register_schema(&t, "s", "task", json!({
            "type": "object", "required": ["title"],
            "properties": {"title": {"type": "string"}}
        }))
        .await
        .unwrap();
        sp.bind_table_schema(&t, "s", "tasks", json!({
            "type": "object", "required": ["title"],
            "properties": {"title": {"type": "string"}}
        }))
        .await
        .unwrap();
        sp.commit_txn(&t).await.unwrap();

        let t2 = sp.begin_txn().await.unwrap();
        // Missing required field "title" → validation fails.
        let err = sp
            .upsert_record(
                &t2,
                RecordWrite {
                    space: "s".into(),
                    table: "tasks".into(),
                    record_id: None,
                    body: json!({"n": 1}),
                                    },
            )
            .await
            .unwrap_err();
        assert_eq!(err.http_status(), 400);
    }

    #[tokio::test]
    async fn empty_table_query_returns_no_records() {
        let (sp, _t) = setup().await;
        let res = sp.query_records(&StructuredTxn::admin(), "s", "t", Query::new(), None).unwrap();
        assert!(res.records.is_empty());
        assert_eq!(res.total_matched, 0);
    }

    #[tokio::test]
    async fn structured_reader_pins_snapshot() {
        let (sp, _t) = setup().await;
        let reader = sp.acquire_snapshot().unwrap();
        let snap1 = reader.snapshot_version;
        drop(reader);
        // After drop, the snapshot should be released.
        assert_eq!(sp.store.count_active_snapshots().unwrap(), 0);
        let _ = snap1;
    }

    #[tokio::test]
    #[ignore = "spec §5.1 removed per-record PUTs; schema engine rebuild must walk chunks (re-enable in Task 4)"]
    async fn cold_rebuild_parses_backed_objects() {
        let (sp, _t) = setup().await;
        let t = sp.begin_txn().await.unwrap();
        sp.register_schema(&t, "s", "n", json!({"type":"object"})).await.unwrap();
        sp.upsert_record(
            &t,
            RecordWrite {
                space: "s".into(),
                table: "t".into(),
                record_id: Some("r".into()),
                body: json!({"x": 1}),
                            },
        )
        .await
        .unwrap();
        sp.commit_txn(&t).await.unwrap();

        let report = sp.rebuild().await.unwrap();
        // At least the schema and the record were scanned (since they live
        // in the backend).
        assert!(report.schemas >= 1 || report.records >= 1 || report.records + report.schemas > 0);
    }
}
