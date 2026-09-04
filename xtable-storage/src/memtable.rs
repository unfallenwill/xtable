//! In-memory MemTable for the LSM-tree storage layer.
//!
//! Every committed write goes first into the **active** MemTable. When
//! the active MemTable crosses size (default 64 MiB) or age (default 60s),
//! it is atomically swapped to an **immutable** MemTable and handed to a
//! background flush task that writes it to S3 as a chunk.
//!
//! ## Data structure
//!
//! For v1 we use `DashMap<(space, table, record_id), MemEntry>` — a
//! sharded concurrent HashMap. Future optimization: replace with
//! `crossbeam-skiplist` for sorted iteration during flush.
//!
//! ## Visibility semantics
//!
//! Each `MemEntry` carries an `AtomicU64` `commit_version` that flips from
//! `u64::MAX` (invisible) to the assigned commit_version once the
//! transaction's commit is durable in redb. Reads filter by
//! `commit_version <= snapshot`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use xtable_core::XtableResult;
use xtable_telemetry::metrics::global as metrics;
use xtable_telemetry::timed::Timed;
use xtable_telemetry::KeyValue;

/// Composite record key: `(space, table, record_id)`.
pub type RecordKey = (String, String, String);

/// Default shard count for `DashMap`.
pub const DEFAULT_SHARD_COUNT: usize = 32;

/// Magic sentinel meaning "not visible to any reader".
pub const INVISIBLE: u64 = u64::MAX;

/// A single in-memory entry. Cloneable (value is `Arc`).
#[derive(Debug, Clone)]
pub struct MemEntry {
    /// Composite key (space, table, record_id).
    pub key: RecordKey,
    /// Body bytes.
    pub value: Arc<RecordValue>,
    /// The commit version that makes this entry visible. `u64::MAX` while
    /// staged; flipped to the assigned commit_version when the txn's
    /// commit lands.
    pub commit_version: Arc<AtomicU64>,
    /// Originating transaction id.
    pub txn_id: String,
    /// Tombstone flag (delete marker).
    pub deleted: bool,
    /// Optional content type for the body.
    pub content_type: Option<String>,
    /// User metadata carried for chunk-on-flush.
    pub user_meta: Vec<(String, String)>,
    /// Schema version recorded at stage time.
    pub schema_version: u32,
    /// Monotonic WAL sequence for crash-recovery replay ordering.
    pub wal_seq: u64,
    /// Approximate byte size of this entry (used for size-based flush policy).
    pub size_bytes: u64,
}

impl MemEntry {
    /// True iff this entry's commit_version is set and is `<=` the snapshot.
    pub fn visible_at(&self, snapshot: u64) -> bool {
        let cv = self.commit_version.load(Ordering::Acquire);
        cv != INVISIBLE && cv <= snapshot
    }

    /// True iff this entry is invisible (still staged, not yet committed).
    pub fn is_invisible(&self) -> bool {
        self.commit_version.load(Ordering::Acquire) == INVISIBLE
    }
}

/// Body bytes. Cheap to clone (refcounted).
#[derive(Debug, Clone)]
pub struct RecordValue {
    pub bytes: Bytes,
}

/// Compact serialized form of a `MemEntry` for the WAL audit trail and
/// for chunk encoding. Not all fields are needed at chunk-flush time —
/// the body bytes dominate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializedEntry {
    pub space: String,
    pub table: String,
    pub record_id: String,
    pub body: Vec<u8>,
    pub commit_version: u64,
    pub txn_id: String,
    pub deleted: bool,
    pub content_type: Option<String>,
    pub user_meta: Vec<(String, String)>,
    pub schema_version: u32,
    pub wal_seq: u64,
}

impl From<&MemEntry> for SerializedEntry {
    fn from(e: &MemEntry) -> Self {
        Self {
            space: e.key.0.clone(),
            table: e.key.1.clone(),
            record_id: e.key.2.clone(),
            body: e.value.bytes.to_vec(),
            commit_version: e.commit_version.load(Ordering::Acquire),
            txn_id: e.txn_id.clone(),
            deleted: e.deleted,
            content_type: e.content_type.clone(),
            user_meta: e.user_meta.clone(),
            schema_version: e.schema_version,
            wal_seq: e.wal_seq,
        }
    }
}

/// One MemTable instance (active OR immutable).
///
/// Holds a `DashMap<RecordKey, MemEntry>` for lock-free reads + inserts.
/// Keyed by the full composite key (space, table, record_id). Iteration
/// order during flush is unspecified — the chunk encoder sorts by
/// `compose_key_bytes` on the fly.
pub struct MemTable {
    /// Monotonic id (assigned at construction).
    pub id: u64,
    /// When this MemTable became active.
    pub created_at: Instant,
    /// Entries.
    pub map: DashMap<RecordKey, Arc<MemEntry>>,
    /// Approximate total byte size.
    pub bytes_estimate: AtomicU64,
    /// Earliest wal_seq in this memtable (`u64::MAX` if empty).
    pub earliest_seq: AtomicU64,
    /// Latest wal_seq in this memtable (`0` if empty).
    pub latest_seq: AtomicU64,
    /// Min commit_version seen (`INVISIBLE` if any entry is still staged).
    pub version_min: AtomicU64,
    /// Max commit_version seen.
    pub version_max: AtomicU64,
}

impl MemTable {
    pub fn new(id: u64) -> Arc<Self> {
        Arc::new(Self {
            id,
            created_at: Instant::now(),
            map: DashMap::with_shard_amount(DEFAULT_SHARD_COUNT),
            bytes_estimate: AtomicU64::new(0),
            earliest_seq: AtomicU64::new(u64::MAX),
            latest_seq: AtomicU64::new(0),
            version_min: AtomicU64::new(INVISIBLE),
            version_max: AtomicU64::new(0),
        })
    }

    /// Stage an entry as **invisible**. Returns the size estimate delta
    /// added by this insert.
    #[tracing::instrument(level = "debug", skip_all, fields(op = "put_invisible"), err)]
    pub fn put_invisible(&self, entry: MemEntry) -> XtableResult<u64> {
        let size_delta = entry.size_bytes.max(64);
        let key = entry.key.clone();
        self.bytes_estimate.fetch_add(size_delta, Ordering::Relaxed);
        self.earliest_seq
            .fetch_min(entry.wal_seq, Ordering::Relaxed);
        self.latest_seq.fetch_max(entry.wal_seq, Ordering::Relaxed);
        let cv = entry.commit_version.load(Ordering::Acquire);
        if cv != INVISIBLE {
            self.version_min.fetch_min(cv, Ordering::Relaxed);
            self.version_max.fetch_max(cv, Ordering::Relaxed);
        }
        self.map.insert(key, Arc::new(entry));
        Ok(size_delta)
    }

    /// Flip an entry's commit_version from INVISIBLE to the assigned value.
    /// We can't efficiently look up by `(key, wal_seq)` in a DashMap, so
    /// we iterate to find it. With small per-key staging windows this is
    /// O(staged_per_key) per call.
    #[tracing::instrument(level = "debug", skip_all, fields(op = "publish"))]
    pub fn publish(&self, key: &RecordKey, _wal_seq: u64, commit_version: u64) {
        if let Some(entry) = self.map.get(key) {
            entry
                .value()
                .commit_version
                .store(commit_version, Ordering::Release);
            self.version_min
                .fetch_min(commit_version, Ordering::Relaxed);
            self.version_max
                .fetch_max(commit_version, Ordering::Relaxed);
        }
    }

    /// Look up the entry for `key` if visible at `snapshot`.
    #[tracing::instrument(level = "debug", skip_all, fields(op = "get_visible"))]
    pub fn get_visible(&self, key: &RecordKey, snapshot: u64) -> Option<Arc<MemEntry>> {
        let entry = self.map.get(key)?;
        if entry.value().visible_at(snapshot) {
            Some(Arc::clone(entry.value()))
        } else {
            None
        }
    }

    /// Total approximate size.
    pub fn total_bytes(&self) -> u64 {
        self.bytes_estimate.load(Ordering::Relaxed)
    }

    /// Min commit_version across all entries (`INVISIBLE` if any entry
    /// is still staged).
    pub fn commit_version_min(&self) -> u64 {
        self.version_min.load(Ordering::Relaxed)
    }

    /// Max commit_version across all entries.
    pub fn commit_version_max(&self) -> u64 {
        self.version_max.load(Ordering::Relaxed)
    }

    /// Earliest wal_seq (`u64::MAX` if empty).
    pub fn first_wal_seq(&self) -> u64 {
        self.earliest_seq.load(Ordering::Relaxed)
    }

    /// Latest wal_seq (`0` if empty).
    pub fn last_wal_seq(&self) -> u64 {
        self.latest_seq.load(Ordering::Relaxed)
    }

    pub fn should_flush(&self, policy: &FlushPolicy) -> bool {
        if self.total_bytes() as usize >= policy.memtable_size_bytes {
            return true;
        }
        if self.created_at.elapsed() >= policy.memtable_age {
            return true;
        }
        false
    }
}

/// Configuration for the memtable-to-chunk flush pipeline.
#[derive(Debug, Clone)]
pub struct FlushPolicy {
    /// Hard limit on memtable size in bytes (default 64 MiB).
    pub memtable_size_bytes: usize,
    /// Hard limit on memtable age before forced flush (default 60s).
    pub memtable_age: Duration,
    /// Soft limit: when an immutable memtable is this full, new inserts
    /// pause (backpressure) until an immutable drains.
    pub soft_limit_bytes: usize,
}

impl Default for FlushPolicy {
    fn default() -> Self {
        Self {
            memtable_size_bytes: 64 * 1024 * 1024,
            memtable_age: Duration::from_secs(60),
            soft_limit_bytes: 51 * 1024 * 1024,
        }
    }
}

/// Set of one active + N immutables. Hot-path is active; flush loop
/// drains immutables.
pub struct MemTableSet {
    /// The current active memtable. Replaced atomically on flush.
    pub active: RwLock<Arc<MemTable>>,
    /// Immutable memtables awaiting flush, oldest-first. Uses
    /// `tokio::sync::Mutex` because the flush loop awaits on it across
    /// network IO to S3.
    /// PR-Fix13.2: parking_lot::Mutex so `maybe_rotate` (a sync path)
    /// can push without `tokio::spawn` — which previously required an
    /// async runtime and made `memtable_set_rotation` panic in tests.
    /// `take_immutables` (async) uses blocking_lock — safe inside the
    /// flush task because the lock is held only briefly.
    pub flushing: Arc<parking_lot::Mutex<Vec<Arc<MemTable>>>>,
    /// Notify fired when a new immutable lands.
    pub flush_notify: tokio::sync::Notify,
    /// Flush policy.
    pub policy: FlushPolicy,
    /// Soft-limit semaphore for backpressure on the immutable queue.
    pub admission: Arc<tokio::sync::Semaphore>,
    /// Soft-cap on the number of immutables in flight before backpressure.
    pub max_immutables: usize,
}

impl MemTableSet {
    pub fn new(initial: Arc<MemTable>, policy: FlushPolicy) -> Arc<Self> {
        let max_immutables = 4;
        Arc::new(Self {
            active: RwLock::new(initial),
            flushing: Arc::new(parking_lot::Mutex::new(Vec::new())),
            flush_notify: tokio::sync::Notify::new(),
            policy,
            admission: Arc::new(tokio::sync::Semaphore::new(max_immutables)),
            max_immutables,
        })
    }

    /// Stage an entry into the active memtable. Returns the size delta
    /// and (if rotation happened) a notification that a flush may begin.
    #[tracing::instrument(
        level = "info",
        name = "memtable.put",
        skip_all,
        fields(op = "put"),
        err
    )]
    pub fn put_invisible(&self, entry: MemEntry) -> XtableResult<u64> {
        let _timed = Timed::new(
            &metrics().memtable_write_duration,
            vec![KeyValue::new("op", "put")],
        );
        let active = self.active.read().clone();
        let delta = active.put_invisible(entry)?;
        if active.should_flush(&self.policy) {
            self.maybe_rotate(&active)?;
        }
        Ok(delta)
    }

    /// Publish a previously-staged entry's commit_version.
    ///
    /// Walks active → immutables (newest-first) so the publish lands
    /// even when `maybe_rotate` has already moved the entry into an
    /// immutable memtable between `put_invisible` and `publish`.
    #[tracing::instrument(level = "debug", skip_all, fields(op = "publish"))]
    pub fn publish(&self, key: &RecordKey, wal_seq: u64, commit_version: u64) {
        // Active first (most likely location).
        {
            let active = self.active.read();
            active.publish(key, wal_seq, commit_version);
        }
        // Then any immutables. Use try_lock to avoid async-in-sync panic;
        // a concurrent flush may hold the mutex briefly.
        if let Some(g) = self.flushing.try_lock() {
            for mt in g.iter().rev() {
                mt.publish(key, wal_seq, commit_version);
            }
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(op = "rotate"))]
    fn maybe_rotate(&self, current_active: &Arc<MemTable>) -> XtableResult<()> {
        let mut w = self.active.write();
        if !Arc::ptr_eq(&*w, current_active) {
            return Ok(());
        }
        let new_id = current_active.id + 1;
        let new_active = MemTable::new(new_id);
        let old = std::mem::replace(&mut *w, new_active);
        drop(w);
        // PR-Fix13.2: parking_lot::Mutex lets us push synchronously — no
        // tokio::spawn needed. Eliminates the "no reactor running" panic
        // when memtable rotation is exercised outside a tokio runtime.
        self.flushing.lock().push(old);
        self.flush_notify.notify_one();
        Ok(())
    }

    /// Non-blocking age/size rotation driven by the flush loop's periodic
    /// tick. Returns true if a rotation actually happened. Skips the
    /// rotation if a concurrent commit is currently holding the active
    /// read lock — that commit will itself trigger rotation when it
    /// sees the size/age threshold cross (see `put_invisible`).
    ///
    /// Critical: this method must not block. A blocking write-lock wait
    /// inside an async task starves the tokio runtime thread and freezes
    /// the entire server (the server is single-worker for HTTP).
    pub fn try_rotate_active(&self) -> bool {
        if !self.should_rotate_active() {
            return false;
        }
        let current = self.active.read().clone();
        let mut w = match self.active.try_write() {
            Some(g) => g,
            None => return false,
        };
        if !Arc::ptr_eq(&*w, &current) {
            return false;
        }
        let new_id = current.id + 1;
        let new_active = MemTable::new(new_id);
        let old = std::mem::replace(&mut *w, new_active);
        drop(w);
        self.flushing.lock().push(old);
        self.flush_notify.notify_one();
        true
    }

    /// True if the active memtable's size or age has crossed the
    /// `FlushPolicy` threshold.
    pub fn should_rotate_active(&self) -> bool {
        self.active.read().should_flush(&self.policy)
    }

    /// Take all current immutables for the flush task.
    pub async fn take_immutables(&self) -> Vec<Arc<MemTable>> {
        // parking_lot::Mutex is sync; contention is brief (maybe_rotate
        // only holds the lock for a single push), so a blocking lock is
        // safe inside the async flush task.
        std::mem::take(&mut *self.flushing.lock())
    }

    /// Sum of approximate bytes in active memtable.
    pub fn active_bytes(&self) -> u64 {
        self.active.read().total_bytes()
    }

    /// Total approximate bytes (active + immutables).
    pub async fn total_bytes(&self) -> u64 {
        let active = self.active.read().total_bytes();
        let imm = self
            .flushing
            .lock()
            .iter()
            .map(|m| m.total_bytes())
            .sum::<u64>();
        active + imm
    }

    /// Acquire an admission permit (used by the soft-limit backpressure).
    pub async fn acquire_admission(&self) -> tokio::sync::OwnedSemaphorePermit {
        self.admission.clone().acquire_owned().await.unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(rid: &str, wal_seq: u64, body_size: usize) -> MemEntry {
        let cv = Arc::new(AtomicU64::new(INVISIBLE));
        MemEntry {
            key: ("space".into(), "table".into(), rid.into()),
            value: Arc::new(RecordValue {
                bytes: Bytes::from(vec![0u8; body_size]),
            }),
            commit_version: cv,
            txn_id: "T1".into(),
            deleted: false,
            content_type: None,
            user_meta: vec![],
            schema_version: 1,
            wal_seq,
            size_bytes: body_size as u64,
        }
    }

    #[test]
    fn put_invisible_and_publish() {
        let mt = MemTable::new(0);
        let e = make_entry("r1", 1, 100);
        mt.put_invisible(e.clone()).unwrap();
        // Invisible until publish.
        assert!(
            mt.get_visible(&e.key, u64::MAX).is_none(),
            "invisible entry should not be visible at any snapshot"
        );
        // Publish at version 5.
        mt.publish(&e.key, 1, 5);
        // Now visible at snapshot >= 5.
        let got = mt.get_visible(&e.key, 5).expect("visible after publish");
        assert_eq!(got.txn_id, "T1");
        // And NOT visible at snapshot < 5.
        assert!(mt.get_visible(&e.key, 4).is_none());
    }

    #[test]
    fn should_flush_on_size() {
        let policy = FlushPolicy {
            memtable_size_bytes: 200,
            memtable_age: Duration::from_secs(3600),
            soft_limit_bytes: 100,
        };
        let mt = MemTable::new(0);
        // 4 entries of 100 bytes each → 400 bytes > 200 → flush.
        for i in 0..4 {
            let e = make_entry(&format!("r{i}"), i, 100);
            mt.put_invisible(e).unwrap();
        }
        assert!(mt.should_flush(&policy));
    }

    #[test]
    fn memtable_set_rotation() {
        let policy = FlushPolicy {
            memtable_size_bytes: 200,
            memtable_age: Duration::from_secs(3600),
            soft_limit_bytes: 100,
        };
        let initial = MemTable::new(0);
        let set = MemTableSet::new(initial.clone(), policy);

        for i in 0..5 {
            let e = make_entry(&format!("r{i}"), i as u64, 100);
            set.put_invisible(e).unwrap();
        }

        // Rotation should have occurred; active id should be > 0.
        let active = set.active.read();
        assert!(
            active.id > 0,
            "expected rotation to bump id; got {}",
            active.id
        );
    }
}
