# MVCC 升级与可靠性证明方案

> 本文是 `xtable` 的 OCC → MVCC 升级的设计文档兼可靠性证明。
> 任何"我认为这是对的"的论断都必须能被证明（用不变量 + 测试 + 形式化推理），而不是凭直觉。

## 1. 现状（OCC）的痛点

当前每个对象存一条 `VersionRecord`（`latest_version: u64`）。每次 Put 都覆盖这条记录。

读路径：
```
读 key → get_version(key) → 看 latest_version
```

写路径（OCC）：
```
Begin → snapshot_version = global_version
Stage → write_set[key] = (version_at_read, ...)
Commit:
  Validate: for each k, current_version == version_at_read
  Upload 到 S3
  Bump versions[k].latest_version
  WAL Committed
```

**痛点**：
1. 写串行化：commit 时所有写并发竞争 redb 的 write txn
2. 读和写互斥：reader 拿到的 latest_version 可能因为 commit ordering 而"瞬移"
3. 全局 commit_version 单一计数器，所有 commit 都走它 → bottleneck

## 2. MVCC 目标

| 指标 | OCC | MVCC（目标） |
|---|---|---|
| 读延迟 | ~1ms | **< 100μs**（无锁遍历） |
| 读阻塞写 | 是（latest_version 翻转） | **否** |
| 写并发度 | 1 commit/时刻 | **N commit 并行**（per-key chain append） |
| 旧版本可读 | 否 | **是**（snapshot pinning） |
| GC 成本 | N/A | 受控（旧版本可剪） |
| **正确性证明** | 经验性 | **不变量 + 测试** |

## 3. MVCC 存储模型

### 3.1 版本链（version chain）

每个对象不再是单条 `VersionRecord`，而是一条**版本链**：

```
key="users/alice"
chain:
  v=1 {etag: "e1", size: 100, txn: "T1", created_ms: 1000, deleted: false}
  v=2 {etag: "e2", size: 150, txn: "T2", created_ms: 2000, deleted: false}
  v=3 {etag: "e3", size: 200, txn: "T3", created_ms: 3000, deleted: true}   ← tombstone
  v=5 {etag: "e5", size: 250, txn: "T5", created_ms: 5000, deleted: false}  ← T4 was aborted
```

链中 `commit_version` 严格单调递增（per key）。链中可能有"洞"（aborted txn 占的版本）。

### 3.2 读取语义

`ReadAt(key, snapshot_version S)`：

```
walk chain[k] from newest to oldest:
  pick first entry e with e.commit_version ≤ S
  return e
```

**关键不变量**：chain[k] 中的 commit_version 单调递增。

### 3.3 写入语义

`CommitTxn`：
1. OCC validate（不变）
2. **append** 新 entry 到每条 chain（per-key 独立原子 append）
3. WAL Committed

注意是 **append**，不是覆盖。所以 reader 永远看不到"消失"的数据——只会看到更新的数据。

## 4. 可靠性证明方案

### 4.1 八条不变量（formal claims）

**I1（链单调）**：对任意 key k，chain[k] 中 entries 的 commit_version 严格递增。
*保障*：append-only，且在 OCC validate 通过后由单 txn 写入；abort 不会留下 entry（已 abort 不 append）。

**I2（链无空洞-已提交）**：对任意 key k，已提交的 txn t 留下的 entry 是 append，且其 commit_version 是 `global_version[t]`（在 t commit 时原子分配）。
*保障*：commit 时 `alloc_versions[k] = next_global_version` 在 OCC validate 后、append 前原子分配。

**I3（快照隔离）**：对任意 txn t，snapshot_version=S_t；对任意 key k，t 内的 read 返回的 entry 是 chain[k] 中 commit_version ≤ S_t 的最新 entry。
*保障*：算法 I3-walk。

**I4（read-your-own-writes）**：对任意活跃 txn t，若 t 已 stage 了 key k 的写入，t 内 read(key) 返回 staged value；否则应用 I3。
*保障*：txn coordinator 的 `stage_body` 在 read 路径优先。

**I5（OCC 兼容性）**：两个 txn t1、t2 都 stage key k，从同一 snapshot_version 出发：
- 如果 t1 先 commit：`t2.commit()` 在 validate 时发现 `versions[k].commit_version > t2.write_set[k].version_at_read` → 409 Conflict。
- 如果 t2 先 commit：对称。
*保障*：MVCC 不改变 OCC validate 逻辑；`version_at_read` 仍然是 `global_version at BeginTxn`。
*与原 OCC 的差异*：在 MVCC 下，validate 用的是 `latest_visible_version_at(t)`（基于链），而不是单一 `latest_version` 指针。这只是读取方式的差异，**冲突语义不变**。

**I6（多对象原子性）**：对任意 txn t 的 write_set[k]，要么 t 全部 commit 后所有 reader 都看到，要么 t abort 后没有 reader 看到。
*保障*：所有 write 在单次 redb write txn 中 append；要么全部成功（commit），要么一个都不 append（abort）。

**I7（崩溃恢复等价性）**：WAL replay 后，对任意 key k，chain[k] 的内容与崩溃前一致。
*保障*：所有 chain append 之前先写 WAL `Committing`；WAL `Committed` 写在 append 之后。

**I8（GC 安全性）**：GC 永远不会删除 chain[k] 中 `commit_version ≤ min_active_snapshot` 的 entry，且会保留至少一个 entry（即 newest entry，其 `commit_version > 任何活跃 snapshot 的最大已观察版本`）。
*保障*：GC 只删 `commit_version < min_active_snapshot AND 不是 newest`。

### 4.2 proptest 不变量测试（9 个）

每个测试对若干随机操作序列（put、commit、abort、snapshot 读）生成 proptest 输入，断言不变量保持。

```
prop_i1_chain_monotonic          // 提交后 chain 单调
prop_i3_snapshot_isolation      // 读 S 看不到 >S 的 entry
prop_i5_occ_compatibility       // 同 snapshot 两个写者恰一个赢
prop_i6_multi_object_atomicity  // 多对象 txn 全有或全无
prop_i7_wal_replay_equivalence  // WAL replay 后状态一致
prop_i8_gc_safety_basic          // GC 不删 newest
prop_i8_gc_preserves_snapshot   // GC 时有活跃 S 仍可读
prop_chain_no_duplicate_version // 同一 txn 不会重复 append
prop_staged_then_committed_visibility // staged 之后 commit，commit_version ≥ S 可见
```

### 4.3 端到端原子性测试（5 个）

```
e2e_mvcc_reader_at_old_snapshot_sees_old_value
e2e_mvcc_two_readers_different_snapshots_see_different_states
e2e_mvcc_gc_old_versions_does_not_break_active_readers
e2e_mvcc_occ_conflict_one_winner
e2e_mvcc_wal_replay_state_equivalence
```

### 4.4 形式化论证（在代码注释 + README 中）

每条不变量必须能在代码里找到对应的位置：
- `chain_append(key, entry)`: 单一 redb write txn → I1, I2
- `chain_read_at_snapshot(key, S)`: 链式遍历 → I3
- `txn.commit()`: OCC validate → upload → bulk-append → I5, I6, I7
- `gc.prune_chain(key, min_active)`: 只删 < min_active 且非 newest → I8

### 4.5 覆盖率门槛

新 MVCC 代码 ≥ 90% 行覆盖；事务核心路径 100%。

## 5. 实现路径

### 5.1 存储层（xtable-storage）

```rust
// 新表
pub const TBL_VERSION_CHAINS: TableDefinition<&str, &[u8]> = ...;

pub struct VersionEntry {
    pub commit_version: u64,    // == global_version at commit time
    pub etag: String,
    pub backend_key: String,
    pub txn_id: String,
    pub size: u64,
    pub content_type: Option<String>,
    pub user_meta: Vec<(String, String)>,
    pub deleted: bool,
    pub created_ms: i64,
}

pub struct VersionChain {
    pub key: String,
    pub entries: Vec<VersionEntry>,  // sorted by commit_version ASC
}

impl LocalStore {
    pub fn append_chain_entry(&self, key: &str, entry: &VersionEntry) -> XtableResult<()>;
    pub fn read_chain_at_snapshot(&self, key: &str, snapshot: u64) -> XtableResult<Option<VersionEntry>>;
    pub fn read_latest_visible(&self, key: &str) -> XtableResult<Option<VersionEntry>>;
    pub fn prune_chain(&self, key: &str, min_snapshot: u64) -> XtableResult<usize>;
    pub fn iter_all_chains(&self) -> XtableResult<Vec<(String, VersionChain)>>;
}
```

### 5.2 快照管理（xtable-tx）

```rust
pub struct SnapshotRegistry {
    active: Arc<Mutex<HashSet<u64>>>,
}

impl SnapshotRegistry {
    pub fn register(&self, snapshot: u64);
    pub fn unregister(&self, snapshot: u64);
    pub fn min_active(&self) -> u64;
}
```

### 5.3 协调器改造

`TxnCoordinator.commit()` 的 step 5（upload）+ step 6（bulk-put versions）改为：

```
5. Upload all keys to backend (unchanged)
6. For each key k:
     read chain[k]
     validate: chain 的 newest commit_version ≤ t.write_set[k].version_at_read  (= OCC I5 兼容)
     append VersionEntry { commit_version: alloc[k], ... } to chain
   all in a single redb write txn
7. WAL Committed
```

### 5.4 GC

```
loop {
  min = snapshot_registry.min_active()
  for each chain[k]:
    prune entries where entry.commit_version < min AND entry != newest
  sleep(config.gc_interval)
}
```

## 6. 验证方案（执行清单）

1. 写完 MVCC 代码 → `cargo test --workspace` 通过
2. 跑 9 个 proptest 不变量 → 全部 pass（≥ 256 随机用例每个）
3. 跑 5 个 e2e 场景 → 全部 pass
4. 跑回归：原有 63 个 OCC 测试全部 pass（保证不破坏）
5. 覆盖率 ≥ 90%
6. 任何失败 = 证明失败，需要修改设计

## 7. 不可证明的部分（诚实声明）

我们**不能**形式化证明：
- 编译器/链接器不会破坏我们的代码（依赖 Rust 类型系统 + 测试）
- 物理介质不会同时损坏（依赖副本/快照）
- S3 后端永不返回错误地"成功"（依赖后端信誉）

这些是工程信任问题，不是算法正确性问题。