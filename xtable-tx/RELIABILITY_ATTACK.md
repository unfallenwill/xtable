# xtable 可靠性攻击报告

> **审计性质**：对抗性可靠性审计（adversarial reliability audit）。目标是攻击本项目自称的核心保证——"多对象 ACID 事务、崩溃安全、灾难恢复不丢数据"——并证明其中哪些保证在当前实现下不成立、在什么输入/时序下会损坏或丢失数据。
>
> **审计日期**：2026-08-29。**对象**：工作区当前代码（`xtable` workspace，8 crates，约 6.3k 行 Rust）。
>
> **方法与证据等级**：全部结论来自源码静态推理，每条发现给出精确的文件与行号、可复现的触发序列；配套的 PoC 测试在 [`../xtable-backend/tests/reliability_attack.rs`](../xtable-backend/tests/reliability_attack.rs)。⚠️ 审计环境没有 Rust 工具链（`cargo` 不存在），PoC **未执行**；但每个 PoC 只依赖公开 API 与确定性行为，无竞态依赖，可直接运行复核。

---

## 0. 结论摘要

**这个项目不可靠，而且会损坏数据。** 它的可靠性来自 README 里的叙事，而不是实现。README 与 `MVCC_RELIABILITY.md` 声称的核心保证，逐条对照实现后，几乎每一条都有对应的结构性缺陷：

| # | 一句话 | 严重度 | 位置 |
|---|--------|--------|------|
| V1 | 冷重建（"灾难恢复、不丢数据"）会把**所有事务写入的对象从后端删除** | 灾难 | `rebuild.rs:74,141-147` |
| V2 | 崩溃在"版本链已发布、WAL 未写 Committed"窗口 → 恢复流程**删除已发布的数据**，索引悬挂 | 灾难 | `coordinator.rs:306→311`、`recovery.rs:94-103` |
| V3 | 上传失败的补偿 `DeleteObject` 会**摧毁该 key 上之前已提交的数据**（无版本化后端上不可恢复） | 灾难 | `coordinator.rs:266-277` |
| V4 | OCC 校验读的表（`TBL_VERSIONS`）在事务提交路径上**从不更新** → 事务 vs 事务**永不冲突**，lost update 静默发生 | 灾难 | `coordinator.rs:213` vs `:306` |
| V5 | 提交全路径**无锁**，状态 CAS 是两个独立写事务 → 并发提交竞态、链条目静默丢失 | 严重 | `coordinator.rs:204-207`、`store.rs:461` |
| V6 | GetObject/ListObjectsV2 **直通后端**，"版本索引是门禁"的原子性论证是空话：半提交数据对读者可见 | 严重 | `service.rs:205,364` |
| V7 | WAL `Committing` 在上传**全部成功之后**才写 → 上传中途崩溃的补偿删除**永远不会执行**，孤儿泄漏 + 脏读 | 严重 | `coordinator.rs:281`、`recovery.rs:89-93` |
| V8 | MVCC 快照读（`read_at_snapshot`/`read_latest`）是**死代码**，"快照隔离"从未存在于服务路径 | 高 | `service.rs:194-214` |
| V9 | 快照注册表按版本号去重、无引用计数 → 共享快照的并发事务互相拔 pin → 活跃事务遭遇**幻影删除** | 高 | `store.rs:527-542` |
| V10 | 事务内 Delete 不是删除：commit 时把 **0 字节对象** Put 上后端；链上无 tombstone | 高 | `service.rs:272-289`、`coordinator.rs:433-436` |
| V11 | Multipart 完全绕过事务：事务中发起的 multipart **立即公开可见** | 高 | `service.rs:468-583` |
| V12 | 提交路径 spill 文件**永不删除**（先删记录再查记录）；`TBL_WAL`、inline write-set 行只增不减 → 磁盘单调耗尽 | 中 | `coordinator.rs:329-338` |
| V13 | 恢复/GC 路径**泄漏快照 pin**（不 unregister）→ `min_active_snapshot` 永久偏低，链剪不动 | 中 | `recovery.rs:119-150`、`gc.rs:41-62` |
| V14 | 后端暂时不可达时冷重建**静默跳过并返回 Ok** → 空索引上线、版本回退、后续重建被污染 | 中 | `rebuild.rs:39-45` |
| V15 | 认证完全没接线：`EdgeAuth` 构造后**零调用**，任何人可 begin/commit/delete | 中 | `app.rs:33`、`main.rs:148-180` |
| V16 | `version_at_read` 取 stage 时刻的当前值而非事务快照值 → 校验对"开始后他人已提交"失明（write skew） | 中 | `coordinator.rs:117-122` |
| V17 | 单元测试后端指向 `127.0.0.1:1`（死端口），所有后端交互**必然失败且被 `let _ =` 吞掉** → 恢复/补偿路径从未被单测真正执行 | 中 | `client.rs:260-274` |
| V18 | HTTP 层 stage 传 `current_global_version` 作 threshold，而事务提交不写 `TBL_VERSIONS` → **从第二笔事务起所有 HTTP 事务写入被 400 拒绝**；e2e 全部传 0 绕过 | 高 | `service.rs:144` |

一句话总结：**读写路径、版本系统、恢复逻辑是三套互不相连的机制**——读直通后端（V6），写校验一张永远不更新的表（V4），MVCC 链是没人读的死代码（V8），而恢复与重建两把"手术刀"会把不该删的删掉（V1/V2/V3）。63 个绿灯测试测不到这些，因为测试走的参数和路径与 HTTP 层真实执行的不同（§8）。

---

## 1. 攻击方法论

不泛泛找 bug，而是把项目自己声称的保证当作靶子，逐条问三个问题：

1. **声明的保证在代码里由哪个机制承担？**（README "Why xtable exists" 四条 + "The OCC protocol — correctness argument" + `MVCC_RELIABILITY.md` 的 I1–I8）
2. **该机制在并发、崩溃、后端故障、运维灾难（redb 丢失）四类扰动下还成立吗？**
3. **验证该机制的测试，测的是不是线上真实路径？**

第 3 问是本次审计最有产出的角度：这个项目的测试与生产路径**系统性脱节**（§8），绿灯数字本身构成了虚假的安全感。

---

## 2. 灾难级发现（直接丢失/摧毁已提交数据）

### V1 冷重建 = 对事务写入数据的全量删除

**声称**：README 灾难恢复表——"`redb` destroyed (disk loss, accidental rm) → Same as above — full cold rebuild from S3 metadata. **No data lost**."

**现实**：`rebuild.rs` 的孤儿判定是：

```rust
// rebuild.rs:73-78
if !txn_id.is_empty() && !txn_is_committed(store, &txn_id)? {
    orphans.push(lo.key.clone());
    ...
}
// rebuild.rs:141-147
fn txn_is_committed(...) {
    match store.get_txn_state(txn_id) {
        Ok(Some(rec)) => Ok(rec.status == TxnStatus::Committed),
        Ok(None) => Ok(false),        // ← 空库上恒为 false
        ...
```

冷重建的前提恰恰是 **redb 已丢失**——重建写入的是一个全新的空库，里面没有任何 `TxnState`。于是**每一个**通过事务写入（带 `x-amz-meta-xtable-txn-id` 元数据）的对象都满足 `!txn_is_committed` → 全部进 `orphans` → `rebuild.rs:121-123` 逐个 `DeleteObject`。

触发序列（运维最普通的场景）：

1. 正常运行若干事务提交（对象都在后端，README：S3 是 source of truth）；
2. 磁盘损坏 / `rm -rf /var/lib/xtable/redb`；
3. 服务器重启，`main.rs:79` 的条件 `global_version == 0 && WAL 为空` 对新库恰好成立 → 触发 rebuild；
4. **后端桶里所有事务写入的对象被删除**。宣称"不丢数据"的机制执行了全量灭数据。

非事务写入的对象（txn-id 为空）逃过删除，但只被写进旧的 `TBL_VERSIONS` 表（`rebuild.rs:108`），MVCC 链不会重建（`TBL_VERSION_CHAINS` 无人填充）——即使数据侥幸存活，索引体系也是空的。

**PoC**：`poc3_cold_rebuild_annihilates_txn_objects`。

**修复方向**：孤儿删除必须以"本地存在该 txn 的记录且状态非 Committed"为前提；本地无任何记录时（空库重建）一律只索引、不删除。v1 更安全的做法是干脆不删孤儿，只告警。

---

### V2 恢复流程会删除"已发布"的提交（README 崩溃分析的漏风口）

**声称**：README "CommitTxn — exact order" 的崩溃分析枚举了三个崩溃点 (a)(b)(c)，并断言 "(c) Crash after step 8 → recovery sees `Committed`/`CommitResult` and does nothing."

**现实**：`Committed` WAL 记录在 **step 9** 才写（`coordinator.rs:311-319`），而版本链发布（原子性点）在 **step 8**（`coordinator.rs:306`）。README 的分析漏掉了 8 与 9 之间的窗口。精确的提交顺序是：

```
coordinator.rs:
  281  WAL Committing{upload_keys}        ← 上传全部成功后
  306  append_chain_entries_bulk(...)     ← 版本链发布（redb 已持久）
  311  WAL Committed                      ← 崩溃窗口在 306 与 311 之间
  320  TxnState.status = Committed
```

触发序列：

1. 事务上传全部成功（对象在后端），`WAL Committing` 已落盘；
2. `append_chain_entries_bulk` 成功——**发布点已过**；
3. 进程被 kill -9 / 断电 / `append_wal` 因磁盘满返回 Err（不需要真崩溃，一次 WAL 写失败即可）；
4. 重启 → `recovery.rs:94-103`：最后一条 WAL 是 `Committing`、TxnState 非终态 → 对每个 `upload_keys` 执行 `DeleteObject`（`recovery.rs:99`）；
5. 结果：**已发布的数据从后端消失**，而版本链（`recovery.rs` 的 `abort_txn_no_uploads` 不清链）仍保留条目——索引指向不存在的对象；TxnState 变 `Aborted`，与链上"已发布"并存，I2/I6/I7 同时被破坏。

`recovery.rs:63-67` 的注释写"versions_bump only happens AFTER ValidateOk + successful upload, so if no Committed record exists we can safely abort"——这句话恰好漏了 bump（链发布）之后、`Committed` 之前的那段。

**PoC**：`poc2_recovery_deletes_published_commit`。

**修复方向**：把"链发布"与"WAL Committed + TxnState=Committed"放进**同一个 redb 写事务**（这是唯一真正的原子性点）；恢复时对 `Committing` 状态的事务先查链上是否已有本 txn 的条目，有则补成 Committed，无才补偿。

---

### V3 补偿删除摧毁"之前已提交"的版本

**机制**：后端是普通 S3 语义（无版本化）。上传一律写**裸用户 key**（`coordinator.rs:453-459`；类型里的 `backend_key` 间接层从未被写路径使用）。任一 key 上传失败时，补偿对每个成功 key 执行 `delete_object(裸key)`（`coordinator.rs:268-270`）。

触发序列（无需崩溃，一次后端抖动即可）：

1. T0 提交 `k = "old"`（已提交、可见）；
2. T1 stage `k = "new"` 与 `j`；commit 时 `k` 上传成功（**覆盖了 "old"**），`j` 上传失败；
3. 补偿逻辑 `delete_object(k)` → **"old" 彻底消失**（覆盖已让它不可恢复，删除是第二次确认死亡）；
4. T1 返回错误、正确回滚；但 T0 的已提交数据被一笔无关事务的失败连带摧毁。版本链仍留着 T0 的条目 → 索引悬挂。

`recovery.rs:98-100` 的补偿删除同样如此。正确做法只能是 per-version 后端 key（把 `backend_key` 间接层真正用起来）或要求后端开版本化；在裸 key 上"delete 当回滚"在语义上就是错的。

**PoC**：`poc4_failed_commit_destroys_prior_committed_object`。

---

### V4 OCC 冲突检测在主场景下不存在

**声称**：README "Lost-update protection at commit: OCC validation rejects the second writer"；`MVCC_RELIABILITY.md` I5"恰有一个赢家"。

**现实**：OCC 校验读的是 `get_version` → `TBL_VERSIONS`（`coordinator.rs:213`）。全仓库写这张表的只有：非事务 S3 路径（`service.rs:176,306,346,575`）和 rebuild（`rebuild.rs:109`）。**事务提交路径只写 `TBL_VERSION_CHAINS`（`coordinator.rs:306`），从不写 `TBL_VERSIONS`。**

于是两个事务写同一个 key：双方的 `version_at_read` 都是 0（stage 时 `get_version` 为空，`coordinator.rs:117-122`），校验时 `current` 还是 0 → `0 == 0` 恒通过 → **第二个提交永远赢**，第一个的写入被静默覆盖，两个客户端都收到成功。所谓冲突检测只在"非事务写介入过该 key"时才可能触发——而事务 vs 事务恰恰是产品的核心卖点。README 声称 "The OCC validate phase is single-writer"，代码里没有任何支撑该声明的锁。

`TBL_VERSIONS` 与 `TBL_VERSION_CHAINS` 是两套互不同步的版本真相——这是 MVCC 半迁移留下的结构性断裂。

**PoC**：`poc1_occ_never_conflicts_between_two_txns`。

**修复方向**：单一版本真相源。校验读链尾 `commit_version`（或发布链的同一写事务里同步 `TBL_VERSIONS`），并删掉其中一套。

---

## 3. 严重级发现（原子性/隔离性承诺落空）

### V5 提交路径无锁：并发提交竞态

`TxnCoordinator` 除上传信号量外无任何锁（`coordinator.rs:42-48`）。"CAS" Active→Validating 实际是"读一次、写一次"两个独立 redb 写事务（`coordinator.rs:204-207`），不是原子 CAS。后果：

- **并发重复 commit 同一事务**：都读到 Active → 都通过 → 各自分配版本、各自上传、各自追加链。链条追加的读-改-写在写事务**之外**（`store.rs:461` 的 `read_chain` 与 `store.rs:472` 的 `with_write` 之间可插入他人），单调性检查跑在过期数据上（`store.rs:463`）→ 后写者**整体覆盖**前写者的链，前一个已提交事务的链条目**静默消失**，且无任何错误。
- **并发不同事务写同一 key**：两个 commit 的 OCC 校验都完成后才开始上传/发布，之间无任何互斥 → 与 V4 复合，两个赢家并存。
- **commit 与 stage 竞态**：`stage` 只在入口读一次状态（`require_active`，`coordinator.rs:115`），之后照写 write_set → 已进入 Validating 的事务还能被追加写入，OCC 校验已收集过的条目集合与实际发布内容不一致。

README 的状态机图里没有任何一步是原子的，"CAS" 只是注释里的愿望。

### V6 读路径直通后端："版本索引是门禁"是虚构

README 原子性论证的核心："the version index is the gate — readers consult `versions[k].latest_version`"。实现里**没有任何读者 consult 索引**：

- `GetObject`：事务头命中 staged body 则返回暂存值（`service.rs:194-202`），否则**直接 `backend.get_object`**（`service.rs:205`）；
- `ListObjectsV2`：直接 `backend.list_objects`（`service.rs:364-368`）。

后果：commit 期间（上传已开始、链未发布），任意非事务读者都能看到**部分提交**的对象集合——A 已上传、B 未上传时，List 返回 A 不返回 B。"多对象原子可见性"在读侧根本不存在。`service.rs:216-227` 对版本漂移只打一行 `warn!`，继续返回后端数据。

### V7 半提交对象永久泄漏 + 脏读（README 场景 (b) 与实现相反）

README 场景 (b)："Crash during step 6 (partial uploads) → recovery … issues DeleteObject for each recorded uploaded key"。实现里 `WAL Committing{upload_keys}` 在**所有上传成功之后**才写（`coordinator.rs:281`）。于是：

- 上传中途崩溃：最后一条 WAL 是 `ValidateOk` → `recovery.rs:89-93` 按"没有任何上传发生"处理 → **不执行任何补偿** → 已上传的半提交对象**永久留在后端**，且经 V6 的直通读路径对所有读者可见（脏读）；
- 恢复用 **WAL 推导的状态**而非 TxnState 判分支（`recovery.rs:88`）：若崩溃发生在 `coordinator.rs:263`（TxnState 已写 Committing）与 `:281`（WAL Committing）之间，同样落入"无上传"分支，同样不补偿。

即：**补偿删除该执行的时候不执行（本条），不该执行的时候执行（V2）**——两头的方向都反了。

### V8 MVCC 快照读是死代码

`read_at_snapshot` / `read_latest` 在整个 workspace 无任何服务层调用者（仅 storage 自测与 gc 测试使用）。事务读不过链（V6），非事务读也不过链。因此 `MVCC_RELIABILITY.md` 的 I3（快照隔离）论证的是一段**没有人调用的代码**；"Snapshot isolation (SI) for reads: Reads never see writes from txns that committed after BeginTxn"在实际服务路径上是虚构的——事务读到的就是后端当前值。同时 I1–I8 的"proptest 证明"证明的是 storage 原语在孤立调用下的性质，覆盖不到任何跨层行为（§8）。

### V9 快照 pin 无引用计数：活跃事务遭遇幻影删除

`register_snapshot` 是 `TBL_ACTIVE_SNAPSHOTS` 表按版本号的 `insert`（`store.rs:527-533`）。两个事务在同一 `global_version` 开启是**常态**（版本只在提交时前进）——它们注册同一个键；先结束的事务 `unregister`（`coordinator.rs:324/368`）把还在运行的事务的 pin 一起拔掉。随后 `min_active_snapshot` 升高（无 pin 时为 `u64::MAX`，`store.rs:546-559`），GC 剪掉旧条目（`store.rs:484-508`）→ 仍在运行的事务在快照 S 上读一个"事务开始时明明存在"的 key，返回 `None`。

当前因为 V8（没人用链读）而未显形；一旦把读路径接到链上（修 V6/V8 的必经之路），此 bug 立即变成线上"数据凭空消失"。`MVCC_RELIABILITY.md` I8 的 GC 安全性证明没有考虑共享快照。

**PoC**：`poc5_shared_snapshot_pin_stolen_by_first_committer`。

### V10 事务内 Delete 不是删除

`service.rs:272-289`：事务删除 = stage 一个空 body。commit 时 `upload_all` 把空 body 当普通对象上传（`coordinator.rs:433-436`）；`VersionEntry::tombstone()` 构造器**从未被 commit 使用**（`coordinator.rs:297-303` 用 `new()`，`deleted=false`）。结果：事务删除提交后，对象仍在后端，内容为 0 字节；链上无墓碑。另外 `DeleteObjects`（批量删除，`service.rs:317-352`）**完全无视事务头**，事务进行中调用即直接删后端。

**PoC**：`poc6_transactional_delete_writes_empty_object`。

### V11 Multipart 完全绕过事务

`create/upload/complete` 全部直打后端（`service.rs:468-583`），`MultipartState.txn_id` 恒为 None。事务中发起 multipart 上传 → complete 即刻公开可见 → 原子性承诺对大数据写入（恰恰是 multipart 存在的意义）作废。

---

## 4. 中等级发现（可用性 / 运维可靠性 / 一致性退化）

### V12 磁盘单调耗尽（三处泄漏叠加）

1. **提交路径 spill 文件永不删除**：`coordinator.rs:330-337` 先 `delete_blob(handle)` 再 `get_blob(handle)`（必然 None）→ `remove_file` 永不执行。对照 abort 路径（`coordinator.rs:354-362`）顺序是对的——只有成功提交路径会把 spill 文件永久留在磁盘。
2. **inline write-set 行永不删除**：`coordinator.rs:329-337` 只对 `body_handle.is_some()` 的条目调 `delete_write_entry`，小对象（≤256KiB，绝大多数）的 write_set 行永远留在 `TBL_WRITE_SET`。
3. **WAL 只增不减**：`TBL_WAL` 无任何截断逻辑，每笔事务至少 4-5 条记录永久累积；`TxnState` 同样永不清理。

三者叠加：长期运行的服务磁盘与恢复扫描（`iter_wal` 全量载入内存）单调恶化，磁盘满时（结合 V2）WAL 写失败还会触发已发布数据被恢复删除。

### V13 恢复/GC 泄漏快照 pin

`recovery.rs:119-150`（`abort_txn_no_uploads`）与 `gc.rs:41-62`（`abort_txn_local`）都不调用 `unregister_snapshot`（对比 `coordinator.rs:368` 的显式 abort 有）。每次崩溃恢复或 GC 清理都泄漏一批持久化的 pin → `min_active_snapshot` 永久偏低 → `gc_chains` 永远剪不动（与 V12 相反方向的另一处无限增长）。

### V14 冷重建静默跳过

`rebuild.rs:39-45`：后端 list 失败 → `return Ok(RebuildReport::default())`。服务器带着**空索引与 global_version=0** 正常上线：新提交从 v1 分配、覆盖后端已有更高版本的对象（版本回退），下一次重建的 `max_v` 被污染。"redb 丢失 + 后端瞬时不可达"这一最常见的复合故障，得到的是一次成功返回的空重建。

### V15 认证零接线

`EdgeAuth` 在 `AppState` 构造后（`app.rs:33-36`）无任何调用；`build_router`（`main.rs:148-180`）没有 auth 中间件；事务路由与 S3 路由全部裸奔；默认凭据是 `xtableadmin/changeme`（`config.rs:69-70`）。SigV4 验证代码（`xtable-auth/verify.rs`，180 行）存在但零使用。对"可靠性"的含义：任何能到端口的人都可以提交、中止、删除事务与对象。

### V16 `version_at_read` 语义错误（write skew 放大）

`coordinator.rs:117-122` 在 **stage 时刻**读当前版本当作 `version_at_read`，而非事务的 `snapshot_version`（`MVCC_RELIABILITY.md` I5 明确声称是后者）。并发提交发生在 begin 与 stage 之间时，`version_at_read` 已是新值 → OCC 对"事务开始后他人已提交"这一事实失明。同一事务内重复 stage 同一 key 也会刷新 `version_at_read`，进一步稀释校验意义。

### V17 测试基建：后端指向死端口

`client.rs:260-274`：`dummy_for_test_async` 连 `http://127.0.0.1:1`。所有以它为后端的单测（coordinator/recovery 单测）中，每个后端操作都**必然失败**，而所有调用点都是 `let _ = backend.delete_object(...)` 之类。也就是说：**恢复的补偿删除、上传失败的补偿，从未在任何单元测试中真正执行过**——这不是"测试不足"，是"测试结构上不可能发现 V2/V3"。

### V18 HTTP 事务路径从第二笔起不可用（且测试全部绕过）

`service.rs:144` 把 `current_global_version()` 作为 stage 的 threshold 传入；`coordinator.rs:123` 的检查是 `version_at_read < threshold → 400`。由于事务提交从不写 `TBL_VERSIONS`（V4），从 global_version≥1 起，任何 `get_version` 为空或偏低的 key（即几乎所有 key）的 stage 都被拒绝。**README 快速上手里的第二条事务就会失败。** 而 63 个测试全绿，是因为 `integration_e2e.rs` 里所有 `stage` 调用 threshold 一律传 **0**（`:302,:334,:363,:386,:425-426,:467`），从未以 HTTP 层的真实参数执行过。

**PoC**：`poc7_http_layer_rejects_every_txn_after_the_first`。

---

## 5. 次要问题（列表）

- `complete_multipart` 等非事务路径"先上传后索引"，索引写失败时客户端已收到成功语义的前半（`service.rs:559-576`）。
- `GetObject` 的 `last_modified` 用 `SystemTime::now()`（`service.rs:234-236`）、`content_type` 硬编码 `application/octet-stream`（`:237`）——返回错误的元数据。
- `ListObjectsV2` 的 `max_keys` 回填为实际返回数（`service.rs:424-426`），continuation 语义用 key 名冒充 token（`:416-420`），与 S3 语义不符。
- 事务元数据键自带 `x-amz-meta-` 前缀又被 SDK 再加一层（`coordinator.rs:446-451`），靠双重前缀往返对称才碰巧可用；对真实 AWS S3 的冷重建依赖这个巧合。
- `idempotency_key` 只存不查（`coordinator.rs:90-97`），README 声称的 `x-xtable-idempotency-key` 去重未实现。
- bincode 序列化无 schema 版本号，存储格式演进无迁移路径。
- `stage`/`read` 不刷新 `last_heartbeat`（README 声称 "heartbeat (any op)"），长事务会被 60s GC 误杀（`gc.rs:26`）。
- `begin` 路由返回的 `x-xtable-snapshot-version` 重新查一次 `current_global_version`（`main.rs:197`），与 begin 内部取值之间存在漂移窗口。

---

## 6. README 声明 vs 实现对照

| README 声明 | 实现现实 | 编号 |
|---|---|---|
| "Crash-safe commit ordering — recovery never produces a half-published multi-object state" | 读路径直通后端：上传一半即可见；恢复会删掉已发布数据；半提交对象无人清理 | V2/V6/V7 |
| "`redb` destroyed → No data lost" | 冷重建删除所有事务写入的对象 | V1 |
| "Optimistic concurrency control with per-key version checking / Lost-update protection" | 事务 vs 事务永不冲突（校验读的表从不更新） | V4 |
| "The OCC validate phase is single-writer" | 提交路径无任何锁 | V5 |
| "Snapshot isolation (SI) for reads" | 快照读是死代码，读的是后端当前值 | V8 |
| "Disaster recovery — version index can be cold-rebuilt" | 重建只填旧表、不填链，且先删光事务对象 | V1 |
| "Idempotent commit"（`x-xtable-idempotency-key`） | 幂等键只存不查；中途状态一律保守报错 | §5 |
| "Total: 63 tests passing" | 测试参数/路径与 HTTP 层系统性脱节（§8） | V17/V18 |

---

## 7. 对崩溃安全论证（README "Why this ordering is crash-safe"）的结构性反驳

README 枚举 (a)(b)(c) 三个崩溃点，均以"版本索引未 bump，读者看不到"为兜底。该兜底有两个致命前提，实现里都不成立：

1. **"读者以索引为门禁"** —— 假（V6）：读者直通后端。于是 (b) 的"读者从未看到部分状态"为假；上传期间的部分状态对全世界可见。
2. **"崩溃点只有三个"** —— 假：真实序列里 `Committing` WAL（`:281`）→ 链发布（`:306`）→ `Committed` WAL（`:311`）→ TxnState（`:321`）是四段独立的持久化动作，中间任何一段失败都产生 README 未分类的状态；其中"链已发布、Committed 未写"这一段会被恢复流程主动销毁数据（V2）。

一个正确的实现只需要一个不可分割的原子性点：**"链发布 + Committed 标记"必须在一个 redb 写事务内**，上传补偿必须能区分"裸 key 上的本事务版本"与"先前已提交版本"（per-version backend key），读路径必须以链为门禁。当前实现三者皆无。

---

## 8. 对"测试证据"的审查：63 个绿灯为什么不说明任何问题

1. **参数脱节**：e2e 测试所有 `stage(..., 0)` 绕过了 HTTP 层真实传入的 `current_global_version`（V18）——生产事务路径（README 快速上手示例本身）从第二笔事务起就会 400，而没有任何测试覆盖。
2. **旗舰测试没有测它声称的东西**：`e2e_occ_conflict_one_winner`（`integration_e2e.rs:394-441`）**从未提交第二个事务**，也没有断言任何 409。它手工往 `TBL_VERSIONS` 塞了一条生产路径永远不会写的种子记录（`:412-420`，这是让 OCC 校验有意义的唯一方式），然后只断言两个 write_set 里都是 0。注释原文承认："We can't trigger that here without a real upload"。README 测试证据表对它的描述（"first wins, second would get 409"）与测试内容不符。
3. **后端死端口**：单测后端指向 `127.0.0.1:1`（V17），所有补偿/恢复删除从未真正执行过。
4. **proptest 证明的是孤立原语**：`proptest_invariants` 与 `mvcc_invariants` 驱动的是 storage/coordinator 原语的直接调用，覆盖不到"HTTP 层传参 → coordinator → 恢复/重建"的跨层组合——而全部灾难级发现都在跨层组合里。
5. **零 HTTP 层事务测试**：`/?transactional=*` 四条路由没有任何测试。

结论：测试套件验证了一个与生产不同的系统。

---

## 9. PoC 使用说明

文件：`xtable-backend/tests/reliability_attack.rs`（7 个 `poc_*` 测试，见文件头与各测试内注释；除 poc5 为纯存储层外，全部走真实的 coordinator/recovery/rebuild 路径与带故障注入的 mock S3）。

设计原则：**断言破坏性结果确实发生——测试通过 = 漏洞复现**。修复对应漏洞后断言应反转（每处已注明正确预期）。运行：

```bash
cargo test -p xtable-backend --test reliability_attack
```

⚠️ 本审计环境无 Rust 工具链，PoC 未执行；其断言基于对公开 API 的静态调用分析，无竞态/时序依赖。若某个 PoC 失败，优先怀疑 mock 与真实 SDK 的元数据往返细节，而不是结论本身——每条发现的行号证据独立于 PoC 成立。

---

## 10. 修复优先级建议

按"数据安全收益 / 工程量"排序：

1. **止血（改判定的方向，量小）**
   - V1：rebuild 空库时不删任何孤儿（删 `rebuild.rs:74` 的删除分支即可显著降低灾难面）；
   - V2：链发布与 `Committed`/TxnState 合并为单个 redb 写事务；恢复时链上有本 txn 条目即补 Committed；
   - V3：在引入 per-version backend key 之前，**禁止对"上传成功的 key"做补偿删除**（宁可留孤儿交给对账，也不删先前版本）。
2. **补上互斥（V5）**：per-txn `Mutex` + 提交临界区；`append_chain_entries_bulk` 的读-改-写移入同一写事务。
3. **统一版本真相（V4/V18）**：校验读链尾；同步或删除 `TBL_VERSIONS`；stage 的 threshold 语义修正（用 `snapshot_version`）。
4. **读路径接链（V6/V8/V9）**：GetObject/List 以链为门禁；快照注册表加引用计数。
5. **语义补全（V10/V11）**：tombstone 贯通；multipart 挂进事务。
6. **运维面（V12-V15）**：spill/write-set/WAL 清理、pin 泄漏、rebuild 失败拒绝启动、auth 接线。

---

## 11. 总结论

xtable 的 README 是一份写得相当好的正确性论证——问题在于实现是另一份答卷。读、写、恢复三套机制各自都"看起来对"，但它们**彼此没有连接**：读者不看索引、校验不看链、恢复不看发布点、重建不认已提交。在这个结构下：

- **不需要任何并发**，一次普通的"redb 丢失 + 重启"就会全量删除事务数据（V1）；
- **不需要崩溃**，一次后端抖动就会摧毁先前已提交的版本（V3），第二笔事务起 HTTP 层干脆不可用（V18）；
- **需要一次普通的进程退出**，落在发布点之后的窗口里，恢复就会亲手删掉刚发布的数据（V2）。

在上述结构问题修复之前，任何关于 ACID、崩溃安全、灾难恢复的表述都应从 README 中移除，或明确标注为未实现的设计意图。
