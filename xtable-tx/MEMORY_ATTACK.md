# xtable 内存效率红队攻击报告

> **审计性质**：红队视角的内存效率攻击（memory-efficiency red team）。目标：证明攻击者（或普通负载）能以远小于服务端消耗的代价，把 xtable-server 的内存打爆或打入慢性退化；同时指出日常路径上的内存浪费。
>
> **审计日期**：2026-08-29。**对象**：工作区当前代码（含 2026-08-29 凌晨的 V1/V2/V3/V4/V9/V10/V18 修复版）。
>
> **证据等级**：⚠️ 本机无 Rust 工具链，无法编译 PoC；但 **M1/M2 的核心数字是活体实测**——用 `target/debug/xtable`（02:21 构建）起了真实服务器，用 `curl` 发起攻击，用内核 `/proc/<pid>/status` 的 `VmHWM`（RSS 高水位）计量。其余发现为行号级静态推理。

---

## 0. 实测结果（先看弹药）

| 攻击 | 输入 | 服务端内存峰值（VmHWM） | 放大倍数 |
|---|---|---|---|
| 1 个未认证 PUT（chunked，无 Content-Length） | 512 MB | **1066 MB** | **2.08×** |
| 3 个并行同款 PUT | 3 × 512 MB | **3116 MB** | **2.03×/请求，线性叠加** |
| 攻击后回落 | — | RSS 回到 45 MB | 纯瞬时尖峰 |

环境：本机 31.7 GB 内存（峰值余量充足，未触发 OOM）。**外推**：8 GB 内存的常规部署，`for i in $(seq 8); do head -c 512M /dev/zero | curl -X PUT --data-binary @- http://xtable/bucket/x & done` 一行 shell（无需认证凭据，见前次审计 V15：认证零接线）即可让内核 OOM-killer 处决 xtable，顺带带走同机所有进程的稳定性。单个 4 GB chunked PUT 同样致命（M1 无上限）。

**结论：这是一个"请求体多大，内存就多大，再乘 2"的服务器，且入口没有任何闸门。**

---

## 1. 攻击面总表

| # | 一句话 | 峰值公式 | 位置 |
|---|---|---|---|
| M1 | PUT/UploadPart **全量缓冲请求体**，无上限、无长度校验 | `≈2×body` | `service.rs:109-125, 504-514` |
| M2 | 入口**无并发限制**，攻击线性叠加 | `N×2×body` | 全局（无 ConcurrencyLimit/信号量） |
| M3 | GET 路径**双份全量拷贝**，无界并发 | `2×obj/GET` | `client.rs:155-164`、`service.rs:229` |
| M4 | 提交时**一次性物化所有 staged body**（clone + 同步读盘） | `Σ(key_i 的 body)` | `coordinator.rs:419-463` |
| M5 | stage/读回路径的**多重拷贝**（clone + redb 序列化） | `≈3×body/阶段` | `coordinator.rs:129-163, 390-407` |
| M6 | GC 每 60 秒**全量物化所有版本链** | `O(全部链数据)` | `store.rs:511-522`、`gc.rs:66-70` |
| M7 | 启动/恢复**全量载入 WAL**（且载入两次），WAL 无截断 | `O(WAL 总量)` | `store.rs:197-210`、`main.rs:79`、`recovery.rs:30-34` |
| M8 | ListObjectsV2 **全桶物化**再内存过滤 | `O(bucket 对象数)` | `client.rs:393-425`、`service.rs:377-433` |
| M9 | 版本链**整链单值存储**：每次 append 重序列化整链 | `O(链长)/次提交` | `store.rs:453-479` |
| M10 | 内存/容量**闸门全是死配置**（`max_staged_bytes` 零引用；阈值硬编码） | — | `config.rs` vs `coordinator.rs:129` |

---

## 2. 详细发现

### M1 单请求无上限全量缓冲（实测 2.08×）—— 最锋利的刀

```rust
// service.rs:109-125 (put_object)，upload_part 同款（:504-514）
let mut bytes_vec: Vec<u8> = Vec::new();
let mut stream = body;
while let Some(chunk) = stream.next().await {
    ...
    bytes_vec.extend_from_slice(&chunk);
}
```

- 没有 `DefaultBodyLimit` / `RequestBodyLimitLayer`（全仓库 grep 为空）。注意 axum 的默认 2 MB body 限制**只作用于 axum 自己的提取器**（`Bytes`/`Json`）；S3 请求走 `fallback_service` 里的 s3s tower 服务，直接消费 `http::Body` 流，绕过该默认限制。
- chunked 传输（无 Content-Length）同样全收——连"先检查长度再决定"的借口都不存在。
- 实测 512 MB 请求 → 1066 MB 峰值：`Vec` 倍增扩容 + hyper 缓冲 + 后续 `put_object(bytes_vec.clone(), ...)`（`service.rs:160-163`，再克隆一份交给后端上传）。
- 事务路径更贵：body 先全量进内存，再 `body.clone()` 存 inline（≤256 KiB 时进 redb，见 M5）或写 spill。

**修复**：`tower_http::limit::RequestBodyLimitLayer` + 在 s3s 层做 Content-Length 预检（超出 413）；PUT 转发改流式（`ByteStream` 直通后端，见 M11 模式）。

### M2 无并发限制：线性叠加（实测 3×512 MB → 3116 MB）

入口对同时进行的请求**没有任何数量或字节预算控制**：唯一的信号量是提交上传的 `commit_upload_concurrency=16`（`coordinator.rs:68`），它管不到 HTTP 入口。实测 3 并发峰值精确为单发的 3 倍（1039 MB/发）。GET 路径同样无界（M3）。也就是说内存峰值 = 并发数 × 2 × 请求体大小，攻击者全权决定前两项。

**修复**：按**字节预算**的全局/每请求信号量（不是按请求数——1 个 4 GB 请求与 4000 个 1 MB 请求是同一笔账）；`tower::limit::ConcurrencyLimitLayer` 兜底。

### M3 GET 双份拷贝 + 无界并发读

```rust
// client.rs:161-164
let body = resp.body.collect().await...;   // 第 1 份：SDK 聚合
let bytes = body.into_bytes().to_vec();    // 第 2 份：to_vec 整体复制
```

`into_bytes()` 已是 `Bytes`（零拷贝视图），`.to_vec()` 是纯粹多余的深拷贝；随后 `service.rs:229` `Bytes::from(r.bytes)` 只是所有权转移。一个 5 GB 对象的单次 GET 峰值 ≈ 10 GB。攻击者只需先把大对象放进桶（经 M1 或直写后端），然后并行 GET。

**修复**：删掉 `.to_vec()`，`ByteStream` 从响应直通（`client.rs` 返回 `ByteStream`，`service.rs` 用 `StreamingBlob::from_stream`），彻底消除聚合。

### M4 提交路径一次性物化全部 staged body

```rust
// coordinator.rs:419-463 (upload_all)
for (key, entry) in write_entries {
    let body = inline.clone()... 或 std::fs::read(&rec.path)...;   // ← 构建期全量物化
    futures.push(async move { ... });   // ← 所有 future 连同 body 一起被 FuturesUnordered 持有
}
```

- 信号量在 future **内部**获取——它限制 S3 并发，**不限制内存**：所有 K 个 body 在循环结束时已全部驻留堆中。
- 一笔事务 stage 1000 个 256 KiB 对象 = 提交瞬间 256 MB 堆尖峰（外加 redb 里的 inline 副本与 WAL/写集记录）；stage 上 GB 数据（spill 文件）则提交时全量读入。
- 附赠：`std::fs::read`（同步 IO）跑在 async 上下文里，大 spill 会阻塞 tokio worker，多笔并发提交可把整个 runtime 卡死（配合无背压的入口，请求继续堆积——内存雪崩的完整闭环）。

**修复**：body 物化移进 future 内、许可获取之后；spill 用 `tokio::fs::read`；更彻底的是上传流式化。

### M5 stage/读回的多重拷贝

`coordinator.rs:157` `inline_body: Some(body.clone())`（堆上一份 + clone 一份）；写 redb 时 bincode 再序列化一份。事务内 GET（`stage_body`，`coordinator.rs:390-407`）：inline `clone()` 全量拷贝，spill `tokio::fs::read` 全量读入。一个 256 KiB 的 staged 对象在生命周期里至少被完整复制 4 次（HTTP 缓冲 → clone → bincode → 读回 clone）。

### M6 GC 周期性全索引尖峰

```rust
// store.rs:511-522 iter_all_chains —— 全部链反序列化进 Vec
// gc.rs:66-70 —— 每 gc_interval_secs（默认 60s）调用一次
```

GC 每分钟把**所有 key 的完整版本链**物化进堆（每条 entry 含 etag/backend_key/txn_id/user_meta 多个堆分配）。估算：100 万 key × 平均 5 版本 × ~250 B ≈ **1.2 GB 的周期性堆尖峰**，伴随等量临时分配（bincode 反序列化）——每分钟一次 GC 风暴，allocator 压力与 RSS 双高。链越长（V13 pin 泄漏让 GC 剪不动旧版本）尖峰越大：这是一条**随运行时间恶化**的曲线。

**修复**：redb 范围迭代分批处理（每批 N 条链），只写回有改动的链；长期方案见 M9。

### M7 启动全量载入 WAL，且载入两次

`store.rs:197-210` `iter_wal()` 把整张 WAL 表反序列化成 `Vec<(u64, WalRecord)>`（全部 owned String）；`recovery.rs:30-34` 再为每个 txn 建 3 个 HashMap。更糟的是 `main.rs:79` 的重建条件判断 `store.iter_wal().unwrap().is_empty()` **又完整载入一遍**。WAL 无截断（前次审计 V12）→ 每笔事务 4-5 条记录永久累积 → **运行越久，重启越难**：按 100 txn/s、每条 ~300 B 估，一天 ≈ 10 GB WAL，重启即 OOM。这是"定时炸弹"型缺陷：平时无感，崩溃恢复（最需要它可靠的时刻）恰好引爆。

**修复**：`last_wal_seq` 判断空表即可（O(1)）；恢复改流式扫描；WAL 加检查点/截断。

### M8 ListObjectsV2 全桶物化

`client.rs:393-425` 把所有分页收进单个 `Vec<ListedObject>`（key/etag 全是 String），`service.rs:377-433` 再在内存里做 prefix/delimiter/pagination 过滤。桶里 1 亿对象 ≈ 数 GB 内存，且每次 LIST 都来一遍、可并行刷。正确形态是后端分页参数直通（prefix/start-after/max-keys 下推），流式输出。

### M9 版本链整链单值：append = 重写全链

`TBL_VERSION_CHAINS` 以 key 为键、**整条链序列化为一个 value**（`store.rs:453-479`）：每次提交追加一个版本 = 读整链 + 内存里 push + 整链重新序列化 + 整链重写。热 key 攒到 1 万版本后，每次 put 的临时分配与写放大都是 O(万 × entry)。GC 修剪同样整链重写。这与 M6 叠加构成 GC 尖峰的主体。

**修复**：改为 `(key, commit_version)` 复合键每 entry 一行——append O(1)、读 latest 用 `range(..).next_back()`、GC 按范围 `drain`。这一改动同时治好 M6 的大半。

### M10 闸门全是死配置

- `max_staged_bytes`（默认 100 GiB）：**全仓库零引用**——配置文件里看起来有容量上限，代码里不存在。
- `staged_body_threshold_bytes`：仅出现在注释（`blob.rs:4`）；真实阈值硬编码 `256 * 1024`（`coordinator.rs:129`），改配置无效。
- `multipart_threshold/part_size`：构造进 `Inner` 后无消费者。

运维界面承诺的每一道内存/容量闸门都没有接执行机构。

---

## 3. 攻击剧本（红队交付物）

| # | 剧本 | 代价 | 效果 |
|---|---|---|---|
| S1 | 单请求 OOM：`head -c 4G /dev/zero \| curl -X PUT --data-binary @- http://xt/b/k` | 1 条命令 | 峰值 ≈8 GB，OOM-kill（无需认证） |
| S2 | 并发耗尽：8 × 512 MB PUT 并行 | 一行 for 循环 | 实测线性叠加（3 发已 3.1 GB），8 GB 机器即死 |
| S3 | 读放大：预置大对象后并行 GET | N 个 curl | 每 GET 2× 对象大小（M3） |
| S4 | 提交炸弹：1 个事务 stage 数千对象后 commit | 一次事务 | 提交瞬间全量物化 + 同步读盘阻塞 runtime（M4） |
| S5 | 慢性窒息：正常跑数日 → 重启 | 什么都不做 | WAL/链无限增长让 recovery/GC 尖峰越拖越高（M6/M7） |
| S6 | 列表风暴：并行 ListObjectsV2 | curl 循环 | 每调用 O(bucket)（M8） |

S1/S2 **已活体验证**（§0 数据）；S3-S6 为行号级推演，机理与 S1 同源（无界物化 × 无并发控制）。

---

## 4. 修复优先级

1. **入口闸门（治 S1/S2）**：`RequestBodyLimitLayer` + Content-Length 预检 + 按字节预算的并发准入；拒绝时 413/503。
2. **流式化三处（治 M1/M3/M5 大半）**：PUT 转发、GET 返回、事务上传全部改 `ByteStream` 直通，消灭 `collect/to_vec/clone`。
3. **链存储改行式（治 M9，顺带 M6）**：每 entry 一行的表结构，append O(1)，GC 范围删除。
4. **批量处理（治 M4/M6/M7/M8）**：提交物化移入许可内、GC/恢复/LIST 分批 + WAL 检查点。
5. **接上死配置（治 M10）**：`max_staged_bytes` 真实执法，阈值从配置读取。

---

## 5. 结论

xtable 的内存模型是"**入口即全量物化**"：每个请求体、每个对象、每条链、每条 WAL 记录，都至少有一次完整的堆驻留，多数路径还有 2-3 倍拷贝放大；而所有本该兜底的结构（body limit、并发预算、容量上限、GC 边界）要么缺失要么是死配置。攻击者用一个 curl 管道就能以 2 倍效率兑换服务端内存，无需任何凭据。

叠加前次可靠性审计的结论：**这个系统当前既不防丢数据，也不防耗内存**——数据路径会自毁（V1-V4），资源路径会被一根管道打穿（M1/M2）。两类问题的根因同构：边界检查全部缺席，正确性/安全性论证停留在 README 层面。
