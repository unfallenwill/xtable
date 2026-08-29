//! 对抗性可靠性审计 PoC（reliability attack proofs of concept）。
//!
//! ⚠️ 审计环境没有 Rust 工具链，本文件**未被执行**，仅静态编写。
//!    所有断言都针对"当前实现的真实行为"编写：测试通过 = 漏洞复现成功。
//!    修复对应漏洞后，断言方向应反转（文件内每处均有注释说明正确预期）。
//!
//! 每个测试对应 RELIABILITY_ATTACK.md 中的一个编号发现：
//!   poc1 → V4  事务 vs 事务的 OCC 冲突检测完全失效（lost update 静默发生）
//!   poc2 → V2  崩溃窗口（链已发布、WAL 未写 Committed）→ 恢复流程删掉已发布数据
//!   poc3 → V1  冷重建把所有事务写入对象判为孤儿并全部删除（"灾难恢复"= 全量灭数据）
//!   poc4 → V3  上传失败补偿 delete 会摧毁该 key 上"之前已提交"的数据
//!   poc5 → V9  共享快照 pin 被先提交者拔掉 → 活跃事务读已提交数据返回"不存在"
//!   poc6 → V10 事务内 Delete 不是删除：commit 时把 0 字节对象 Put 上后端
//!   poc7 → V18 HTTP 层 stage 传 current_global_version 作为 threshold，
//!              从第二笔事务起所有事务写入被 400 拒绝（测试全部传 0 绕过了此检查）
//!
//! 注意：除 poc7 外，所有 stage 调用 threshold 一律传 0——这正是现有
//! integration_e2e.rs 的做法（它因此绕过了 HTTP 层的真实参数），这里的目的是
//! 走到更深的提交/恢复/重建路径去复现那些漏洞。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::extract::{Query, State};
use axum::http::{Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;
use xtable_backend::BackendClient;
use xtable_core::headers::TxnStatus;
use xtable_core::ObjectKey;
use xtable_storage::{LocalStore, VersionEntry, WalRecord};
use xtable_tx::{gc, rebuild, recovery, TxnCoordinator};

// =========================================================================
// 带故障注入的 mock S3 后端
// =========================================================================

#[derive(Clone, Default)]
struct AttackMock {
    objects: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    /// 原始 x-amz-meta-* 头（与真实 S3 一样按对象存储，HEAD/GET 原样返回）。
    meta: Arc<Mutex<HashMap<String, HashMap<String, String>>>>,
    /// 以该前缀开头的 key 的 PUT 一律返回 503（模拟后端局部故障）。
    fail_put_prefix: Arc<Mutex<String>>,
}

impl AttackMock {
    fn set_fail_put_prefix(&self, prefix: &str) {
        *self.fail_put_prefix.lock().unwrap() = prefix.to_string();
    }

    fn contains(&self, key: &str) -> bool {
        self.objects.lock().unwrap().contains_key(key)
    }

    fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.objects.lock().unwrap().get(key).cloned()
    }

    fn keys(&self) -> Vec<String> {
        self.objects.lock().unwrap().keys().cloned().collect()
    }
}

async fn attack_s3_server() -> (String, AttackMock) {
    let mock = AttackMock::default();
    let state = mock.clone();

    async fn root_handler(
        State(s): State<AttackMock>,
        method: Method,
        uri: Uri,
        headers: axum::http::HeaderMap,
        Query(params): Query<HashMap<String, String>>,
        body: axum::body::Bytes,
    ) -> Response {
        let path = uri.path().to_string();
        let trimmed = path.trim_start_matches('/');
        let (bucket, key) = match trimmed.find('/') {
            Some(i) => (&trimmed[..i], trimmed[i + 1..].to_string()),
            None => (trimmed, String::new()),
        };
        let _ = bucket;

        // GET /bucket → ListObjectsV2
        if key.is_empty() && method == Method::GET {
            let objs = s.objects.lock().unwrap();
            let mut keys: Vec<String> = objs.keys().cloned().collect();
            keys.sort();
            let xml = format!(
                r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><IsTruncated>false</IsTruncated>{}</ListBucketResult>"#,
                keys.iter()
                    .map(|k| format!(
                        "<Contents><Key>{}</Key><Size>{}</Size><ETag>\"e\"</ETag></Contents>",
                        k,
                        objs.get(k).map(|v| v.len()).unwrap_or(0)
                    ))
                    .collect::<Vec<_>>()
                    .join("")
            );
            return (StatusCode::OK, [("content-type", "application/xml")], xml).into_response();
        }

        let mut meta = HashMap::new();
        for (k, v) in headers.iter() {
            let name = k.as_str().to_ascii_lowercase();
            if name.starts_with("x-amz-meta-") {
                meta.insert(name, v.to_str().unwrap_or_default().to_string());
            }
        }

        match method.as_str() {
            "PUT" => {
                let fail = s.fail_put_prefix.lock().unwrap().clone();
                if !fail.is_empty() && key.starts_with(&fail) {
                    return (StatusCode::SERVICE_UNAVAILABLE, "injected failure").into_response();
                }
                s.objects.lock().unwrap().insert(key.clone(), body.to_vec());
                s.meta.lock().unwrap().insert(key, meta);
                (StatusCode::OK, "").into_response()
            }
            "GET" => {
                let objs = s.objects.lock().unwrap();
                match objs.get(&key) {
                    Some(bytes) => {
                        let mut b = axum::http::Response::builder()
                            .status(200)
                            .header("content-length", bytes.len());
                        for (k, v) in s.meta.lock().unwrap().get(&key).cloned().unwrap_or_default() {
                            b = b.header(k, v);
                        }
                        b.body(axum::body::Body::from(bytes.clone())).unwrap().into_response()
                    }
                    None => (StatusCode::NOT_FOUND, "not found").into_response(),
                }
            }
            "HEAD" => {
                let objs = s.objects.lock().unwrap();
                if !objs.contains_key(&key) {
                    return (StatusCode::NOT_FOUND, "not found").into_response();
                }
                let len = objs.get(&key).map(|v| v.len()).unwrap_or(0);
                let mut b = axum::http::Response::builder().status(200).header("content-length", len);
                for (k, v) in s.meta.lock().unwrap().get(&key).cloned().unwrap_or_default() {
                    b = b.header(k, v);
                }
                b.body(axum::body::Body::empty()).unwrap().into_response()
            }
            "DELETE" => {
                s.objects.lock().unwrap().remove(&key);
                s.meta.lock().unwrap().remove(&key);
                (StatusCode::NO_CONTENT, "").into_response()
            }
            _ => (StatusCode::NOT_FOUND, "unmatched").into_response(),
        }
    }

    let app = Router::new().fallback(any(root_handler)).with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let url = format!("http://{}", addr);
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    (url, mock)
}

async fn build_backend(endpoint: &str) -> BackendClient {
    BackendClient::build(
        endpoint, "us-east-1", "xtable-data",
        "test", "test", true, 5_000,
        16 * 1024 * 1024, 16 * 1024 * 1024,
    ).await.unwrap()
}

async fn setup() -> (TxnCoordinator, LocalStore, Arc<BackendClient>, AttackMock, tempfile::TempDir) {
    let (endpoint, mock) = attack_s3_server().await;
    let backend = Arc::new(build_backend(&endpoint).await);
    let tmp = tempfile::TempDir::new().unwrap();
    let store = LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap();
    let coord = TxnCoordinator::new(
        Arc::new(store.clone()),
        Arc::clone(&backend),
        tmp.path().join("staged"),
        4,
    );
    (coord, store, backend, mock, tmp)
}

/// threshold=0 与 integration_e2e.rs 的做法一致（绕过 HTTP 层真实传参，见 poc7）。
/// V10/V18 fix: the `deleted` flag (last arg) was added and threshold was removed.
async fn stage(coord: &TxnCoordinator, txn: &str, key: &str, body: &[u8]) {
    coord
        .stage(txn, &ObjectKey::new(key), body.to_vec(), None, HashMap::new(), false)
        .await
        .expect("stage");
}

// =========================================================================
// PoC 1 — OCC 在"事务 vs 事务"场景下永远不冲突（V4）
// =========================================================================

#[tokio::test]
async fn poc1_occ_never_conflicts_between_two_txns() {
    let (coord, store, _backend, mock, _tmp) = setup().await;

    let t1 = coord.begin(None).await.unwrap();
    let t2 = coord.begin(None).await.unwrap();
    stage(&coord, &t1, "k", b"A").await;
    stage(&coord, &t2, "k", b"B").await;

    coord.commit(&t1).await.unwrap();

    // 正确行为（README "Lost-update protection"、MVCC_RELIABILITY I5）：
    // 第二个提交必须返回 409 Conflict。
    // 实际行为：Ok —— 因为 OCC 校验读的 TBL_VERSIONS 表在事务提交路径上
    // 从不更新（coordinator 只写 TBL_VERSION_CHAINS），双方 version_at_read
    // 恒为 0，校验恒通过。
    let second = coord.commit(&t2).await;
    assert!(second.is_ok(), "V4 复现：两个并发写者都赢了，无任何冲突上报");

    // 根因证据：OCC 校验所依据的版本表里，这个 key 根本不存在。
    assert!(store.get_version(&ObjectKey::new("k")).unwrap().is_none(),
        "V4 复现：TBL_VERSIONS 从未被事务提交更新，OCC 校验建立在空表上");

    // 后果：t1 已提交的 "A" 被 t2 静默覆盖（lost update），两个客户端都收到成功。
    assert_eq!(mock.get("k").unwrap(), b"B");
    let chain = store.read_chain("k").unwrap();
    assert_eq!(chain.entries.len(), 2, "两个写者都成了赢家，同时进入版本链");
}

// =========================================================================
// PoC 2 — 崩溃在"链已发布、WAL 未写 Committed"窗口 → 恢复删除已发布数据（V2）
// =========================================================================

#[tokio::test]
async fn poc2_recovery_deletes_published_commit() {
    let (coord, store, backend, mock, _tmp) = setup().await;

    let txn = coord.begin(None).await.unwrap();
    stage(&coord, &txn, "k", b"payload").await;

    // 手工构造 coordinator.rs 执行到第 306 行（append_chain_entries_bulk 已落盘）
    // 之后、第 311 行（WAL Committed）之前的崩溃现场。
    store.append_wal(&WalRecord::ValidateOk {
        txn_id: txn.clone(),
        write_keys: vec!["k".into()],
    }).unwrap();
    store.append_wal(&WalRecord::Committing {
        txn_id: txn.clone(),
        upload_keys: vec!["k".into()],
    }).unwrap();
    let mut ts = store.get_txn_state(&txn).unwrap().unwrap();
    ts.status = TxnStatus::Committing;
    ts.uploaded_keys = vec!["k".into()];
    ts.alloc_versions = vec![("k".into(), 1)];
    store.put_txn_state(&txn, &ts).unwrap();
    store.append_chain_entry("k", &VersionEntry::new(1, "e1".into(), "k".into(), txn.clone(), 7)).unwrap();
    backend.put_object(&ObjectKey::new("k"), b"payload".to_vec(), None, HashMap::new()).await.unwrap();

    // 崩溃后重启：恢复流程看到最后一条 WAL 是 Committing → 补偿删除。
    recovery::recover(&store, &*backend).await.unwrap();

    // 正确行为：版本链已发布（原子性点已过），恢复应将其补成 Committed。
    // 实际行为：把后端对象删了。
    assert!(!mock.contains("k"),
        "V2 复现：恢复流程把已发布的对象从后端删除了");
    // 而版本链（和读者可见性依据）仍声称该数据存在 → 悬挂索引。
    let chain = store.read_chain("k").unwrap();
    assert_eq!(chain.entries.len(), 1,
        "V2 复现：版本链仍保留已被删除数据的条目（索引悬挂）");
    let post = store.get_txn_state(&txn).unwrap().unwrap();
    assert_eq!(post.status, TxnStatus::Aborted,
        "V2 复现：同一笔数据'链上已发布'与'状态为 Aborted'并存，I2/I7 被破坏");
}

// =========================================================================
// PoC 3 — 冷重建把所有事务写入对象当孤儿删光（V1）
// =========================================================================

#[tokio::test]
async fn poc3_cold_rebuild_annihilates_txn_objects() {
    let (coord, _store, backend, mock, _tmp) = setup().await;

    // 通过真实提交路径写入三笔已提交数据。
    for k in ["a", "b", "c"] {
        let t = coord.begin(None).await.unwrap();
        stage(&coord, &t, k, format!("value-{}", k).as_bytes()).await;
        coord.commit(&t).await.expect("commit");
    }
    assert_eq!(mock.keys().len(), 3);

    // 灾难场景：redb 目录被删（README: "No data lost"）。服务器以空库启动
    // （main.rs:79 条件成立）→ 冷重建。
    let tmp2 = tempfile::TempDir::new().unwrap();
    let fresh = LocalStore::open_path(&tmp2.path().join("xt.redb")).unwrap();
    let report = rebuild::rebuild(&fresh, &*backend).await.unwrap();

    // 正确行为：三笔都已提交（后端对象携带 txn 元数据，但本地 TxnState 已随
    // redb 丢失）→ 至少不应删除。实际行为：txn_is_committed 对空库恒为 false
    // → 全部判为孤儿 → 全部删除。
    assert!(report.orphans_deleted >= 3,
        "V1 复现：冷重建把 {} 个已提交对象判为孤儿", report.orphans_deleted);
    assert!(mock.keys().is_empty(),
        "V1 复现：README 声称'不丢数据'的灾难恢复把桶清空了");
    // 重建后的索引（旧表）也没有任何可用条目；MVCC 链更是从未被重建填充。
    assert!(fresh.read_chain("a").unwrap().entries.is_empty());
}

// =========================================================================
// PoC 4 — 上传失败的补偿 delete 摧毁该 key 上"之前已提交"的数据（V3）
// =========================================================================

#[tokio::test]
async fn poc4_failed_commit_destroys_prior_committed_object() {
    let (coord, store, _backend, mock, _tmp) = setup().await;

    // T0：提交 k = "old"（正常已提交数据）。
    let t0 = coord.begin(None).await.unwrap();
    stage(&coord, &t0, "k", b"old").await;
    coord.commit(&t0).await.unwrap();
    assert_eq!(mock.get("k").unwrap(), b"old");

    // T1：重写 k，同时写一个会被注入故障的 key。
    mock.set_fail_put_prefix("poison/");
    let t1 = coord.begin(None).await.unwrap();
    stage(&coord, &t1, "k", b"new").await;
    stage(&coord, &t1, "poison/x", b"z").await;

    let r = coord.commit(&t1).await;
    assert!(r.is_err(), "上传失败，T1 整体回滚——这部分是对的");

    // 正确行为：T1 回滚后，k 应回到 T0 提交的 "old"。
    // 实际行为：k 的上传成功 → 覆盖了 "old" → 补偿路径对裸 key 执行
    // DeleteObject → "old" 彻底消失。后端无版本化，覆盖+删除=不可恢复。
    assert!(!mock.contains("k"),
        "V3 复现：一笔无关上传失败，把之前已提交的数据从后端抹掉了");
    // 而版本链仍留着 T0 的条目 → 索引指向已不存在的数据。
    let chain = store.read_chain("k").unwrap();
    assert_eq!(chain.entries.len(), 1);
}

// =========================================================================
// PoC 5 — 共享快照 pin 被先提交者拔掉 → 活跃事务遭遇幻影删除（V9）
// =========================================================================

#[tokio::test]
async fn poc5_shared_snapshot_pin_stolen_by_first_committer() {
    let tmp = tempfile::TempDir::new().unwrap();
    let store = LocalStore::open_path(&tmp.path().join("xt.redb")).unwrap();

    store.append_chain_entry("k", &VersionEntry::new(1, "e1".into(), "k".into(), "T1".into(), 1)).unwrap();
    store.append_chain_entry("k", &VersionEntry::new(6, "e6".into(), "k".into(), "T6".into(), 1)).unwrap();

    // 两个事务在同一 global_version=1 开启（常态：版本只在提交时前进）。
    // register_snapshot 是按版本号 insert，无引用计数 → 第二次注册是空操作。
    store.register_snapshot(1).unwrap();
    store.register_snapshot(1).unwrap();

    // 第一个事务提交 → unregister 把共享 pin 拔掉，第二个事务仍在运行。
    store.unregister_snapshot(1).unwrap();

    // GC 认为已无活跃快照 → 剪掉旧版本。
    gc::gc_version_chains(&store).unwrap();

    // 正确行为（MVCC_RELIABILITY I3/I8）：仍在运行的第二个事务在快照 1 上
    // 应读到 v1。实际行为：v1 已被剪掉 → 返回 None → 该 key 对活跃事务
    // "从未存在过"。
    let r = store.read_at_snapshot("k", 1).unwrap();
    assert!(r.is_none(),
        "V9 复现：活跃快照读返回 None——事务开始时明明存在的数据被 GC 幻影删除");
}

// =========================================================================
// PoC 6 — 事务内 Delete 不是删除：commit 时写入 0 字节对象（V10）
// =========================================================================

#[tokio::test]
async fn poc6_transactional_delete_writes_empty_object() {
    let (coord, store, _backend, mock, _tmp) = setup().await;

    let t0 = coord.begin(None).await.unwrap();
    stage(&coord, &t0, "k", b"real-data").await;
    coord.commit(&t0).await.unwrap();

    // service.rs 的 delete_object 事务路径：stage 空 body（service.rs:272-289）。
    let t1 = coord.begin(None).await.unwrap();
    coord
        .stage(&t1, &ObjectKey::new("k"), Vec::new(), None, HashMap::new(), true)
        .await
        .unwrap();
    coord.commit(&t1).await.unwrap();

    // 正确行为：对象应被删除，链上留下 tombstone（deleted=true）。
    // 实际行为：commit 把空 body 当普通 PutObject 上传 → 对象还在，内容为空；
    // 链上条目 deleted=false（VersionEntry::tombstone 从未被 commit 使用）。
    let body = mock.get("k")
        .expect("V10 复现：事务删除后对象仍存在于后端");
    assert!(body.is_empty(),
        "V10 复现：'删除'的结果是一个 0 字节对象，而不是删除");
    let last = store.read_chain("k").unwrap().entries.last().cloned().unwrap();
    assert!(!last.deleted, "V10 复现：链上无 tombstone 标记");
}

// =========================================================================
// PoC 7 — HTTP 层 stage 的 threshold 传参使第二笔事务起全部被拒（V18）
// =========================================================================

#[tokio::test]
async fn poc7_http_layer_rejects_every_txn_after_the_first() {
    let (coord, store, _backend, mock, _tmp) = setup().await;

    // 第一笔事务（global_version == 0）：threshold 检查恰好通过。
    let t1 = coord.begin(None).await.unwrap();
    let g1 = store.current_global_version().unwrap(); // = 0
    coord
        .stage(&t1, &ObjectKey::new("k"), b"x".to_vec(), None, HashMap::new(), false)
        .await
        .unwrap();
    coord.commit(&t1).await.unwrap(); // global_version → 1

    // 第二笔事务：service.rs:144 传 current_global_version() 作为 threshold。
    // 由于事务提交从不写 TBL_VERSIONS，get_version(new key) 恒为 0 < 1 → 拒绝。
    let t2 = coord.begin(None).await.unwrap();
    let g2 = store.current_global_version().unwrap(); // = 1
    let r = coord
        .stage(&t2, &ObjectKey::new("fresh"), b"y".to_vec(), None, HashMap::new(), false)
        .await;

    // 正确行为：stage 应成功。实际行为：Err(InvalidArgument) → HTTP 400。
    // 现有 e2e 测试全部传 threshold=0，从未覆盖 HTTP 层的真实传参，
    // 所以 63 个绿灯测不出这个问题。
    assert!(r.is_err(),
        "V18 复现：第二笔事务的 stage 被拒——HTTP 事务路径从第二笔起不可用");
    assert!(mock.keys().iter().all(|k| k == "k"),
        "只有第一笔事务真正到达过后端");
}
