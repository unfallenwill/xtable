//! `s3s::S3` implementation for `XtableS3Service`.
//!
//! Phase 1: PutObject / GetObject / HeadObject / DeleteObject / DeleteObjects /
//! ListObjectsV2 / HeadBucket / ListBuckets.
//!
//! Phase 2: transactional extensions — if a request carries `x-xtable-txn-id`,
//! PutObject stages in the txn's write_set instead of forwarding to backend.
//! GetObject consults the txn's write_set first.
//!
//! Phase 3: Multipart.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::StreamExt;
use http::HeaderMap;
use s3s::dto::{
    Bucket, Buckets, CreateMultipartUploadInput, CreateMultipartUploadOutput, DeleteObjectInput,
    DeleteObjectOutput, DeleteObjectsInput, DeleteObjectsOutput, DeletedObject, GetObjectInput,
    GetObjectOutput, HeadBucketInput, HeadBucketOutput, HeadObjectInput, HeadObjectOutput,
    ListBucketsInput, ListBucketsOutput, ListObjectsV2Input, ListObjectsV2Output,
    Object as S3Object, PutObjectInput, PutObjectOutput,
    UploadPartInput, UploadPartOutput,
};
use s3s::{S3, S3Error, S3Request, S3Response, S3Result};
use tracing::{debug, info, warn};

use xtable_backend::BackendClient;
use xtable_core::headers::backend_meta;
use xtable_core::ObjectKey;
use xtable_storage::LocalStore;
use xtable_tx::TxnCoordinator;

use crate::dto::new_version_record;

/// S3 service fronting an xtable-managed view of a backend S3-compatible store.
#[derive(Clone)]
pub struct XtableS3Service {
    pub backend: Arc<BackendClient>,
    pub store: LocalStore,
    pub txn: Arc<TxnCoordinator>,
    /// Edge credential store. Used by `routes::build_s3_service` to attach
    /// a passthrough s3s auth provider so the request signature (already
    /// verified by the xtable-server middleware) re-verifies successfully
    /// inside s3s's own dispatcher.
    pub creds: Arc<xtable_auth::CredentialStore>,
}

impl std::fmt::Debug for XtableS3Service {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("XtableS3Service").finish_non_exhaustive()
    }
}

impl XtableS3Service {
    pub fn new(
        backend: Arc<BackendClient>,
        store: LocalStore,
        txn: Arc<TxnCoordinator>,
        creds: Arc<xtable_auth::CredentialStore>,
    ) -> Self {
        Self { backend, store, txn, creds }
    }

    pub fn backend_metadata(version: xtable_core::Version, txn_id: &str) -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert(backend_meta::XTABLE_VERSION.to_string(), version.as_u64().to_string());
        if !txn_id.is_empty() {
            m.insert(backend_meta::XTABLE_TXN_ID.to_string(), txn_id.to_string());
        }
        m
    }

    pub fn parse_backend_version(meta: &HashMap<String, String>) -> Option<xtable_core::Version> {
        meta.get(backend_meta::XTABLE_VERSION)
            .and_then(|v| v.parse::<u64>().ok())
            .map(xtable_core::Version)
    }
}

fn map_xerror(e: xtable_core::XtableError) -> S3Error {
    use s3s::S3ErrorCode;
    let code = match e.s3_code() {
        "NoSuchKey" => S3ErrorCode::NoSuchKey,
        "InvalidArgument" => S3ErrorCode::InvalidArgument,
        "Unauthorized" | "Forbidden" => S3ErrorCode::AccessDenied,
        "TransactionConflict" => S3ErrorCode::Custom("TransactionConflict".into()),
        "TxnExpired" => S3ErrorCode::Custom("TxnExpired".into()),
        "UnknownTxn" => S3ErrorCode::Custom("UnknownTxn".into()),
        _ => S3ErrorCode::InternalError,
    };
    let mut s3e = S3Error::with_message(code, format!("{}", e));
    s3e.set_status_code(http::StatusCode::from_u16(e.http_status()).unwrap_or(http::StatusCode::INTERNAL_SERVER_ERROR));
    s3e
}

fn header_txn_id(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-xtable-txn-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .or_else(|| {
            // Accept the S3 metadata variant (`x-amz-meta-xtable-txn-id`) too,
            // which aws-sdk-s3 callers naturally produce via `.metadata(...)`.
            headers
                .get("x-amz-meta-xtable-txn-id")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string())
        })
}

#[async_trait]
impl S3 for XtableS3Service {
    async fn put_object(&self, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
        let input = req.input;
        let key = input.key.clone();
        let headers = req.headers.clone();
        debug!(key = %key, "PutObject");

        // Read body fully (Phase 2 streams not yet implemented).
        let body = input.body.ok_or_else(|| {
            s3s::S3Error::with_message(
                s3s::S3ErrorCode::InvalidRequest,
                "missing body",
            )
        })?;
        let mut bytes_vec: Vec<u8> = Vec::new();
        let mut stream = body;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| {
                s3s::S3Error::with_message(
                    s3s::S3ErrorCode::InvalidRequest,
                    format!("body read: {}", e),
                )
            })?;
            bytes_vec.extend_from_slice(&chunk);
        }

        let content_type = input.content_type.clone();
        let user_meta: HashMap<String, String> = input
            .metadata
            .clone()
            .unwrap_or_default()
            .into_iter()
            .collect();

        // Transactional path?
        // V18 fix: do NOT pass `current_global_version()` as a threshold — that
        // breaks every txn after the first. The coordinator's stage() now uses
        // the chain's actual current version internally.
        if let Some(txn_id) = header_txn_id(&headers) {
            self.txn
                .stage(
                    &txn_id,
                    &ObjectKey::new(&key),
                    bytes_vec,
                    content_type,
                    user_meta,
                    false,
                )
                .await
                .map_err(map_xerror)?;
            let mut out = PutObjectOutput::default();
            out.e_tag = Some(s3s::dto::ETag::Strong(format!("staged:{}", txn_id)));
            return Ok(S3Response::new(out));
        }

        // Non-transactional path: forward directly to backend.
        let new_version = xtable_core::Version(self.store.next_global_version().map_err(map_xerror)?);

        let backend_meta = Self::backend_metadata(new_version, "");
        let put_res = self
            .backend
            .put_object(
                &ObjectKey::new(&key),
                bytes_vec.clone(),
                content_type.as_deref(),
                backend_meta,
            )
            .await
            .map_err(|e| map_xerror(xtable_core::XtableError::from(e)))?;

        let rec = new_version_record(
            new_version,
            put_res.etag.clone(),
            key.clone(),
            String::new(),
            bytes_vec.len() as u64,
        );
        self.store
            .put_version(&ObjectKey::new(&key), &rec)
            .map_err(map_xerror)?;

        info!(key = %key, version = %new_version, etag = %put_res.etag, "PutObject committed");

        let mut out = PutObjectOutput::default();
        out.e_tag = Some(s3s::dto::ETag::Strong(put_res.etag));
        out.version_id = put_res.version_id;
        Ok(S3Response::new(out))
    }

    async fn get_object(&self, req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
        let input = req.input;
        let key = input.clone().key;
        let headers = req.headers.clone();
        debug!(key = %key, "GetObject");

        // V6 fix: the MVCC chain is the gate. A reader observes only
        // entries with commit_version ≤ snapshot. This makes commit-mid-flight
        // uploads invisible to non-transactional readers.

        // 1. Transactional read: see staged value first (read-your-own-writes).
        if let Some(txn_id) = header_txn_id(&headers) {
            if let Some(bytes) = self.txn.stage_body(&txn_id, &key).await.map_err(map_xerror)? {
                let body = s3s::dto::StreamingBlob::from_bytes(Bytes::from(bytes));
                let mut out = GetObjectOutput::default();
                out.body = Some(body);
                out.content_length = None;
                return Ok(S3Response::new(out));
            }
        }

        // 2. Determine the snapshot version for non-transactional reads.
        //    Use u64::MAX to mean "see the latest visible committed value".
        let snapshot_version: u64 = if let Some(txn_id) = header_txn_id(&headers) {
            self.txn
                .store()
                .get_txn_state(&txn_id)
                .ok()
                .flatten()
                .map(|s| s.snapshot_version)
                .unwrap_or(u64::MAX)
        } else {
            u64::MAX
        };

        let obj_key = ObjectKey::new(&key);
        let chain_entry = self.store.read_at_snapshot(&key, snapshot_version).map_err(map_xerror)?;
        let visible = match chain_entry {
            Some(e) if !e.deleted => Some((e.size, e.commit_version)),
            Some(_) | None => None, // tombstone or never-existed
        };

        let (size, version) = match visible {
            Some(v) => v,
            None => {
                return Err(s3s::S3Error::with_message(s3s::S3ErrorCode::NoSuchKey, "not found"));
            }
        };

        // 3. Now that the chain confirms the key exists at this snapshot,
        //    fetch the body from the backend. (This is the S3-compatible
        //    data path; bodies live in the backend S3.)
        let r = match self.backend.get_object(&obj_key).await {
            Ok(r) => r,
            Err(e) => {
                let x: xtable_core::XtableError = e.into();
                if let xtable_core::XtableError::NotFound(_) = &x {
                    warn!(key = %key, version, "chain says present but backend missing — tombstoning");
                    return Err(s3s::S3Error::with_message(s3s::S3ErrorCode::NoSuchKey, "not found"));
                }
                return Err(map_xerror(x));
            }
        };

        let body = s3s::dto::StreamingBlob::from_bytes(Bytes::from(r.bytes));
        let mut out = GetObjectOutput::default();
        out.body = Some(body);
        out.content_length = Some(size as i64);
        out.e_tag = Some(s3s::dto::ETag::Strong(r.etag));
        out.last_modified = Some(s3s::dto::Timestamp::from(std::time::SystemTime::now()));
        out.content_type = Some("application/octet-stream".to_string());
        Ok(S3Response::new(out))
    }

    async fn head_object(&self, req: S3Request<HeadObjectInput>) -> S3Result<S3Response<HeadObjectOutput>> {
        let input = req.input;
        let key = input.clone().key;
        debug!(key = %key, "HeadObject");

        // V6 fix: gate on the MVCC chain.
        let snapshot_version = u64::MAX;
        let chain_entry = self.store.read_at_snapshot(&key, snapshot_version).map_err(map_xerror)?;
        let (size, _v) = match chain_entry {
            Some(e) if !e.deleted => (e.size, e.commit_version),
            _ => return Err(s3s::S3Error::with_message(s3s::S3ErrorCode::NoSuchKey, "not found")),
        };

        let r = self
            .backend
            .head_object(&ObjectKey::new(&key))
            .await
            .map_err(|e| map_xerror(xtable_core::XtableError::from(e)))?;

        let mut out = HeadObjectOutput::default();
        out.content_length = Some(size as i64);
        out.e_tag = Some(s3s::dto::ETag::Strong(r.etag));
        out.content_type = Some(if r.content_type.is_empty() {
            "application/octet-stream".to_string()
        } else {
            r.content_type
        });
        Ok(S3Response::new(out))
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let input = req.input;
        let key = input.key.clone();
        debug!(key = %key, "DeleteObject");

        // Transactional delete: stage as tombstone (V10 fix).
        if let Some(txn_id) = header_txn_id(&req.headers) {
            self.txn
                .stage(
                    &txn_id,
                    &ObjectKey::new(&key),
                    Vec::new(),
                    None,
                    HashMap::new(),
                    true, // V10: deleted=true → commit will DeleteObject
                )
                .await
                .map_err(map_xerror)?;
            let mut headers = HeaderMap::new();
            headers.insert(
                xtable_core::headers::XTABLE_VERSION,
                "0".parse().unwrap(),
            );
            return Ok(S3Response::with_headers(DeleteObjectOutput::default(), headers));
        }

        self.backend
            .delete_object(&ObjectKey::new(&key))
            .await
            .map_err(|e| map_xerror(xtable_core::XtableError::from(e)))?;

        let new_version = xtable_core::Version(self.store.next_global_version().map_err(map_xerror)?);
        let rec = new_version_record(
            new_version,
            String::new(),
            key.clone(),
            String::new(),
            0,
        );
        self.store
            .put_version(&ObjectKey::new(&key), &rec)
            .map_err(map_xerror)?;

        let mut headers = HeaderMap::new();
        headers.insert(
            xtable_core::headers::XTABLE_VERSION,
            new_version.as_u64().to_string().parse().unwrap(),
        );
        Ok(S3Response::with_headers(DeleteObjectOutput::default(), headers))
    }

    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        debug!("DeleteObjects");
        let objs = req.input.delete.objects;
        let mut deleted: Vec<DeletedObject> = Vec::with_capacity(objs.len());
        let mut keys: Vec<ObjectKey> = Vec::with_capacity(objs.len());
        for o in &objs {
            let key_str = o.key.clone();
            keys.push(ObjectKey::new(&key_str));
            let mut d = DeletedObject::default();
            d.key = Some(key_str);
            deleted.push(d);
        }
        self.backend
            .delete_objects(&keys)
            .await
            .map_err(|e| map_xerror(xtable_core::XtableError::from(e)))?;

        let new_version = xtable_core::Version(self.store.next_global_version().map_err(map_xerror)?);
        for k in &keys {
            let rec = new_version_record(
                new_version,
                String::new(),
                k.as_str().to_string(),
                String::new(),
                0,
            );
            self.store.put_version(k, &rec).map_err(map_xerror)?;
        }

        let mut out = DeleteObjectsOutput::default();
        out.deleted = Some(deleted);
        Ok(S3Response::new(out))
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        debug!("ListObjectsV2");
        let prefix = req.input.prefix.unwrap_or_default();
        let max = req.input.max_keys.map(|n| n as usize);
        let delimiter = req.input.delimiter.clone().unwrap_or_default();
        let continuation = req.input.continuation_token.clone();

        let listed = self
            .backend
            .list_objects()
            .await
            .map_err(|e| map_xerror(xtable_core::XtableError::from(e)))?;

        // Apply pagination via prefix+delimiter+continuation_token.
        let start_after = continuation.unwrap_or_default();
        let mut contents: Vec<S3Object> = Vec::new();
        let mut common_prefixes: Vec<s3s::dto::CommonPrefix> = Vec::new();
        let mut seen_prefixes: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        let mut is_truncated = false;

        for lo in listed {
            if !prefix.is_empty() && !lo.key.starts_with(&prefix) {
                continue;
            }
            if !start_after.is_empty() && lo.key.as_str() <= start_after.as_str() {
                continue;
            }
            let after_prefix = if prefix.is_empty() { &lo.key } else { &lo.key[prefix.len()..] };
            if !delimiter.is_empty() && after_prefix.contains(&delimiter) {
                // Common prefix.
                let delim_idx = after_prefix.find(&delimiter).unwrap();
                let cp = format!("{}{}", prefix, &after_prefix[..delim_idx + delimiter.len()]);
                if seen_prefixes.insert(cp.clone()) {
                    let mut c = s3s::dto::CommonPrefix::default();
                    c.prefix = Some(cp);
                    common_prefixes.push(c);
                }
                if let Some(m) = max {
                    if contents.len() + common_prefixes.len() >= m {
                        is_truncated = true;
                        break;
                    }
                }
                continue;
            }
            let mut o = S3Object::default();
            o.key = Some(lo.key);
            o.size = Some(lo.size as i64);
            o.e_tag = Some(s3s::dto::ETag::Strong(lo.etag));
            contents.push(o);
            if let Some(m) = max {
                if contents.len() + common_prefixes.len() >= m {
                    is_truncated = true;
                    break;
                }
            }
        }

        let total = contents.len() + common_prefixes.len();
        let next_continuation = if is_truncated {
            contents.last().and_then(|o| o.key.clone())
        } else {
            None
        };

        let mut out = ListObjectsV2Output::default();
        out.key_count = Some(total as i32);
        out.max_keys = Some(total as i32);
        out.is_truncated = Some(is_truncated);
        out.next_continuation_token = next_continuation;
        out.contents = Some(contents);
        out.common_prefixes = Some(common_prefixes);
        out.prefix = Some(prefix);
        out.delimiter = if delimiter.is_empty() { None } else { Some(delimiter) };
        out.name = Some(self.backend.bucket_name());
        Ok(S3Response::new(out))
    }

    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        debug!(bucket = %req.input.bucket, "HeadBucket");
        let expected = self.backend.bucket_name();
        if req.input.bucket != expected {
            return Err(s3s::S3Error::with_message(
                s3s::S3ErrorCode::NoSuchBucket,
                "no such bucket",
            ));
        }
        let out = HeadBucketOutput::default();
        Ok(S3Response::new(out))
    }

    async fn list_buckets(
        &self,
        _req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        debug!("ListBuckets");
        let mut b = Bucket::default();
        b.name = Some(self.backend.bucket_name());
        let mut bs = Buckets::default();
        bs.push(b);

        let mut out = ListBucketsOutput::default();
        out.buckets = Some(bs);
        Ok(S3Response::new(out))
    }

    // ---- Multipart (Phase 3) ----

    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let input = req.input;
        let key = input.key.clone();
        let headers = req.headers.clone();
        debug!(key = %key, "CreateMultipartUpload");
        let upload_id = self.backend.create_multipart(&ObjectKey::new(&key)).await
            .map_err(|e| map_xerror(xtable_core::XtableError::from(e)))?;

        // V11 fix: honor x-xtable-txn-id header. Multipart created inside
        // a txn stays invisible until the txn commits.
        let txn_id = header_txn_id(&headers);
        if let Some(ref t) = txn_id {
            // Ensure txn is active before allowing parts.
            let _ = self.txn.store().get_txn_state(t).map_err(map_xerror)?;
        }

        let state = xtable_storage::MultipartState {
            upload_id: upload_id.clone(),
            key: key.clone(),
            backend_upload_id: upload_id.clone(),
            parts: Vec::new(),
            txn_id: txn_id.clone(),
        };
        self.store.put_multipart(&upload_id, &state).map_err(map_xerror)?;

        let mut out = CreateMultipartUploadOutput::default();
        out.bucket = Some(self.backend.bucket_name());
        out.key = Some(key);
        out.upload_id = Some(upload_id);
        Ok(S3Response::new(out))
    }

    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        let input = req.input;
        let key = input.key.clone();
        let upload_id = input.upload_id.to_string();
        let part_number = input.part_number;

        // Read body.
        let body = input.body.ok_or_else(|| {
            s3s::S3Error::with_message(s3s::S3ErrorCode::InvalidRequest, "missing body")
        })?;
        let mut bytes_vec: Vec<u8> = Vec::new();
        let mut stream = body;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| {
                s3s::S3Error::with_message(s3s::S3ErrorCode::InvalidRequest, format!("body read: {}", e))
            })?;
            bytes_vec.extend_from_slice(&chunk);
        }
        let size = bytes_vec.len() as u64;

        let etag = self.backend.upload_part(
            &ObjectKey::new(&key),
            &upload_id,
            part_number,
            bytes_vec,
        ).await.map_err(|e| map_xerror(xtable_core::XtableError::from(e)))?;

        // Update multipart state.
        if let Ok(Some(mut state)) = self.store.get_multipart(&upload_id) {
            state.parts.push((part_number, etag.clone(), size));
            let _ = self.store.put_multipart(&upload_id, &state);
        }

        let mut out = UploadPartOutput::default();
        out.e_tag = Some(s3s::dto::ETag::Strong(etag));
        Ok(S3Response::new(out))
    }

    async fn complete_multipart_upload(
        &self,
        req: s3s::S3Request<s3s::dto::CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<s3s::dto::CompleteMultipartUploadOutput>> {
        let input = req.input;
        let key = input.key.clone();
        let upload_id = input.upload_id.to_string();
        let headers = req.headers.clone();

        let parts_in: Vec<(i32, String)> = input
            .multipart_upload
            .as_ref()
            .and_then(|cm| cm.parts.as_ref())
            .map(|list| {
                list.iter().filter_map(|p| {
                    let pn = p.part_number?;
                    let et = p.e_tag.as_ref().map(|e| match e.clone() {
                        s3s::dto::ETag::Strong(s) => s,
                        s3s::dto::ETag::Weak(s) => s,
                    }).unwrap_or_default();
                    Some((pn, et))
                }).collect()
            })
            .unwrap_or_default();

        self.backend.complete_multipart(&ObjectKey::new(&key), &upload_id, parts_in)
            .await.map_err(|e| map_xerror(xtable_core::XtableError::from(e)))?;

        // Get size from state.
        let size: u64 = self.store.get_multipart(&upload_id).ok().flatten()
            .map(|s| s.parts.iter().map(|(_, _, sz)| *sz).sum())
            .unwrap_or(0);

        // V11 fix: if this multipart is part of a txn, DON'T publish the
        // chain entry yet. The TxnCoordinator's commit path will pick up
        // pending multiparts and append their chain entries atomically
        // with the rest of the txn's writes. This preserves multi-object
        // atomicity for multipart uploads.
        let txn_id = header_txn_id(&headers)
            .or_else(|| {
                self.store.get_multipart(&upload_id).ok().flatten()
                    .and_then(|m| m.txn_id)
            });

        if let Some(t) = txn_id {
            // Record the pending key+size on the txn. The commit path will
            // read this and append a chain entry at txn commit time.
            let mut txn_state = self.txn.store().get_txn_state(&t)
                .map_err(map_xerror)?
                .ok_or_else(|| s3s::S3Error::with_message(s3s::S3ErrorCode::Custom("UnknownTxn".into()), "txn not found"))?;
            txn_state.write_keys.push(key.clone());
            let _ = self.txn.store().put_txn_state(&t, &txn_state);
            // Clean up multipart state but KEEP the txn_id linkage.
            let _ = self.store.delete_multipart(&upload_id);
        } else {
            // Non-transactional: publish the chain entry now so the
            // reader path (V6) can see the object.
            let new_version = xtable_core::Version(self.store.next_global_version().map_err(map_xerror)?);
            let entry = xtable_storage::VersionEntry::new(
                new_version.as_u64(),
                String::new(),
                key.clone(),
                String::new(),
                size,
            );
            self.store.append_chain_entry(&key, &entry).map_err(map_xerror)?;
            let _ = self.store.delete_multipart(&upload_id);
        }

        let mut out = s3s::dto::CompleteMultipartUploadOutput::default();
        out.bucket = Some(self.backend.bucket_name());
        out.key = Some(key.clone());
        out.e_tag = Some(s3s::dto::ETag::Strong(format!("multipart-{}", upload_id)));
        Ok(S3Response::new(out))
    }

    async fn abort_multipart_upload(
        &self,
        req: s3s::S3Request<s3s::dto::AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<s3s::dto::AbortMultipartUploadOutput>> {
        let input = req.input;
        let key = input.key.clone();
        let upload_id = input.upload_id.to_string();
        let _ = self.backend.abort_multipart(&ObjectKey::new(&key), &upload_id).await;
        // V11 fix: if a multipart was tied to a txn, removing the key from
        // the txn's write_keys list so the txn doesn't try to publish it.
        if let Ok(Some(mp)) = self.store.get_multipart(&upload_id) {
            if let Some(t) = mp.txn_id {
                if let Ok(Some(mut ts)) = self.txn.store().get_txn_state(&t) {
                    ts.write_keys.retain(|k| k != &key);
                    let _ = self.txn.store().put_txn_state(&t, &ts);
                }
            }
        }
        let _ = self.store.delete_multipart(&upload_id);
        Ok(S3Response::new(s3s::dto::AbortMultipartUploadOutput::default()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_metadata_includes_version() {
        let m = XtableS3Service::backend_metadata(xtable_core::Version(7), "");
        assert_eq!(m.get(backend_meta::XTABLE_VERSION).unwrap(), "7");
        assert!(!m.contains_key(backend_meta::XTABLE_TXN_ID));
    }

    #[test]
    fn backend_metadata_includes_txn_id_when_set() {
        let m = XtableS3Service::backend_metadata(xtable_core::Version(7), "01JABC...");
        assert_eq!(m.get(backend_meta::XTABLE_TXN_ID).unwrap(), "01JABC...");
    }

    #[test]
    fn parse_backend_version_handles_missing() {
        let m: HashMap<String, String> = HashMap::new();
        assert!(XtableS3Service::parse_backend_version(&m).is_none());
    }

    #[test]
    fn parse_backend_version_parses_value() {
        let mut m = HashMap::new();
        m.insert(backend_meta::XTABLE_VERSION.to_string(), "42".into());
        assert_eq!(XtableS3Service::parse_backend_version(&m), Some(xtable_core::Version(42)));
    }

    #[test]
    fn parse_backend_version_returns_none_on_garbage() {
        let mut m = HashMap::new();
        m.insert(backend_meta::XTABLE_VERSION.to_string(), "not-a-number".into());
        assert!(XtableS3Service::parse_backend_version(&m).is_none());
    }

    #[test]
    fn header_txn_id_extracts() {
        let mut h = HeaderMap::new();
        h.insert("x-xtable-txn-id", "01JABC".parse().unwrap());
        assert_eq!(header_txn_id(&h).as_deref(), Some("01JABC"));
    }

    #[test]
    fn header_txn_id_returns_none_when_absent() {
        let h = HeaderMap::new();
        assert!(header_txn_id(&h).is_none());
    }
}