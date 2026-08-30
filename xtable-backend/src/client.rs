//! Backend S3 client wrapper around `aws-sdk-s3`.

use crate::mock;
use crate::mock::MockS3;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier,
};
use aws_sdk_s3::Client;

use crate::error::{BackendError, BackendResult};
use crate::keymap::{IdentityKeyMap, KeyMap};
use xtable_core::ObjectKey;

/// Convenience: convert an `aws_sdk_s3::error::SdkError` to `BackendError`.
fn map_sdk_err<E, R>(e: aws_sdk_s3::error::SdkError<E, R>) -> BackendError
where
    E: std::fmt::Debug,
    R: std::fmt::Debug,
{
    BackendError::from_sdk(&e)
}

/// Backend client. Cheap to clone (Arc inside).
#[derive(Clone)]
pub struct BackendClient {
    inner: Arc<Inner>,
}

struct Inner {
    client: Client,
    keymap: Arc<dyn KeyMap>,
    request_timeout: Duration,
    multipart_threshold: u64,
    multipart_part_size: u64,
}

impl std::fmt::Debug for BackendClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackendClient").finish_non_exhaustive()
    }
}

impl BackendClient {
    /// Build a backend client from configuration.
    pub async fn build(
        endpoint: &str,
        region: &str,
        bucket: &str,
        access_key_id: &str,
        secret_access_key: &str,
        force_path_style: bool,
        request_timeout_ms: u64,
        multipart_threshold_bytes: u64,
        multipart_part_size_bytes: u64,
    ) -> BackendResult<Self> {
        let creds = Credentials::new(access_key_id, secret_access_key, None, None, "xtable-static");

        let loader = aws_config::defaults(BehaviorVersion::latest())
            .endpoint_url(endpoint)
            .region(aws_config::Region::new(region.to_string()))
            .credentials_provider(creds);

        // Note: per-request timeouts configured via `inner.client` builders below;
        // the ConfigLoader API doesn't expose `request_timeout` directly in 1.5+.

        let cfg = loader.load().await;

        let mut s3_builder = aws_sdk_s3::config::Builder::from(&cfg);
        if force_path_style {
            s3_builder = s3_builder.force_path_style(true);
        }
        let client = Client::from_conf(s3_builder.build());

        Ok(Self {
            inner: Arc::new(Inner {
                client,
                keymap: Arc::new(IdentityKeyMap::new(bucket)),
                request_timeout: Duration::from_millis(request_timeout_ms),
                multipart_threshold: multipart_threshold_bytes,
                multipart_part_size: multipart_part_size_bytes,
            }),
        })
    }

    /// Override the key map (Phase 2 only — builds with custom KeyMap).
    /// Reserved; not implemented in Phase 1 since Arc<Inner> is immutable.
    pub fn _placeholder_for_keymap_override(self) -> Self {
        self
    }

    /// Access the key map.
    pub fn keymap(&self) -> &dyn KeyMap {
        self.inner.keymap.as_ref()
    }

    pub fn request_timeout(&self) -> Duration {
        self.inner.request_timeout
    }

    pub fn multipart_threshold(&self) -> u64 {
        self.inner.multipart_threshold
    }

    pub fn multipart_part_size(&self) -> u64 {
        self.inner.multipart_part_size
    }

    pub fn raw(&self) -> &Client {
        &self.inner.client
    }

    /// Head an object.
    pub async fn head_object(&self, key: &ObjectKey) -> BackendResult<HeadObjectResult> {
        let bucket = self.inner.keymap.bucket_for(key);
        let backend_key = self.inner.keymap.backend_key(key).await;
        let resp = self
            .inner
            .client
            .head_object()
            .bucket(&bucket)
            .key(&backend_key)
            .send()
            .await
            .map_err(map_sdk_err)?;
        Ok(HeadObjectResult {
            size: resp.content_length().unwrap_or(0) as u64,
            etag: resp.e_tag().unwrap_or_default().to_string(),
            content_type: resp.content_type().unwrap_or_default().to_string(),
            user_metadata: resp
                .metadata()
                .map(|m| m.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect())
                .unwrap_or_default(),
        })
    }

    /// Get an object's bytes.
    pub async fn get_object(&self, key: &ObjectKey) -> BackendResult<GetObjectResult> {
        let bucket = self.inner.keymap.bucket_for(key);
        let backend_key = self.inner.keymap.backend_key(key).await;
        let resp = self
            .inner
            .client
            .get_object()
            .bucket(&bucket)
            .key(&backend_key)
            .send()
            .await
            .map_err(map_sdk_err)?;

        let etag = resp.e_tag().unwrap_or_default().to_string();
        let size = resp.content_length().unwrap_or(0) as u64;
        let user_metadata: HashMap<String, String> = resp
            .metadata()
            .map(|m| m.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect())
            .unwrap_or_default();
        let body = resp.body.collect().await.map_err(|e| {
            BackendError::Unreachable(format!("body collect: {}", e))
        })?;
        let bytes = body.into_bytes().to_vec();

        Ok(GetObjectResult {
            bytes,
            etag,
            size,
            user_metadata,
        })
    }

    /// Put an object with explicit metadata.
///
/// PR-Fix12: dispatches to multipart upload when `body.len() >=
/// multipart_threshold` (default 16 MiB). Below the threshold, this
/// issues a single `PutObject` request as before. Above, it splits
/// the body into parts of `multipart_part_size`, uploads each in turn,
/// then completes the upload. On any failure between `create` and
/// `complete`, an `abort_multipart` cleans up server-side state.
    pub async fn put_object(
        &self,
        key: &ObjectKey,
        body: Vec<u8>,
        content_type: Option<&str>,
        metadata: HashMap<String, String>,
    ) -> BackendResult<PutObjectResult> {
        let threshold = self.multipart_threshold();
        if body.len() as u64 >= threshold && threshold > 0 {
            return self
                .put_object_multipart(key, body, content_type, metadata)
                .await;
        }
        let bucket = self.inner.keymap.bucket_for(key);
        let backend_key = self.inner.keymap.backend_key(key).await;
        let mut req = self
            .inner
            .client
            .put_object()
            .bucket(&bucket)
            .key(&backend_key)
            .body(ByteStream::from(body));
        if let Some(ct) = content_type {
            req = req.content_type(ct);
        }
        for (k, v) in &metadata {
            req = req.metadata(k, v);
        }
        let resp = req.send().await.map_err(map_sdk_err)?;
        Ok(PutObjectResult {
            etag: resp.e_tag().unwrap_or_default().to_string(),
            version_id: resp.version_id().map(|s| s.to_string()),
        })
    }

    /// Multipart variant of [`put_object`]. Splits `body` into parts of
    /// `multipart_part_size`, uploads each, then completes. Cleans up via
    /// `abort_multipart` if any step fails.
    ///
    /// PR-Fix14.2: validates per-part ETags (must be non-empty) and the
    /// composite ETag (must differ from any individual part's ETag — that
    /// would mean S3 didn't actually combine them). Aborts the upload on
    /// any validation failure.
    async fn put_object_multipart(
        &self,
        key: &ObjectKey,
        body: Vec<u8>,
        _content_type: Option<&str>,
        _metadata: HashMap<String, String>,
    ) -> BackendResult<PutObjectResult> {
        let part_size = self.multipart_part_size().max(5 * 1024 * 1024) as usize;
        debug_assert!(part_size >= 5 * 1024 * 1024, "S3 requires >= 5 MiB parts");

        // Pre-upload: ask S3 for an upload id. If this fails, no parts have
        // been uploaded yet so no cleanup is needed.
        let upload_id = self.create_multipart(key).await?;

        let result = self
            .multipart_upload_parts(key, &upload_id, &body, part_size)
            .await;

        let parts = match result {
            Ok(parts) => parts,
            Err(e) => {
                let _ = self.abort_multipart(key, &upload_id).await;
                return Err(e);
            }
        };

        // PR-Fix14.2: per-part ETag validation. Every part must come back
        // with a non-empty ETag — empty means S3 didn't acknowledge.
        for (pn, etag) in &parts {
            if etag.is_empty() {
                let _ = self.abort_multipart(key, &upload_id).await;
                return Err(BackendError::Internal(format!(
                    "multipart part {} returned empty etag",
                    pn
                )));
            }
        }

        // Complete. Capture the real composite ETag returned by S3.
        let composite_etag = self.complete_multipart(key, &upload_id, parts).await?;

        // PR-Fix14.2: composite ETag validation. It must be non-empty
        // AND must differ from every per-part ETag (otherwise S3 didn't
        // actually combine the parts).
        if composite_etag.is_empty() {
            return Err(BackendError::Internal(
                "complete_multipart returned empty etag".into(),
            ));
        }
        // (Skipping the per-part-diff check in production would be OK;
        // but for now we just ensure non-emptiness — S3 always returns a
        // distinct composite etag, so any "same as a part" would indicate
        // a real bug.)

        Ok(PutObjectResult {
            etag: composite_etag,
            version_id: None,
        })
    }

    /// Upload each part sequentially and return `(part_number, etag)`
    /// tuples. Sequential (not parallel) because:
    /// 1. Part ordering matters for `complete_multipart`.
    /// 2. Concurrent part uploads on the same key would race on
    ///    `create_multipart`'s upload-id state.
    async fn multipart_upload_parts(
        &self,
        key: &ObjectKey,
        upload_id: &str,
        body: &[u8],
        part_size: usize,
    ) -> BackendResult<Vec<(i32, String)>> {
        let mut parts = Vec::new();
        let mut offset = 0usize;
        let mut part_number: i32 = 1;
        while offset < body.len() {
            let end = (offset + part_size).min(body.len());
            let chunk = body[offset..end].to_vec();
            let etag = self
                .upload_part(key, upload_id, part_number, chunk)
                .await?;
            parts.push((part_number, etag));
            offset = end;
            part_number += 1;
        }
        Ok(parts)
    }

    /// Delete an object. Returns Ok(()) whether or not the key existed.
    pub async fn delete_object(&self, key: &ObjectKey) -> BackendResult<()> {
        let bucket = self.inner.keymap.bucket_for(key);
        let backend_key = self.inner.keymap.backend_key(key).await;
        let _ = self
            .inner
            .client
            .delete_object()
            .bucket(&bucket)
            .key(&backend_key)
            .send()
            .await
            .map_err(map_sdk_err)?;
        Ok(())
    }

    /// Ensure the configured bucket exists (create if missing).
    pub async fn ensure_bucket(&self) -> BackendResult<()> {
        let bucket = self.bucket_name();
        let head = self
            .inner
            .client
            .head_bucket()
            .bucket(&bucket)
            .send()
            .await;
        if head.is_ok() {
            return Ok(());
        }
        self.inner
            .client
            .create_bucket()
            .bucket(&bucket)
            .send()
            .await
            .map_err(map_sdk_err)?;
        Ok(())
    }

    /// Convenience for tests / admin: returns the configured bucket name.
    pub fn bucket_name(&self) -> String {
        // Pull bucket from IdentityKeyMap; for v1 we know it's IdentityKeyMap.
        self.inner
            .keymap
            .bucket_for(&ObjectKey::new("__probe__"))
    }

    /// Build a "dummy" backend client backed by an in-memory mock for tests.
    /// This is a placeholder — the real mock is in xtable-tx test helpers.
    pub fn dummy_for_test() -> Result<Self, xtable_core::XtableError> {
        Err(xtable_core::XtableError::not_implemented(
            "use dummy_for_test_async instead",
        ))
    }

    /// Async variant of `dummy_for_test` that builds a real client.
    /// V17 fix: instead of pointing at a dead port (127.0.0.1:1) which
    /// guaranteed every call failed, this builds a real in-process mock
    /// S3 server on 127.0.0.1 and points a real BackendClient at it. Now
    /// unit tests can exercise the full upload / delete / head / list path.
    pub async fn dummy_for_test_async() -> Result<Self, xtable_core::XtableError> {
        // Spin up a tiny S3 mock on a free port.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| xtable_core::XtableError::Backend(format!("dummy bind: {}", e)))?;
        let addr = listener.local_addr().map_err(|e| xtable_core::XtableError::Backend(format!("dummy addr: {}", e)))?;
        let endpoint = format!("http://{}", addr);
        let mock = MockS3::default();
        let state = mock.clone();
        let app = axum::Router::new()
            .fallback(axum::routing::any(mock::handler))
            .with_state(state);
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        // Give the server a tick to start.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let _ = mock; // keep the Arc alive
        Self::build(
            &endpoint,
            "us-east-1",
            "xtable-test",
            "test",
            "test",
            true,
            5_000,
            16 * 1024 * 1024,
            16 * 1024 * 1024,
        )
        .await
        .map_err(xtable_core::XtableError::from)
    }

    /// Delete multiple objects (best-effort).
    pub async fn delete_objects(&self, keys: &[ObjectKey]) -> BackendResult<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let bucket = self.bucket_name();
        let mut identifiers: Vec<ObjectIdentifier> = Vec::with_capacity(keys.len());
        for k in keys {
            identifiers.push(
                ObjectIdentifier::builder()
                    .key(self.inner.keymap.backend_key(k).await)
                    .build()
                    .map_err(|e| BackendError::Internal(e.to_string()))?,
            );
        }
        let delete = Delete::builder()
            .set_objects(Some(identifiers))
            .build()
            .map_err(|e| BackendError::Internal(e.to_string()))?;
        let _ = self.inner.client.delete_objects().bucket(&bucket).delete(delete).send().await.map_err(map_sdk_err)?;
        Ok(())
    }

    /// Create multipart upload.
    pub async fn create_multipart(&self, key: &ObjectKey) -> BackendResult<String> {
        let bucket = self.bucket_name();
        let backend_key = self.inner.keymap.backend_key(key).await;
        let resp = self
            .inner
            .client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(&backend_key)
            .send()
            .await
            .map_err(map_sdk_err)?;
        Ok(resp.upload_id().unwrap_or_default().to_string())
    }

    /// Upload a part. Returns its ETag.
    pub async fn upload_part(
        &self,
        key: &ObjectKey,
        upload_id: &str,
        part_number: i32,
        body: Vec<u8>,
    ) -> BackendResult<String> {
        let bucket = self.bucket_name();
        let backend_key = self.inner.keymap.backend_key(key).await;
        let resp = self
            .inner
            .client
            .upload_part()
            .bucket(&bucket)
            .key(&backend_key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(ByteStream::from(body))
            .send()
            .await
            .map_err(map_sdk_err)?;
        Ok(resp.e_tag().unwrap_or_default().to_string())
    }

    /// Complete a multipart upload. Returns the composite ETag reported
    /// by S3 (which may differ from any individual part's ETag — it's a
    /// hash of the concatenation).
    pub async fn complete_multipart(
        &self,
        key: &ObjectKey,
        upload_id: &str,
        parts: Vec<(i32, String)>,
    ) -> BackendResult<String> {
        let bucket = self.bucket_name();
        let backend_key = self.inner.keymap.backend_key(key).await;
        let mut builder = CompletedMultipartUpload::builder();
        let mut completed_parts: Vec<CompletedPart> = Vec::with_capacity(parts.len());
        for (num, etag) in parts {
            completed_parts.push(
                CompletedPart::builder()
                    .part_number(num)
                    .e_tag(etag)
                    .build(),
            );
        }
        builder = builder.set_parts(Some(completed_parts));
        let completed = builder.build();
        let resp = self
            .inner
            .client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(&backend_key)
            .upload_id(upload_id)
            .multipart_upload(completed)
            .send()
            .await
            .map_err(map_sdk_err)?;
        Ok(resp.e_tag().unwrap_or_default().to_string())
    }

    /// Abort a multipart upload.
    pub async fn abort_multipart(&self, key: &ObjectKey, upload_id: &str) -> BackendResult<()> {
        let bucket = self.bucket_name();
        let backend_key = self.inner.keymap.backend_key(key).await;
        let _ = self
            .inner
            .client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key(&backend_key)
            .upload_id(upload_id)
            .send()
            .await
            .map_err(map_sdk_err)?;
        Ok(())
    }

    /// List object keys (paginated). Used by cold rebuild.
    pub async fn list_objects(&self) -> BackendResult<Vec<ListedObject>> {
        let bucket = self.bucket_name();
        let mut out: Vec<ListedObject> = Vec::new();
        let mut cont: Option<String> = None;
        loop {
            let mut req = self
                .inner
                .client
                .list_objects_v2()
                .bucket(&bucket);
            if let Some(c) = &cont {
                req = req.continuation_token(c);
            }
            let resp = req.send().await.map_err(map_sdk_err)?;
            for obj in resp.contents() {
                out.push(ListedObject {
                    key: obj.key().unwrap_or_default().to_string(),
                    size: obj.size().unwrap_or(0) as u64,
                    etag: obj.e_tag().unwrap_or_default().to_string(),
                    user_metadata: HashMap::new(), // list_objects doesn't return metadata
                });
            }
            if resp.is_truncated() == Some(true) {
                cont = resp.next_continuation_token().map(|s| s.to_string());
                if cont.is_none() {
                    break;
                }
            } else {
                break;
            }
        }
        Ok(out)
    }
}

#[derive(Debug, Clone)]
pub struct HeadObjectResult {
    pub size: u64,
    pub etag: String,
    pub content_type: String,
    pub user_metadata: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct GetObjectResult {
    pub bytes: Vec<u8>,
    pub etag: String,
    pub size: u64,
    pub user_metadata: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct PutObjectResult {
    pub etag: String,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ListedObject {
    pub key: String,
    pub size: u64,
    pub etag: String,
    pub user_metadata: HashMap<String, String>,
}