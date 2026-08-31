//! In-process recording backend for tests that need to assert
//! call counts (e.g. "commit did NOT issue per-record put_object calls").
//!
//! This is a thin wrapper around [`MockS3`] that runs its own axum
//! HTTP server with a recording handler. Every `PUT` / `GET` /
//! `DELETE` is counted via [`RecordingCounters`], which the test
//! inspects after exercising the system.
//!
//! Use it in place of [`BackendClient::dummy_for_test_async`] when
//! the assertion under test depends on knowing which backend calls
//! happened (and which did NOT).
//!
//! ## Example
//!
//! ```ignore
//! let recording = RecordingBackend::new();
//! let (_endpoint, backend) = recording.serve().await.unwrap();
//! // ... use `backend` as a `BackendClient` ...
//! assert_eq!(recording.counters.put_object_calls.load(Ordering::Relaxed), 0);
//! ```

use std::sync::atomic::{AtomicU64, Ordering};

use axum::extract::{Query, State};
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;

use xtable_backend::mock::MockS3;
use xtable_backend::BackendClient;

/// Public atomic counters for the recording backend. Inspect these
/// after exercising the system under test.
#[derive(Default, Debug)]
pub struct RecordingCounters {
    pub put_object_calls: AtomicU64,
    pub get_object_calls: AtomicU64,
    pub delete_object_calls: AtomicU64,
    pub head_object_calls: AtomicU64,
}

impl RecordingCounters {
    pub fn put_object_calls(&self) -> u64 {
        self.put_object_calls.load(Ordering::Relaxed)
    }
    pub fn get_object_calls(&self) -> u64 {
        self.get_object_calls.load(Ordering::Relaxed)
    }
    pub fn delete_object_calls(&self) -> u64 {
        self.delete_object_calls.load(Ordering::Relaxed)
    }
    pub fn head_object_calls(&self) -> u64 {
        self.head_object_calls.load(Ordering::Relaxed)
    }
}

/// Recording backend. Holds the in-memory object state from
/// [`MockS3`] plus a shared [`RecordingCounters`]. Spawns its own
/// axum server in [`RecordingBackend::serve`].
pub struct RecordingBackend {
    /// In-memory object store (same shape as `MockS3`).
    pub mock: MockS3,
    /// Counters shared between the recording handler and the test.
    pub counters: std::sync::Arc<RecordingCounters>,
}

impl RecordingBackend {
    /// Build a new recording backend.
    pub fn new() -> Self {
        Self {
            mock: MockS3::default(),
            counters: std::sync::Arc::new(RecordingCounters::default()),
        }
    }

    /// Spin up an axum server with the recording handler and
    /// build a `BackendClient` pointing at it. Returns the
    /// endpoint URL plus the client.
    pub async fn serve(&self) -> Result<(String, BackendClient), RecordingError> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| RecordingError::Bind(e.to_string()))?;
        let addr = listener
            .local_addr()
            .map_err(|e| RecordingError::Bind(e.to_string()))?;
        let endpoint = format!("http://{}", addr);

        // Handler state: (mock, counters). Cloneable.
        let state = (self.mock.clone(), self.counters.clone());
        let app = Router::new()
            .fallback(any(recording_handler))
            .with_state(state);
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        // Give the server a tick to start.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let backend = BackendClient::build(
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
        .map_err(|e| RecordingError::BuildBackend(e.to_string()))?;
        Ok((endpoint, backend))
    }
}

impl Default for RecordingBackend {
    fn default() -> Self {
        Self::new()
    }
}

/// Errors that can occur when serving a recording backend.
#[derive(Debug, thiserror::Error)]
pub enum RecordingError {
    #[error("bind/addr: {0}")]
    Bind(String),
    #[error("build backend: {0}")]
    BuildBackend(String),
}

/// Axum handler that mirrors `mock::handler` semantics (PUT/GET/HEAD/DELETE
/// for the bucket+key path) but increments `RecordingCounters` for every
/// call. Counters are kept separately from the mock state so a test can
/// observe them.
async fn recording_handler(
    State((mock, counters)): State<(MockS3, std::sync::Arc<RecordingCounters>)>,
    method: Method,
    uri: axum::http::Uri,
    headers: axum::http::HeaderMap,
    Query(_params): Query<std::collections::HashMap<String, String>>,
    body: axum::body::Bytes,
) -> Response {
    let path = uri.path().to_string();
    let trimmed = path.trim_start_matches('/');
    let (_bucket, key) = match trimmed.find('/') {
        Some(i) => (&trimmed[..i], trimmed[i + 1..].to_string()),
        None => (trimmed, String::new()),
    };

    match method {
        Method::PUT => {
            // Count even if key is empty (some impls PUT the bucket).
            if !key.is_empty() {
                counters.put_object_calls.fetch_add(1, Ordering::Relaxed);
            }
            // Mirror mock behaviour: extract metadata and store body.
            let mut meta = std::collections::HashMap::new();
            for (k, v) in headers.iter() {
                let name = k.as_str().to_ascii_lowercase();
                if name.starts_with("x-amz-meta-") {
                    meta.insert(name, v.to_str().unwrap_or_default().to_string());
                }
            }
            mock.objects
                .lock()
                .unwrap()
                .insert(key.clone(), body.to_vec());
            mock.meta.lock().unwrap().insert(key.clone(), meta);
            let etag = format!("\"recording-etag-{}\"", key);
            (StatusCode::OK, [("ETag", etag)]).into_response()
        }
        Method::GET => {
            counters.get_object_calls.fetch_add(1, Ordering::Relaxed);
            // ListObjectsV2 for empty key — return XML.
            if key.is_empty() {
                let objs = mock.objects.lock().unwrap();
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
                return (StatusCode::OK, [("content-type", "application/xml")], xml)
                    .into_response();
            }
            let objs = mock.objects.lock().unwrap();
            match objs.get(&key) {
                Some(bytes) => {
                    (StatusCode::OK, axum::body::Body::from(bytes.clone())).into_response()
                }
                None => (StatusCode::NOT_FOUND, "not found").into_response(),
            }
        }
        Method::HEAD => {
            counters.head_object_calls.fetch_add(1, Ordering::Relaxed);
            let objs = mock.objects.lock().unwrap();
            if objs.contains_key(&key) {
                (StatusCode::OK, "").into_response()
            } else {
                (StatusCode::NOT_FOUND, "not found").into_response()
            }
        }
        Method::DELETE => {
            if !key.is_empty() {
                counters.delete_object_calls.fetch_add(1, Ordering::Relaxed);
            }
            mock.objects.lock().unwrap().remove(&key);
            mock.meta.lock().unwrap().remove(&key);
            (StatusCode::NO_CONTENT, "").into_response()
        }
        _ => (StatusCode::NOT_FOUND, "unmatched").into_response(),
    }
}
