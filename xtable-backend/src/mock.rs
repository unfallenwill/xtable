//! In-process mock S3 backend for unit tests.
//!
//! V17 fix: `BackendClient::dummy_for_test_async` now spawns an actual
//! axum server backed by this mock, so unit tests can exercise the
//! full upload / delete / list / head path against a real HTTP client
//! (rather than always-failing calls to a dead port).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::extract::{Query, State};
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::Router;

#[derive(Clone, Default)]
pub struct MockS3 {
    pub objects: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    pub meta: Arc<Mutex<HashMap<String, HashMap<String, String>>>>,
    pub multiparts: Arc<Mutex<HashMap<String, MultipartState>>>,
}

#[derive(Clone, Default)]
pub struct MultipartState {
    key: String,
    parts: Vec<(i32, Vec<u8>)>,
}

pub async fn handler(
    State(s): State<MockS3>,
    method: Method,
    uri: axum::http::Uri,
    headers: axum::http::HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    body: axum::body::Bytes,
) -> Response {
    let path = uri.path().to_string();
    let trimmed = path.trim_start_matches('/');
    let (_, key) = match trimmed.find('/') {
        Some(i) => (&trimmed[..i], trimmed[i + 1..].to_string()),
        None => (trimmed, String::new()),
    };

    // ListObjectsV2: GET /bucket
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

    // Initiate multipart: POST /bucket/key with ?uploads
    if method == Method::POST && params.contains_key("uploads") {
        let upload_id = format!("mock-upload-{}", uuid_like(&key));
        s.multiparts.lock().unwrap().insert(
            upload_id.clone(),
            MultipartState {
                key: key.clone(),
                parts: vec![],
            },
        );
        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?><InitiateMultipartUploadResult><Bucket>xtable-test</Bucket><Key>{}</Key><UploadId>{}</UploadId></InitiateMultipartUploadResult>"#,
            key, upload_id
        );
        return (StatusCode::OK, [("content-type", "application/xml")], xml).into_response();
    }

    // Multipart operations: PUT/POST/DELETE with ?uploadId
    if let Some(upload_id) = params.get("uploadId") {
        let mut mp = s.multiparts.lock().unwrap();
        if !mp.contains_key(upload_id) {
            return (StatusCode::NOT_FOUND, "no such upload").into_response();
        }
        // UploadPart: PUT ?uploadId&partNumber
        if method == Method::PUT {
            if let Some(pn_str) = params.get("partNumber") {
                if let Ok(pn) = pn_str.parse::<i32>() {
                    if let Some(state) = mp.get_mut(upload_id) {
                        state.parts.push((pn, body.to_vec()));
                        let etag = format!("\"etag-{}\"", pn);
                        return (StatusCode::OK, [("ETag", etag)]).into_response();
                    }
                }
            }
        }
        // AbortMultipartUpload: DELETE
        if method == Method::DELETE {
            mp.remove(upload_id);
            return (StatusCode::NO_CONTENT, "").into_response();
        }
        // CompleteMultipartUpload: POST
        if method == Method::POST {
            if let Some(state) = mp.remove(upload_id) {
                let mut all = Vec::new();
                let mut parts = state.parts.clone();
                parts.sort_by_key(|(n, _)| *n);
                for (_, b) in parts {
                    all.extend(b);
                }
                s.objects.lock().unwrap().insert(state.key.clone(), all);
                let xml = format!(
                    r#"<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUploadResult><Bucket>xtable-test</Bucket><Key>{}</Key></CompleteMultipartUploadResult>"#,
                    state.key
                );
                return (StatusCode::OK, [("content-type", "application/xml")], xml)
                    .into_response();
            }
            return (StatusCode::NOT_FOUND, "no such upload").into_response();
        }
    }

    // PutObject
    if method == Method::PUT {
        let mut meta = HashMap::new();
        for (k, v) in headers.iter() {
            let name = k.as_str().to_ascii_lowercase();
            if name.starts_with("x-amz-meta-") {
                meta.insert(name, v.to_str().unwrap_or_default().to_string());
            }
        }
        s.objects.lock().unwrap().insert(key.clone(), body.to_vec());
        s.meta.lock().unwrap().insert(key.clone(), meta);
        // PR-Fix12: return a stable ETag header so callers (incl. multipart
        // complete-multipart) can verify upload identity. AWS-SDK requires the
        // canonical capitalisation.
        let etag = format!("\"mock-etag-{}\"", key);
        return (StatusCode::OK, [("ETag", etag.as_str())], "").into_response();
    }

    // GetObject
    if method == Method::GET {
        let objs = s.objects.lock().unwrap();
        match objs.get(&key) {
            Some(bytes) => (StatusCode::OK, axum::body::Body::from(bytes.clone())).into_response(),
            None => (StatusCode::NOT_FOUND, "not found").into_response(),
        }
    } else if method == Method::HEAD {
        let objs = s.objects.lock().unwrap();
        if objs.contains_key(&key) {
            (StatusCode::OK, "").into_response()
        } else {
            (StatusCode::NOT_FOUND, "not found").into_response()
        }
    } else if method == Method::DELETE {
        s.objects.lock().unwrap().remove(&key);
        s.meta.lock().unwrap().remove(&key);
        (StatusCode::NO_CONTENT, "").into_response()
    } else {
        (StatusCode::NOT_FOUND, "unmatched").into_response()
    }
}

fn uuid_like(s: &str) -> String {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(s.as_bytes());
    hex::encode(&hasher.finalize()[..8])
}

#[allow(dead_code)]
pub fn mock_router(state: MockS3) -> Router {
    Router::new().fallback(any(handler)).with_state(state)
}
