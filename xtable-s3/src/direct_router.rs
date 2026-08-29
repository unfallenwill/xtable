//! Direct S3 router — bypasses s3s's auth/dispatch layer.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;

use xtable_auth::verify_request as xtable_verify_request;
use xtable_auth::EdgeAuth;

use crate::service::XtableS3Service;

/// State for the direct router.
#[derive(Clone)]
pub struct DirectRouterState(pub Arc<XtableS3Service>);

pub fn build_direct_router(auth: Arc<EdgeAuth>) -> axum::Router<DirectRouterState> {
    let auth_layer = middleware::from_fn_with_state(auth, direct_auth_middleware);
    axum::Router::new()
        .route(
            "/:bucket/*key",
            axum::routing::put(put_object)
                .get(get_object)
                .head(head_object)
                .delete(delete_object),
        )
        .route(
            "/:bucket",
            axum::routing::get(list_objects_v2).head(head_bucket),
        )
        .layer(auth_layer)
}

async fn direct_auth_middleware(
    axum::extract::State(auth): axum::extract::State<Arc<EdgeAuth>>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let method = req.method().clone();
    let is_read = matches!(method, axum::http::Method::GET | axum::http::Method::HEAD);
    if let Err(e) = xtable_verify_request(&auth, &req, is_read) {
        let status = StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::UNAUTHORIZED);
        return (status, format!("{}", e)).into_response();
    }
    next.run(req).await
}

fn extract_metadata(
    headers: &HeaderMap,
) -> Option<std::collections::HashMap<String, String>> {
    let mut map = std::collections::HashMap::new();
    for (k, v) in headers.iter() {
        let kn = k.as_str();
        if let Some(rest) = kn.strip_prefix("x-amz-meta-") {
            if let Ok(vs) = v.to_str() {
                map.insert(rest.to_string(), vs.to_string());
            }
        }
    }
    if map.is_empty() {
        None
    } else {
        Some(map)
    }
}

fn make_s3req<T>(
    input: T,
    headers: HeaderMap,
    method: axum::http::Method,
) -> s3s::S3Request<T> {
    s3s::S3Request {
        input,
        method,
        uri: "/".parse().unwrap(),
        headers,
        extensions: Default::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

async fn put_object(
    State(svc): State<DirectRouterState>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    use s3s::dto::{PutObjectInput, StreamingBlob};
    use s3s::S3;
    let input = PutObjectInput {
        bucket,
        key,
        content_length: Some(body.len() as i64),
        content_type: headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(String::from),
        metadata: extract_metadata(&headers),
        body: Some(StreamingBlob::from_bytes(Bytes::from(body.to_vec()))),
        ..Default::default()
    };
    let req = make_s3req(input, headers, axum::http::Method::PUT);
    match svc.0.put_object(req).await {
        Ok(r) => s3_response_to_axum(r),
        Err(e) => s3_error_to_axum(e),
    }
}

async fn get_object(
    State(svc): State<DirectRouterState>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    use s3s::dto::GetObjectInput;
    use s3s::S3;
    let input = GetObjectInput {
        bucket,
        key,
        ..Default::default()
    };
    let req = make_s3req(input, headers, axum::http::Method::GET);
    match svc.0.get_object(req).await {
        Ok(r) => {
            // For GetObject, convert StreamingBlob → axum Body via the
            // `From<StreamingBlob> for Body` impl s3s provides.
            let (status, hdrs, body_stream_opt) = (r.status, r.headers, r.output.body);
            let mut resp = Response::builder().status(status.unwrap_or(StatusCode::OK));
            for (k, v) in hdrs.into_iter() {
                if let Some(name) = k {
                    if let Ok(vs) = v.to_str() {
                        resp = resp.header(name.as_str(), vs);
                    }
                }
            }
            let mut body = match body_stream_opt {
                Some(stream) => s3s::Body::from(stream),
                None => s3s::Body::empty(),
            };
            let body_bytes = body.take_bytes().unwrap_or_default();
            resp.body(Body::from(body_bytes.to_vec()))
                .unwrap_or_else(|_| (StatusCode::INTERNAL_SERVER_ERROR, "internal").into_response())
        }
        Err(e) => s3_error_to_axum(e),
    }
}

async fn head_object(
    State(svc): State<DirectRouterState>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    use s3s::dto::HeadObjectInput;
    use s3s::S3;
    let input = HeadObjectInput {
        bucket,
        key,
        ..Default::default()
    };
    let req = make_s3req(input, headers, axum::http::Method::HEAD);
    match svc.0.head_object(req).await {
        Ok(r) => s3_response_to_axum(r),
        Err(e) => s3_error_to_axum(e),
    }
}

async fn delete_object(
    State(svc): State<DirectRouterState>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    use s3s::dto::DeleteObjectInput;
    use s3s::S3;
    let input = DeleteObjectInput {
        bucket,
        key,
        ..Default::default()
    };
    let req = make_s3req(input, headers, axum::http::Method::DELETE);
    match svc.0.delete_object(req).await {
        Ok(r) => s3_response_to_axum(r),
        Err(e) => s3_error_to_axum(e),
    }
}

async fn list_objects_v2(
    State(svc): State<DirectRouterState>,
    Path(bucket): Path<String>,
    headers: HeaderMap,
) -> Response {
    use s3s::dto::ListObjectsV2Input;
    use s3s::S3;
    let q = headers
        .get("x-amz-prefix")
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    let input = ListObjectsV2Input {
        bucket,
        prefix: q,
        ..Default::default()
    };
    let req = make_s3req(input, headers, axum::http::Method::GET);
    match svc.0.list_objects_v2(req).await {
        Ok(r) => s3_response_to_axum(r),
        Err(e) => s3_error_to_axum(e),
    }
}

async fn head_bucket(
    State(svc): State<DirectRouterState>,
    Path(bucket): Path<String>,
    headers: HeaderMap,
) -> Response {
    use s3s::dto::HeadBucketInput;
    use s3s::S3;
    let input = HeadBucketInput {
        bucket,
        ..Default::default()
    };
    let req = make_s3req(input, headers, axum::http::Method::HEAD);
    match svc.0.head_bucket(req).await {
        Ok(r) => s3_response_to_axum(r),
        Err(e) => s3_error_to_axum(e),
    }
}

fn s3_response_to_axum<R: std::fmt::Debug + 'static>(resp: s3s::S3Response<R>) -> Response {
    let (status, headers, output) = (resp.status, resp.headers, resp.output);

    let mut response = Response::builder().status(status.unwrap_or(StatusCode::OK));
    for (k, v) in headers.into_iter() {
        if let Some(name) = k {
            if let Ok(vs) = v.to_str() {
                response = response.header(name.as_str(), vs);
            }
        }
    }
    let body = format!("{:?}", output);
    response
        .header("content-type", "application/xml")
        .body(Body::from(body.into_bytes()))
        .unwrap_or_else(|_| (StatusCode::INTERNAL_SERVER_ERROR, "internal").into_response())
}

fn s3_error_to_axum(e: s3s::S3Error) -> Response {
    let status = e.status_code().unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let msg = e.message().unwrap_or("").to_string();
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>InternalError</Code><Message>{}</Message></Error>",
        msg
    );
    (
        status,
        [(axum::http::header::CONTENT_TYPE, "application/xml")],
        body,
    )
        .into_response()
}
