//! Re-exports + helpers for xtable HTTP headers.

pub use xtable_core::headers::*;

use http::HeaderMap;

/// Pull `x-xtable-txn-id` from a header map, if present. Also accepts
/// `x-amz-meta-xtable-txn-id` (S3 metadata style) for compatibility with
/// aws-sdk-s3 callers that route through `.metadata(...)`.
pub fn txn_id_header(headers: &HeaderMap) -> Option<String> {
    headers
        .get(XTABLE_TXN_ID)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .or_else(|| {
            headers
                .get(backend_meta::XTABLE_TXN_ID)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string())
        })
}

/// Pull `x-xtable-idempotency-key` from a header map, if present.
pub fn idempotency_key_header(headers: &HeaderMap) -> Option<String> {
    headers
        .get(XTABLE_IDEMPOTENCY_KEY)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}