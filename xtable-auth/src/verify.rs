//! SigV4 verification helpers.

use std::sync::Arc;

use crate::credentials::CredentialStore;
use xtable_core::XtableError;

/// Trait implemented by any verifier we plug in front of the S3 router.
pub trait XtableAuthenticator: Send + Sync {
    fn verify(&self, req: &http::Request<axum::body::Body>) -> Result<(), XtableError>;
}

/// Combined policy + credential store.
pub struct EdgeAuth {
    pub creds: Arc<CredentialStore>,
    pub allow_anonymous_read: bool,
    /// Region used for SigV4 signature verification. SigV4 folds the region
    /// into the HMAC signing key, so this must match the region the client
    /// used when signing. Read from `[backend].region` in production;
    /// non-AWS S3-compatible providers (volcengine TOS, etc.) require the
    /// bucket's actual region — `us-east-1` is only correct for AWS.
    pub region: String,
}

impl std::fmt::Debug for EdgeAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EdgeAuth")
            .field("creds", &self.creds)
            .field("allow_anonymous_read", &self.allow_anonymous_read)
            .finish()
    }
}

/// Inspect the Authorization header to decide whether the request is
/// anonymous. SigV4 requests always have it; presigned URLs put the
/// signature in the query string.
pub fn is_anonymous<B>(req: &http::Request<B>) -> bool {
    !req.headers().contains_key("authorization")
        && !req
            .uri()
            .query()
            .map(|q| q.contains("X-Amz-Signature="))
            .unwrap_or(false)
}

/// Extract the access key id from a SigV4 Authorization header value, if any.
pub fn extract_access_key_id<B>(req: &http::Request<B>) -> Option<String> {
    let h = req.headers().get("authorization")?.to_str().ok()?;
    let creds_marker = "Credential=";
    let start = h.find(creds_marker)? + creds_marker.len();
    let rest = &h[start..];
    let end = rest.find(',').unwrap_or(rest.len());
    let ak = rest[..end].trim();
    let slash = ak.find('/').unwrap_or(ak.len());
    Some(ak[..slash].to_string())
}

/// Verify a request. Returns Ok(()) if the request is authenticated or
/// (in anonymous-read mode) is a safe read.
pub fn verify_request<B>(
    auth: &EdgeAuth,
    req: &http::Request<B>,
    is_read: bool,
) -> Result<(), XtableError> {
    if is_read && auth.allow_anonymous_read && is_anonymous(req) {
        return Ok(());
    }

    let ak = extract_access_key_id(req).ok_or_else(|| {
        XtableError::Unauthorized("missing or malformed Authorization header".into())
    })?;
    let _entry = auth
        .creds
        .lookup(&ak)
        .ok_or_else(|| XtableError::Unauthorized(format!("unknown access key: {}", ak)))?;

    verify_sigv4_signature(req, &ak, &_entry.secret_access_key, &auth.region)?;

    Ok(())
}

/// Hand-rolled SigV4 verification. Matches the canonical request format used
/// by xtable's middleware (which s3s's verifier, and our probe, also use).
///
/// `region` must match the region the client used when signing — SigV4 folds
/// the region into the HMAC signing key, so a mismatch produces a different
/// signature and verification fails. Production callers pass
/// `&auth.region`, which is populated from `[backend].region` in the
/// server config. Non-AWS S3-compatible providers (volcengine TOS, etc.)
/// require the bucket's actual region here; `us-east-1` is only correct
/// for AWS.
pub fn verify_sigv4_signature<B>(
    req: &http::Request<B>,
    access_key_id: &str,
    secret_access_key: &str,
    region: &str,
) -> Result<(), XtableError> {
    use sha2::{Digest, Sha256};

    let h = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| XtableError::Unauthorized("missing Authorization".into()))?;
    let ak_in_hdr = h
        .split("Credential=")
        .nth(1)
        .and_then(|s| s.split(',').next())
        .and_then(|s| s.split('/').next())
        .ok_or_else(|| XtableError::Unauthorized("malformed Credential".into()))?;
    if ak_in_hdr != access_key_id {
        return Err(XtableError::Unauthorized("access key mismatch".into()));
    }
    let signature = h
        .split("Signature=")
        .nth(1)
        .ok_or_else(|| XtableError::Unauthorized("missing Signature".into()))?
        .trim();
    let signed_headers_str = h
        .split("SignedHeaders=")
        .nth(1)
        .and_then(|s| s.split(',').next())
        .ok_or_else(|| XtableError::Unauthorized("missing SignedHeaders".into()))?
        .trim();
    let date = req
        .headers()
        .get("x-amz-date")
        .and_then(|v| v.to_str().ok())
        .or_else(|| req.headers().get("date").and_then(|v| v.to_str().ok()))
        .ok_or_else(|| XtableError::Unauthorized("missing date".into()))?
        .to_string();

    let mut signed_headers = Vec::<String>::new();
    for h in signed_headers_str.split(';') {
        signed_headers.push(h.trim().to_lowercase());
    }
    let mut canonical_headers = String::new();
    for h in &signed_headers {
        let name = h.as_str();
        if let Some(v) = req.headers().get(name).and_then(|v| v.to_str().ok()) {
            canonical_headers.push_str(&format!("{}:{}\n", name, v.trim()));
        }
    }
    let signed_headers_canonical = signed_headers.join(";");

    let payload_hash = req
        .headers()
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("UNSIGNED-PAYLOAD")
        .to_string();

    // SigV4 spec: each section is newline-terminated; the empty line
    // between CanonicalHeaders and SignedHeaders is a single `\n` after
    // the headers' trailing `\n`. With `canonical_headers` already ending
    // in `\n`, the separator is one additional `\n`.
    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        req.method(),
        req.uri().path(),
        req.uri().query().unwrap_or(""),
        canonical_headers,
        signed_headers_canonical,
        payload_hash,
    );
    let canonical_request_hash = {
        let mut h = Sha256::new();
        h.update(canonical_request.as_bytes());
        hex::encode(h.finalize())
    };

    let date_short = &date[..8];
    let scope = format!("{}/{}/s3/aws4_request", date_short, region);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        date, scope, canonical_request_hash
    );

    let k_secret = format!("AWS4{}", secret_access_key);
    let k_date = hmac_sha256(k_secret.as_bytes(), date_short.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, b"s3");
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    let computed = hex::encode(hmac_sha256(&k_signing, string_to_sign.as_bytes()));

    if !constant_time_eq(computed.as_bytes(), signature.as_bytes()) {
        return Err(XtableError::Unauthorized(format!(
            "signature mismatch (expected={}, got={})",
            computed, signature
        )));
    }
    Ok(())
}

fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(msg);
    let result = mac.finalize().into_bytes();
    let mut out = [0u8; 32];
    out.copy_from_slice(&result);
    out
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for i in 0..a.len() {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Request;

    fn edge_auth(allow_anon: bool, ak: &str, sk: &str) -> EdgeAuth {
        let store = Arc::new(CredentialStore::new());
        store.put(
            crate::credentials::StaticCredential {
                access_key_id: ak.into(),
                secret_access_key: sk.into(),
            }
            .into_entry(),
        );
        EdgeAuth {
            creds: store,
            allow_anonymous_read: allow_anon,
            region: "us-east-1".into(),
        }
    }

    #[test]
    fn anonymous_read_when_allowed() {
        let auth = edge_auth(true, "ak", "sk");
        let req = Request::builder().uri("/foo").body(()).unwrap();
        assert!(verify_request(&auth, &req, true).is_ok());
    }

    #[test]
    fn anonymous_write_rejected() {
        let auth = edge_auth(true, "ak", "sk");
        let req = Request::builder().uri("/foo").body(()).unwrap();
        let err = verify_request(&auth, &req, false).unwrap_err();
        assert_eq!(err.http_status(), 401);
    }

    #[test]
    fn anonymous_denied_when_disallowed() {
        let auth = edge_auth(false, "ak", "sk");
        let req = Request::builder().uri("/foo").body(()).unwrap();
        let err = verify_request(&auth, &req, true).unwrap_err();
        assert_eq!(err.http_status(), 401);
    }

    #[test]
    fn known_access_key_passes() {
        // V15 fix: verify_request now checks the SigV4 signature, not just
        // the access key. This test builds a correctly-signed request and
        // asserts it passes.
        use sha2::{Digest, Sha256};
        let auth = edge_auth(false, "ak", "sk");
        let body = b"";
        let payload_hash = hex::encode(Sha256::digest(body));
        let date = "20260101T000000Z";
        let date_short = "20260101";
        let region = "us-east-1";
        let service = "s3";
        let host = "example.com";
        let canonical_uri = "/foo";
        let canonical_querystring = "";
        let signed_headers = "host;x-amz-content-sha256;x-amz-date";
        let canonical_headers = format!(
            "host:{}\nx-amz-content-sha256:{}\nx-amz-date:{}\n",
            host, payload_hash, date
        );
        let canonical_request = format!(
            "GET\n{}\n{}\n{}\n{}\n{}",
            canonical_uri, canonical_querystring, canonical_headers, signed_headers, payload_hash
        );
        let canonical_request_hash = hex::encode(Sha256::digest(canonical_request.as_bytes()));
        let scope = format!("{}/{}/{}/aws4_request", date_short, region, service);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            date, scope, canonical_request_hash
        );
        let k_secret = format!("AWS4sk");
        let k_date = hmac_sha256(k_secret.as_bytes(), date_short.as_bytes());
        let k_region = hmac_sha256(&k_date, region.as_bytes());
        let k_service = hmac_sha256(&k_region, service.as_bytes());
        let k_signing = hmac_sha256(&k_service, b"aws4_request");
        let signature = hex::encode(hmac_sha256(&k_signing, string_to_sign.as_bytes()));
        let auth_header = format!(
            "AWS4-HMAC-SHA256 Credential=ak/{}/{}, SignedHeaders={}, Signature={}",
            date_short, scope, signed_headers, signature
        );
        let req = Request::builder()
            .uri("/foo")
            .header("host", host)
            .header("x-amz-date", date)
            .header("x-amz-content-sha256", &payload_hash)
            .header("authorization", &auth_header)
            .body(())
            .unwrap();
        assert!(
            verify_request(&auth, &req, false).is_ok(),
            "signed request must pass"
        );
    }

    #[test]
    fn unknown_access_key_rejected() {
        let auth = edge_auth(false, "ak", "sk");
        let req = Request::builder()
            .uri("/foo")
            .header(
                "authorization",
                "AWS4-HMAC-SHA256 Credential=other/20260101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abc",
            )
            .body(())
            .unwrap();
        let err = verify_request(&auth, &req, false).unwrap_err();
        assert!(format!("{}", err).contains("unknown access key"));
    }

    #[test]
    fn extract_ak_handles_no_scope() {
        let req = Request::builder()
            .uri("/")
            .header("authorization", "AWS4-HMAC-SHA256 Credential=akid")
            .body(())
            .unwrap();
        assert_eq!(extract_access_key_id(&req).as_deref(), Some("akid"));
    }

    #[test]
    fn extract_ak_returns_none_when_missing() {
        let req = Request::builder().uri("/").body(()).unwrap();
        assert!(extract_access_key_id(&req).is_none());
    }

    #[test]
    fn is_anonymous_detects_query_sigv4() {
        let req = Request::builder()
            .uri("/foo?X-Amz-Signature=abc")
            .body(())
            .unwrap();
        assert!(!is_anonymous(&req));
    }
}
