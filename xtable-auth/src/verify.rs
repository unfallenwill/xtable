//! JWT verification for the xtable HTTP API.
//!
//! The API accepts Authorization: Bearer <JWT> and verifies HS256 tokens.
//! S3/TOS authentication is handled separately by xtable-backend.

use std::sync::Arc;

use base64::Engine;
use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::Sha256;
use xtable_core::XtableError;

type HmacSha256 = Hmac<Sha256>;

/// API authentication policy. The secret must be shared with the token issuer.
pub struct EdgeAuth {
    pub jwt_secret: Arc<Vec<u8>>,
    pub jwt_issuer: Option<String>,
    pub jwt_audience: Option<String>,
    pub allow_anonymous_read: bool,
}

impl EdgeAuth {
    pub fn new(
        jwt_secret: impl Into<Vec<u8>>,
        jwt_issuer: Option<String>,
        jwt_audience: Option<String>,
        allow_anonymous_read: bool,
    ) -> Self {
        Self {
            jwt_secret: Arc::new(jwt_secret.into()),
            jwt_issuer,
            jwt_audience,
            allow_anonymous_read,
        }
    }
}

impl std::fmt::Debug for EdgeAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EdgeAuth")
            .field("jwt_secret", &"[redacted]")
            .field("jwt_issuer", &self.jwt_issuer)
            .field("jwt_audience", &self.jwt_audience)
            .field("allow_anonymous_read", &self.allow_anonymous_read)
            .finish()
    }
}

/// Trait implemented by the server middleware boundary.
pub trait XtableAuthenticator: Send + Sync {
    fn verify(&self, req: &http::Request<axum::body::Body>) -> Result<(), XtableError>;
}

/// Verify a request. Anonymous GET/HEAD is allowed only when explicitly
/// enabled; all writes require a valid JWT.
pub fn verify_request<B>(
    auth: &EdgeAuth,
    req: &http::Request<B>,
    is_read: bool,
) -> Result<(), XtableError> {
    if is_read && auth.allow_anonymous_read && is_anonymous(req) {
        return Ok(());
    }
    let header = req
        .headers()
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| XtableError::Unauthorized("missing Authorization header".into()))?;
    let token = header
        .strip_prefix("Bearer ")
        .or_else(|| header.strip_prefix("bearer "))
        .ok_or_else(|| XtableError::Unauthorized("Authorization must use Bearer scheme".into()))?;
    verify_jwt(token.trim(), auth)
}

pub fn is_anonymous<B>(req: &http::Request<B>) -> bool {
    !req.headers().contains_key(http::header::AUTHORIZATION)
}

fn verify_jwt(token: &str, auth: &EdgeAuth) -> Result<(), XtableError> {
    let mut parts = token.split('.');
    let header_b64 = parts.next().unwrap_or_default();
    let payload_b64 = parts.next().unwrap_or_default();
    let signature_b64 = parts.next().unwrap_or_default();
    if header_b64.is_empty()
        || payload_b64.is_empty()
        || signature_b64.is_empty()
        || parts.next().is_some()
    {
        return Err(XtableError::Unauthorized("malformed JWT".into()));
    }

    let decoder = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let header = decoder
        .decode(header_b64)
        .map_err(|_| XtableError::Unauthorized("malformed JWT header".into()))?;
    let payload = decoder
        .decode(payload_b64)
        .map_err(|_| XtableError::Unauthorized("malformed JWT payload".into()))?;
    let signature = decoder
        .decode(signature_b64)
        .map_err(|_| XtableError::Unauthorized("malformed JWT signature".into()))?;

    let header: Value = serde_json::from_slice(&header)
        .map_err(|_| XtableError::Unauthorized("malformed JWT header".into()))?;
    if header.get("alg").and_then(Value::as_str) != Some("HS256") {
        return Err(XtableError::Unauthorized("JWT alg must be HS256".into()));
    }

    let signing_input = format!("{header_b64}.{payload_b64}");
    let mut mac = HmacSha256::new_from_slice(auth.jwt_secret.as_ref())
        .map_err(|_| XtableError::Unauthorized("invalid JWT secret".into()))?;
    mac.update(signing_input.as_bytes());
    mac.verify_slice(&signature)
        .map_err(|_| XtableError::Unauthorized("JWT signature mismatch".into()))?;

    let claims: Value = serde_json::from_slice(&payload)
        .map_err(|_| XtableError::Unauthorized("malformed JWT claims".into()))?;
    let now = chrono::Utc::now().timestamp();
    let exp = claims
        .get("exp")
        .and_then(Value::as_i64)
        .ok_or_else(|| XtableError::Unauthorized("JWT exp claim is required".into()))?;
    if exp <= now {
        return Err(XtableError::Unauthorized("JWT has expired".into()));
    }
    if let Some(nbf) = claims.get("nbf").and_then(Value::as_i64) {
        if nbf > now {
            return Err(XtableError::Unauthorized("JWT is not yet valid".into()));
        }
    }
    if let Some(issuer) = &auth.jwt_issuer {
        if claims.get("iss").and_then(Value::as_str) != Some(issuer.as_str()) {
            return Err(XtableError::Unauthorized("JWT issuer mismatch".into()));
        }
    }
    if let Some(audience) = &auth.jwt_audience {
        let matches = claims.get("aud").is_some_and(|aud| {
            aud.as_str() == Some(audience.as_str())
                || aud
                    .as_array()
                    .is_some_and(|values| values.iter().any(|v| v.as_str() == Some(audience)))
        });
        if !matches {
            return Err(XtableError::Unauthorized("JWT audience mismatch".into()));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use http::Request;

    fn token(secret: &str, claims: Value) -> String {
        let h = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
        let p = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap());
        let input = format!("{h}.{p}");
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(input.as_bytes());
        format!(
            "{input}.{}",
            URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
        )
    }

    fn auth(allow_anonymous_read: bool) -> EdgeAuth {
        EdgeAuth::new("secret", None, None, allow_anonymous_read)
    }

    #[test]
    fn valid_hs256_bearer_token_passes() {
        let jwt = token(
            "secret",
            serde_json::json!({"sub":"u1","exp":4_000_000_000i64}),
        );
        let req = Request::builder()
            .uri("/v1/spaces/snapshot")
            .header("authorization", format!("Bearer {jwt}"))
            .body(())
            .unwrap();
        assert!(verify_request(&auth(false), &req, true).is_ok());
    }

    #[test]
    fn expired_or_wrong_secret_is_rejected() {
        let jwt = token("secret", serde_json::json!({"exp":1i64}));
        let req = Request::builder()
            .uri("/")
            .header("authorization", format!("Bearer {jwt}"))
            .body(())
            .unwrap();
        assert!(verify_request(&auth(false), &req, false).is_err());
        let jwt = token("wrong", serde_json::json!({"exp":4_000_000_000i64}));
        let req = Request::builder()
            .uri("/")
            .header("authorization", format!("Bearer {jwt}"))
            .body(())
            .unwrap();
        assert!(verify_request(&auth(false), &req, false).is_err());
    }

    #[test]
    fn anonymous_reads_follow_policy_but_writes_do_not() {
        let req = Request::builder().uri("/").body(()).unwrap();
        assert!(verify_request(&auth(true), &req, true).is_ok());
        assert!(verify_request(&auth(true), &req, false).is_err());
    }

    #[test]
    fn issuer_and_audience_are_checked() {
        let auth = EdgeAuth::new("secret", Some("issuer".into()), Some("api".into()), false);
        let jwt = token(
            "secret",
            serde_json::json!({"exp":4_000_000_000i64,"iss":"issuer","aud":["other","api"]}),
        );
        let req = Request::builder()
            .uri("/")
            .header("authorization", format!("Bearer {jwt}"))
            .body(())
            .unwrap();
        assert!(verify_request(&auth, &req, false).is_ok());
    }
}
