//! Helpers for building the S3 service.
//!
//! The actual axum wiring lives in xtable-server (to avoid a circular
//! dependency between xtable-s3 and xtable-server).

use std::sync::Arc;

use async_trait::async_trait;
use s3s::auth::{S3Auth, SecretKey};
use s3s::service::S3ServiceBuilder;

use xtable_auth::CredentialStore;

use crate::service::XtableS3Service;

/// Build the s3s service (the `S3Service` itself), wrapped in
/// `S3ServiceBuilder`. The caller (typically xtable-server) wraps this in
/// `HandleError` and mounts it as an axum fallback service.
///
/// We attach a passthrough `S3Auth` so that s3s's auth layer is satisfied
/// (it refuses to dispatch any S3 op without an auth provider). Real SigV4
/// verification is performed by the axum middleware in xtable-server; we
/// still return the *real* secret key per access key here so that s3s's
/// in-house signature recomputation matches what the middleware already
/// verified.
pub fn build_s3_service(svc: XtableS3Service) -> s3s::service::S3Service {
    let creds = svc.creds.clone();
    let mut builder = S3ServiceBuilder::new(svc);
    builder.set_auth(CredentialStoreAuth { creds });
    builder.build()
}

/// s3s S3Auth backed by xtable's edge `CredentialStore`. Returns the real
/// secret key for the given access key so s3s's own SigV4 re-verification
/// (which always runs) succeeds.
struct CredentialStoreAuth {
    creds: Arc<CredentialStore>,
}

#[async_trait]
impl S3Auth for CredentialStoreAuth {
    async fn get_secret_key(&self, access_key: &str) -> s3s::S3Result<SecretKey> {
        match self.creds.lookup(access_key) {
            Some(entry) => Ok(SecretKey::from(entry.secret_access_key)),
            None => Err(s3s::s3_error!(InvalidAccessKeyId)),
        }
    }
}
