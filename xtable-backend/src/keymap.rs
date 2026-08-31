//! Key mapping from xtable logical key to backend S3 key.
//!
//! v1 single-tenant single-bucket: identity mapping.
//! v2 multi-tenant will introduce `{tenant_id}/{table_id}/{key}` prefixes.

use async_trait::async_trait;
use xtable_core::ObjectKey;

/// Translates an xtable object key into the S3 backend key (and optionally
/// a bucket name to read from).
#[async_trait]
pub trait KeyMap: Send + Sync {
    /// Bucket to read/write this key in.
    fn bucket_for(&self, key: &ObjectKey) -> String;
    /// Backend S3 object key (may differ from the logical key).
    async fn backend_key(&self, key: &ObjectKey) -> String;
    /// Reverse mapping from a backend key back to logical key (used during
    /// cold rebuild / ListObjects).
    async fn logical_key(&self, backend_key: &str) -> Option<ObjectKey>;
}

/// Identity mapping: bucket == cfg.bucket, backend_key == logical key.
pub struct IdentityKeyMap {
    pub bucket: String,
}

impl IdentityKeyMap {
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
        }
    }
}

#[async_trait]
impl KeyMap for IdentityKeyMap {
    fn bucket_for(&self, _key: &ObjectKey) -> String {
        self.bucket.clone()
    }
    async fn backend_key(&self, key: &ObjectKey) -> String {
        key.as_str().to_string()
    }
    async fn logical_key(&self, backend_key: &str) -> Option<ObjectKey> {
        Some(ObjectKey::new(backend_key.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn identity_roundtrip() {
        let km = IdentityKeyMap::new("xtable-data");
        let k = ObjectKey::new("path/to/file.txt");
        assert_eq!(km.bucket_for(&k), "xtable-data");
        assert_eq!(km.backend_key(&k).await, "path/to/file.txt");
        assert_eq!(
            km.logical_key("path/to/file.txt").await.unwrap(),
            ObjectKey::new("path/to/file.txt")
        );
    }
}
