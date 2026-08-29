//! Static credential holder for v1.

use std::collections::HashMap;
use std::sync::RwLock;

/// A single access-key entry.
#[derive(Debug, Clone)]
pub struct CredentialEntry {
    pub access_key_id: String,
    pub secret_access_key: String,
    /// Optional principal name (only used for logging).
    pub principal: Option<String>,
}

/// A credential store. v1 just holds a static set loaded from config.
pub struct CredentialStore {
    inner: RwLock<HashMap<String, CredentialEntry>>,
}

impl std::fmt::Debug for CredentialStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let guard = self.inner.read().expect("poisoned");
        f.debug_struct("CredentialStore")
            .field("entries", &guard.len())
            .finish()
    }
}

impl Default for CredentialStore {
    fn default() -> Self {
        Self::new()
    }
}

impl CredentialStore {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    pub fn put(&self, entry: CredentialEntry) {
        let mut guard = self.inner.write().expect("poisoned");
        guard.insert(entry.access_key_id.clone(), entry);
    }

    pub fn lookup(&self, access_key_id: &str) -> Option<CredentialEntry> {
        let guard = self.inner.read().expect("poisoned");
        guard.get(access_key_id).cloned()
    }

    pub fn len(&self) -> usize {
        self.inner.read().expect("poisoned").len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().expect("poisoned").is_empty()
    }
}

/// Helper for v1 config: build a static entry.
#[derive(Debug, Clone)]
pub struct StaticCredential {
    pub access_key_id: String,
    pub secret_access_key: String,
}

impl StaticCredential {
    pub fn into_entry(self) -> CredentialEntry {
        CredentialEntry {
            access_key_id: self.access_key_id,
            secret_access_key: self.secret_access_key,
            principal: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_and_lookup() {
        let store = CredentialStore::new();
        store.put(
            StaticCredential {
                access_key_id: "ak1".into(),
                secret_access_key: "sk1".into(),
            }
            .into_entry(),
        );
        let e = store.lookup("ak1").unwrap();
        assert_eq!(e.secret_access_key, "sk1");
        assert!(store.lookup("missing").is_none());
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn put_overwrites() {
        let store = CredentialStore::new();
        store.put(
            StaticCredential {
                access_key_id: "ak".into(),
                secret_access_key: "sk1".into(),
            }
            .into_entry(),
        );
        store.put(
            StaticCredential {
                access_key_id: "ak".into(),
                secret_access_key: "sk2".into(),
            }
            .into_entry(),
        );
        assert_eq!(store.len(), 1);
        assert_eq!(store.lookup("ak").unwrap().secret_access_key, "sk2");
    }
}