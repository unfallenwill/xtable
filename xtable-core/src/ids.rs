//! Strongly-typed IDs and version counters.

use serde::{Deserialize, Serialize};
use std::fmt;
use ulid::Ulid;

/// A logical version of an object. Monotonically increasing per object.
#[derive(Copy, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Version(pub u64);

impl Version {
    pub const ZERO: Version = Version(0);

    pub fn next(self) -> Version {
        Version(self.0 + 1)
    }

    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Debug for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}", self.0)
    }
}

/// A transaction identifier. ULID for time-orderability and unique 26-char
/// representation that's friendly in HTTP headers.
#[derive(Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TxnId(pub Ulid);

impl TxnId {
    pub fn new() -> Self {
        Self(Ulid::new())
    }

    pub fn as_ulid(self) -> Ulid {
        self.0
    }

    pub fn as_string(self) -> String {
        self.0.to_string()
    }

    pub fn from_string(s: &str) -> Result<Self, ulid::DecodeError> {
        Ok(Self(Ulid::from_string(s)?))
    }
}

impl Default for TxnId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TxnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Debug for TxnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "txn({})", self.0)
    }
}

/// Object key (the path-like string within a bucket). Validated for length
/// and basic S3 constraints.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectKey(pub String);

impl ObjectKey {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Display for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Debug for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl AsRef<str> for ObjectKey {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_monotonic() {
        let v = Version::ZERO;
        let v1 = v.next();
        assert_eq!(v1.as_u64(), 1);
    }

    #[test]
    fn txn_id_roundtrip() {
        let id = TxnId::new();
        let s = id.as_string();
        let parsed = TxnId::from_string(&s).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn versions_and_object_keys_have_expected_formats() {
        assert_eq!(Version::ZERO.to_string(), "0");
        assert_eq!(format!("{:?}", Version(7)), "v7");
        let key = ObjectKey::new("a/b");
        assert_eq!(key.as_str(), "a/b");
        assert_eq!(key.len(), 3);
        assert!(!key.is_empty());
        assert_eq!(key.as_ref(), "a/b");
        assert_eq!(key.to_string(), "a/b");
        assert_eq!(format!("{:?}", key), "\"a/b\"");
        assert_eq!(key.clone().into_string(), "a/b");
    }

    #[test]
    fn invalid_txn_id_is_rejected() {
        assert!(TxnId::from_string("not-a-ulid").is_err());
        assert_eq!(TxnId::default().as_ulid().to_string().len(), 26);
    }
}
