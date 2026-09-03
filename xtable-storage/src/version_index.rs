//! Version index record type.

use serde::{Deserialize, Serialize};
use xtable_core::Version;

/// Per-object version index entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionRecord {
    pub latest_version: Version,
    pub latest_etag: String,
    pub latest_backend_key: String,
    pub last_writer_txn_id: String,
    pub tombstone: bool,
    pub size: u64,
    pub last_modified_unix_ms: i64,
}

impl Default for VersionRecord {
    fn default() -> Self {
        Self {
            latest_version: Version::ZERO,
            latest_etag: String::new(),
            latest_backend_key: String::new(),
            last_writer_txn_id: String::new(),
            tombstone: false,
            size: 0,
            last_modified_unix_ms: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_record_is_an_empty_non_tombstone() {
        let record = VersionRecord::default();
        assert_eq!(record.latest_version, Version::ZERO);
        assert!(record.latest_etag.is_empty());
        assert!(record.latest_backend_key.is_empty());
        assert!(record.last_writer_txn_id.is_empty());
        assert!(!record.tombstone);
        assert_eq!(record.size, 0);
        assert_eq!(record.last_modified_unix_ms, 0);
    }
}
