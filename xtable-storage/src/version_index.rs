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
