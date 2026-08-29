//! Names of xtable-specific HTTP headers and their semantics.

/// `x-xtable-txn-id`: identifies a transaction the request belongs to.
pub const XTABLE_TXN_ID: &str = "x-xtable-txn-id";

/// `x-xtable-version`: returned with PutObject/GetObject responses; the
/// logical version of the object as seen by xtable.
pub const XTABLE_VERSION: &str = "x-xtable-version";

/// `x-xtable-commit-version`: returned on successful CommitTxn.
pub const XTABLE_COMMIT_VERSION: &str = "x-xtable-commit-version";

/// `x-xtable-snapshot-version`: returned on BeginTxn; the snapshot version
/// the txn reads at.
pub const XTABLE_SNAPSHOT_VERSION: &str = "x-xtable-snapshot-version";

/// `x-xtable-txn-status`: returned by TxnStatus.
pub const XTABLE_TXN_STATUS: &str = "x-xtable-txn-status";

/// `x-xtable-conflict-keys`: CSV of keys that triggered OCC conflict.
pub const XTABLE_CONFLICT_KEYS: &str = "x-xtable-conflict-keys";

/// `x-xtable-idempotency-key`: optional client-supplied idempotency token.
pub const XTABLE_IDEMPOTENCY_KEY: &str = "x-xtable-idempotency-key";

/// `x-xtable-retry-after-ms`: hint to client when to retry.
pub const XTABLE_RETRY_AFTER_MS: &str = "x-xtable-retry-after-ms";

/// Object metadata keys attached to backend S3 objects so we can rebuild
/// the version index from the bucket alone.
pub mod backend_meta {
    /// Object's logical version.
    pub const XTABLE_VERSION: &str = "x-amz-meta-xtable-version";
    /// Txn that wrote this object (used for orphan detection).
    pub const XTABLE_TXN_ID: &str = "x-amz-meta-xtable-txn-id";
}

/// Transaction status values used in `x-xtable-txn-status` and in WAL.
#[derive(Debug, Copy, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TxnStatus {
    Active,
    Validating,
    Committing,
    Committed,
    Aborted,
}

impl TxnStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Validating => "validating",
            Self::Committing => "committing",
            Self::Committed => "committed",
            Self::Aborted => "aborted",
        }
    }
}

impl std::fmt::Display for TxnStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}