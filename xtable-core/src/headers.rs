//! Transaction status values used in the local WAL/recovery protocol.
//!
//! The state machine is `Active → Committing → {Committed, Aborted}`. SI
//! locks are acquired during the `Active` phase; the `Committing` phase
//! performs Cahill cycle detection, S3 uploads, atomic chain append, and
//! MemTable publish.

/// Transaction state machine values written to the local WAL and to
/// `TxnStateRecord.status`. This is independent of any HTTP layer.
#[derive(Debug, Copy, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TxnStatus {
    Active,
    /// SI locks acquired; about to publish + append chain entries.
    /// (Replaces the prior "uploads in flight" semantic.)
    Committing,
    Committed,
    Aborted,
}

impl TxnStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
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
