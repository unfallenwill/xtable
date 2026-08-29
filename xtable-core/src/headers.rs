//! Transaction status values used in the local WAL/recovery protocol.

/// Transaction state machine values written to the local WAL and to
/// `TxnStateRecord.status`. This is independent of any HTTP layer.
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