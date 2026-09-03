//! Transaction errors.

use thiserror::Error;
use xtable_core::XtableError;

#[derive(Debug, Error)]
pub enum TxnError {
    #[error("conflict on keys: {0}")]
    Conflict(String),

    #[error("transaction aborted: {0}")]
    Aborted(String),

    #[error("transaction expired")]
    Expired,

    #[error("unknown transaction: {0}")]
    UnknownTxn(String),

    #[error("backend error: {0}")]
    Backend(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("invalid state: {0}")]
    InvalidState(String),
}

impl From<TxnError> for XtableError {
    fn from(e: TxnError) -> Self {
        match e {
            TxnError::Conflict(k) => Self::conflict(format!("conflict on keys: {}", k)),
            TxnError::Aborted(reason) => Self::Conflict(format!("aborted: {}", reason)),
            TxnError::Expired => Self::TxnExpired("txn expired".into()),
            TxnError::UnknownTxn(id) => Self::UnknownTxn(id),
            TxnError::Backend(s) => Self::backend(s),
            TxnError::Storage(s) => Self::storage(s),
            TxnError::InvalidState(s) => Self::internal(format!("invalid state: {}", s)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_transaction_errors_map_to_public_errors() {
        let cases = [
            (TxnError::Conflict("k".into()), 409),
            (TxnError::Aborted("cancelled".into()), 409),
            (TxnError::Expired, 410),
            (TxnError::UnknownTxn("t".into()), 404),
            (TxnError::Backend("s3".into()), 502),
            (TxnError::Storage("disk".into()), 500),
            (TxnError::InvalidState("state".into()), 500),
        ];
        for (error, status) in cases {
            assert_eq!(XtableError::from(error).http_status(), status);
        }
    }
}
