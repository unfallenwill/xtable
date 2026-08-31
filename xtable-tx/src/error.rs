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
