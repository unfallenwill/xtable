//! Crate-wide error type.

use std::fmt;

/// Result alias used across xtable.
pub type XtableResult<T> = std::result::Result<T, XtableError>;

/// All errors produced inside xtable. Other crates map their specific errors
/// into this enum at the boundary (e.g. `From<BackendError>` in xtable-backend).
#[derive(Debug, thiserror::Error)]
pub enum XtableError {
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("transaction expired: {0}")]
    TxnExpired(String),

    #[error("unknown transaction: {0}")]
    UnknownTxn(String),

    #[error("unauthorized: {0}")]
    Unauthorized(String),

    #[error("forbidden: {0}")]
    Forbidden(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("backend error: {0}")]
    Backend(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Serde(String),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("not implemented: {0}")]
    NotImplemented(String),
}

impl XtableError {
    pub fn invalid<S: fmt::Display>(s: S) -> Self {
        Self::InvalidArgument(s.to_string())
    }
    pub fn not_found<S: fmt::Display>(s: S) -> Self {
        Self::NotFound(s.to_string())
    }
    pub fn conflict<S: fmt::Display>(s: S) -> Self {
        Self::Conflict(s.to_string())
    }
    pub fn storage<S: fmt::Display>(s: S) -> Self {
        Self::Storage(s.to_string())
    }
    pub fn backend<S: fmt::Display>(s: S) -> Self {
        Self::Backend(s.to_string())
    }
    pub fn internal<S: fmt::Display>(s: S) -> Self {
        Self::Internal(s.to_string())
    }
    pub fn not_implemented<S: fmt::Display>(s: S) -> Self {
        Self::NotImplemented(s.to_string())
    }

    /// HTTP status code that best represents this error.
    pub fn http_status(&self) -> u16 {
        match self {
            Self::InvalidArgument(_) => 400,
            Self::Unauthorized(_) => 401,
            Self::Forbidden(_) => 403,
            Self::NotFound(_) | Self::UnknownTxn(_) => 404,
            Self::Conflict(_) => 409,
            Self::TxnExpired(_) => 410,
            Self::Storage(_) | Self::Io(_) => 500,
            Self::Backend(_) => 502,
            Self::Serde(_) | Self::Internal(_) => 500,
            Self::NotImplemented(_) => 501,
        }
    }

    /// S3-style error code, when applicable.
    pub fn s3_code(&self) -> &'static str {
        match self {
            Self::InvalidArgument(_) => "InvalidArgument",
            Self::Unauthorized(_) => "Unauthorized",
            Self::Forbidden(_) => "Forbidden",
            Self::NotFound(_) => "NoSuchKey",
            Self::UnknownTxn(_) => "UnknownTxn",
            Self::Conflict(_) => "TransactionConflict",
            Self::TxnExpired(_) => "TxnExpired",
            Self::Storage(_) => "InternalError",
            Self::Io(_) => "InternalError",
            Self::Backend(_) => "BackendError",
            Self::Serde(_) => "InternalError",
            Self::Internal(_) => "InternalError",
            Self::NotImplemented(_) => "NotImplemented",
        }
    }
}

impl From<serde_json::Error> for XtableError {
    fn from(e: serde_json::Error) -> Self {
        Self::Serde(e.to_string())
    }
}

impl From<Box<bincode::ErrorKind>> for XtableError {
    fn from(e: Box<bincode::ErrorKind>) -> Self {
        Self::Serde(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructors_and_wire_mappings_cover_all_variants() {
        let errors = vec![
            (XtableError::invalid("bad"), 400, "InvalidArgument"),
            (
                XtableError::Unauthorized("no token".into()),
                401,
                "Unauthorized",
            ),
            (XtableError::Forbidden("no access".into()), 403, "Forbidden"),
            (XtableError::not_found("missing"), 404, "NoSuchKey"),
            (XtableError::UnknownTxn("txn".into()), 404, "UnknownTxn"),
            (XtableError::conflict("race"), 409, "TransactionConflict"),
            (XtableError::TxnExpired("old".into()), 410, "TxnExpired"),
            (XtableError::storage("disk"), 500, "InternalError"),
            (
                XtableError::Io(std::io::Error::other("io")),
                500,
                "InternalError",
            ),
            (XtableError::backend("s3"), 502, "BackendError"),
            (XtableError::Serde("json".into()), 500, "InternalError"),
            (XtableError::internal("bug"), 500, "InternalError"),
            (XtableError::not_implemented("later"), 501, "NotImplemented"),
        ];
        for (error, status, code) in errors {
            assert_eq!(error.http_status(), status);
            assert_eq!(error.s3_code(), code);
            assert!(!error.to_string().is_empty());
        }
    }

    #[test]
    fn serde_errors_are_converted() {
        let json = serde_json::from_str::<u8>("not-json").unwrap_err();
        assert!(matches!(XtableError::from(json), XtableError::Serde(_)));
        let bin = Box::new(bincode::ErrorKind::Custom("bad".into()));
        assert!(matches!(XtableError::from(bin), XtableError::Serde(_)));
    }
}
