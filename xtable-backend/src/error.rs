//! Backend errors.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum BackendError {
    #[error("aws sdk error: {0}")]
    Sdk(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("backend unreachable: {0}")]
    Unreachable(String),

    #[error("upload failed: {0}")]
    Upload(String),

    #[error("internal: {0}")]
    Internal(String),
}

impl BackendError {
    /// Generic stringification of any SdkError.
    pub fn from_sdk<E, R>(e: &aws_sdk_s3::error::SdkError<E, R>) -> Self
    where
        E: std::fmt::Debug,
        R: std::fmt::Debug,
    {
        match e.as_service_error() {
            Some(svc) => {
                let dbg = format!("{:?}", svc);
                if dbg.to_lowercase().contains("notfound") || dbg.to_lowercase().contains("no such")
                {
                    Self::NotFound(dbg)
                } else {
                    Self::Sdk(dbg)
                }
            }
            None => Self::Unreachable(format!("{:?}", e)),
        }
    }
}

impl From<BackendError> for xtable_core::XtableError {
    fn from(e: BackendError) -> Self {
        match e {
            BackendError::NotFound(s) => Self::not_found(s),
            BackendError::InvalidArgument(s) => Self::invalid(s),
            BackendError::Unreachable(s) => Self::Backend(format!("unreachable: {}", s)),
            BackendError::Upload(s) => Self::Backend(format!("upload: {}", s)),
            BackendError::Sdk(s) => Self::Backend(s),
            BackendError::Internal(s) => Self::Backend(s),
        }
    }
}

pub type BackendResult<T> = std::result::Result<T, BackendError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_errors_map_to_xtable_errors() {
        let cases = [
            (BackendError::NotFound("x".into()), 404),
            (BackendError::InvalidArgument("x".into()), 400),
            (BackendError::Unreachable("x".into()), 502),
            (BackendError::Upload("x".into()), 502),
            (BackendError::Sdk("x".into()), 502),
            (BackendError::Internal("x".into()), 502),
        ];
        for (error, status) in cases {
            assert_eq!(xtable_core::XtableError::from(error).http_status(), status);
        }
    }
}
