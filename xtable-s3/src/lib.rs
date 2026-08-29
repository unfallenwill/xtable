//! xtable-s3: S3-protocol adapter using `s3s`.
//!
//! Implements `s3s::S3` for `XtableS3Service`. The transactional extensions
//! live alongside as parallel axum routes (added in Phase 2).

pub mod direct_router;
pub mod service;
pub mod dto;
pub mod headers;
pub mod routes;

pub use service::XtableS3Service;