//! xtable-backend: S3-compatible backend client.
//!
//! Wraps `aws-sdk-s3` and exposes the small surface xtable needs.
//! Key mapping is identity in v1 (single-tenant, single-bucket).

pub mod client;
pub mod error;
pub mod keymap;
pub mod mock;
pub mod recording;

pub use client::BackendClient;
pub use error::BackendError;
pub use keymap::{IdentityKeyMap, KeyMap};