//! xtable-core: pure types, errors, IDs, config schemas.
//!
//! No IO. Every other crate depends on this.

pub mod config;
pub mod error;
pub mod ids;
pub mod headers;

pub use error::{XtableError, XtableResult};
pub use ids::{ObjectKey, TxnId, Version};