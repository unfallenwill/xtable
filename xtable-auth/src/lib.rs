//! xtable-auth: SigV4 verifier at the edge.
//!
//! v1: single static access key from config. Future: dynamic credentials.

pub mod credentials;
pub mod verify;

pub use credentials::{CredentialEntry, CredentialStore, StaticCredential};
pub use verify::{EdgeAuth, XtableAuthenticator, verify_request};