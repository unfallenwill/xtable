//! xtable-auth: JWT verifier for the xtable HTTP API.

pub mod credentials;
pub mod verify;

#[doc(hidden)]
pub use credentials::{CredentialEntry, CredentialStore, StaticCredential};
pub use verify::{verify_request, EdgeAuth, XtableAuthenticator};
