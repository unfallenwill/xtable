//! xtable-storage: redb-backed local state.
//!
//! Phase 1 surfaces: version index, meta.
//! Phase 2 surfaces: WAL, txn_state, read/write sets, staged blobs.

pub mod blob;
pub mod cf;
pub mod store;
pub mod txn_state;
pub mod version_chain;
pub mod version_index;
pub mod wal;

pub use store::LocalStore;
pub use txn_state::{BlobRecord, MultipartState, ReadSetEntry, TxnStateRecord, WriteSetEntry};
pub use version_chain::{VersionChain, VersionEntry};
pub use version_index::VersionRecord;
pub use wal::{encode_seq, WalRecord};