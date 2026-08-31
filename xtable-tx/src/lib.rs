//! xtable-tx: MVCC + Cahill SSI transaction coordinator.
//!
//! MVCC + Cahill SSI commit protocol. The `TxnCoordinator` runs the SI
//! lock manager (`si_lock_manager`) for rw-edge capture, performs Cahill
//! cycle detection inside `commit`, publishes new entries to the in-memory
//! MemTable, and writes a crash-safe WAL.

pub mod cahill;
pub mod coordinator;
pub mod error;
pub mod gc;
pub mod rebuild;
pub mod recovery;
pub mod si_lock_manager;

pub use cahill::detect_dangerous_structure;
pub use coordinator::{CommitEvent, CommitOutcome, CommitWrite, PostCommitHook, TxnCoordinator};
pub use error::TxnError;
pub use rebuild::{rebuild, RebuildReport};
pub use recovery::{recover, RecoveryReport};
pub use si_lock_manager::SiLockManager;
