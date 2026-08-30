//! xtable-tx: MVCC + SSI transaction coordinator.
//!
//! MVCC + Cahill SSI commit protocol. PR #3 removed the OCC era; PR #4 wired
//! the SI lock manager + MemTable publish. PR #1-#3 + Fix8 + Fix9 are live.
//! SSI via `si_lock_manager`.

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