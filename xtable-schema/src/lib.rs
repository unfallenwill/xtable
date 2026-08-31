//! xtable-schema: structured data space on top of the transactional
//! object store.
//!
//! User flow:
//! ```text
//! JSON Schema
//!     ↓ register (transactional)
//! Records (validated against current schema)
//!     ↓ query / time-travel read
//! Snapshot-view, table view, agent-friendly output
//! ```
//!
//! Implementation:
//! - Schema documents are stored as S3 objects via the existing txn
//!   coordinator (object key = `_xtable/<space>/_schema/<name>/v<N>.json`).
//! - Record documents are stored as S3 objects (object key =
//!   `_xtable/<space>/<table>/<record_id>.json`).
//! - A redb sidecar index (managed by [`TxIndexHook`] subscribed to
//!   `TxnCoordinator`'s post-commit hook) keeps `record_index` and
//!   `schema_index` up to date so queries don't have to walk every object.
//!
//! See `xtable-schema/tests/invariants.rs` for the I-REC-* invariants
//! this layer preserves.

pub mod key;
pub mod validation;
pub mod engine;
pub mod query;

pub use engine::{RecordWrite, SchemaInfo, StructuredReader, StructuredSpace, StructuredTxn, WriteOutcome};
pub use key::{schema_key, record_key, parse_record_key, parse_schema_key, RecordKeyParts, SchemaKeyParts};
pub use validation::{JsonSchema, validate};
pub use query::{Filter, OrderBy, OrderDir, Query, QueryResult, Record};
