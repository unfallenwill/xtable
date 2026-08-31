//! xtable-cli: operator-facing `xtctl` binary.
//!
//! Phase 1 subcommands:
//! - `serve`: dev convenience to spawn xtable-server with a config
//! - `doctor`: connectivity check to backend S3
//!
//! Phase 2 adds: `txn begin/commit/abort/status`.

pub mod commands;
