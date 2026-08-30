//! Centralised OpenTelemetry integration for xtable-server.
//!
//! See `docs/superpowers/specs/2026-08-30-otel-server-design.md`.

pub mod baggage;
pub mod config;
pub mod extract_route;
pub mod http_semconv;
pub mod init;
pub mod metrics;
pub mod profiles;
pub mod providers;
pub mod resource;
pub mod shutdown;
pub mod timed;
pub mod testing;

// Re-export commonly-needed OTel types so downstream crates (e.g.
// `xtable-server`) do not need to depend on `opentelemetry` directly.
pub use opentelemetry::global;
pub use opentelemetry::KeyValue;
