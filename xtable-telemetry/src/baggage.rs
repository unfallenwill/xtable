//! Baggage helpers for propagating tenant / space / request-id /
//! sampling-priority through the active OpenTelemetry context.
//!
//! Each helper returns a [`BaggageGuard`] that the caller binds with `let`.
//! While the guard is alive, the modified baggage is attached as the
//! current OTel context, so any child spans created on this task observe
//! it. Dropping the guard restores the previous context — bind the guard
//! at the same scope as the work you want tagged, e.g.:
//!
//! ```ignore
//! let _bg = baggage::set_tenant("acme");
//! let _span = tracing::info_span!("handle");
//! // child spans inside `handle` carry tenant=acme baggage
//! ```

use opentelemetry::baggage::{BaggageExt, BaggageMetadata, KeyValueMetadata};
use opentelemetry::{Context, ContextGuard};

/// RAII guard produced by the baggage helpers.
///
/// Holds an attached [`ContextGuard`] that keeps the modified baggage live
/// as the current OTel `Context` for the duration of the binding. Dropping
/// this guard restores the prior context.
pub struct BaggageGuard {
    _ctx: ContextGuard,
}

impl BaggageGuard {
    fn attach(key: &'static str, value: String) -> Self {
        let cx = Context::current();
        let kvm = KeyValueMetadata::new(key, value, BaggageMetadata::default());
        Self {
            _ctx: cx.with_baggage(vec![kvm]).attach(),
        }
    }
}

/// Tag the active context with the tenant identifier.
///
/// Bind the returned guard for the lifetime over which child spans should
/// observe the tenant baggage.
pub fn set_tenant(v: impl Into<String>) -> BaggageGuard {
    BaggageGuard::attach("tenant", v.into())
}

/// Tag the active context with the space identifier.
pub fn set_space(v: impl Into<String>) -> BaggageGuard {
    BaggageGuard::attach("space", v.into())
}

/// Tag the active context with the originating request id.
pub fn set_request_id(v: impl Into<String>) -> BaggageGuard {
    BaggageGuard::attach("request.id", v.into())
}

/// Mark the active context as a high-priority sample.
pub fn mark_sampling_priority() -> BaggageGuard {
    BaggageGuard::attach("sampling.priority", "true".into())
}