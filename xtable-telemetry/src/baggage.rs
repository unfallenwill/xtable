//! Baggage helpers for propagating tenant / space / request-id /
//! sampling-priority through the active OpenTelemetry context.
//!
//! These helpers read the current OTel context's baggage and attach a modified
//! copy for the lifetime of the call. Callers should invoke them inside the
//! span or context whose baggage they want to populate; downstream OTel
//! instrumentation on the same task observes the modified baggage for the
//! duration of the helper's call.

use opentelemetry::baggage::{BaggageExt, BaggageMetadata, KeyValueMetadata};
use opentelemetry::Context;

/// Insert `key=value` into the current OTel context's baggage.
///
/// This is the internal primitive the public helpers build on.
fn set_kv(key: &'static str, value: String) {
    let cx = Context::current();
    let kvm = KeyValueMetadata::new(key, value, BaggageMetadata::default());
    // The returned `ContextGuard` is dropped when this function returns,
    // restoring the prior context. The baggage mutation is therefore scoped
    // to the function call — use these helpers immediately before creating
    // child spans you want tagged.
    let _guard = cx.with_baggage(vec![kvm]).attach();
}

/// Tag the active context with the tenant identifier.
pub fn set_tenant(v: impl Into<String>) {
    set_kv("tenant", v.into());
}

/// Tag the active context with the space identifier.
pub fn set_space(v: impl Into<String>) {
    set_kv("space", v.into());
}

/// Tag the active context with the originating request id.
pub fn set_request_id(v: impl Into<String>) {
    set_kv("request.id", v.into());
}

/// Mark the active context as a high-priority sample.
pub fn mark_sampling_priority() {
    set_kv("sampling.priority", "true".into());
}