//! Profile presets for sampler, env filters, and metric interval.

use std::time::Duration;

use opentelemetry_sdk::trace::Sampler;
use tracing_subscriber::EnvFilter;

use crate::config::Profile;

/// Logical telemetry layers. Each can have its own `EnvFilter`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Layer {
    Stdout,
    OtlpTrace,
    OtlpLog,
}

/// Build an OTel `Sampler` from the active `Profile` and optional override ratio.
///
/// Always wraps a `TraceIdRatioBased` in `ParentBased` so child spans inherit
/// the parent's sampling decision.
pub fn sampler_for(profile: Profile, ratio: Option<f64>) -> Sampler {
    let ratio = ratio.unwrap_or(match profile {
        Profile::Dev | Profile::Debug => 1.0,
        Profile::Staging => 0.10,
        Profile::Production => 0.05,
    });
    Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(ratio)))
}

/// Default `EnvFilter` per layer. Honours `RUST_LOG` if set.
pub fn env_filter_for_layer(layer: Layer) -> EnvFilter {
    let s = match layer {
        Layer::Stdout => "info",
        Layer::OtlpTrace => "debug,xtable=trace",
        Layer::OtlpLog => "info",
    };
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(s))
}

/// Metric export interval for the given profile, unless overridden.
pub fn metric_interval_for(profile: Profile, override_: Option<Duration>) -> Duration {
    override_.unwrap_or_else(|| match profile {
        Profile::Dev | Profile::Debug => Duration::from_secs(10),
        Profile::Staging => Duration::from_secs(30),
        Profile::Production => Duration::from_secs(60),
    })
}