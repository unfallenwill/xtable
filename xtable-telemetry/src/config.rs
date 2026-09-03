//! Telemetry configuration: parses OTel env vars + TOML observability section.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// OTLP transport protocol.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum OtlpProtocol {
    #[default]
    Grpc,
    HttpProtobuf,
}

/// Operational profile — drives sampler, env filters, and metric intervals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    Dev,
    Staging,
    Production,
    Debug,
}

impl Profile {
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Result<Self, String> {
        match s.to_ascii_lowercase().as_str() {
            "dev" => Ok(Self::Dev),
            "staging" => Ok(Self::Staging),
            "production" | "prod" => Ok(Self::Production),
            "debug" | "incident" => Ok(Self::Debug),
            other => Err(format!(
                "unknown profile '{other}'; expected one of dev|staging|production|debug"
            )),
        }
    }
}

/// Fully-resolved telemetry configuration.
#[derive(Debug, Clone)]
pub struct TelemetryConfig {
    pub endpoint: Option<String>,
    pub protocol: OtlpProtocol,
    pub service_name: String,
    pub service_instance_id: String,
    pub environment: String,
    pub profile: Profile,
    pub trace_sample_ratio: Option<f64>,
    pub metric_export_interval_secs: Duration,
    pub shutdown_flush_timeout_secs: Duration,
    pub enable_per_table_metrics: bool,
}

/// Load configuration from environment variables.
///
/// Returns `None` when `OTEL_EXPORTER_OTLP_ENDPOINT` is unset or empty — i.e.
/// telemetry is disabled.
pub fn load_from_env() -> Option<TelemetryConfig> {
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .ok()
        .filter(|s| !s.trim().is_empty())?;
    let protocol = match std::env::var("OTEL_EXPORTER_OTLP_PROTOCOL")
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "http/protobuf" | "http" => OtlpProtocol::HttpProtobuf,
        _ => OtlpProtocol::Grpc,
    };
    let profile_str = std::env::var("OTEL_PROFILE").unwrap_or_else(|_| "production".into());
    let profile = Profile::from_str(&profile_str).unwrap_or(Profile::Production);
    let service_name = std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "xtable".into());
    let service_instance_id =
        std::env::var("OTEL_SERVICE_INSTANCE_ID").unwrap_or_else(|_| ulid::Ulid::new().to_string());
    let environment = std::env::var("XTABLE_ENV").unwrap_or_else(|_| "dev".into());
    let trace_sample_ratio = std::env::var("OTEL_TRACES_SAMPLER_ARG")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|f| (0.0..=1.0).contains(f));
    let metric_export_interval_secs = std::env::var("OTEL_METRIC_EXPORT_INTERVAL")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(60);
    let shutdown_flush_timeout_secs = std::env::var("OTEL_SHUTDOWN_FLUSH_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(10);
    Some(TelemetryConfig {
        endpoint: Some(endpoint),
        protocol,
        service_name,
        service_instance_id,
        environment,
        profile,
        trace_sample_ratio,
        metric_export_interval_secs: Duration::from_secs(metric_export_interval_secs),
        shutdown_flush_timeout_secs: Duration::from_secs(shutdown_flush_timeout_secs),
        enable_per_table_metrics: false,
    })
}

/// Merge env-derived config over TOML `observability` section.
///
/// Returns `None` when env-derived config is `None` (telemetry disabled).
/// TOML fills resource and operational defaults without overriding explicit
/// environment-selected endpoint, protocol, or profile values.
pub fn merge_with_toml(
    env_cfg: Option<TelemetryConfig>,
    toml: &xtable_core::config::ObservabilityConfig,
) -> Option<TelemetryConfig> {
    let mut cfg = env_cfg?;
    if cfg.service_name == "xtable" {
        cfg.service_name = toml.service_name.clone();
    }
    if cfg.environment == "dev" && toml.environment != "dev" {
        cfg.environment = toml.environment.clone();
    }
    if let Some(id) = &toml.service_instance_id {
        cfg.service_instance_id = id.clone();
    }
    // The endpoint is the opt-in switch and all explicitly resolved
    // environment values must remain authoritative once telemetry is
    // enabled. TOML still supplies the service/resource defaults below.
    if cfg.trace_sample_ratio.is_none() {
        cfg.trace_sample_ratio = toml.trace_sample_ratio;
    }
    if toml.metric_export_interval_secs != 60 {
        cfg.metric_export_interval_secs = Duration::from_secs(toml.metric_export_interval_secs);
    }
    if toml.shutdown_flush_timeout_secs != 10 {
        cfg.shutdown_flush_timeout_secs = Duration::from_secs(toml.shutdown_flush_timeout_secs);
    }
    cfg.enable_per_table_metrics = toml.enable_per_table_metrics;
    Some(cfg)
}

/// Build a `TelemetryConfig` from the server's `ObservabilityConfig`.
///
/// This conversion deliberately leaves `endpoint` and `protocol` set to
/// `None` / `OtlpProtocol::default()` so that the env-driven
/// `OTEL_EXPORTER_OTLP_ENDPOINT` lookup still gates whether the exporter is
/// installed — i.e. a TOML `observability` block alone does not turn on
/// telemetry; it only shapes the resource attributes, profile, and
/// intervals when telemetry is enabled.
///
/// Phase 6 (Task 6.2) wires this into `xtable-server/src/main.rs` so the
/// server can call `xtable_telemetry::init::init(&cfg.observability.into())`
/// unconditionally — `init` itself returns `Ok(None)` when no endpoint is
/// configured, keeping telemetry opt-in.
impl From<xtable_core::config::ObservabilityConfig> for TelemetryConfig {
    fn from(toml: xtable_core::config::ObservabilityConfig) -> Self {
        TelemetryConfig {
            endpoint: None,
            protocol: OtlpProtocol::default(),
            service_name: toml.service_name,
            service_instance_id: toml
                .service_instance_id
                .unwrap_or_else(|| ulid::Ulid::new().to_string()),
            environment: toml.environment,
            profile: Profile::from_str(&toml.profile).unwrap_or(Profile::Production),
            trace_sample_ratio: toml.trace_sample_ratio,
            metric_export_interval_secs: Duration::from_secs(toml.metric_export_interval_secs),
            shutdown_flush_timeout_secs: Duration::from_secs(toml.shutdown_flush_timeout_secs),
            enable_per_table_metrics: toml.enable_per_table_metrics,
        }
    }
}
