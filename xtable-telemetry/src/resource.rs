//! OTel `Resource` builder using semantic conventions.

use opentelemetry::KeyValue;
use opentelemetry_sdk::Resource;
use opentelemetry_semantic_conventions::resource::{
    DEPLOYMENT_ENVIRONMENT_NAME, HOST_ARCH, HOST_NAME, OS_TYPE, PROCESS_PID, PROCESS_RUNTIME_NAME,
    PROCESS_RUNTIME_VERSION, SERVICE_INSTANCE_ID, SERVICE_NAME, SERVICE_VERSION,
};

use crate::config::TelemetryConfig;

/// Build a `Resource` from `TelemetryConfig`, populating OTel semconv attributes
/// for service, host, and process info.
pub fn build_resource(cfg: &TelemetryConfig) -> Resource {
    let hostname = std::env::var("HOSTNAME")
        .ok()
        .or_else(|| {
            hostname::get()
                .ok()
                .and_then(|h| h.into_string().ok().map(|s| s.trim().to_string()))
        })
        .unwrap_or_else(|| "unknown".into());

    Resource::new([
        KeyValue::new(SERVICE_NAME, cfg.service_name.clone()),
        KeyValue::new(SERVICE_VERSION, env!("CARGO_PKG_VERSION").to_string()),
        KeyValue::new(SERVICE_INSTANCE_ID, cfg.service_instance_id.clone()),
        KeyValue::new(DEPLOYMENT_ENVIRONMENT_NAME, cfg.environment.clone()),
        KeyValue::new(HOST_NAME, hostname),
        KeyValue::new(HOST_ARCH, std::env::consts::ARCH.to_string()),
        KeyValue::new(PROCESS_PID, std::process::id().to_string()),
        KeyValue::new(PROCESS_RUNTIME_NAME, "rustc"),
        KeyValue::new(
            PROCESS_RUNTIME_VERSION,
            rustc_version_runtime::version().to_string(),
        ),
        KeyValue::new(OS_TYPE, std::env::consts::OS.to_string()),
    ])
}
