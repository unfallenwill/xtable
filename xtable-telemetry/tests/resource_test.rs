use opentelemetry::Value;
use std::time::Duration;
use xtable_telemetry::config::{OtlpProtocol, Profile, TelemetryConfig};
use xtable_telemetry::resource;

fn cfg() -> TelemetryConfig {
    TelemetryConfig {
        endpoint: Some("x".into()),
        protocol: OtlpProtocol::Grpc,
        service_name: "xtable-test".into(),
        service_instance_id: "01TEST".into(),
        environment: "ci".into(),
        profile: Profile::Production,
        trace_sample_ratio: None,
        metric_export_interval_secs: Duration::from_secs(60),
        shutdown_flush_timeout_secs: Duration::from_secs(10),
        enable_per_table_metrics: false,
    }
}

#[test]
fn resource_contains_required_attrs() {
    let r = resource::build_resource(&cfg());
    let keys: Vec<&str> = r.iter().map(|(k, _)| k.as_str()).collect();
    assert!(keys.contains(&"service.name"));
    assert!(keys.contains(&"service.version"));
    assert!(keys.contains(&"service.instance.id"));
    assert!(keys.contains(&"deployment.environment.name"));
    assert!(keys.contains(&"host.arch"));
    assert!(keys.contains(&"process.pid"));
    assert!(keys.contains(&"process.runtime.name"));
    let svc_name = r
        .iter()
        .find(|(k, _)| k.as_str() == "service.name")
        .unwrap()
        .1;
    assert!(matches!(svc_name, Value::String(_)));
}