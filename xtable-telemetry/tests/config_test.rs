use std::sync::Mutex;
use xtable_telemetry::config::*;

// Serialise env-var-touching tests so they don't race each other.
static LOCK: Mutex<()> = Mutex::new(());

fn clear<F: FnOnce()>(f: F) {
    let _g = LOCK.lock().unwrap();
    for v in [
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_PROTOCOL",
        "OTEL_SERVICE_NAME",
        "OTEL_TRACES_SAMPLER_ARG",
        "OTEL_METRIC_EXPORT_INTERVAL",
        "XTABLE_ENV",
        "OTEL_PROFILE",
        "OTEL_SERVICE_INSTANCE_ID",
        "OTEL_SHUTDOWN_FLUSH_TIMEOUT_SECS",
    ] {
        std::env::remove_var(v);
    }
    f()
}

#[test]
fn returns_none_when_endpoint_unset() {
    clear(|| assert!(load_from_env().is_none()))
}

#[test]
fn parses_grpc_default() {
    clear(|| {
        std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel:4317");
        let cfg = load_from_env().unwrap();
        assert_eq!(cfg.protocol, OtlpProtocol::Grpc);
        assert_eq!(cfg.service_name, "xtable");
    })
}

#[test]
fn parses_http_protobuf() {
    clear(|| {
        std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel:4318");
        std::env::set_var("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf");
        let cfg = load_from_env().unwrap();
        assert_eq!(cfg.protocol, OtlpProtocol::HttpProtobuf);
    })
}

#[test]
fn invalid_profile_falls_back() {
    clear(|| {
        std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", "x");
        std::env::set_var("OTEL_PROFILE", "garbage");
        let cfg = load_from_env().unwrap();
        assert_eq!(cfg.profile, Profile::Production);
    })
}

#[test]
fn trace_sample_ratio_clamped_or_ignored() {
    clear(|| {
        std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", "x");
        std::env::set_var("OTEL_TRACES_SAMPLER_ARG", "1.5");
        assert!(load_from_env().unwrap().trace_sample_ratio.is_none());
    })
}
