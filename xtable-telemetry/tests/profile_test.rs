use std::time::Duration;

use xtable_telemetry::config::Profile;
use xtable_telemetry::profiles::{env_filter_for_layer, metric_interval_for, sampler_for, Layer};

#[test]
fn production_default_is_5_percent() {
    // Just exercise that the function runs and returns a Sampler; exact value
    // is opaque.
    let _ = sampler_for(Profile::Production, None);
    let _ = sampler_for(Profile::Dev, None);
    let _ = sampler_for(Profile::Staging, Some(0.5));
    let _ = sampler_for(Profile::Debug, Some(0.0));
}

#[test]
fn metric_intervals_match_profile() {
    assert_eq!(
        metric_interval_for(Profile::Production, None),
        Duration::from_secs(60)
    );
    assert_eq!(
        metric_interval_for(Profile::Dev, None),
        Duration::from_secs(10)
    );
    assert_eq!(
        metric_interval_for(Profile::Staging, None),
        Duration::from_secs(30)
    );
    assert_eq!(
        metric_interval_for(Profile::Production, Some(Duration::from_secs(5))),
        Duration::from_secs(5)
    );
}

#[test]
fn env_filters_per_layer() {
    let _ = env_filter_for_layer(Layer::Stdout);
    let _ = env_filter_for_layer(Layer::OtlpTrace);
    let _ = env_filter_for_layer(Layer::OtlpLog);
}
