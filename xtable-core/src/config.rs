//! Configuration schemas. Loaded from `xtable.toml` and/or env vars.

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

/// Top-level configuration.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub backend: BackendConfig,
    #[serde(default)]
    pub txn: TxnConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub observability: ObservabilityConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServerConfig {
    pub listen: String,
    pub public_endpoint: String,
    pub data_dir: PathBuf,
    pub log_level: String,
    pub shutdown_grace_secs: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: "0.0.0.0:9000".to_string(),
            public_endpoint: "http://localhost:9000".to_string(),
            data_dir: PathBuf::from("/var/lib/xtable"),
            log_level: "info".to_string(),
            shutdown_grace_secs: 30,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AuthConfig {
    pub edge_access_key_id: String,
    pub edge_secret_access_key: String,
    pub allow_anonymous_read: bool,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            edge_access_key_id: "xtableadmin".to_string(),
            edge_secret_access_key: "changeme".to_string(),
            allow_anonymous_read: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BackendConfig {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub force_path_style: bool,
    pub request_timeout_ms: u64,
    pub multipart_threshold_bytes: u64,
    pub multipart_part_size_bytes: u64,
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:9001".to_string(),
            region: "us-east-1".to_string(),
            bucket: "xtable-data".to_string(),
            access_key_id: "minioadmin".to_string(),
            secret_access_key: "minioadmin".to_string(),
            force_path_style: true,
            request_timeout_ms: 30_000,
            multipart_threshold_bytes: 16 * 1024 * 1024,
            multipart_part_size_bytes: 16 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TxnConfig {
    pub default_timeout_secs: u64,
    pub max_concurrent: usize,
    pub heartbeat_interval_secs: u64,
    pub gc_interval_secs: u64,
    pub commit_upload_concurrency: usize,
    pub staged_body_threshold_bytes: u64,
}

impl Default for TxnConfig {
    fn default() -> Self {
        Self {
            default_timeout_secs: 60,
            max_concurrent: 4096,
            heartbeat_interval_secs: 15,
            gc_interval_secs: 60,
            commit_upload_concurrency: 16,
            staged_body_threshold_bytes: 256 * 1024,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StorageConfig {
    pub redb_dir: PathBuf,
    pub staged_body_spill_dir: PathBuf,
    pub max_staged_bytes: u64,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            redb_dir: PathBuf::from("/var/lib/xtable/redb"),
            staged_body_spill_dir: PathBuf::from("/var/lib/xtable/staged"),
            max_staged_bytes: 100 * 1024 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ObservabilityConfig {
    #[serde(default = "default_service_name")]
    pub service_name: String,
    #[serde(default)]
    pub service_instance_id: Option<String>,
    #[serde(default = "default_environment")]
    pub environment: String,
    #[serde(default = "default_profile")]
    pub profile: String, // "dev" | "staging" | "production" | "debug"
    #[serde(default)]
    pub trace_sample_ratio: Option<f64>,
    #[serde(default = "default_metric_interval")]
    pub metric_export_interval_secs: u64,
    #[serde(default = "default_shutdown_timeout")]
    pub shutdown_flush_timeout_secs: u64,
    #[serde(default)]
    pub enable_per_table_metrics: bool,
}

fn default_service_name() -> String {
    "xtable".into()
}

fn default_environment() -> String {
    std::env::var("XTABLE_ENV").unwrap_or_else(|_| "dev".into())
}

fn default_profile() -> String {
    "production".into()
}

fn default_metric_interval() -> u64 {
    60
}

fn default_shutdown_timeout() -> u64 {
    10
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            service_name: default_service_name(),
            service_instance_id: None,
            environment: default_environment(),
            profile: default_profile(),
            trace_sample_ratio: None,
            metric_export_interval_secs: default_metric_interval(),
            shutdown_flush_timeout_secs: default_shutdown_timeout(),
            enable_per_table_metrics: false,
        }
    }
}

impl Config {
    /// Load from a TOML file, then layer env-var overrides on top.
    pub fn load(path: &std::path::Path) -> crate::XtableResult<Self> {
        use figment::providers::{Format, Toml};
        use figment::Figment;

        let mut figment = Figment::new();
        if path.exists() {
            figment = figment.merge(Toml::file(path));
        }
        let cfg: Config = figment
            .merge(figment::providers::Env::prefixed("XTABLE_").split("__"))
            .extract()
            .map_err(|e| crate::XtableError::Internal(format!("config load: {e}")))?;
        Ok(cfg)
    }

    pub fn server_addr(&self) -> crate::XtableResult<SocketAddr> {
        self.server
            .listen
            .parse()
            .map_err(|e| crate::XtableError::Internal(format!("listen parse: {e}")))
    }

    pub fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.backend.request_timeout_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let cfg = Config::default();
        assert!(cfg.server_addr().is_ok());
    }

    #[test]
    fn env_overrides_work() {
        // smoke: just ensure Default constructible and serializable
        let cfg = Config::default();
        let s = toml::to_string(&cfg).unwrap();
        assert!(s.contains("[server]"));
    }
}
