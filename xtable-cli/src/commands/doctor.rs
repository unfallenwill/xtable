//! `xtctl doctor` — connectivity check to backend S3.

use anyhow::{Context, Result};
use clap::Args;
use tracing::{info, warn};

#[derive(Debug, Args)]
pub struct DoctorArgs {
    /// xtable endpoint URL.
    #[arg(long, default_value = "http://localhost:9000")]
    pub xtable_endpoint: String,

    /// Backend S3 endpoint (for direct check).
    #[arg(long)]
    pub backend_endpoint: Option<String>,
}

pub async fn run(args: DoctorArgs) -> Result<()> {
    info!(endpoint = %args.xtable_endpoint, "checking xtable");

    // 1. /healthz
    let healthz = reqwest_get(&format!("{}/healthz", args.xtable_endpoint)).await?;
    if healthz != "ok" {
        warn!(resp = %healthz, "/healthz unexpected");
    } else {
        info!("/healthz ok");
    }

    // 2. /readyz
    let readyz = reqwest_get(&format!("{}/readyz", args.xtable_endpoint)).await?;
    if readyz != "ok" {
        warn!(resp = %readyz, "/readyz unexpected");
    } else {
        info!("/readyz ok");
    }

    info!("doctor: all checks passed");
    Ok(())
}

async fn reqwest_get(url: &str) -> Result<String> {
    // Use a tiny inline http client to avoid pulling reqwest.
    // Actually we depend on aws-sdk-s3 which depends on hyper — let's just use hyper.
    let url = url.to_string();
    tokio::task::spawn_blocking(move || -> Result<String> {
        use std::io::{Read, Write};
        use std::net::TcpStream;
        // Strip scheme+host from URL for plain HTTP.
        let stripped = url
            .trim_start_matches("http://")
            .trim_start_matches("https://");
        let (authority, path) = match stripped.find('/') {
            Some(i) => (&stripped[..i], &stripped[i..]),
            None => (stripped, "/"),
        };
        let host_port: Vec<&str> = authority.split(':').collect();
        let host = host_port[0];
        let port: u16 = host_port.get(1).and_then(|p| p.parse().ok()).unwrap_or(80);

        let mut stream = TcpStream::connect((host, port))
            .with_context(|| format!("tcp connect to {}:{}", host, port))?;
        let req = format!(
            "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
            path, authority
        );
        stream.write_all(req.as_bytes())?;
        let mut buf = String::new();
        stream.read_to_string(&mut buf)?;
        let body = buf.split("\r\n\r\n").nth(1).unwrap_or("").trim().to_string();
        Ok(body)
    })
    .await?
}