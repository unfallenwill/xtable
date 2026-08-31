//! `xtctl serve` — dev convenience that delegates to `xtable-server`.

use anyhow::Context;
use clap::Args;
use std::path::PathBuf;

#[derive(Debug, Args)]
pub struct ServeArgs {
    /// Path to config TOML.
    #[arg(long, env = "XTABLE_CONFIG")]
    pub config: Option<PathBuf>,
}

pub async fn run(args: ServeArgs) -> anyhow::Result<()> {
    let exe = std::env::current_exe().context("locate current exe")?;
    let dir = exe.parent().context("locate parent dir of exe")?;
    // Try sibling binary `xtable`, fall back to PATH.
    let candidate = dir.join("xtable");
    let cmd = if candidate.exists() {
        candidate
    } else {
        PathBuf::from("xtable")
    };
    let status = tokio::process::Command::new(&cmd)
        .arg("serve")
        .args(
            args.config
                .iter()
                .map(|p| format!("--config={}", p.display())),
        )
        .status()
        .await
        .with_context(|| format!("spawning {:?}", cmd))?;
    std::process::exit(status.code().unwrap_or(1));
}
