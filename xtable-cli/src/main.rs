//! `xtctl` CLI entry point.

use anyhow::Result;
use clap::{Parser, Subcommand};

use xtable_cli::commands::{doctor, serve, txn};

#[derive(Parser, Debug)]
#[command(name = "xtctl", about = "xtable operator CLI", version)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Run xtable server in-process (delegates to xtable-server binary).
    Serve(serve::ServeArgs),
    /// Connectivity check.
    Doctor(doctor::DoctorArgs),
    /// Transaction operations (Phase 2).
    Txn(txn::TxnArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Serve(args) => serve::run(args).await,
        Cmd::Doctor(args) => doctor::run(args).await,
        Cmd::Txn(args) => txn::run(args).await,
    }
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with_target(false)
        .try_init();
}