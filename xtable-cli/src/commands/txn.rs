//! `xtctl txn` — transaction subcommands (Phase 2 stub).

use anyhow::Result;
use clap::{Args, Subcommand};

#[derive(Debug, Args)]
pub struct TxnArgs {
    #[command(subcommand)]
    pub cmd: TxnCmd,
}

#[derive(Debug, Subcommand)]
pub enum TxnCmd {
    /// Begin a new transaction.
    Begin {
        /// xtable endpoint URL.
        #[arg(long, default_value = "http://localhost:9000")]
        xtable_endpoint: String,
    },
    /// Commit a transaction.
    Commit {
        /// Transaction ID.
        #[arg(long)]
        txn_id: String,
        /// xtable endpoint URL.
        #[arg(long, default_value = "http://localhost:9000")]
        xtable_endpoint: String,
    },
    /// Abort a transaction.
    Abort {
        /// Transaction ID.
        #[arg(long)]
        txn_id: String,
        /// xtable endpoint URL.
        #[arg(long, default_value = "http://localhost:9000")]
        xtable_endpoint: String,
    },
    /// Query transaction status.
    Status {
        /// Transaction ID.
        #[arg(long)]
        txn_id: String,
        /// xtable endpoint URL.
        #[arg(long, default_value = "http://localhost:9000")]
        xtable_endpoint: String,
    },
}

pub async fn run(args: TxnArgs) -> Result<()> {
    match args.cmd {
        TxnCmd::Begin { .. } => Err(anyhow::anyhow!(
            "Phase 2 stub: BeginTxn not yet implemented in xtable-server"
        )),
        TxnCmd::Commit { txn_id, .. } => Err(anyhow::anyhow!(
            "Phase 2 stub: CommitTxn({}) not yet implemented",
            txn_id
        )),
        TxnCmd::Abort { txn_id, .. } => Err(anyhow::anyhow!(
            "Phase 2 stub: AbortTxn({}) not yet implemented",
            txn_id
        )),
        TxnCmd::Status { txn_id, .. } => Err(anyhow::anyhow!(
            "Phase 2 stub: TxnStatus({}) not yet implemented",
            txn_id
        )),
    }
}