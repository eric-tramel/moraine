mod cli;
mod commands;
mod managed_clickhouse;
mod mcp_health;
mod paths;
mod process;
mod progress;
mod render;
mod service;

use anyhow::Result;
use clap::Parser;
use moraine_clickhouse::QueryRuntime;
use std::process::ExitCode;

use crate::cli::Cli;
use crate::render::CliOutput;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<ExitCode> {
    let cli = Cli::parse();
    let output = CliOutput::from_cli(&cli);
    let query_runtime = QueryRuntime::new();
    let result = commands::dispatch(cli, output, &query_runtime).await;
    query_runtime.close_and_drain().await;
    result
}
