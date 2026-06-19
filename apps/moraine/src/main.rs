mod cli;
mod commands;
mod managed_clickhouse;
mod paths;
mod process;
mod render;
mod service;

use anyhow::Result;
use clap::Parser;
use std::process::ExitCode;

use crate::cli::Cli;
use crate::render::CliOutput;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<ExitCode> {
    let cli = Cli::parse();
    let output = CliOutput::from_cli(&cli);
    commands::dispatch(cli, output).await
}
