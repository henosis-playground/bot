use std::process::ExitCode;

use clap::Parser as _;
use henosis_cli::{Cli, run};

#[tokio::main]
async fn main() -> ExitCode {
    match run(Cli::parse()).await {
        Ok(output) => {
            print!("{output}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{}", error.diagnostic());
            ExitCode::FAILURE
        }
    }
}
