use std::process::ExitCode;

use anyhow::{Context, Result};
use pulse_processor::replay::{evaluate, generate_manifest, DEFAULT_EVENT_COUNT, DEFAULT_SEED};

fn run() -> Result<bool> {
    let seed = std::env::args()
        .nth(1)
        .map(|value| value.parse().context("seed must be an unsigned integer"))
        .transpose()?
        .unwrap_or(DEFAULT_SEED);
    let manifest = generate_manifest(seed, DEFAULT_EVENT_COUNT);
    let report = evaluate(&manifest)?;
    serde_json::to_writer(std::io::stdout().lock(), &report)?;
    println!();
    Ok(report.passed)
}

fn main() -> ExitCode {
    match run() {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(error) => {
            eprintln!("replay failed: {error:#}");
            ExitCode::FAILURE
        }
    }
}
