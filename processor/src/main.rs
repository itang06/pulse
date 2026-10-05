//! Pulse stream processor: acknowledged raw-to-enriched/DLQ Kafka pipeline.

use std::{str::FromStr, time::Duration};

use anyhow::{bail, Context};
use tracing::{error, info};

use pulse_processor::{
    kafka, metrics,
    pipeline::{process_next_batch, BatchConfig, BatchContext, SystemClock},
    state::{DetectorStateStore, StateStoreConfig},
};

fn env_or(key: &str, fallback: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| fallback.to_string())
}

fn parse_env<T: FromStr>(key: &str, fallback: &str) -> anyhow::Result<T>
where
    T::Err: std::fmt::Display,
{
    env_or(key, fallback)
        .parse()
        .map_err(|error| anyhow::anyhow!("invalid {key}: {error}"))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let brokers = env_or("PULSE_KAFKA_BROKERS", "localhost:9092");
    let group_id = env_or("PULSE_CONSUMER_GROUP", "pulse-processor");
    let metrics_addr = env_or("PULSE_METRICS_ADDR", "127.0.0.1:9465");
    let config = BatchConfig {
        max_records: parse_env("PULSE_BATCH_MAX_RECORDS", "500")?,
        max_wait: Duration::from_millis(parse_env("PULSE_BATCH_MAX_WAIT_MS", "10")?),
    }
    .validate()
    .context("invalid Kafka batch configuration")?;
    let mut store = DetectorStateStore::new(StateStoreConfig::default())
        .map_err(|error| anyhow::anyhow!("invalid detector state config: {error}"))?;
    let metrics = metrics::Metrics::new();
    let listener = tokio::net::TcpListener::bind(&metrics_addr)
        .await
        .with_context(|| format!("binding metrics server on {metrics_addr}"))?;
    tokio::spawn(metrics::serve(listener, metrics.clone()));
    let (mut receiver, mut committer, publisher) = kafka::create(&brokers, &group_id)?;
    let mut clock = SystemClock;
    // Force a harmless initial reset once Kafka reports its first assignment.
    let mut last_seen_generation = u64::MAX;
    info!(%brokers, %group_id, max_records = config.max_records, max_wait_ms = config.max_wait.as_millis(), %metrics_addr, "processor started");

    loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal.context("waiting for Ctrl-C")?;
                info!("shutting down after Ctrl-C");
                break;
            }
            result = process_next_batch(
                &mut receiver,
                publisher.clone(),
                BatchContext {
                    committer: &mut committer,
                    store: &mut store,
                    metrics: &metrics,
                    config,
                    clock: &mut clock,
                    last_seen_generation: &mut last_seen_generation,
                },
            ) => {
                match result {
                    Ok(true) => {}
                    Ok(false) => bail!("Kafka receiver ended unexpectedly"),
                    Err(error) => {
                        error!(%error, "batch failed; no offsets for this batch were committed");
                        return Err(error);
                    }
                }
            }
        }
    }
    Ok(())
}
