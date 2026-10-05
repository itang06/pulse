use std::{process::ExitCode, time::Duration};

use anyhow::{Context, Result};
use prost::Message;
use prost_types::Timestamp;
use pulse_proto::pulse::v1::{EnrichedTelemetry, TelemetryEvent};
use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use serde::Serialize;

const TOPIC: &str = "telemetry.enriched";
const PARTITIONS: i32 = 3;

#[derive(Serialize)]
struct Summary {
    topic: &'static str,
    requested: u64,
    acknowledged: u64,
    partition_distribution: [u64; PARTITIONS as usize],
    first_event_id: Option<String>,
    last_event_id: Option<String>,
}

fn event_id(index: u64) -> Result<String> {
    anyhow::ensure!(
        index < (1_u64 << 48),
        "event index exceeds 12 hexadecimal digits"
    );
    Ok(format!("00000000-0000-4000-8000-{index:012x}"))
}

async fn publish(count: u64) -> Result<Summary> {
    let brokers = std::env::var("PULSE_KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .create()
        .context("creating deterministic enriched producer")?;
    let mut distribution = [0_u64; PARTITIONS as usize];

    for index in 0..count {
        let id = event_id(index)?;
        let partition = (index % PARTITIONS as u64) as i32;
        let timestamp = Timestamp {
            seconds: 1_791_200_000,
            nanos: 0,
        };
        let message = EnrichedTelemetry {
            event: Some(TelemetryEvent {
                event_id: id.clone(),
                event_time: Some(timestamp),
                service_name: "sink-crash-harness".into(),
                route: "/deterministic".into(),
                latency_us: 250 + (index % 10_000),
                status_code: 200,
                trace_id: format!("sink-crash-{index:012x}"),
                attributes: Default::default(),
            }),
            ewma_mean_us: 250.0,
            ewma_stddev_us: 25.0,
            anomaly_score: 0.0,
            is_anomaly: false,
            processed_at: Some(timestamp),
        };
        let payload = message.encode_to_vec();
        producer
            .send(
                FutureRecord::to(TOPIC)
                    .key(&id)
                    .payload(&payload)
                    .partition(partition),
                Duration::from_secs(30),
            )
            .await
            .map_err(|(error, _)| error)
            .with_context(|| format!("publishing event {id} to partition {partition}"))?;
        distribution[partition as usize] += 1;
    }

    Ok(Summary {
        topic: TOPIC,
        requested: count,
        acknowledged: count,
        partition_distribution: distribution,
        first_event_id: (count > 0).then(|| event_id(0)).transpose()?,
        last_event_id: count.checked_sub(1).map(event_id).transpose()?,
    })
}

fn run() -> Result<()> {
    let count = std::env::args()
        .nth(1)
        .context("usage: publish-enriched COUNT")?
        .parse::<u64>()
        .context("COUNT must be an unsigned integer")?;
    anyhow::ensure!(count > 0, "COUNT must be greater than zero");
    let summary = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(publish(count))?;
    serde_json::to_writer(std::io::stdout().lock(), &summary)?;
    println!();
    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("deterministic producer failed: {error:#}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::event_id;

    #[test]
    fn event_ids_are_stable_sql_regenerable_and_unique_at_adjacent_indexes() {
        assert_eq!(event_id(0).unwrap(), "00000000-0000-4000-8000-000000000000");
        assert_eq!(event_id(1).unwrap(), "00000000-0000-4000-8000-000000000001");
        assert_ne!(event_id(1).unwrap(), event_id(2).unwrap());
        assert!(event_id(1_u64 << 48).is_err());
    }
}
