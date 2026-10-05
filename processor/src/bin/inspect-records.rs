use std::{
    env,
    time::{Duration, Instant},
};

use anyhow::{bail, ensure, Context, Result};
use prost::Message;
use pulse_proto::{DeadLetterRecord, EnrichedTelemetry, TelemetryEvent};
use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    util::Timeout,
    ClientConfig, Message as KafkaMessage, Offset, TopicPartitionList,
};
use serde_json::json;

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn main() -> Result<()> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    ensure!(
        args.len() == 4,
        "usage: inspect-records KIND PARTITION START_OFFSET END_OFFSET"
    );
    let kind = &args[0];
    let partition: i32 = args[1].parse().context("partition must be an integer")?;
    let start: i64 = args[2].parse().context("start offset must be an integer")?;
    let end: i64 = args[3].parse().context("end offset must be an integer")?;
    let topic = match kind.as_str() {
        "raw" => "telemetry.raw",
        "enriched" => "telemetry.enriched",
        "dlq" => "telemetry.dlq",
        _ => bail!("KIND must be raw, enriched, or dlq"),
    };
    ensure!(
        partition >= 0 && start >= 0 && end >= start,
        "invalid partition or offset range"
    );
    if start == end {
        return Ok(());
    }

    let consumer: BaseConsumer = ClientConfig::new()
        .set(
            "bootstrap.servers",
            env::var("PULSE_KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into()),
        )
        .set(
            "group.id",
            format!("pulse-record-inspector-{}", std::process::id()),
        )
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("enable.partition.eof", "true")
        .create()
        .context("creating Kafka range inspector")?;
    let mut assignment = TopicPartitionList::new();
    assignment
        .add_partition_offset(topic, partition, Offset::Offset(start))
        .context("assigning Kafka topic partition")?;
    consumer
        .assign(&assignment)
        .context("assigning Kafka inspector")?;

    let deadline = Instant::now() + Duration::from_secs(120);
    let mut expected = start;
    while expected < end {
        if Instant::now() >= deadline {
            bail!("timed out reading {topic}[{partition}] offsets {start}..{end}");
        }
        match consumer.poll(Timeout::After(Duration::from_secs(1))) {
            Some(Ok(message)) if message.offset() < expected => continue,
            Some(Ok(message)) if message.offset() >= end => break,
            Some(Ok(message)) => {
                ensure!(
                    message.offset() == expected,
                    "offset gap in {topic}[{partition}]: expected {expected}, got {}",
                    message.offset()
                );
                let payload = message.payload().unwrap_or_default();
                let record = match kind.as_str() {
                    "raw" => match TelemetryEvent::decode(payload) {
                        Ok(event) => json!({
                            "kind": "valid",
                            "topic": topic,
                            "partition": partition,
                            "offset": expected,
                            "event_id": event.event_id,
                            "trace_id": event.trace_id,
                        }),
                        Err(_) => json!({
                            "kind": "malformed",
                            "topic": topic,
                            "partition": partition,
                            "offset": expected,
                            "key_hex": hex(message.key().unwrap_or_default()),
                            "payload_hex": hex(payload),
                        }),
                    },
                    "enriched" => {
                        let enriched = EnrichedTelemetry::decode(payload)
                            .context("decoding enriched record in measured range")?;
                        let event = enriched.event.context("enriched record has no event")?;
                        json!({
                            "kind": "enriched",
                            "topic": topic,
                            "partition": partition,
                            "offset": expected,
                            "event_id": event.event_id,
                            "trace_id": event.trace_id,
                        })
                    }
                    "dlq" => {
                        let dead_letter = DeadLetterRecord::decode(payload)
                            .context("decoding DLQ record in measured range")?;
                        json!({
                            "kind": "dlq",
                            "topic": topic,
                            "partition": partition,
                            "offset": expected,
                            "stage": dead_letter.stage,
                            "source_topic": dead_letter.source_topic,
                            "source_partition": dead_letter.source_partition,
                            "source_offset": dead_letter.source_offset,
                            "error_category": dead_letter.error_category,
                            "reason": dead_letter.reason,
                            "event_id": dead_letter.event_id,
                            "original_payload_hex": hex(&dead_letter.original_payload),
                        })
                    }
                    _ => unreachable!(),
                };
                println!("{record}");
                expected += 1;
            }
            Some(Err(error)) => return Err(error).context("reading Kafka record range"),
            None => {}
        }
    }
    ensure!(
        expected == end,
        "range ended at offset {expected}, expected {end}"
    );
    Ok(())
}
