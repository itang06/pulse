//! Kafka adapters for the sink's owned receive, acknowledged publish, and commit interfaces.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use rdkafka::{
    client::ClientContext,
    consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance, StreamConsumer},
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
    ClientConfig, Message,
};

use crate::{
    metrics::Metrics,
    model::SourceRecord,
    pipeline::{
        self, AcknowledgedDlqPublisher, CommitOutcome, OffsetCommitter, RecordReceiver,
        TopicPartition,
    },
};

pub const DLQ_ENQUEUE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);
const DLQ_DELIVERY_TIMEOUT_MS: &str = "10000";

pub struct KafkaReceiver {
    consumer: Arc<StreamConsumer<GenerationContext>>,
    assignment_generation: Arc<AtomicU64>,
    last_received_generation: u64,
}

pub struct KafkaCommitter {
    consumer: Arc<StreamConsumer<GenerationContext>>,
    assignment_generation: Arc<AtomicU64>,
}

pub struct KafkaPublisher {
    producer: FutureProducer,
}

struct GenerationContext {
    assignment_generation: Arc<AtomicU64>,
}

impl ClientContext for GenerationContext {}

impl ConsumerContext for GenerationContext {
    fn pre_rebalance(&self, _: &BaseConsumer<Self>, _: &Rebalance<'_>) {
        self.assignment_generation.fetch_add(1, Ordering::SeqCst);
    }
}

impl RecordReceiver for KafkaReceiver {
    fn assignment_generation(&self) -> u64 {
        self.assignment_generation.load(Ordering::SeqCst)
    }

    async fn receive(&mut self) -> Result<Option<SourceRecord>> {
        let message = self
            .consumer
            .recv()
            .await
            .context("receiving Kafka message")?;
        // Sampling immediately after recv means a rebalance completed while
        // idle does not invalidate the first record of the new assignment.
        self.last_received_generation = self.assignment_generation.load(Ordering::SeqCst);
        Ok(Some(source_record_from_parts(
            message.payload().unwrap_or_default().to_vec(),
            message.topic(),
            message.partition(),
            message.offset(),
        )?))
    }

    fn received_assignment_generation(&self) -> u64 {
        self.last_received_generation
    }
}

impl KafkaReceiver {
    /// Periodically estimate total lag away from the async receive path.
    pub fn start_lag_monitor(&self, metrics: Metrics) {
        let consumer = Arc::clone(&self.consumer);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(15));
            loop {
                interval.tick().await;
                let consumer = Arc::clone(&consumer);
                let estimate =
                    tokio::task::spawn_blocking(move || estimate_consumer_lag(&consumer)).await;
                match estimate {
                    Ok(Some(records)) => metrics.set_consumer_lag_estimate(Some(records)),
                    Ok(None) => {
                        metrics.set_consumer_lag_estimate(None);
                        tracing::debug!("Kafka consumer lag estimate unavailable");
                    }
                    Err(error) => {
                        metrics.set_consumer_lag_estimate(None);
                        tracing::debug!(%error, "Kafka lag estimate task failed");
                    }
                }
            }
        });
    }
}

fn estimate_consumer_lag(consumer: &StreamConsumer<GenerationContext>) -> Option<i64> {
    let assignment = consumer.assignment().ok()?;
    if assignment.elements().is_empty() {
        return None;
    }
    let committed = consumer
        .committed_offsets(assignment.clone(), Duration::from_secs(2))
        .ok()?;
    let mut total = 0i64;
    for partition in assignment.elements() {
        let (low, high) = consumer
            .fetch_watermarks(
                partition.topic(),
                partition.partition(),
                Duration::from_millis(250),
            )
            .ok()?;
        let committed_offset = committed
            .find_partition(partition.topic(), partition.partition())?
            .offset();
        let current = committed_offset_base(committed_offset, low, high)?;
        total = total.saturating_add(high.saturating_sub(current).max(0));
    }
    Some(total)
}

fn committed_offset_base(offset: rdkafka::Offset, low: i64, high: i64) -> Option<i64> {
    match offset {
        rdkafka::Offset::Offset(value) if (low..=high).contains(&value) => Some(value),
        // librdkafka resets an out-of-range offset to the earliest available record.
        rdkafka::Offset::Offset(_) => Some(low),
        rdkafka::Offset::Beginning => Some(low),
        rdkafka::Offset::End => Some(high),
        // The consumer is configured with auto.offset.reset=earliest.
        rdkafka::Offset::Invalid => Some(low),
        rdkafka::Offset::Stored | rdkafka::Offset::OffsetTail(_) => None,
    }
}

impl AcknowledgedDlqPublisher for KafkaPublisher {
    async fn publish(&self, payload: Vec<u8>) -> Result<()> {
        self.producer
            .send(
                FutureRecord::to(pipeline::DLQ_TOPIC)
                    .key(&[] as &[u8])
                    .payload(&payload),
                Timeout::After(DLQ_ENQUEUE_TIMEOUT),
            )
            .await
            .map(|_| ())
            .map_err(|(error, _)| anyhow!(error))
    }
}

impl OffsetCommitter for KafkaCommitter {
    async fn commit(
        &mut self,
        expected_generation: u64,
        offsets: std::collections::BTreeMap<TopicPartition, i64>,
    ) -> Result<CommitOutcome> {
        // Recheck immediately before the sync call. If a rebalance races this
        // load, the broker fences the stale group generation during commit.
        if self.assignment_generation.load(Ordering::SeqCst) != expected_generation {
            return Ok(CommitOutcome::AssignmentChanged);
        }
        let list = topic_partition_list(&offsets)?;
        self.consumer
            .commit(&list, rdkafka::consumer::CommitMode::Sync)
            .context("synchronous Kafka offset commit")?;
        Ok(CommitOutcome::Committed)
    }
}

/// Convert broker metadata into an owned source record. Invalid metadata is an
/// infrastructure error and must never be mistaken for poison payload data.
pub fn source_record_from_parts(
    payload: Vec<u8>,
    topic: &str,
    partition: i32,
    offset: i64,
) -> Result<SourceRecord> {
    if topic != pipeline::SOURCE_TOPIC {
        anyhow::bail!("sink received unexpected Kafka topic {topic:?}");
    }
    SourceRecord::try_new(payload, topic, partition, offset).map_err(anyhow::Error::new)
}

pub fn topic_partition_list(
    offsets: &std::collections::BTreeMap<TopicPartition, i64>,
) -> Result<rdkafka::TopicPartitionList> {
    let mut list = rdkafka::TopicPartitionList::new();
    for (topic_partition, offset) in offsets {
        list.add_partition_offset(
            &topic_partition.topic,
            topic_partition.partition,
            rdkafka::Offset::Offset(*offset),
        )
        .map_err(|error| anyhow!(error))?;
    }
    Ok(list)
}

pub fn create(
    brokers: &str,
    group_id: &str,
) -> Result<(KafkaReceiver, KafkaCommitter, Arc<KafkaPublisher>)> {
    let assignment_generation = Arc::new(AtomicU64::new(0));
    let consumer: StreamConsumer<GenerationContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest")
        .create_with_context(GenerationContext {
            assignment_generation: Arc::clone(&assignment_generation),
        })
        .context("creating Kafka consumer")?;
    let consumer = Arc::new(consumer);
    consumer
        .fetch_metadata(
            Some(pipeline::SOURCE_TOPIC),
            std::time::Duration::from_secs(10),
        )
        .context("Kafka broker metadata check")?;
    consumer
        .subscribe(&[pipeline::SOURCE_TOPIC])
        .context("subscribing to telemetry.enriched")?;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("message.timeout.ms", DLQ_DELIVERY_TIMEOUT_MS)
        .set("linger.ms", "5")
        .create()
        .context("creating acknowledged DLQ producer")?;

    Ok((
        KafkaReceiver {
            consumer: Arc::clone(&consumer),
            assignment_generation: Arc::clone(&assignment_generation),
            last_received_generation: 0,
        },
        KafkaCommitter {
            consumer,
            assignment_generation,
        },
        Arc::new(KafkaPublisher { producer }),
    ))
}

#[cfg(test)]
mod tests {
    use super::{committed_offset_base, source_record_from_parts, DLQ_ENQUEUE_TIMEOUT};
    use crate::model::SourceMetadataError;

    #[test]
    fn invalid_source_metadata_is_reported_as_infrastructure_error() {
        let error = source_record_from_parts(vec![0xff], "telemetry.enriched", -1, 3)
            .expect_err("negative partition is invalid broker metadata");
        assert!(error.downcast_ref::<SourceMetadataError>().is_some());
    }

    #[test]
    fn unexpected_source_topic_is_rejected_before_decode_or_dlq() {
        let error = source_record_from_parts(vec![0xff], "telemetry.alerts", 0, 3)
            .expect_err("sink only consumes enriched records");
        assert!(error.to_string().contains("unexpected Kafka topic"));
    }

    #[test]
    fn dlq_enqueue_wait_is_bounded() {
        assert_eq!(DLQ_ENQUEUE_TIMEOUT, std::time::Duration::from_secs(1));
    }

    #[test]
    fn committed_offsets_resolve_against_partition_watermarks() {
        assert_eq!(
            committed_offset_base(rdkafka::Offset::Offset(12), 4, 20),
            Some(12)
        );
        assert_eq!(
            committed_offset_base(rdkafka::Offset::Offset(2), 4, 20),
            Some(4)
        );
        assert_eq!(
            committed_offset_base(rdkafka::Offset::Offset(23), 4, 20),
            Some(4)
        );
        assert_eq!(
            committed_offset_base(rdkafka::Offset::Beginning, 4, 20),
            Some(4)
        );
        assert_eq!(committed_offset_base(rdkafka::Offset::End, 4, 20), Some(20));
        assert_eq!(
            committed_offset_base(rdkafka::Offset::Invalid, 4, 20),
            Some(4)
        );
        assert_eq!(committed_offset_base(rdkafka::Offset::Stored, 4, 20), None);
    }
}
