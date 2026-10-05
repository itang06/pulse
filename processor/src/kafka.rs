//! Kafka adapters for the pipeline's owned, acknowledged interfaces.

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::{anyhow, Context, Result};
use rdkafka::{
    client::ClientContext,
    consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance, StreamConsumer},
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
    ClientConfig, Message,
};

use crate::pipeline::{
    self, AcknowledgedPublisher, OffsetCommitter, OwnedRecord, PublishRecord, RecordReceiver,
    TopicPartition,
};

pub struct KafkaReceiver {
    consumer: Arc<StreamConsumer<GenerationContext>>,
    assignment_generation: Arc<AtomicU64>,
}
pub struct KafkaCommitter {
    consumer: Arc<StreamConsumer<GenerationContext>>,
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

    async fn receive(&mut self) -> Result<Option<OwnedRecord>> {
        let message = self
            .consumer
            .recv()
            .await
            .context("receiving Kafka message")?;
        Ok(Some(OwnedRecord {
            payload: message.payload().unwrap_or_default().to_vec(),
            key: message.key().unwrap_or_default().to_vec(),
            topic: message.topic().to_owned(),
            partition: message.partition(),
            offset: message.offset(),
        }))
    }
}

impl AcknowledgedPublisher for KafkaPublisher {
    async fn publish(&self, record: PublishRecord) -> Result<()> {
        self.producer
            .send(
                FutureRecord::to(&record.topic)
                    .key(&record.key)
                    .payload(&record.payload),
                Timeout::Never,
            )
            .await
            .map(|_| ())
            .map_err(|(error, _)| anyhow!(error))
    }
}

impl OffsetCommitter for KafkaCommitter {
    async fn commit(
        &mut self,
        offsets: std::collections::BTreeMap<TopicPartition, i64>,
    ) -> Result<()> {
        let list = pipeline::topic_partition_list(&offsets)?;
        self.consumer
            .commit(&list, rdkafka::consumer::CommitMode::Sync)
            .context("synchronous Kafka offset commit")
    }
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
        .fetch_metadata(Some("telemetry.raw"), Duration::from_secs(10))
        .context("Kafka broker metadata check")?;
    consumer
        .subscribe(&["telemetry.raw"])
        .context("subscribing to telemetry.raw")?;
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("linger.ms", "5")
        .set("compression.type", "lz4")
        .create()
        .context("creating acknowledged Kafka producer")?;
    Ok((
        KafkaReceiver {
            consumer: Arc::clone(&consumer),
            assignment_generation,
        },
        KafkaCommitter { consumer },
        Arc::new(KafkaPublisher { producer }),
    ))
}
