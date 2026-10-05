//! Bounded persistence batches with database, DLQ acknowledgement, and offset barriers.

use std::{
    collections::BTreeMap,
    future::Future,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Context, Result};
use prost_types::Timestamp;
use tokio::{sync::watch, task::JoinSet};

use crate::{
    db::{DbFailure, PersistOutcome},
    decode::decode_record,
    model::{DecodedRecord, SourceMetadataError, SourceRecord, ValidatedEvent},
};

pub const SOURCE_TOPIC: &str = "telemetry.enriched";
pub const DLQ_TOPIC: &str = "telemetry.dlq";
pub const MAX_BATCH_RECORDS: usize = 500;
pub const MAX_BATCH_WAIT: Duration = Duration::from_millis(10);
const INITIAL_DB_BACKOFF: Duration = Duration::from_millis(50);
const MAX_DB_BACKOFF: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BatchConfig {
    pub max_records: usize,
    pub max_wait: Duration,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_records: MAX_BATCH_RECORDS,
            max_wait: MAX_BATCH_WAIT,
        }
    }
}

impl BatchConfig {
    pub fn validate(self) -> Result<Self> {
        if self.max_records == 0 || self.max_records > MAX_BATCH_RECORDS {
            anyhow::bail!("batch max_records must be between 1 and {MAX_BATCH_RECORDS}");
        }
        if self.max_wait.is_zero() || self.max_wait > MAX_BATCH_WAIT {
            anyhow::bail!("batch max_wait must be between 1ns and {MAX_BATCH_WAIT:?}");
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryConfig {
    pub max_attempts: usize,
    pub max_backoff: Duration,
    pub max_backoff_budget: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            max_backoff: MAX_DB_BACKOFF,
            max_backoff_budget: Duration::from_secs(30),
        }
    }
}

impl RetryConfig {
    pub fn validate(self) -> Result<Self> {
        if self.max_attempts == 0 {
            anyhow::bail!("database retry max_attempts must be greater than zero");
        }
        if self.max_backoff < INITIAL_DB_BACKOFF || self.max_backoff > MAX_DB_BACKOFF {
            anyhow::bail!("database max_backoff must be between 50ms and 5s");
        }
        if self.max_backoff_budget.is_zero() {
            anyhow::bail!("database retry backoff budget must be greater than zero");
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PipelineConfig {
    pub batch: BatchConfig,
    pub retry: RetryConfig,
}

impl PipelineConfig {
    pub fn validate(self) -> Result<Self> {
        self.batch.validate()?;
        self.retry.validate()?;
        Ok(self)
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

impl TopicPartition {
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }
}

pub trait RecordReceiver {
    fn receive(&mut self) -> impl Future<Output = Result<Option<SourceRecord>>> + Send;
    fn assignment_generation(&self) -> u64;

    /// Generation sampled by the adapter immediately after receiving its last record.
    /// Receivers without a more precise snapshot use the current generation.
    fn received_assignment_generation(&self) -> u64 {
        self.assignment_generation()
    }
}

pub trait AcknowledgedDlqPublisher: Send + Sync + 'static {
    fn publish(&self, payload: Vec<u8>) -> impl Future<Output = Result<()>> + Send;
}

pub trait OffsetCommitter {
    fn commit(
        &mut self,
        expected_generation: u64,
        offsets: BTreeMap<TopicPartition, i64>,
    ) -> impl Future<Output = Result<CommitOutcome>> + Send;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitOutcome {
    Committed,
    AssignmentChanged,
}

pub trait Database: Sync {
    fn persist_batch<'a>(
        &'a self,
        events: &'a [ValidatedEvent],
    ) -> impl Future<Output = std::result::Result<PersistOutcome, DbFailure>> + Send + 'a;
}

/// Optional metrics hooks at the sink's owned fetch and side-effect boundaries.
pub trait PipelineObserver: Sync {
    fn source_record_consumed(&self) {}
    fn persisted(&self, _outcome: &PersistOutcome) {}
    /// Called after a successful DB transaction and before DLQ publication or
    /// source offset commit. This is also called when every event conflicted.
    fn database_committed_before_ack(&self, outcome: &PersistOutcome) {
        self.persisted(outcome);
    }
    fn database_retry(&self) {}
    fn dlq_record_published(&self) {}
    fn batch_finished(&self, _records: usize, _duration: Duration) {}
}

struct NoopObserver;

impl PipelineObserver for NoopObserver {}

struct BatchObservation<'a> {
    observer: &'a dyn PipelineObserver,
    started: Instant,
    records: usize,
}

impl BatchObservation<'_> {
    fn fetched(&mut self) {
        self.records += 1;
        self.observer.source_record_consumed();
    }
}

impl Drop for BatchObservation<'_> {
    fn drop(&mut self) {
        self.observer
            .batch_finished(self.records, self.started.elapsed());
    }
}

#[derive(Debug)]
pub enum PipelineError {
    Retryable(anyhow::Error),
    Fatal(anyhow::Error),
}

impl std::fmt::Display for PipelineError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Retryable(error) => write!(formatter, "retryable sink failure: {error:#}"),
            Self::Fatal(error) => write!(formatter, "fatal sink failure: {error:#}"),
        }
    }
}

impl std::error::Error for PipelineError {}

/// Process one bounded batch. `false` means shutdown or receiver EOF.
/// A generation change after processing is a replay-safe `true` outcome: the
/// database and acknowledged DLQ work may repeat, but no stale offset commits.
pub async fn process_next_batch<R, D, P, C>(
    receiver: &mut R,
    database: &D,
    publisher: Arc<P>,
    committer: &mut C,
    shutdown: &mut watch::Receiver<bool>,
    config: PipelineConfig,
) -> std::result::Result<bool, PipelineError>
where
    R: RecordReceiver,
    D: Database,
    P: AcknowledgedDlqPublisher,
    C: OffsetCommitter,
{
    process_next_batch_observed(
        receiver,
        database,
        publisher,
        committer,
        shutdown,
        config,
        &NoopObserver,
    )
    .await
}

pub async fn process_next_batch_observed<R, D, P, C>(
    receiver: &mut R,
    database: &D,
    publisher: Arc<P>,
    committer: &mut C,
    shutdown: &mut watch::Receiver<bool>,
    config: PipelineConfig,
    observer: &dyn PipelineObserver,
) -> std::result::Result<bool, PipelineError>
where
    R: RecordReceiver,
    D: Database,
    P: AcknowledgedDlqPublisher,
    C: OffsetCommitter,
{
    let config = config.validate().map_err(PipelineError::Fatal)?;
    let first = tokio::select! {
        changed = shutdown.changed() => {
            if changed.is_err() || *shutdown.borrow() {
                return Ok(false);
            }
            return Ok(false);
        }
        received = receiver.receive() => received.map_err(classify_receive_error)?,
    };
    let Some(first) = first else {
        return Ok(false);
    };
    let batch_generation = receiver.received_assignment_generation();

    let batch_started = Instant::now();
    let mut batch_observation = BatchObservation {
        observer,
        started: batch_started,
        records: 0,
    };
    batch_observation.fetched();
    let deadline = batch_started + config.batch.max_wait;
    let mut records = Vec::with_capacity(config.batch.max_records);
    records.push(first);
    while records.len() < config.batch.max_records {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(false);
                }
            }
            _ = tokio::time::sleep(remaining) => break,
            received = receiver.receive() => match received {
                Ok(Some(record)) => {
                    records.push(record);
                    batch_observation.fetched();
                }
                Ok(None) => break,
                Err(error) => return Err(classify_receive_error(error)),
            }
        }
    }

    let failed_at = current_timestamp();
    let mut valid = Vec::with_capacity(records.len());
    let mut dead_letters = Vec::new();
    let mut offsets = BTreeMap::new();
    for source in records {
        let topic_partition = TopicPartition::new(source.topic(), source.partition());
        let next_offset = source
            .offset()
            .checked_add(1)
            .ok_or_else(|| PipelineError::Fatal(anyhow!("source offset overflow")))?;
        offsets
            .entry(topic_partition)
            .and_modify(|highest: &mut i64| *highest = (*highest).max(next_offset))
            .or_insert(next_offset);
        match decode_record(source, failed_at) {
            DecodedRecord::Valid(event) => valid.push(event),
            DecodedRecord::DeadLetter(payload) => dead_letters.push(payload),
        }
    }

    if !valid.is_empty() {
        let outcome = persist_with_retry(database, &valid, config.retry, observer).await?;
        observer.database_committed_before_ack(&outcome);
    }

    if !dead_letters.is_empty() && !publish_all(publisher, dead_letters, shutdown, observer).await?
    {
        return Ok(false);
    }

    if receiver.assignment_generation() != batch_generation {
        tracing::warn!(
            captured_generation = batch_generation,
            current_generation = receiver.assignment_generation(),
            "Kafka assignment changed during batch; leaving offsets uncommitted"
        );
        return Ok(true);
    }
    match committer
        .commit(batch_generation, offsets)
        .await
        .context("committing processed Kafka offsets")
        .map_err(PipelineError::Retryable)?
    {
        CommitOutcome::Committed => {}
        CommitOutcome::AssignmentChanged => {
            tracing::warn!(
                captured_generation = batch_generation,
                current_generation = receiver.assignment_generation(),
                "Kafka assignment changed at commit boundary; leaving offsets uncommitted"
            );
        }
    }
    Ok(true)
}

async fn persist_with_retry<D: Database>(
    database: &D,
    events: &[ValidatedEvent],
    config: RetryConfig,
    observer: &dyn PipelineObserver,
) -> std::result::Result<PersistOutcome, PipelineError> {
    let mut backoff = INITIAL_DB_BACKOFF;
    let mut slept = Duration::ZERO;
    for attempt in 1..=config.max_attempts {
        match database.persist_batch(events).await {
            Ok(outcome) => return Ok(outcome),
            Err(DbFailure::Fatal(error)) => {
                return Err(PipelineError::Fatal(
                    anyhow!(error).context("persisting sink batch"),
                ));
            }
            Err(DbFailure::Retryable(error)) if attempt == config.max_attempts => {
                return Err(PipelineError::Retryable(
                    anyhow!(error)
                        .context(format!("database retry attempts exhausted ({attempt})")),
                ));
            }
            Err(DbFailure::Retryable(error)) => {
                let delay = backoff.min(config.max_backoff);
                if slept.saturating_add(delay) > config.max_backoff_budget {
                    return Err(PipelineError::Retryable(anyhow!(error).context(
                        "database retry backoff budget exhausted; offsets remain uncommitted",
                    )));
                }
                observer.database_retry();
                tracing::warn!(attempt, ?delay, %error, "transient sink database failure; retrying");
                tokio::time::sleep(delay).await;
                slept += delay;
                backoff = backoff.saturating_mul(2).min(config.max_backoff);
            }
        }
    }
    unreachable!("validated retry configuration always attempts at least once")
}

async fn publish_all<P: AcknowledgedDlqPublisher>(
    publisher: Arc<P>,
    payloads: Vec<Vec<u8>>,
    shutdown: &mut watch::Receiver<bool>,
    observer: &dyn PipelineObserver,
) -> std::result::Result<bool, PipelineError> {
    let mut deliveries = JoinSet::new();
    for payload in payloads {
        let publisher = Arc::clone(&publisher);
        deliveries.spawn(async move { publisher.publish(payload).await });
    }
    let mut failed = false;
    loop {
        let result = tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    abort_and_count_completed_deliveries(&mut deliveries, observer).await;
                    return Ok(false);
                }
                continue;
            }
            result = deliveries.join_next() => result,
        };
        let Some(result) = result else {
            break;
        };
        match result {
            Ok(Ok(())) => observer.dlq_record_published(),
            Ok(Err(error)) => {
                tracing::error!(%error, "DLQ delivery failed; source batch will replay");
                failed = true;
            }
            Err(error) => {
                tracing::error!(%error, "DLQ delivery task failed; source batch will replay");
                failed = true;
            }
        }
    }
    if failed {
        return Err(PipelineError::Retryable(anyhow!(
            "one or more DLQ deliveries failed; source offsets remain uncommitted"
        )));
    }
    Ok(true)
}

async fn abort_and_count_completed_deliveries(
    deliveries: &mut JoinSet<Result<()>>,
    observer: &dyn PipelineObserver,
) {
    deliveries.abort_all();
    while let Some(result) = deliveries.join_next().await {
        if matches!(result, Ok(Ok(()))) {
            observer.dlq_record_published();
        }
    }
}

fn classify_receive_error(error: anyhow::Error) -> PipelineError {
    if error.downcast_ref::<SourceMetadataError>().is_some() {
        PipelineError::Fatal(error.context("invalid Kafka source metadata"))
    } else {
        PipelineError::Retryable(error.context("receiving Kafka source record"))
    }
}

fn current_timestamp() -> Timestamp {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    Timestamp {
        seconds: elapsed.as_secs().min(i64::MAX as u64) as i64,
        nanos: elapsed.subsec_nanos() as i32,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{abort_and_count_completed_deliveries, PipelineObserver};
    use anyhow::Result;
    use tokio::task::JoinSet;

    #[derive(Default)]
    struct DlqCounter(AtomicUsize);

    impl PipelineObserver for DlqCounter {
        fn dlq_record_published(&self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn shutdown_drain_counts_completed_acknowledgements_only() {
        let mut deliveries = JoinSet::<Result<()>>::new();
        let (completed_tx, completed_rx) = tokio::sync::oneshot::channel();
        deliveries.spawn(async move {
            let _ = completed_tx.send(());
            Ok(())
        });
        completed_rx.await.unwrap();
        deliveries.spawn(std::future::pending());

        let metrics = DlqCounter::default();
        abort_and_count_completed_deliveries(&mut deliveries, &metrics).await;

        assert_eq!(metrics.0.load(Ordering::SeqCst), 1);
        assert!(deliveries.is_empty());
    }
}
