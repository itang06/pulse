use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use anyhow::{anyhow, Result};
use prost::Message;
use prost_types::Timestamp;
use pulse_proto::pulse::v1::{EnrichedTelemetry, TelemetryEvent};
use pulse_sink::{
    db::{DbFailure, PersistOutcome},
    metrics::Metrics,
    model::{SourceMetadataError, SourceRecord, ValidatedEvent},
    pipeline::{
        process_next_batch, process_next_batch_observed, AcknowledgedDlqPublisher, BatchConfig,
        CommitOutcome, Database, OffsetCommitter, PipelineConfig, PipelineObserver, RecordReceiver,
        RetryConfig, TopicPartition,
    },
};
use tokio::sync::{oneshot, watch};
use uuid::Uuid;

type RecordQueue = Arc<Mutex<VecDeque<(SourceRecord, Option<Duration>)>>>;

#[derive(Clone)]
struct FakeReceiver {
    records: RecordQueue,
    generation: Arc<AtomicUsize>,
}

impl FakeReceiver {
    fn new(records: Vec<SourceRecord>) -> Self {
        Self {
            records: Arc::new(Mutex::new(
                records.into_iter().map(|record| (record, None)).collect(),
            )),
            generation: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn with_delays(records: Vec<(SourceRecord, Duration)>) -> Self {
        Self {
            records: Arc::new(Mutex::new(
                records
                    .into_iter()
                    .map(|(record, delay)| (record, Some(delay)))
                    .collect(),
            )),
            generation: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn bump_generation(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
    }
}

impl RecordReceiver for FakeReceiver {
    async fn receive(&mut self) -> Result<Option<SourceRecord>> {
        let next = self.records.lock().unwrap().front().cloned();
        match next {
            Some((record, Some(delay))) => {
                tokio::time::sleep(delay).await;
                self.records.lock().unwrap().pop_front();
                Ok(Some(record))
            }
            Some((record, None)) => {
                self.records.lock().unwrap().pop_front();
                Ok(Some(record))
            }
            None => std::future::pending().await,
        }
    }

    fn assignment_generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst) as u64
    }
}

struct IdleRebalanceReceiver {
    record: Option<SourceRecord>,
    generation: Arc<AtomicUsize>,
    started: Option<oneshot::Sender<()>>,
    resume: Option<oneshot::Receiver<()>>,
}

impl RecordReceiver for IdleRebalanceReceiver {
    async fn receive(&mut self) -> Result<Option<SourceRecord>> {
        if let Some(started) = self.started.take() {
            let _ = started.send(());
            if let Some(resume) = self.resume.take() {
                let _ = resume.await;
            }
            return Ok(self.record.take());
        }
        std::future::pending().await
    }

    fn assignment_generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst) as u64
    }
}

struct InvalidMetadataReceiver;

impl RecordReceiver for InvalidMetadataReceiver {
    async fn receive(&mut self) -> Result<Option<SourceRecord>> {
        Err(anyhow::Error::new(SourceMetadataError::NegativePartition))
    }

    fn assignment_generation(&self) -> u64 {
        0
    }
}

#[derive(Default)]
struct FakeDatabase {
    attempts: AtomicUsize,
    failures: Mutex<VecDeque<DbFailure>>,
    entered: Mutex<Option<oneshot::Sender<()>>>,
    gate: Mutex<Option<oneshot::Receiver<()>>>,
    persisted: Mutex<Vec<Vec<Uuid>>>,
    outcomes: Mutex<VecDeque<PersistOutcome>>,
}

impl Database for FakeDatabase {
    async fn persist_batch(
        &self,
        events: &[ValidatedEvent],
    ) -> std::result::Result<PersistOutcome, DbFailure> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        if let Some(entered) = self.entered.lock().unwrap().take() {
            let _ = entered.send(());
        }
        let gate = self.gate.lock().unwrap().take();
        if let Some(gate) = gate {
            let _ = gate.await;
        }
        if let Some(error) = self.failures.lock().unwrap().pop_front() {
            return Err(error);
        }
        let ids = events
            .iter()
            .map(|event| event.event_id)
            .collect::<Vec<_>>();
        *self.persisted.lock().unwrap() = vec![ids.clone()];
        Ok(self
            .outcomes
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| PersistOutcome {
                inserted_ids: ids.into_iter().collect(),
                conflict_count: 0,
            }))
    }
}

#[derive(Default)]
struct FakePublisher {
    published: Mutex<Vec<Vec<u8>>>,
    failures_remaining: AtomicUsize,
    gate: Mutex<Option<oneshot::Receiver<()>>>,
    entered: Mutex<Option<oneshot::Sender<()>>>,
}

impl AcknowledgedDlqPublisher for FakePublisher {
    async fn publish(&self, payload: Vec<u8>) -> Result<()> {
        self.published.lock().unwrap().push(payload);
        if let Some(entered) = self.entered.lock().unwrap().take() {
            let _ = entered.send(());
        }
        let gate = self.gate.lock().unwrap().take();
        if let Some(gate) = gate {
            let _ = gate.await;
        }
        if self
            .failures_remaining
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            Err(anyhow!("DLQ broker rejected message"))
        } else {
            Ok(())
        }
    }
}

#[derive(Default)]
struct FakeCommitter {
    commits: Mutex<Vec<BTreeMap<TopicPartition, i64>>>,
    failure: Mutex<bool>,
    late_assignment_change: Mutex<bool>,
}

impl OffsetCommitter for FakeCommitter {
    fn commit(
        &mut self,
        _expected_generation: u64,
        offsets: BTreeMap<TopicPartition, i64>,
    ) -> impl Future<Output = Result<CommitOutcome>> + Send {
        let fail = *self.failure.lock().unwrap();
        let assignment_changed = *self.late_assignment_change.lock().unwrap();
        if !assignment_changed {
            self.commits.lock().unwrap().push(offsets);
        }
        async move {
            if fail {
                Err(anyhow!("commit failed"))
            } else if assignment_changed {
                Ok(CommitOutcome::AssignmentChanged)
            } else {
                Ok(CommitOutcome::Committed)
            }
        }
    }
}

fn source(offset: i64, valid: bool, partition: i32) -> SourceRecord {
    let payload = if valid {
        EnrichedTelemetry {
            event: Some(TelemetryEvent {
                event_id: Uuid::from_u128(offset as u128 + 1).to_string(),
                event_time: Some(Timestamp {
                    seconds: 1_700_000_000,
                    nanos: 0,
                }),
                service_name: "api".into(),
                route: "/events".into(),
                latency_us: 1,
                status_code: 200,
                trace_id: "trace".into(),
                attributes: Default::default(),
            }),
            ewma_mean_us: 1.0,
            ewma_stddev_us: 0.0,
            anomaly_score: 0.0,
            is_anomaly: false,
            processed_at: Some(Timestamp {
                seconds: 1_700_000_001,
                nanos: 0,
            }),
        }
        .encode_to_vec()
    } else {
        vec![0xff]
    };
    SourceRecord::try_new(payload, "telemetry.enriched", partition, offset).unwrap()
}

fn config(max_records: usize, max_wait: Duration, attempts: usize) -> PipelineConfig {
    PipelineConfig {
        batch: BatchConfig {
            max_records,
            max_wait,
        },
        retry: RetryConfig {
            max_attempts: attempts,
            max_backoff: Duration::from_secs(5),
            max_backoff_budget: Duration::from_secs(30),
        },
    }
}

#[test]
fn pipeline_settings_enforce_record_time_and_retry_bounds() {
    assert!(config(0, Duration::from_millis(10), 1).validate().is_err());
    assert!(config(501, Duration::from_millis(10), 1)
        .validate()
        .is_err());
    assert!(config(500, Duration::from_millis(11), 1)
        .validate()
        .is_err());
    assert!(config(500, Duration::from_millis(10), 0)
        .validate()
        .is_err());
    let mut retry_above_cap = config(500, Duration::from_millis(10), 1);
    retry_above_cap.retry.max_backoff = Duration::from_secs(6);
    assert!(retry_above_cap.validate().is_err());
    let mut no_retry_budget = config(500, Duration::from_millis(10), 1);
    no_retry_budget.retry.max_backoff_budget = Duration::ZERO;
    assert!(no_retry_budget.validate().is_err());
}

async fn run<R: RecordReceiver>(
    receiver: &mut R,
    database: &FakeDatabase,
    publisher: Arc<FakePublisher>,
    committer: &mut FakeCommitter,
    shutdown: &mut watch::Receiver<bool>,
    config: PipelineConfig,
) -> std::result::Result<bool, pulse_sink::pipeline::PipelineError> {
    process_next_batch(receiver, database, publisher, committer, shutdown, config).await
}

#[tokio::test]
async fn caps_batch_at_500_records_and_commits_highest_next_offset_per_partition() {
    let records = (0..502)
        .map(|offset| source(offset, true, (offset % 2) as i32))
        .collect();
    let mut receiver = FakeReceiver::new(records);
    let database = FakeDatabase::default();
    let publisher = Arc::new(FakePublisher::default());
    let mut committer = FakeCommitter::default();
    let (_shutdown_tx, mut shutdown) = watch::channel(false);

    run(
        &mut receiver,
        &database,
        publisher,
        &mut committer,
        &mut shutdown,
        config(500, Duration::from_millis(10), 1),
    )
    .await
    .unwrap();

    let commits = committer.commits.lock().unwrap();
    assert_eq!(commits.len(), 1);
    assert_eq!(commits[0].len(), 2);
    assert_eq!(
        commits[0][&TopicPartition::new("telemetry.enriched", 0)],
        499
    );
    assert_eq!(
        commits[0][&TopicPartition::new("telemetry.enriched", 1)],
        500
    );
    assert_eq!(database.persisted.lock().unwrap()[0].len(), 500);
}

#[tokio::test]
async fn ten_millisecond_deadline_stops_accumulation() {
    let records = vec![
        (source(0, true, 0), Duration::ZERO),
        (source(1, true, 0), Duration::from_millis(15)),
    ];
    let mut receiver = FakeReceiver::with_delays(records);
    let database = FakeDatabase::default();
    let publisher = Arc::new(FakePublisher::default());
    let mut committer = FakeCommitter::default();
    let (_shutdown_tx, mut shutdown) = watch::channel(false);

    run(
        &mut receiver,
        &database,
        publisher,
        &mut committer,
        &mut shutdown,
        config(500, Duration::from_millis(10), 1),
    )
    .await
    .unwrap();

    assert_eq!(database.persisted.lock().unwrap()[0].len(), 1);
    assert_eq!(
        committer.commits.lock().unwrap()[0].values().next(),
        Some(&1)
    );
}

#[tokio::test]
async fn mixed_valid_and_poison_batch_waits_for_ack_then_commits() {
    let mut receiver = FakeReceiver::new(vec![source(4, true, 0), source(5, false, 0)]);
    let database = FakeDatabase::default();
    let publisher = Arc::new(FakePublisher::default());
    let mut committer = FakeCommitter::default();
    let (_shutdown_tx, mut shutdown) = watch::channel(false);

    run(
        &mut receiver,
        &database,
        publisher.clone(),
        &mut committer,
        &mut shutdown,
        config(10, Duration::from_millis(10), 1),
    )
    .await
    .unwrap();

    assert_eq!(database.persisted.lock().unwrap()[0].len(), 1);
    assert_eq!(publisher.published.lock().unwrap().len(), 1);
    assert_eq!(
        committer.commits.lock().unwrap()[0].values().next(),
        Some(&6)
    );
}

#[tokio::test]
async fn no_commit_occurs_while_dlq_acknowledgement_is_gated() {
    let mut receiver = FakeReceiver::new(vec![source(3, false, 0)]);
    let receiver_for_change = receiver.clone();
    let database = FakeDatabase::default();
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let publisher = Arc::new(FakePublisher::default());
    *publisher.entered.lock().unwrap() = Some(entered_tx);
    *publisher.gate.lock().unwrap() = Some(release_rx);
    let mut committer = FakeCommitter::default();
    let (_shutdown_tx, mut shutdown) = watch::channel(false);
    let task = tokio::spawn(async move {
        let result = run(
            &mut receiver,
            &database,
            publisher,
            &mut committer,
            &mut shutdown,
            config(10, Duration::from_millis(10), 1),
        )
        .await;
        (result, committer.commits.into_inner().unwrap())
    });

    entered_rx.await.unwrap();
    assert!(!task.is_finished());
    receiver_for_change.bump_generation();
    release_tx.send(()).unwrap();
    let (result, commits) = task.await.unwrap();
    assert!(result.unwrap());
    assert!(commits.is_empty());
}

#[tokio::test]
async fn shutdown_during_gated_dlq_drops_waits_and_never_commits() {
    let mut receiver = FakeReceiver::new(vec![source(3, false, 0)]);
    let database = FakeDatabase::default();
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let publisher = Arc::new(FakePublisher::default());
    *publisher.entered.lock().unwrap() = Some(entered_tx);
    *publisher.gate.lock().unwrap() = Some(release_rx);
    let mut committer = FakeCommitter::default();
    let (shutdown_tx, mut shutdown) = watch::channel(false);
    let task = tokio::spawn(async move {
        let result = run(
            &mut receiver,
            &database,
            publisher,
            &mut committer,
            &mut shutdown,
            config(10, Duration::from_millis(10), 1),
        )
        .await;
        (result, committer.commits.into_inner().unwrap())
    });

    entered_rx.await.unwrap();
    shutdown_tx.send(true).unwrap();
    let (result, commits) = task.await.unwrap();
    assert!(!result.unwrap());
    assert!(commits.is_empty());
    let _ = release_tx.send(());
}

#[tokio::test]
async fn bounded_dlq_enqueue_failure_returns_without_committing() {
    let mut receiver = FakeReceiver::new(vec![source(7, false, 0)]);
    let database = FakeDatabase::default();
    let publisher = Arc::new(FakePublisher::default());
    publisher.failures_remaining.store(1, Ordering::SeqCst);
    let mut committer = FakeCommitter::default();
    let (_shutdown_tx, mut shutdown) = watch::channel(false);

    let result = run(
        &mut receiver,
        &database,
        publisher,
        &mut committer,
        &mut shutdown,
        config(10, Duration::from_millis(10), 1),
    )
    .await;

    assert!(matches!(
        result,
        Err(pulse_sink::pipeline::PipelineError::Retryable(_))
    ));
    assert!(committer.commits.lock().unwrap().is_empty());
}

#[tokio::test]
async fn record_timed_out_while_accumulating_is_available_to_the_next_batch() {
    let mut receiver = FakeReceiver::with_delays(vec![
        (source(0, true, 0), Duration::ZERO),
        (source(1, true, 0), Duration::from_millis(15)),
    ]);
    let database = FakeDatabase::default();
    let publisher = Arc::new(FakePublisher::default());
    let mut committer = FakeCommitter::default();
    let (_shutdown_tx, mut shutdown) = watch::channel(false);
    let cfg = config(10, Duration::from_millis(10), 1);

    run(
        &mut receiver,
        &database,
        publisher.clone(),
        &mut committer,
        &mut shutdown,
        cfg,
    )
    .await
    .unwrap();
    run(
        &mut receiver,
        &database,
        publisher,
        &mut committer,
        &mut shutdown,
        cfg,
    )
    .await
    .unwrap();

    let persisted = database.persisted.lock().unwrap();
    assert_eq!(persisted[0], vec![Uuid::from_u128(2)]);
    assert_eq!(committer.commits.lock().unwrap().len(), 2);
    assert_eq!(
        committer.commits.lock().unwrap()[1][&TopicPartition::new("telemetry.enriched", 0)],
        2
    );
}

#[tokio::test]
async fn rebalance_during_idle_first_receive_uses_the_new_generation_for_batch() {
    let generation = Arc::new(AtomicUsize::new(0));
    let (started_tx, started_rx) = oneshot::channel();
    let (resume_tx, resume_rx) = oneshot::channel();
    let mut receiver = IdleRebalanceReceiver {
        record: Some(source(8, true, 0)),
        generation: Arc::clone(&generation),
        started: Some(started_tx),
        resume: Some(resume_rx),
    };
    let database = FakeDatabase::default();
    let mut committer = FakeCommitter::default();
    let (_shutdown_tx, mut shutdown) = watch::channel(false);
    let task = tokio::spawn(async move {
        run(
            &mut receiver,
            &database,
            Arc::new(FakePublisher::default()),
            &mut committer,
            &mut shutdown,
            config(1, Duration::from_millis(10), 1),
        )
        .await
        .unwrap();
        committer.commits.into_inner().unwrap()
    });

    started_rx.await.unwrap();
    generation.fetch_add(1, Ordering::SeqCst);
    resume_tx.send(()).unwrap();
    let commits = task.await.unwrap();
    assert_eq!(commits.len(), 1);
    assert_eq!(commits[0][&TopicPartition::new("telemetry.enriched", 0)], 9);
}

#[tokio::test]
async fn retryable_database_failure_retries_then_commits_but_fatal_never_retries() {
    let mut receiver = FakeReceiver::new(vec![source(1, true, 0)]);
    let database = FakeDatabase::default();
    database
        .failures
        .lock()
        .unwrap()
        .push_back(DbFailure::Retryable(sqlx::Error::Io(
            std::io::Error::other("connection reset"),
        )));
    let publisher = Arc::new(FakePublisher::default());
    let mut committer = FakeCommitter::default();
    let (_shutdown_tx, mut shutdown) = watch::channel(false);

    run(
        &mut receiver,
        &database,
        publisher,
        &mut committer,
        &mut shutdown,
        config(10, Duration::from_millis(10), 2),
    )
    .await
    .unwrap();
    assert_eq!(database.attempts.load(Ordering::SeqCst), 2);
    assert_eq!(committer.commits.lock().unwrap().len(), 1);

    let mut receiver = FakeReceiver::new(vec![source(1, true, 0)]);
    let database = FakeDatabase::default();
    database
        .failures
        .lock()
        .unwrap()
        .push_back(DbFailure::Fatal(sqlx::Error::PoolClosed));
    let mut committer = FakeCommitter::default();
    let result = run(
        &mut receiver,
        &database,
        Arc::new(FakePublisher::default()),
        &mut committer,
        &mut shutdown,
        config(10, Duration::from_millis(10), 3),
    )
    .await;
    assert!(matches!(
        result,
        Err(pulse_sink::pipeline::PipelineError::Fatal(_))
    ));
    assert_eq!(database.attempts.load(Ordering::SeqCst), 1);
    assert!(committer.commits.lock().unwrap().is_empty());
}

#[tokio::test]
async fn observer_counts_owned_fetch_and_only_successful_database_outcome() {
    let mut receiver = FakeReceiver::new(vec![source(1, true, 0)]);
    let database = FakeDatabase::default();
    database
        .failures
        .lock()
        .unwrap()
        .push_back(DbFailure::Retryable(sqlx::Error::Io(
            std::io::Error::other("connection reset"),
        )));
    let metrics = Metrics::new();
    let mut committer = FakeCommitter::default();
    let (_shutdown_tx, mut shutdown) = watch::channel(false);

    process_next_batch_observed(
        &mut receiver,
        &database,
        Arc::new(FakePublisher::default()),
        &mut committer,
        &mut shutdown,
        config(1, Duration::from_millis(10), 2),
        &metrics,
    )
    .await
    .unwrap();

    let mut output = Vec::new();
    prometheus::Encoder::encode(
        &prometheus::TextEncoder::new(),
        &metrics.registry.gather(),
        &mut output,
    )
    .unwrap();
    let output = String::from_utf8(output).unwrap();
    assert!(output.contains("pulse_sink_records_consumed_total 1"));
    assert!(output.contains("pulse_sink_events_persisted_total 1"));
    assert!(output.contains("pulse_sink_database_retries_total 1"));
    assert!(output.contains("pulse_sink_batch_size_count 1"));

    database.outcomes.lock().unwrap().push_back(PersistOutcome {
        inserted_ids: Default::default(),
        conflict_count: 1,
    });
    let mut replay_receiver = FakeReceiver::new(vec![source(1, true, 0)]);
    process_next_batch_observed(
        &mut replay_receiver,
        &database,
        Arc::new(FakePublisher::default()),
        &mut committer,
        &mut shutdown,
        config(1, Duration::from_millis(10), 2),
        &metrics,
    )
    .await
    .unwrap();
    let mut output = Vec::new();
    prometheus::Encoder::encode(
        &prometheus::TextEncoder::new(),
        &metrics.registry.gather(),
        &mut output,
    )
    .unwrap();
    let output = String::from_utf8(output).unwrap();
    assert!(output.contains("pulse_sink_events_persisted_total 1"));
    assert!(output.contains("pulse_sink_conflicts_skipped_total 1"));
}

#[derive(Default)]
struct DatabaseCommitHook {
    calls: AtomicUsize,
    inserted: AtomicUsize,
    conflicts: AtomicUsize,
}

impl PipelineObserver for DatabaseCommitHook {
    fn database_committed_before_ack(&self, outcome: &PersistOutcome) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inserted
            .store(outcome.inserted_ids.len(), Ordering::SeqCst);
        self.conflicts
            .store(outcome.conflict_count as usize, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn post_database_commit_hook_runs_for_all_conflict_batch_before_offset_commit() {
    let mut receiver = FakeReceiver::new(vec![source(1, true, 0)]);
    let database = FakeDatabase::default();
    database.outcomes.lock().unwrap().push_back(PersistOutcome {
        inserted_ids: Default::default(),
        conflict_count: 1,
    });
    let hook = DatabaseCommitHook::default();
    let mut committer = FakeCommitter::default();
    let (_shutdown_tx, mut shutdown) = watch::channel(false);

    process_next_batch_observed(
        &mut receiver,
        &database,
        Arc::new(FakePublisher::default()),
        &mut committer,
        &mut shutdown,
        config(1, Duration::from_millis(10), 1),
        &hook,
    )
    .await
    .unwrap();

    assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
    assert_eq!(hook.inserted.load(Ordering::SeqCst), 0);
    assert_eq!(hook.conflicts.load(Ordering::SeqCst), 1);
    assert_eq!(committer.commits.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn observer_records_failed_batch_without_counting_uncommitted_database_work() {
    let mut receiver = FakeReceiver::new(vec![source(1, true, 0)]);
    let database = FakeDatabase::default();
    database
        .failures
        .lock()
        .unwrap()
        .push_back(DbFailure::Fatal(sqlx::Error::PoolClosed));
    let metrics = Metrics::new();
    let mut committer = FakeCommitter::default();
    let (_shutdown_tx, mut shutdown) = watch::channel(false);

    let result = process_next_batch_observed(
        &mut receiver,
        &database,
        Arc::new(FakePublisher::default()),
        &mut committer,
        &mut shutdown,
        config(1, Duration::from_millis(10), 2),
        &metrics,
    )
    .await;
    assert!(matches!(
        result,
        Err(pulse_sink::pipeline::PipelineError::Fatal(_))
    ));

    let mut output = Vec::new();
    prometheus::Encoder::encode(
        &prometheus::TextEncoder::new(),
        &metrics.registry.gather(),
        &mut output,
    )
    .unwrap();
    let output = String::from_utf8(output).unwrap();
    assert!(output.contains("pulse_sink_records_consumed_total 1"));
    assert!(output.contains("pulse_sink_events_persisted_total 0"));
    assert!(output.contains("pulse_sink_batch_size_count 1"));
}

#[tokio::test]
async fn no_commit_occurs_while_database_is_gated() {
    let mut receiver = FakeReceiver::new(vec![source(1, true, 0)]);
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let database = FakeDatabase::default();
    *database.entered.lock().unwrap() = Some(entered_tx);
    *database.gate.lock().unwrap() = Some(release_rx);
    let publisher = Arc::new(FakePublisher::default());
    let mut committer = FakeCommitter::default();
    let (_shutdown_tx, mut shutdown) = watch::channel(false);
    let task = tokio::spawn(async move {
        let result = run(
            &mut receiver,
            &database,
            publisher,
            &mut committer,
            &mut shutdown,
            config(10, Duration::from_millis(10), 1),
        )
        .await;
        (result, committer.commits.into_inner().unwrap())
    });
    entered_rx.await.unwrap();
    assert!(!task.is_finished());
    release_tx.send(()).unwrap();
    assert_eq!(task.await.unwrap().1.len(), 1);
}

#[tokio::test]
async fn failed_dlq_delivery_drains_all_and_commits_none() {
    let mut receiver = FakeReceiver::new(vec![source(1, false, 0), source(2, false, 0)]);
    let database = FakeDatabase::default();
    let publisher = Arc::new(FakePublisher::default());
    publisher.failures_remaining.store(1, Ordering::SeqCst);
    let mut committer = FakeCommitter::default();
    let (_shutdown_tx, mut shutdown) = watch::channel(false);

    let result = run(
        &mut receiver,
        &database,
        publisher.clone(),
        &mut committer,
        &mut shutdown,
        config(10, Duration::from_millis(10), 1),
    )
    .await;

    assert!(matches!(
        result,
        Err(pulse_sink::pipeline::PipelineError::Retryable(_))
    ));
    assert_eq!(publisher.published.lock().unwrap().len(), 2);
    assert!(committer.commits.lock().unwrap().is_empty());
}

#[tokio::test]
async fn commit_failure_is_returned_and_leaves_offsets_replayable() {
    let mut receiver = FakeReceiver::new(vec![source(1, true, 0)]);
    let database = FakeDatabase::default();
    let mut committer = FakeCommitter::default();
    *committer.failure.lock().unwrap() = true;
    let (_shutdown_tx, mut shutdown) = watch::channel(false);

    let result = run(
        &mut receiver,
        &database,
        Arc::new(FakePublisher::default()),
        &mut committer,
        &mut shutdown,
        config(10, Duration::from_millis(10), 1),
    )
    .await;
    assert!(matches!(
        result,
        Err(pulse_sink::pipeline::PipelineError::Retryable(_))
    ));
    assert_eq!(committer.commits.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn cancellation_during_receive_returns_shutdown_without_commit() {
    let mut receiver = FakeReceiver::new(vec![]);
    let database = FakeDatabase::default();
    let mut committer = FakeCommitter::default();
    let (shutdown_tx, mut shutdown) = watch::channel(false);
    let signal = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(5)).await;
        shutdown_tx.send(true).unwrap();
    });

    let result = run(
        &mut receiver,
        &database,
        Arc::new(FakePublisher::default()),
        &mut committer,
        &mut shutdown,
        config(10, Duration::from_millis(10), 1),
    )
    .await;
    signal.await.unwrap();
    assert!(!result.unwrap());
    assert!(committer.commits.lock().unwrap().is_empty());
}

#[tokio::test]
async fn source_metadata_errors_are_fatal_and_never_routed_to_dlq() {
    let mut receiver = InvalidMetadataReceiver;
    let database = FakeDatabase::default();
    let publisher = Arc::new(FakePublisher::default());
    let mut committer = FakeCommitter::default();
    let (_shutdown_tx, mut shutdown) = watch::channel(false);

    let result = process_next_batch(
        &mut receiver,
        &database,
        publisher.clone(),
        &mut committer,
        &mut shutdown,
        config(10, Duration::from_millis(10), 1),
    )
    .await;

    assert!(matches!(
        result,
        Err(pulse_sink::pipeline::PipelineError::Fatal(_))
    ));
    assert!(publisher.published.lock().unwrap().is_empty());
    assert!(committer.commits.lock().unwrap().is_empty());
}

#[tokio::test]
async fn generation_change_after_database_barrier_suppresses_stale_commit() {
    let mut receiver = FakeReceiver::new(vec![source(1, true, 0)]);
    let receiver_for_change = receiver.clone();
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let database = FakeDatabase::default();
    *database.entered.lock().unwrap() = Some(entered_tx);
    *database.gate.lock().unwrap() = Some(release_rx);
    let mut committer = FakeCommitter::default();
    let (_shutdown_tx, mut shutdown) = watch::channel(false);
    let task = tokio::spawn(async move {
        let result = run(
            &mut receiver,
            &database,
            Arc::new(FakePublisher::default()),
            &mut committer,
            &mut shutdown,
            config(10, Duration::from_millis(10), 1),
        )
        .await
        .unwrap();
        (result, committer.commits.into_inner().unwrap())
    });
    entered_rx.await.unwrap();
    receiver_for_change.bump_generation();
    release_tx.send(()).unwrap();
    let (result, commits) = task.await.unwrap();
    assert!(result);
    assert!(commits.is_empty());
}

#[tokio::test]
async fn generation_change_at_committer_boundary_suppresses_stale_commit() {
    let mut receiver = FakeReceiver::new(vec![source(9, true, 0)]);
    let database = FakeDatabase::default();
    let mut committer = FakeCommitter::default();
    *committer.late_assignment_change.lock().unwrap() = true;
    let (_shutdown_tx, mut shutdown) = watch::channel(false);

    let result = run(
        &mut receiver,
        &database,
        Arc::new(FakePublisher::default()),
        &mut committer,
        &mut shutdown,
        config(1, Duration::from_millis(10), 1),
    )
    .await
    .unwrap();

    assert!(result);
    assert!(committer.commits.lock().unwrap().is_empty());
}

#[tokio::test]
async fn retry_exhaustion_returns_retryable_error_without_commit() {
    let mut receiver = FakeReceiver::new(vec![source(1, true, 0)]);
    let database = FakeDatabase::default();
    for _ in 0..2 {
        database
            .failures
            .lock()
            .unwrap()
            .push_back(DbFailure::Retryable(sqlx::Error::Io(
                std::io::Error::other("temporary"),
            )));
    }
    let mut committer = FakeCommitter::default();
    let (_shutdown_tx, mut shutdown) = watch::channel(false);
    let result = run(
        &mut receiver,
        &database,
        Arc::new(FakePublisher::default()),
        &mut committer,
        &mut shutdown,
        config(10, Duration::from_millis(10), 2),
    )
    .await;
    assert!(matches!(
        result,
        Err(pulse_sink::pipeline::PipelineError::Retryable(_))
    ));
    assert!(committer.commits.lock().unwrap().is_empty());
}
