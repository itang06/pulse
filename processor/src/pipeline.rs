//! Bounded acknowledged processing with an all-or-nothing source offset barrier.

use std::{
    collections::BTreeMap,
    future::Future,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, bail, Context, Result};
use prost_types::Timestamp;
use rdkafka::TopicPartitionList;
use tokio::task::JoinSet;

use crate::{
    metrics::Metrics,
    state::DetectorStateStore,
    transform::{self, RawRecord, TransformOutput},
};

pub const TOPIC_ENRICHED: &str = "telemetry.enriched";
pub const TOPIC_DLQ: &str = "telemetry.dlq";

#[derive(Clone, Copy, Debug)]
pub struct BatchConfig {
    pub max_records: usize,
    pub max_wait: Duration,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_records: 500,
            max_wait: Duration::from_millis(10),
        }
    }
}

impl BatchConfig {
    pub fn validate(self) -> Result<Self> {
        if self.max_records == 0 {
            bail!("batch maximum record count must be greater than zero");
        }
        if self.max_wait.is_zero() {
            bail!("batch maximum wait must be greater than zero");
        }
        Ok(self)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnedRecord {
    pub payload: Vec<u8>,
    pub key: Vec<u8>,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishRecord {
    pub topic: String,
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

pub trait RecordReceiver {
    fn receive(&mut self) -> impl Future<Output = Result<Option<OwnedRecord>>> + Send;
    fn assignment_generation(&self) -> u64 {
        0
    }
}
pub trait AcknowledgedPublisher: Sync {
    fn publish(&self, record: PublishRecord) -> impl Future<Output = Result<()>> + Send;
}
pub trait OffsetCommitter {
    fn commit(
        &mut self,
        offsets: BTreeMap<TopicPartition, i64>,
    ) -> impl Future<Output = Result<()>> + Send;
}
pub trait Clock {
    fn now(&mut self) -> (Instant, Timestamp);
}

pub struct SystemClock;
impl Clock for SystemClock {
    fn now(&mut self) -> (Instant, Timestamp) {
        let mono = Instant::now();
        let wall = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        (
            mono,
            Timestamp {
                seconds: wall.as_secs().min(i64::MAX as u64) as i64,
                nanos: wall.subsec_nanos() as i32,
            },
        )
    }
}

/// Mutable processing dependencies and configuration shared across batches.
pub struct BatchContext<'a, C, T> {
    pub committer: &'a mut C,
    pub store: &'a mut DetectorStateStore,
    pub metrics: &'a Metrics,
    pub config: BatchConfig,
    pub clock: &'a mut T,
    pub last_seen_generation: &'a mut u64,
}

/// Processes one bounded batch. `false` means the receiver reached EOF.
///
/// Every output delivery is awaited before committing. A partial delivery can
/// be replayed after another publish or commit fails, so outputs are at least
/// once. The assignment-generation guard rejects a batch if a rebalance occurs
/// while processing, leaving its source offsets uncommitted for replay by the
/// new owner. Each assignment change advances the generation and clears every
/// in-memory detector baseline, including state for partitions still assigned.
/// Since detector state is in memory, a replayed record can update its EWMA a
/// second time as well; this preserves delivery safety but is not exactly-once
/// state processing.
pub async fn process_next_batch<R, P, C, T>(
    receiver: &mut R,
    publisher: Arc<P>,
    context: BatchContext<'_, C, T>,
) -> Result<bool>
where
    R: RecordReceiver,
    P: AcknowledgedPublisher + Send + Sync + 'static,
    C: OffsetCommitter,
    T: Clock,
{
    let BatchContext {
        committer,
        store,
        metrics,
        config,
        clock,
        last_seen_generation,
    } = context;
    let config = config.validate()?;
    let Some(first) = receiver.receive().await? else {
        return Ok(false);
    };
    let batch_generation = receiver.assignment_generation();
    if batch_generation != *last_seen_generation {
        store.clear();
        metrics.active_state_keys.set(0);
        *last_seen_generation = batch_generation;
    }
    let deadline = Instant::now() + config.max_wait;
    let mut batch = vec![first];
    while batch.len() < config.max_records {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, receiver.receive()).await {
            Ok(Ok(Some(record))) => batch.push(record),
            Ok(Ok(None)) => break,
            Ok(Err(error)) => return Err(error).context("receiving Kafka batch record"),
            Err(_) => break,
        }
    }

    let mut transformed = Vec::with_capacity(batch.len());
    let mut offsets = BTreeMap::new();
    for source in batch {
        let started = Instant::now();
        let (now, wall_time) = clock.now();
        let output = transform::transform(
            RawRecord {
                payload: source.payload,
                key: source.key,
                topic: source.topic.clone(),
                partition: source.partition,
                offset: source.offset,
                now,
                wall_time,
            },
            store,
        );
        metrics.record_transform(&output, started.elapsed().as_secs_f64());
        if let TransformOutput::RetryableError { reason } = &output {
            bail!(
                "retryable transform failure at {}[{}]@{}: {reason}",
                source.topic,
                source.partition,
                source.offset
            );
        }
        let tp = TopicPartition {
            topic: source.topic,
            partition: source.partition,
        };
        let next_offset = source
            .offset
            .checked_add(1)
            .context("source offset overflow")?;
        offsets
            .entry(tp)
            .and_modify(|highest: &mut i64| *highest = (*highest).max(next_offset))
            .or_insert(next_offset);
        transformed.push(output);
    }

    let mut deliveries = JoinSet::new();
    for output in transformed {
        let publish_record = match &output {
            TransformOutput::Enriched { payload, key, .. } => PublishRecord {
                topic: TOPIC_ENRICHED.into(),
                key: key.clone(),
                payload: payload.clone(),
            },
            TransformOutput::DeadLetter { payload } => PublishRecord {
                topic: TOPIC_DLQ.into(),
                key: Vec::new(),
                payload: payload.clone(),
            },
            TransformOutput::RetryableError { .. } => {
                unreachable!("retryable transforms returned above")
            }
        };
        let publisher = Arc::clone(&publisher);
        deliveries.spawn(async move {
            let result = publisher.publish(publish_record).await;
            (output, result)
        });
    }
    let mut failed = false;
    while let Some(joined) = deliveries.join_next().await {
        let (output, result) = match joined {
            Ok(delivery) => delivery,
            Err(error) => {
                failed = true;
                tracing::error!(%error, "output delivery task failed");
                continue;
            }
        };
        match result {
            Ok(()) => metrics.record_delivery_success(&output),
            Err(error) => {
                failed = true;
                tracing::error!(%error, "output delivery failed; source batch will replay");
            }
        }
    }
    if failed {
        bail!("one or more output deliveries failed; source offsets were not committed");
    }
    if receiver.assignment_generation() != batch_generation {
        bail!("Kafka assignment changed while processing; source offsets were not committed");
    }
    committer
        .commit(offsets)
        .await
        .context("committing acknowledged source batch")?;
    Ok(true)
}

/// Commit adapter helper. Kafka interprets each offset as the next record to read.
pub fn topic_partition_list(offsets: &BTreeMap<TopicPartition, i64>) -> Result<TopicPartitionList> {
    use rdkafka::Offset;
    let mut list = TopicPartitionList::new();
    for (tp, offset) in offsets {
        list.add_partition_offset(&tp.topic, tp.partition, Offset::Offset(*offset))
            .map_err(|error| anyhow!("invalid commit offset: {error}"))?;
    }
    Ok(list)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{detector::DetectorConfig, state::StateStoreConfig};
    use prost::Message;
    use pulse_proto::pulse::v1::TelemetryEvent;
    use std::{
        collections::VecDeque,
        sync::{
            atomic::{AtomicU64, AtomicUsize, Ordering},
            Mutex,
        },
        time::Duration,
    };

    struct Receiver {
        records: VecDeque<OwnedRecord>,
        delay: Duration,
        generation: Arc<AtomicU64>,
        last_seen_generation: u64,
    }
    impl RecordReceiver for Receiver {
        fn assignment_generation(&self) -> u64 {
            self.generation.load(Ordering::SeqCst)
        }

        async fn receive(&mut self) -> Result<Option<OwnedRecord>> {
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            Ok(self.records.pop_front())
        }
    }
    struct Publisher {
        fail: Option<usize>,
        delay: Duration,
        calls: AtomicUsize,
        order: Mutex<Vec<usize>>,
        generation_on_publish: Option<Arc<AtomicU64>>,
        gate: Option<(usize, Arc<DeliveryGate>)>,
    }
    struct DeliveryGate {
        entered: tokio::sync::Notify,
        release: tokio::sync::Semaphore,
    }
    impl AcknowledgedPublisher for Publisher {
        async fn publish(&self, _record: PublishRecord) -> Result<()> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some(generation) = &self.generation_on_publish {
                generation.fetch_add(1, Ordering::SeqCst);
            }
            if let Some((blocked_call, gate)) = &self.gate {
                if call == *blocked_call {
                    gate.entered.notify_one();
                    gate.release.acquire().await.unwrap().forget();
                }
            }
            tokio::time::sleep(
                self.delay
                    .saturating_mul((3usize.saturating_sub(call)) as u32),
            )
            .await;
            self.order.lock().unwrap().push(call);
            if self.fail == Some(call) {
                bail!("delivery failed");
            }
            Ok(())
        }
    }
    #[derive(Default)]
    struct Committer {
        commits: Vec<BTreeMap<TopicPartition, i64>>,
        fail: bool,
    }
    impl OffsetCommitter for Committer {
        async fn commit(&mut self, offsets: BTreeMap<TopicPartition, i64>) -> Result<()> {
            if self.fail {
                bail!("commit failed");
            }
            self.commits.push(offsets);
            Ok(())
        }
    }
    struct CountingCommitter(Arc<AtomicUsize>);
    impl OffsetCommitter for CountingCommitter {
        async fn commit(&mut self, _: BTreeMap<TopicPartition, i64>) -> Result<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    struct TestClock {
        count: usize,
        backwards_at: Option<usize>,
        origin: Instant,
    }
    impl Clock for TestClock {
        fn now(&mut self) -> (Instant, Timestamp) {
            self.count += 1;
            let seconds = if self.backwards_at == Some(self.count) {
                -1
            } else {
                self.count as i64
            };
            let mono = if self.backwards_at == Some(self.count) {
                self.origin
            } else {
                self.origin + Duration::from_secs(self.count as u64)
            };
            (mono, Timestamp { seconds, nanos: 0 })
        }
    }
    fn store() -> DetectorStateStore {
        store_with_warmup(1)
    }
    fn store_with_warmup(warmup_observations: usize) -> DetectorStateStore {
        DetectorStateStore::new(StateStoreConfig {
            detector: DetectorConfig {
                warmup_observations,
                ..Default::default()
            },
            ..Default::default()
        })
        .unwrap()
    }
    fn record(offset: i64, partition: i32, valid: bool) -> OwnedRecord {
        let payload = if valid {
            TelemetryEvent {
                event_id: format!("550e8400-e29b-41d4-a716-{offset:012x}"),
                event_time: Some(Timestamp {
                    seconds: 1,
                    nanos: 0,
                }),
                service_name: "svc".into(),
                route: format!("/r{partition}"),
                latency_us: offset as u64,
                ..Default::default()
            }
            .encode_to_vec()
        } else {
            vec![0xff]
        };
        OwnedRecord {
            payload,
            key: format!("svc\0/r{partition}").into_bytes(),
            topic: "telemetry.raw".into(),
            partition,
            offset,
        }
    }
    fn receiver(records: Vec<OwnedRecord>) -> Receiver {
        Receiver {
            records: records.into(),
            delay: Duration::ZERO,
            generation: Arc::new(AtomicU64::new(0)),
            last_seen_generation: 0,
        }
    }
    fn publisher() -> Arc<Publisher> {
        Arc::new(Publisher {
            fail: None,
            delay: Duration::from_millis(1),
            calls: AtomicUsize::new(0),
            order: Mutex::new(Vec::new()),
            generation_on_publish: None,
            gate: None,
        })
    }
    async fn run(
        r: &mut Receiver,
        p: Arc<Publisher>,
        c: &mut Committer,
        s: &mut DetectorStateStore,
        m: &Metrics,
        cfg: BatchConfig,
        clock: &mut TestClock,
    ) -> Result<bool> {
        let mut seen_generation = r.last_seen_generation;
        let result = process_next_batch(
            r,
            p,
            BatchContext {
                committer: c,
                store: s,
                metrics: m,
                config: cfg,
                clock,
                last_seen_generation: &mut seen_generation,
            },
        )
        .await;
        r.last_seen_generation = seen_generation;
        result
    }

    #[test]
    fn batch_config_rejects_zero_bounds() {
        assert!(BatchConfig {
            max_records: 0,
            max_wait: Duration::from_millis(1)
        }
        .validate()
        .is_err());
        assert!(BatchConfig {
            max_records: 1,
            max_wait: Duration::ZERO
        }
        .validate()
        .is_err());
    }

    #[tokio::test]
    async fn batches_by_max_count_and_commits_highest_next_offset_per_partition() {
        let mut r = receiver(vec![
            record(1, 0, true),
            record(4, 0, true),
            record(7, 1, true),
        ]);
        let p = publisher();
        let mut c = Committer::default();
        let mut s = store();
        let m = Metrics::new();
        let mut clock = TestClock {
            count: 0,
            backwards_at: None,
            origin: Instant::now(),
        };
        let cfg = BatchConfig {
            max_records: 2,
            max_wait: Duration::from_secs(1),
        };
        assert!(run(&mut r, p.clone(), &mut c, &mut s, &m, cfg, &mut clock)
            .await
            .unwrap());
        assert_eq!(
            c.commits[0].get(&TopicPartition {
                topic: "telemetry.raw".into(),
                partition: 0
            }),
            Some(&5)
        );
        assert_eq!(
            c.commits[0].get(&TopicPartition {
                topic: "telemetry.raw".into(),
                partition: 1
            }),
            None
        );
        assert!(run(&mut r, p, &mut c, &mut s, &m, cfg, &mut clock)
            .await
            .unwrap());
        assert_eq!(c.commits[1].values().copied().collect::<Vec<_>>(), vec![8]);
    }

    #[tokio::test]
    async fn assignment_generation_change_clears_all_warmed_state_before_transform() {
        let mut s = store_with_warmup(2);
        let base = Instant::now() - Duration::from_secs(10);
        s.observe_at("svc", "/r0", 10.0, base).unwrap();
        s.observe_at("svc", "/r0", 11.0, base + Duration::from_secs(1))
            .unwrap();
        assert_eq!(s.active_key_count(), 1);
        let mut r = receiver(vec![record(0, 0, true)]);
        r.generation.store(1, Ordering::SeqCst);
        let mut c = Committer::default();
        let m = Metrics::new();
        let mut clock = TestClock {
            count: 0,
            backwards_at: None,
            origin: Instant::now(),
        };
        run(
            &mut r,
            publisher(),
            &mut c,
            &mut s,
            &m,
            BatchConfig::default(),
            &mut clock,
        )
        .await
        .unwrap();
        assert_eq!(r.last_seen_generation, 1);
        assert_eq!(m.warmup_suppressions.get(), 1);
        assert_eq!(s.active_key_count(), 1);
    }

    #[tokio::test]
    async fn generation_change_during_batch_prevents_commit() {
        let mut r = receiver(vec![record(0, 0, true), record(1, 0, true)]);
        let generation = Arc::clone(&r.generation);
        let p = Arc::new(Publisher {
            fail: None,
            delay: Duration::from_millis(1),
            calls: AtomicUsize::new(0),
            order: Mutex::new(Vec::new()),
            generation_on_publish: Some(generation),
            gate: None,
        });
        let mut c = Committer::default();
        let mut s = store();
        let m = Metrics::new();
        let mut clock = TestClock {
            count: 0,
            backwards_at: None,
            origin: Instant::now(),
        };
        assert!(run(
            &mut r,
            p,
            &mut c,
            &mut s,
            &m,
            BatchConfig::default(),
            &mut clock
        )
        .await
        .is_err());
        assert!(c.commits.is_empty());
    }

    #[tokio::test]
    async fn commit_waits_until_the_gated_delivery_is_released() {
        let mut r = receiver(vec![record(0, 0, true), record(1, 0, true)]);
        let gate = Arc::new(DeliveryGate {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
        });
        let p = Arc::new(Publisher {
            fail: None,
            delay: Duration::ZERO,
            calls: AtomicUsize::new(0),
            order: Mutex::new(Vec::new()),
            generation_on_publish: None,
            gate: Some((0, Arc::clone(&gate))),
        });
        let commit_calls = Arc::new(AtomicUsize::new(0));
        let mut c = CountingCommitter(Arc::clone(&commit_calls));
        let mut s = store();
        let m = Metrics::new();
        let mut clock = TestClock {
            count: 0,
            backwards_at: None,
            origin: Instant::now(),
        };
        let mut seen = 0;
        let task = tokio::spawn(async move {
            process_next_batch(
                &mut r,
                p,
                BatchContext {
                    committer: &mut c,
                    store: &mut s,
                    metrics: &m,
                    config: BatchConfig::default(),
                    clock: &mut clock,
                    last_seen_generation: &mut seen,
                },
            )
            .await
        });
        gate.entered.notified().await;
        assert_eq!(commit_calls.load(Ordering::SeqCst), 0);
        gate.release.add_permits(1);
        task.await.unwrap().unwrap();
        assert_eq!(commit_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn max_wait_ends_partial_batch() {
        let mut r = Receiver {
            records: vec![record(0, 0, true), record(1, 0, true)].into(),
            delay: Duration::from_millis(30),
            generation: Arc::new(AtomicU64::new(0)),
            last_seen_generation: 0,
        };
        let mut c = Committer::default();
        let mut s = store();
        let m = Metrics::new();
        let mut clock = TestClock {
            count: 0,
            backwards_at: None,
            origin: Instant::now(),
        };
        let started = Instant::now();
        run(
            &mut r,
            publisher(),
            &mut c,
            &mut s,
            &m,
            BatchConfig {
                max_records: 10,
                max_wait: Duration::from_millis(5),
            },
            &mut clock,
        )
        .await
        .unwrap();
        assert!(started.elapsed() < Duration::from_millis(70));
        assert_eq!(c.commits[0].values().copied().collect::<Vec<_>>(), vec![1]);
    }

    #[tokio::test]
    async fn failed_delivery_waits_for_all_and_never_commits() {
        let mut r = receiver(vec![record(0, 0, true), record(1, 0, true)]);
        let mut c = Committer::default();
        let mut s = store();
        let m = Metrics::new();
        let mut clock = TestClock {
            count: 0,
            backwards_at: None,
            origin: Instant::now(),
        };
        let p = Arc::new(Publisher {
            fail: Some(0),
            delay: Duration::from_millis(3),
            calls: AtomicUsize::new(0),
            order: Mutex::new(Vec::new()),
            generation_on_publish: None,
            gate: None,
        });
        assert!(run(
            &mut r,
            p.clone(),
            &mut c,
            &mut s,
            &m,
            BatchConfig::default(),
            &mut clock
        )
        .await
        .is_err());
        assert_eq!(p.calls.load(Ordering::SeqCst), 2);
        assert!(c.commits.is_empty());
        assert_eq!(m.events_enriched.get(), 1);
    }

    #[tokio::test]
    async fn retryable_transform_aborts_batch_before_publish_or_commit() {
        let mut r = receiver(vec![record(0, 0, true), record(1, 0, true)]);
        let p = publisher();
        let mut c = Committer::default();
        let mut s = store();
        let m = Metrics::new();
        let mut clock = TestClock {
            count: 0,
            backwards_at: Some(2),
            origin: Instant::now(),
        };
        assert!(run(
            &mut r,
            p.clone(),
            &mut c,
            &mut s,
            &m,
            BatchConfig::default(),
            &mut clock
        )
        .await
        .is_err());
        assert_eq!(p.calls.load(Ordering::SeqCst), 0);
        assert!(c.commits.is_empty());
    }

    #[tokio::test]
    async fn dlq_delivery_is_inside_the_same_commit_barrier() {
        let mut r = receiver(vec![record(0, 0, true), record(1, 0, false)]);
        let mut c = Committer::default();
        let mut s = store();
        let m = Metrics::new();
        let mut clock = TestClock {
            count: 0,
            backwards_at: None,
            origin: Instant::now(),
        };
        run(
            &mut r,
            publisher(),
            &mut c,
            &mut s,
            &m,
            BatchConfig::default(),
            &mut clock,
        )
        .await
        .unwrap();
        assert_eq!(m.events_enriched.get(), 1);
        assert_eq!(m.dlq_events.get(), 1);
        assert_eq!(c.commits.len(), 1);
    }

    #[tokio::test]
    async fn empty_batch_and_commit_failure_are_reported_without_fake_success() {
        let mut r = receiver(Vec::new());
        let mut c = Committer::default();
        let mut s = store();
        let m = Metrics::new();
        let mut clock = TestClock {
            count: 0,
            backwards_at: None,
            origin: Instant::now(),
        };
        assert!(!run(
            &mut r,
            publisher(),
            &mut c,
            &mut s,
            &m,
            BatchConfig::default(),
            &mut clock
        )
        .await
        .unwrap());
        assert!(c.commits.is_empty());
        let mut r = receiver(vec![record(0, 0, true)]);
        c.fail = true;
        assert!(run(
            &mut r,
            publisher(),
            &mut c,
            &mut s,
            &m,
            BatchConfig::default(),
            &mut clock
        )
        .await
        .is_err());
        assert!(c.commits.is_empty());
    }

    #[tokio::test]
    async fn acknowledgements_can_finish_out_of_order_before_commit() {
        let mut r = receiver(vec![
            record(0, 0, true),
            record(1, 0, true),
            record(2, 0, true),
        ]);
        let mut c = Committer::default();
        let mut s = store();
        let m = Metrics::new();
        let mut clock = TestClock {
            count: 0,
            backwards_at: None,
            origin: Instant::now(),
        };
        let p = Arc::new(Publisher {
            fail: None,
            delay: Duration::from_millis(3),
            calls: AtomicUsize::new(0),
            order: Mutex::new(Vec::new()),
            generation_on_publish: None,
            gate: None,
        });
        run(
            &mut r,
            p.clone(),
            &mut c,
            &mut s,
            &m,
            BatchConfig {
                max_records: 3,
                max_wait: Duration::from_secs(1),
            },
            &mut clock,
        )
        .await
        .unwrap();
        assert_eq!(m.events_enriched.get(), 3);
        assert_eq!(*p.order.lock().unwrap(), vec![2, 1, 0]);
        assert_eq!(c.commits[0].values().copied().next(), Some(3));
    }

    #[tokio::test]
    async fn cancellation_during_delivery_never_reaches_the_commit_barrier() {
        let mut r = receiver(vec![record(0, 0, true), record(1, 0, true)]);
        let p = Arc::new(Publisher {
            fail: None,
            delay: Duration::from_millis(100),
            calls: AtomicUsize::new(0),
            order: Mutex::new(Vec::new()),
            generation_on_publish: None,
            gate: None,
        });
        let monitor = Arc::clone(&p);
        let commit_calls = Arc::new(AtomicUsize::new(0));
        let mut c = CountingCommitter(commit_calls.clone());
        let mut s = store();
        let m = Metrics::new();
        let mut clock = TestClock {
            count: 0,
            backwards_at: None,
            origin: Instant::now(),
        };
        let task = tokio::spawn(async move {
            let mut seen_generation = r.last_seen_generation;
            process_next_batch(
                &mut r,
                p,
                BatchContext {
                    committer: &mut c,
                    store: &mut s,
                    metrics: &m,
                    config: BatchConfig {
                        max_records: 2,
                        max_wait: Duration::from_secs(1),
                    },
                    clock: &mut clock,
                    last_seen_generation: &mut seen_generation,
                },
            )
            .await
        });
        for _ in 0..500 {
            if task.is_finished() || monitor.calls.load(Ordering::SeqCst) == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        task.abort();
        let _ = task.await;
        assert_eq!(monitor.calls.load(Ordering::SeqCst), 2);
        assert_eq!(commit_calls.load(Ordering::SeqCst), 0);
    }
}
