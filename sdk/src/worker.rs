use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use pulse_proto::{
    telemetry_service_client::TelemetryServiceClient, ExportBatchRequest, TelemetryEvent,
};
use tokio::{
    sync::{mpsc, oneshot},
    time::{sleep, sleep_until, Instant},
};
use tonic::{transport::Channel, Code, Request, Status};
use uuid::Uuid;

use crate::{config::ClientConfig, metrics::Metrics};

pub(crate) enum Command {
    Event(TelemetryEvent),
    Flush(oneshot::Sender<Result<(), WorkerFailure>>),
}

/// A durable worker failure copied to every later flush barrier.
///
/// Flush failures are deliberately sticky for the lifetime of a client. Once
/// any accepted batch is permanently lost, a later successful batch cannot
/// make a verification or shutdown flush report a false clean delivery history.
#[derive(Debug, Clone)]
pub(crate) enum WorkerFailure {
    PermanentExport { code: String, message: String },
}

pub(crate) struct CommandReceiver {
    inner: mpsc::Receiver<Command>,
    metrics: Arc<Metrics>,
}

impl CommandReceiver {
    pub(crate) fn new(inner: mpsc::Receiver<Command>, metrics: Arc<Metrics>) -> Self {
        Self { inner, metrics }
    }

    pub(crate) async fn recv(&mut self) -> Option<Command> {
        let command = self.inner.recv().await?;
        if matches!(&command, Command::Event(_)) {
            let previous = self.metrics.queue_depth.fetch_sub(1, Ordering::Relaxed);
            debug_assert!(previous > 0, "queue depth cannot underflow");
        }
        Some(command)
    }
}

pub(crate) trait JitterSource: Send + Sync {
    fn delay(&self, upper_bound: Duration) -> Duration;
}

impl<J: JitterSource + ?Sized> JitterSource for &J {
    fn delay(&self, upper_bound: Duration) -> Duration {
        (*self).delay(upper_bound)
    }
}

pub(crate) struct FullJitter;

impl JitterSource for FullJitter {
    fn delay(&self, upper_bound: Duration) -> Duration {
        let maximum_nanos = upper_bound.as_nanos().min(u128::from(u64::MAX)) as u64;
        Duration::from_nanos(rand::random_range(0..=maximum_nanos))
    }
}

struct RetryBackoff<J: JitterSource> {
    current_upper_bound: Duration,
    maximum: Duration,
    jitter: J,
}

impl<J: JitterSource> RetryBackoff<J> {
    fn new(initial: Duration, maximum: Duration, jitter: J) -> Self {
        Self {
            current_upper_bound: initial,
            maximum,
            jitter,
        }
    }

    fn next_delay(&mut self) -> Duration {
        let upper_bound = self.current_upper_bound;
        self.current_upper_bound = self.current_upper_bound.saturating_mul(2).min(self.maximum);
        self.jitter.delay(upper_bound)
    }
}

pub(crate) async fn run(
    receiver: CommandReceiver,
    metrics: Arc<Metrics>,
    config: ClientConfig,
    client: TelemetryServiceClient<Channel>,
) {
    run_with_jitter(receiver, metrics, config, client, FullJitter).await;
}

async fn run_with_jitter<J: JitterSource>(
    mut receiver: CommandReceiver,
    metrics: Arc<Metrics>,
    config: ClientConfig,
    mut client: TelemetryServiceClient<Channel>,
    jitter: J,
) {
    let mut sticky_failure: Option<WorkerFailure> = None;

    loop {
        match receiver.recv().await {
            Some(Command::Event(first_event)) => {
                let mut events = Vec::with_capacity(config.max_batch_size);
                events.push(first_event);
                let mut barrier = None;
                let mut sender_closed = false;
                let deadline = Instant::now() + config.flush_interval;
                let timer = sleep_until(deadline);
                tokio::pin!(timer);

                while events.len() < config.max_batch_size {
                    tokio::select! {
                        command = receiver.recv() => match command {
                            Some(Command::Event(event)) => events.push(event),
                            Some(Command::Flush(sender)) => {
                                barrier = Some(sender);
                                break;
                            }
                            None => {
                                sender_closed = true;
                                break;
                            }
                        },
                        () = &mut timer => break,
                    }
                }

                if let Err(failure) =
                    export_batch(&mut client, &events, &config, &metrics, &jitter).await
                {
                    metrics.permanent_failures.fetch_add(1, Ordering::Relaxed);
                    sticky_failure.get_or_insert(failure);
                }

                if let Some(sender) = barrier {
                    let result = sticky_failure.clone().map_or(Ok(()), Err);
                    let _ = sender.send(result);
                }

                if sender_closed {
                    return;
                }
            }
            Some(Command::Flush(sender)) => {
                let result = sticky_failure.clone().map_or(Ok(()), Err);
                let _ = sender.send(result);
            }
            None => return,
        }
    }
}

async fn export_batch<J: JitterSource>(
    client: &mut TelemetryServiceClient<Channel>,
    events: &[TelemetryEvent],
    config: &ClientConfig,
    metrics: &Metrics,
    jitter: &J,
) -> Result<(), WorkerFailure> {
    debug_assert!(!events.is_empty(), "the worker never exports empty batches");
    let batch_id = Uuid::new_v4().to_string();
    let expected_count = events.len() as u64;
    let request = ExportBatchRequest {
        batch_id: batch_id.clone(),
        events: events.to_vec(),
    };
    let mut backoff = RetryBackoff::new(config.retry_initial, config.retry_max, jitter);

    loop {
        match export_attempt(client, request.clone(), config.rpc_timeout).await {
            AttemptOutcome::Acknowledged => {
                metrics
                    .acknowledged
                    .fetch_add(expected_count, Ordering::Relaxed);
                return Ok(());
            }
            AttemptOutcome::Permanent(status) => {
                return Err(WorkerFailure::PermanentExport {
                    code: format!("{:?}", status.code()),
                    message: status.message().to_owned(),
                });
            }
            AttemptOutcome::Retryable | AttemptOutcome::Malformed => {
                metrics.batch_retries.fetch_add(1, Ordering::Relaxed);
                sleep(backoff.next_delay()).await;
            }
        }
    }
}

enum AttemptOutcome {
    Acknowledged,
    Retryable,
    Permanent(Status),
    Malformed,
}

async fn export_attempt(
    client: &mut TelemetryServiceClient<Channel>,
    request: ExportBatchRequest,
    rpc_timeout: Duration,
) -> AttemptOutcome {
    let expected_batch_id = request.batch_id.clone();
    let expected_count = request.events.len() as u64;
    let mut grpc_request = Request::new(request);
    grpc_request.set_timeout(rpc_timeout);

    let response = match tokio::time::timeout(rpc_timeout, client.export_batch(grpc_request)).await
    {
        Err(_) => return AttemptOutcome::Retryable,
        Ok(Err(status)) if is_retryable(&status) => return AttemptOutcome::Retryable,
        Ok(Err(status)) => return AttemptOutcome::Permanent(status),
        Ok(Ok(response)) => response.into_inner(),
    };

    if response.batch_id != expected_batch_id {
        return AttemptOutcome::Malformed;
    }
    if response.accepted_count != expected_count {
        return AttemptOutcome::Malformed;
    }

    AttemptOutcome::Acknowledged
}

fn is_retryable(status: &Status) -> bool {
    matches!(
        status.code(),
        Code::Aborted
            | Code::Cancelled
            | Code::DeadlineExceeded
            | Code::Internal
            | Code::ResourceExhausted
            | Code::Unavailable
            | Code::Unknown
    )
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    use super::{JitterSource, RetryBackoff};

    struct RecordingJitter {
        upper_bounds: Arc<Mutex<Vec<Duration>>>,
    }

    impl JitterSource for RecordingJitter {
        fn delay(&self, upper_bound: Duration) -> Duration {
            self.upper_bounds
                .lock()
                .expect("recording jitter lock is not poisoned")
                .push(upper_bound);
            Duration::ZERO
        }
    }

    #[test]
    fn retry_backoff_reaches_and_stays_at_caps_beyond_a_u32_shift_range() {
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let jitter = RecordingJitter {
            upper_bounds: Arc::clone(&recorded),
        };
        let cap = Duration::from_secs(5);
        let mut backoff = RetryBackoff::new(Duration::from_nanos(1), cap, jitter);

        for _ in 0..36 {
            assert_eq!(backoff.next_delay(), Duration::ZERO);
        }

        let upper_bounds = recorded
            .lock()
            .expect("recording jitter lock is not poisoned");
        assert_eq!(upper_bounds[0], Duration::from_nanos(1));
        assert_eq!(upper_bounds[1], Duration::from_nanos(2));
        assert_eq!(upper_bounds[2], Duration::from_nanos(4));
        assert_eq!(upper_bounds[33], cap);
        assert_eq!(upper_bounds[34], cap);
        assert_eq!(upper_bounds[35], cap);
    }
}
