//! Pulse client SDK.
//!
//! Services import this crate to emit structured telemetry to the Pulse
//! gateway. Design goals:
//!   - `emit` must be cheap and non-blocking: instrumented request paths never
//!     wait on the network
//!   - events batch locally and use acknowledged unary gRPC requests over a
//!     persistent connection
//!   - the SDK degrades gracefully: if the gateway is down, telemetry is
//!     dropped (bounded buffer), never the caller's request
//!
//! Accepted events are exported by one background worker. A flush barrier can
//! be used during controlled shutdowns and verification to wait for a valid
//! batch acknowledgement; request instrumentation should call only `emit`.

mod config;
mod metrics;
mod worker;

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::SystemTime,
};

use tokio::sync::{mpsc, oneshot, Notify};
use tonic::transport::Endpoint;
use uuid::Uuid;

pub use config::ClientConfig;
pub use metrics::MetricsSnapshot;

use metrics::Metrics;
use worker::{Command, CommandReceiver, WorkerFailure};

/// One completed request measurement supplied by an instrumented service.
///
/// The SDK adds transport-owned fields such as the event ID when the event is
/// accepted into its local queue.
#[derive(Debug, Clone)]
pub struct Telemetry {
    pub event_time: SystemTime,
    pub service_name: String,
    pub route: String,
    pub latency_us: u64,
    pub status_code: u32,
    pub trace_id: Option<String>,
    pub attributes: HashMap<String, String>,
}

impl Telemetry {
    fn checked_timestamp(&self) -> Option<prost_types::Timestamp> {
        const MIN_SECONDS: u64 = 62_135_596_800;
        const MAX_SECONDS: u64 = 253_402_300_799;

        match self.event_time.duration_since(std::time::UNIX_EPOCH) {
            Ok(duration) => {
                if duration.as_secs() > MAX_SECONDS {
                    return None;
                }
                Some(prost_types::Timestamp {
                    seconds: duration.as_secs() as i64,
                    nanos: duration.subsec_nanos() as i32,
                })
            }
            Err(error) => {
                let duration = error.duration();
                if duration.as_secs() > MIN_SECONDS
                    || (duration.as_secs() == MIN_SECONDS && duration.subsec_nanos() > 0)
                {
                    return None;
                }

                if duration.subsec_nanos() == 0 {
                    Some(prost_types::Timestamp {
                        seconds: -(duration.as_secs() as i64),
                        nanos: 0,
                    })
                } else {
                    Some(prost_types::Timestamp {
                        seconds: -(duration.as_secs() as i64) - 1,
                        nanos: 1_000_000_000 - duration.subsec_nanos() as i32,
                    })
                }
            }
        }
    }

    fn into_proto(self, event_time: prost_types::Timestamp) -> (Uuid, pulse_proto::TelemetryEvent) {
        let event_id = Uuid::new_v4();
        let wire_event = pulse_proto::TelemetryEvent {
            event_id: event_id.to_string(),
            event_time: Some(event_time),
            service_name: self.service_name,
            route: self.route,
            latency_us: self.latency_us,
            status_code: self.status_code,
            trace_id: self.trace_id.unwrap_or_default(),
            attributes: self.attributes,
        };

        (event_id, wire_event)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid gateway endpoint: {0}")]
    InvalidEndpoint(#[from] tonic::transport::Error),
    #[error("queue capacity must be greater than zero")]
    InvalidQueueCapacity,
    #[error("max batch size must be between 1 and 500 events, got {0}")]
    InvalidMaxBatchSize(usize),
    #[error("flush interval must be greater than zero")]
    InvalidFlushInterval,
    #[error("rpc timeout must be greater than zero")]
    InvalidRpcTimeout,
    #[error("initial retry delay must be greater than zero")]
    InvalidRetryInitial,
    #[error("maximum retry delay must be greater than or equal to the initial retry delay")]
    InvalidRetryRange,
    #[error("PulseClient::connect requires an active Tokio runtime")]
    NoTokioRuntime,
    #[error("the telemetry export worker is closed")]
    WorkerClosed,
    #[error("telemetry export failed permanently with {code}: {message}")]
    PermanentExport { code: String, message: String },
}

impl From<WorkerFailure> for Error {
    fn from(value: WorkerFailure) -> Self {
        match value {
            WorkerFailure::PermanentExport { code, message } => {
                Self::PermanentExport { code, message }
            }
        }
    }
}

/// Outcome of one nonblocking enqueue attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmitResult {
    Queued { event_id: Uuid },
    InvalidEvent,
    DroppedFull,
    WorkerClosed,
}

/// Handle for emitting telemetry. Cheap to clone; all clones share one queue
/// and one set of local counters.
pub struct PulseClient {
    sender: mpsc::Sender<Command>,
    metrics: Arc<Metrics>,
    lifecycle: Arc<ClientLifecycle>,
}

struct ClientLifecycle {
    client_count: AtomicUsize,
    last_client_dropped: Notify,
}

impl ClientLifecycle {
    fn new() -> Self {
        Self {
            client_count: AtomicUsize::new(1),
            last_client_dropped: Notify::new(),
        }
    }

    async fn cancelled(&self) {
        if self.client_count.load(Ordering::Acquire) == 0 {
            return;
        }
        self.last_client_dropped.notified().await;
    }
}

impl Clone for PulseClient {
    fn clone(&self) -> Self {
        self.lifecycle.client_count.fetch_add(1, Ordering::Relaxed);
        Self {
            sender: self.sender.clone(),
            metrics: Arc::clone(&self.metrics),
            lifecycle: Arc::clone(&self.lifecycle),
        }
    }
}

impl Drop for PulseClient {
    fn drop(&mut self) {
        if self.lifecycle.client_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.lifecycle.last_client_dropped.notify_one();
        }
    }
}

impl PulseClient {
    /// Validates the local settings and constructs the bounded queue.
    ///
    /// Construction performs no network I/O, so gateway availability never
    /// blocks service startup.
    pub fn connect(config: ClientConfig) -> Result<Self, Error> {
        config.validate()?;
        let endpoint = Endpoint::from_shared(config.endpoint.clone())?;
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| Error::NoTokioRuntime)?;
        let channel = endpoint.connect_lazy();
        let grpc_client =
            pulse_proto::telemetry_service_client::TelemetryServiceClient::new(channel);
        let (sender, receiver) = mpsc::channel(config.queue_capacity);
        let metrics = Arc::new(Metrics::default());
        let lifecycle = Arc::new(ClientLifecycle::new());
        let command_receiver = CommandReceiver::new(receiver, Arc::clone(&metrics));
        let worker_metrics = Arc::clone(&metrics);
        let worker_lifecycle = Arc::clone(&lifecycle);
        runtime.spawn(async move {
            tokio::select! {
                () = worker_lifecycle.cancelled() => {}
                () = worker::run(command_receiver, worker_metrics, config, grpc_client) => {}
            }
        });

        Ok(Self {
            sender,
            metrics,
            lifecycle,
        })
    }

    /// Queues one telemetry event for delivery.
    ///
    /// This method performs no network I/O and never waits for queue capacity.
    /// It drops the new event when the bounded queue is full. The returned UUID
    /// is the pipeline idempotency key and exists only for accepted events.
    pub fn emit(&self, event: Telemetry) -> EmitResult {
        self.metrics.attempted.fetch_add(1, Ordering::Relaxed);

        let Some(event_time) = event.checked_timestamp() else {
            self.metrics.invalid_events.fetch_add(1, Ordering::Relaxed);
            return EmitResult::InvalidEvent;
        };

        let permit = match self.sender.clone().try_reserve_owned() {
            Ok(permit) => permit,
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.metrics.dropped_full.fetch_add(1, Ordering::Relaxed);
                return EmitResult::DroppedFull;
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.metrics.dropped_closed.fetch_add(1, Ordering::Relaxed);
                return EmitResult::WorkerClosed;
            }
        };

        let (event_id, wire_event) = event.into_proto(event_time);
        self.metrics.queued.fetch_add(1, Ordering::Relaxed);
        self.metrics.queue_depth.fetch_add(1, Ordering::Relaxed);
        permit.send(Command::Event(wire_event));

        EmitResult::Queued { event_id }
    }

    /// Returns a point-in-time view of counters shared by every client clone.
    pub fn metrics(&self) -> MetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Waits until every event queued before this call has completed export.
    ///
    /// The first permanent delivery failure is sticky: this and all later
    /// flushes return it even when newer batches succeed. That conservative
    /// contract prevents a clean shutdown or verification result from hiding an
    /// earlier accepted batch that was lost.
    pub async fn flush(&self) -> Result<(), Error> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::Flush(sender))
            .await
            .map_err(|_| Error::WorkerClosed)?;
        receiver
            .await
            .map_err(|_| Error::WorkerClosed)?
            .map_err(Error::from)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{atomic::Ordering, Arc},
        time::Duration,
    };

    use tokio::sync::mpsc;
    use uuid::{Uuid, Version};

    use super::{
        metrics::Metrics,
        worker::{Command, CommandReceiver},
        ClientLifecycle, PulseClient, Telemetry,
    };

    #[test]
    fn event_conversion_generates_uuid_and_preserves_fields() {
        let event_time = std::time::UNIX_EPOCH + Duration::new(1_700_000_000, 123_456_789);
        let attributes = HashMap::from([
            ("region".to_owned(), "us-east".to_owned()),
            ("method".to_owned(), "GET".to_owned()),
        ]);
        let event = Telemetry {
            event_time,
            service_name: "checkout".to_owned(),
            route: "/orders/:id".to_owned(),
            latency_us: 42_000,
            status_code: 201,
            trace_id: Some("trace-123".to_owned()),
            attributes: attributes.clone(),
        };

        let event_time = event.checked_timestamp().expect("timestamp is valid");
        let (event_id, wire_event) = event.into_proto(event_time);

        assert_eq!(Uuid::parse_str(&wire_event.event_id), Ok(event_id));
        assert_eq!(event_id.get_version(), Some(Version::Random));
        assert_eq!(wire_event.event_time, Some(event_time));
        assert_eq!(wire_event.service_name, "checkout");
        assert_eq!(wire_event.route, "/orders/:id");
        assert_eq!(wire_event.latency_us, 42_000);
        assert_eq!(wire_event.status_code, 201);
        assert_eq!(wire_event.trace_id, "trace-123");
        assert_eq!(wire_event.attributes, attributes);
    }

    #[test]
    fn event_conversion_normalizes_one_nanosecond_before_epoch() {
        let event_time = std::time::UNIX_EPOCH
            .checked_sub(Duration::from_nanos(1))
            .expect("one nanosecond before the epoch is representable");
        let event = Telemetry {
            event_time,
            service_name: "checkout".to_owned(),
            route: "/orders/:id".to_owned(),
            latency_us: 1_000,
            status_code: 200,
            trace_id: None,
            attributes: HashMap::new(),
        };

        let event_time = event.checked_timestamp().expect("timestamp is valid");
        let (_, wire_event) = event.into_proto(event_time);

        assert_eq!(
            wire_event.event_time,
            Some(prost_types::Timestamp {
                seconds: -1,
                nanos: 999_999_999,
            })
        );
    }

    #[tokio::test]
    async fn receiving_event_decrements_queue_depth() {
        let metrics = Arc::new(Metrics::default());
        let (sender, receiver) = mpsc::channel(1);
        let mut receiver = CommandReceiver::new(receiver, Arc::clone(&metrics));
        let event = Telemetry {
            event_time: std::time::UNIX_EPOCH,
            service_name: "checkout".to_owned(),
            route: "/orders/:id".to_owned(),
            latency_us: 1_000,
            status_code: 200,
            trace_id: None,
            attributes: HashMap::new(),
        };
        let event_time = event.checked_timestamp().expect("valid event time");
        let (_, event) = event.into_proto(event_time);

        metrics.queue_depth.store(1, Ordering::Relaxed);
        sender
            .send(Command::Event(event))
            .await
            .expect("queue is open");
        assert_eq!(metrics.snapshot().queue_depth, 1);

        let received = receiver.recv().await;

        assert!(received.is_some());
        assert_eq!(metrics.snapshot().queue_depth, 0);
    }

    #[tokio::test]
    async fn emit_counts_event_dropped_after_worker_closes() {
        let metrics = Arc::new(Metrics::default());
        let (sender, receiver) = mpsc::channel(1);
        drop(receiver);
        let client = PulseClient {
            sender,
            metrics,
            lifecycle: Arc::new(ClientLifecycle::new()),
        };
        let event = Telemetry {
            event_time: std::time::UNIX_EPOCH,
            service_name: "checkout".to_owned(),
            route: "/orders/:id".to_owned(),
            latency_us: 1_000,
            status_code: 200,
            trace_id: None,
            attributes: HashMap::new(),
        };

        let result = client.emit(event);

        assert_eq!(result, super::EmitResult::WorkerClosed);
        let metrics = client.metrics();
        assert_eq!(metrics.dropped_closed, 1);
        assert_eq!(metrics.queue_depth, 0);
        assert_eq!(
            metrics.attempted,
            metrics.queued + metrics.invalid_events + metrics.dropped_full + metrics.dropped_closed
        );
    }

    #[tokio::test]
    async fn last_drop_before_cancellation_wait_is_observed() {
        let lifecycle = Arc::new(ClientLifecycle::new());
        let client = PulseClient {
            sender: mpsc::channel(1).0,
            metrics: Arc::new(Metrics::default()),
            lifecycle: Arc::clone(&lifecycle),
        };

        drop(client);

        tokio::time::timeout(Duration::from_millis(100), lifecycle.cancelled())
            .await
            .expect("a late worker must observe the final client drop");
    }
}
