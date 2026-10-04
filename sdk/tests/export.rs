use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use pulse_proto::{
    telemetry_service_server::{TelemetryService, TelemetryServiceServer},
    ExportBatchRequest, ExportBatchResponse,
};
use pulse_sdk::{ClientConfig, EmitResult, Error, PulseClient, Telemetry};
use tokio::{
    net::TcpListener,
    sync::{oneshot, Mutex, Notify, Semaphore},
    task::JoinHandle,
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{transport::Server, Code, Request, Response, Status};

#[derive(Clone)]
enum Reply {
    Success,
    Status(Code),
    WrongBatchId,
    WrongAcceptedCount,
    Block(Arc<Semaphore>),
    BlockUntilCancelled(Arc<CancellationProbe>),
}

struct CancellationProbe {
    cancelled: StdMutex<Option<oneshot::Sender<()>>>,
}

impl CancellationProbe {
    fn new() -> (Arc<Self>, oneshot::Receiver<()>) {
        let (sender, receiver) = oneshot::channel();
        (
            Arc::new(Self {
                cancelled: StdMutex::new(Some(sender)),
            }),
            receiver,
        )
    }

    fn guard(self: &Arc<Self>) -> CancellationGuard {
        CancellationGuard(Arc::clone(self))
    }
}

struct CancellationGuard(Arc<CancellationProbe>);

impl Drop for CancellationGuard {
    fn drop(&mut self) {
        if let Some(sender) = self
            .0
            .cancelled
            .lock()
            .expect("cancellation probe lock is not poisoned")
            .take()
        {
            let _ = sender.send(());
        }
    }
}

struct ScriptState {
    replies: Mutex<VecDeque<Reply>>,
    requests: Mutex<Vec<ExportBatchRequest>>,
    request_received: Notify,
}

impl ScriptState {
    fn new(replies: impl IntoIterator<Item = Reply>) -> Arc<Self> {
        Arc::new(Self {
            replies: Mutex::new(replies.into_iter().collect()),
            requests: Mutex::new(Vec::new()),
            request_received: Notify::new(),
        })
    }

    async fn wait_for_requests(&self, expected: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let notified = self.request_received.notified();
                if self.requests.lock().await.len() >= expected {
                    return;
                }
                notified.await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {expected} export request(s)"));
    }

    async fn recorded_requests(&self) -> Vec<ExportBatchRequest> {
        self.requests.lock().await.clone()
    }
}

#[derive(Clone)]
struct ScriptedTelemetryService {
    state: Arc<ScriptState>,
}

#[tonic::async_trait]
impl TelemetryService for ScriptedTelemetryService {
    async fn export_batch(
        &self,
        request: Request<ExportBatchRequest>,
    ) -> Result<Response<ExportBatchResponse>, Status> {
        let request = request.into_inner();
        self.state.requests.lock().await.push(request.clone());
        self.state.request_received.notify_waiters();

        let reply = self
            .state
            .replies
            .lock()
            .await
            .pop_front()
            .unwrap_or(Reply::Success);
        match reply {
            Reply::Success => Ok(Response::new(success_response(&request))),
            Reply::Status(code) => Err(Status::new(code, "scripted export failure")),
            Reply::WrongBatchId => Ok(Response::new(ExportBatchResponse {
                batch_id: "wrong-batch-id".to_owned(),
                accepted_count: request.events.len() as u64,
            })),
            Reply::WrongAcceptedCount => Ok(Response::new(ExportBatchResponse {
                batch_id: request.batch_id,
                accepted_count: request.events.len().saturating_sub(1) as u64,
            })),
            Reply::Block(release) => {
                release
                    .acquire()
                    .await
                    .expect("test response semaphore remains open")
                    .forget();
                Ok(Response::new(success_response(&request)))
            }
            Reply::BlockUntilCancelled(probe) => {
                let _guard = probe.guard();
                std::future::pending::<()>().await;
                unreachable!("the scripted request only ends through cancellation")
            }
        }
    }
}

fn success_response(request: &ExportBatchRequest) -> ExportBatchResponse {
    ExportBatchResponse {
        batch_id: request.batch_id.clone(),
        accepted_count: request.events.len() as u64,
    }
}

struct TestServer {
    endpoint: String,
    state: Arc<ScriptState>,
    task: JoinHandle<()>,
}

impl TestServer {
    async fn start(replies: impl IntoIterator<Item = Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test gRPC server");
        let address = listener.local_addr().expect("test server address");
        let state = ScriptState::new(replies);
        let service = ScriptedTelemetryService {
            state: Arc::clone(&state),
        };
        let task = tokio::spawn(async move {
            Server::builder()
                .add_service(TelemetryServiceServer::new(service))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .expect("serve test gRPC server");
        });

        Self {
            endpoint: format!("http://{address}"),
            state,
            task,
        }
    }

    fn config(&self) -> ClientConfig {
        ClientConfig {
            endpoint: self.endpoint.clone(),
            queue_capacity: 16,
            max_batch_size: 500,
            flush_interval: Duration::from_millis(5),
            rpc_timeout: Duration::from_secs(1),
            retry_initial: Duration::from_millis(1),
            retry_max: Duration::from_millis(2),
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn telemetry(route: &str) -> Telemetry {
    Telemetry {
        event_time: std::time::UNIX_EPOCH + Duration::from_secs(1_700_000_000),
        service_name: "checkout".to_owned(),
        route: route.to_owned(),
        latency_us: 12_500,
        status_code: 200,
        trace_id: None,
        attributes: HashMap::new(),
    }
}

fn queued_id(result: EmitResult) -> String {
    match result {
        EmitResult::Queued { event_id } => event_id.to_string(),
        other => panic!("event should queue, got {other:?}"),
    }
}

#[tokio::test]
async fn full_batch_flushes_without_waiting_for_timer() {
    let server = TestServer::start([]).await;
    let mut config = server.config();
    config.max_batch_size = 2;
    config.flush_interval = Duration::from_secs(30);
    let client = PulseClient::connect(config).expect("valid client");

    client.emit(telemetry("/first"));
    client.emit(telemetry("/second"));

    server.state.wait_for_requests(1).await;
    let requests = server.state.recorded_requests().await;
    assert_eq!(requests[0].events.len(), 2);
}

#[tokio::test(start_paused = true)]
async fn partial_batch_flushes_when_interval_elapses() {
    let server = TestServer::start([]).await;
    let mut config = server.config();
    config.flush_interval = Duration::from_millis(25);
    let client = PulseClient::connect(config).expect("valid client");

    client.emit(telemetry("/partial"));
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(24)).await;
    tokio::task::yield_now().await;
    assert!(server.state.recorded_requests().await.is_empty());

    tokio::time::advance(Duration::from_millis(1)).await;
    for _ in 0..100 {
        if !server.state.recorded_requests().await.is_empty() {
            break;
        }
        tokio::task::yield_now().await;
    }

    let requests = server.state.recorded_requests().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].events.len(), 1);
}

#[tokio::test]
async fn retryable_status_retries_same_batch_id_and_event_ids() {
    for code in [
        Code::Unavailable,
        Code::ResourceExhausted,
        Code::DeadlineExceeded,
    ] {
        let server = TestServer::start([Reply::Status(code), Reply::Success]).await;
        let client = PulseClient::connect(server.config()).expect("valid client");
        let event_id = queued_id(client.emit(telemetry("/retry")));

        client
            .flush()
            .await
            .expect("retry should eventually succeed");

        let requests = server.state.recorded_requests().await;
        assert_eq!(requests.len(), 2, "status {code:?}");
        assert_eq!(
            requests[0].batch_id, requests[1].batch_id,
            "status {code:?}"
        );
        assert_eq!(requests[0].events, requests[1].events, "status {code:?}");
        assert_eq!(requests[0].events[0].event_id, event_id);
        assert_eq!(client.metrics().batch_retries, 1);
        assert_eq!(client.metrics().acknowledged, 1);
    }
}

#[tokio::test]
async fn rpc_timeout_retries_the_identity_preserving_batch() {
    let blocked_response = Arc::new(Semaphore::new(0));
    let server =
        TestServer::start([Reply::Block(Arc::clone(&blocked_response)), Reply::Success]).await;
    let mut config = server.config();
    config.rpc_timeout = Duration::from_millis(10);
    let client = PulseClient::connect(config).expect("valid client");
    client.emit(telemetry("/timeout"));

    client.flush().await.expect("timeout should be retried");

    let requests = server.state.recorded_requests().await;
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0], requests[1]);
    assert_eq!(client.metrics().batch_retries, 1);
    blocked_response.add_permits(1);
}

#[tokio::test]
async fn permanent_status_counts_failure_and_remains_visible_to_later_flushes() {
    let server = TestServer::start([Reply::Status(Code::InvalidArgument), Reply::Success]).await;
    let client = PulseClient::connect(server.config()).expect("valid client");
    client.emit(telemetry("/permanent"));

    let first_error = client
        .flush()
        .await
        .expect_err("batch should fail permanently");
    assert!(matches!(first_error, Error::PermanentExport { .. }));
    assert_eq!(client.metrics().permanent_failures, 1);
    assert_eq!(client.metrics().acknowledged, 0);

    client.emit(telemetry("/later-success"));
    let second_error = client
        .flush()
        .await
        .expect_err("sticky failure prevents false clean flush");
    assert_eq!(first_error.to_string(), second_error.to_string());
    assert_eq!(client.metrics().acknowledged, 1);
}

#[tokio::test]
async fn flush_waits_for_acknowledged_export_response() {
    let release = Arc::new(Semaphore::new(0));
    let server = TestServer::start([Reply::Block(Arc::clone(&release))]).await;
    let client = PulseClient::connect(server.config()).expect("valid client");
    client.emit(telemetry("/wait"));
    let flush_client = client.clone();
    let flush = tokio::spawn(async move { flush_client.flush().await });

    server.state.wait_for_requests(1).await;
    tokio::task::yield_now().await;
    assert!(!flush.is_finished());
    assert_eq!(client.metrics().acknowledged, 0);

    release.add_permits(1);
    flush
        .await
        .expect("flush task should complete")
        .expect("valid acknowledgement should succeed");
    assert_eq!(client.metrics().acknowledged, 1);
}

#[tokio::test]
async fn malformed_success_retries_the_identity_preserving_batch() {
    for reply in [Reply::WrongBatchId, Reply::WrongAcceptedCount] {
        let server = TestServer::start([reply, Reply::Success]).await;
        let client = PulseClient::connect(server.config()).expect("valid client");
        let event_id = queued_id(client.emit(telemetry("/malformed")));

        client
            .flush()
            .await
            .expect("malformed acknowledgement should retry");

        let requests = server.state.recorded_requests().await;
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].batch_id, requests[1].batch_id);
        assert_eq!(requests[0].events, requests[1].events);
        assert_eq!(requests[0].events[0].event_id, event_id);
        assert_eq!(client.metrics().acknowledged, 1);
        assert_eq!(client.metrics().batch_retries, 1);
        assert_eq!(client.metrics().permanent_failures, 0);
    }
}

#[tokio::test]
async fn worker_does_not_drain_the_queue_while_a_batch_is_in_flight() {
    let release = Arc::new(Semaphore::new(0));
    let server = TestServer::start([Reply::Block(Arc::clone(&release))]).await;
    let mut config = server.config();
    config.queue_capacity = 2;
    config.max_batch_size = 1;
    config.rpc_timeout = Duration::from_secs(30);
    let client = PulseClient::connect(config).expect("valid client");
    client.emit(telemetry("/in-flight"));
    server.state.wait_for_requests(1).await;

    assert!(matches!(
        client.emit(telemetry("/queued-one")),
        EmitResult::Queued { .. }
    ));
    assert!(matches!(
        client.emit(telemetry("/queued-two")),
        EmitResult::Queued { .. }
    ));
    assert_eq!(client.emit(telemetry("/dropped")), EmitResult::DroppedFull);
    assert_eq!(client.metrics().queue_depth, 2);

    release.add_permits(1);
    client.flush().await.expect("queued events should drain");
    assert_eq!(client.metrics().acknowledged, 3);
}

#[tokio::test]
async fn flushing_an_empty_queue_does_not_export_an_empty_batch() {
    let server = TestServer::start([]).await;
    let client = PulseClient::connect(server.config()).expect("valid client");

    client.flush().await.expect("empty flush should succeed");

    assert!(server.state.recorded_requests().await.is_empty());
}

#[tokio::test]
async fn dropping_the_last_client_cancels_an_in_flight_export() {
    let (probe, mut cancelled) = CancellationProbe::new();
    let server = TestServer::start([Reply::BlockUntilCancelled(probe)]).await;
    let mut config = server.config();
    config.max_batch_size = 1;
    config.rpc_timeout = Duration::from_secs(30);
    let client = PulseClient::connect(config).expect("valid client");
    let remaining_client = client.clone();
    client.emit(telemetry("/blocked"));
    server.state.wait_for_requests(1).await;

    drop(client);
    assert!(matches!(
        cancelled.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        remaining_client.emit(telemetry("/still-open")),
        EmitResult::Queued { .. }
    ));

    drop(remaining_client);
    tokio::time::timeout(Duration::from_secs(2), cancelled)
        .await
        .expect("last client drop should cancel the in-flight RPC")
        .expect("cancellation probe should send");
}

#[test]
fn connect_without_a_tokio_runtime_returns_an_error_instead_of_panicking() {
    let error = PulseClient::connect(ClientConfig::default())
        .err()
        .expect("connect outside a runtime should fail");

    assert!(matches!(error, Error::NoTokioRuntime));
}
