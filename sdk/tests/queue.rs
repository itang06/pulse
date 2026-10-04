use std::{collections::HashMap, time::Duration};

use pulse_sdk::{ClientConfig, EmitResult, Error, PulseClient, Telemetry};

fn telemetry(route: &str) -> Telemetry {
    telemetry_at(
        route,
        std::time::UNIX_EPOCH + Duration::from_secs(1_700_000_000),
    )
}

fn telemetry_at(route: &str, event_time: std::time::SystemTime) -> Telemetry {
    Telemetry {
        event_time,
        service_name: "checkout".to_owned(),
        route: route.to_owned(),
        latency_us: 12_500,
        status_code: 200,
        trace_id: None,
        attributes: HashMap::new(),
    }
}

#[tokio::test]
async fn emit_rejects_out_of_range_timestamps_without_consuming_capacity() {
    let config = ClientConfig {
        queue_capacity: 1,
        ..ClientConfig::default()
    };
    let client = PulseClient::connect(config).expect("valid client config");
    let before_protobuf_range = std::time::UNIX_EPOCH
        .checked_sub(Duration::from_secs(62_135_596_801))
        .expect("test platform represents year zero");
    let after_protobuf_range = std::time::UNIX_EPOCH
        .checked_add(Duration::from_secs(253_402_300_800))
        .expect("test platform represents year 10000");
    let extreme_pre_epoch = std::time::UNIX_EPOCH
        .checked_sub(Duration::from_secs(i64::MAX as u64))
        .expect("test platform represents extreme pre-epoch time");

    for invalid_time in [
        before_protobuf_range,
        after_protobuf_range,
        extreme_pre_epoch,
    ] {
        assert_eq!(
            client.emit(telemetry_at("/invalid", invalid_time)),
            EmitResult::InvalidEvent
        );
    }
    let valid_pre_epoch = std::time::UNIX_EPOCH
        .checked_sub(Duration::from_nanos(1))
        .expect("one nanosecond before the epoch is representable");
    assert!(matches!(
        client.emit(telemetry_at("/valid", valid_pre_epoch)),
        EmitResult::Queued { .. }
    ));
    assert_eq!(client.metrics().attempted, 4);
    assert_eq!(client.metrics().invalid_events, 3);
    assert_eq!(client.metrics().queued, 1);
    assert_eq!(client.metrics().queue_depth, 1);
}

#[test]
fn defaults_match_the_architecture_contract() {
    let config = ClientConfig::default();

    assert_eq!(config.endpoint, "http://localhost:50051");
    assert_eq!(config.queue_capacity, 65_536);
    assert_eq!(config.max_batch_size, 500);
    assert_eq!(config.flush_interval, Duration::from_millis(5));
    assert_eq!(config.rpc_timeout, Duration::from_secs(2));
    assert_eq!(config.retry_initial, Duration::from_millis(50));
    assert_eq!(config.retry_max, Duration::from_secs(5));
}

#[tokio::test]
async fn emit_returns_immediately_and_queues_when_capacity_exists() {
    let config = ClientConfig {
        queue_capacity: 1,
        ..ClientConfig::default()
    };
    let client = PulseClient::connect(config).expect("valid client config");

    let result = client.emit(telemetry("/orders/:id"));

    assert!(matches!(result, EmitResult::Queued { .. }));
    assert_eq!(
        client.metrics(),
        pulse_sdk::MetricsSnapshot {
            attempted: 1,
            queued: 1,
            invalid_events: 0,
            dropped_full: 0,
            dropped_closed: 0,
            acknowledged: 0,
            batch_retries: 0,
            permanent_failures: 0,
            queue_depth: 1,
        }
    );
}

#[tokio::test]
async fn emit_drops_new_event_when_queue_is_full() {
    let config = ClientConfig {
        queue_capacity: 1,
        ..ClientConfig::default()
    };
    let client = PulseClient::connect(config).expect("valid client config");

    let first = client.emit(telemetry("/first"));
    let second = client.emit(telemetry("/second"));

    assert!(matches!(first, EmitResult::Queued { .. }));
    assert_eq!(second, EmitResult::DroppedFull);
    assert_eq!(client.metrics().attempted, 2);
    assert_eq!(client.metrics().queued, 1);
    assert_eq!(client.metrics().dropped_full, 1);
    assert_eq!(client.metrics().queue_depth, 1);
}

#[tokio::test]
async fn cloned_clients_share_queue_and_metrics() {
    let config = ClientConfig {
        queue_capacity: 2,
        ..ClientConfig::default()
    };
    let client = PulseClient::connect(config).expect("valid client config");
    let clone = client.clone();

    let first = client.emit(telemetry("/first"));
    let second = clone.emit(telemetry("/second"));
    let third = client.emit(telemetry("/third"));

    let EmitResult::Queued { event_id: first_id } = first else {
        panic!("first event should be queued");
    };
    let EmitResult::Queued {
        event_id: second_id,
    } = second
    else {
        panic!("second event should be queued");
    };
    assert_ne!(first_id, second_id);
    assert_eq!(third, EmitResult::DroppedFull);
    assert_eq!(
        client.metrics(),
        pulse_sdk::MetricsSnapshot {
            attempted: 3,
            queued: 2,
            invalid_events: 0,
            dropped_full: 1,
            dropped_closed: 0,
            acknowledged: 0,
            batch_retries: 0,
            permanent_failures: 0,
            queue_depth: 2,
        }
    );
    assert_eq!(clone.metrics(), client.metrics());
}

#[test]
fn connect_rejects_invalid_worker_configuration() {
    enum ExpectedError {
        QueueCapacity,
        BatchSize(usize),
        FlushInterval,
        RpcTimeout,
        RetryInitial,
        RetryRange,
    }

    type Case = (
        &'static str,
        fn(&mut ClientConfig),
        ExpectedError,
        &'static str,
    );
    let cases: [Case; 7] = [
        (
            "zero queue capacity",
            |config| config.queue_capacity = 0,
            ExpectedError::QueueCapacity,
            "queue capacity must be greater than zero",
        ),
        (
            "zero batch size",
            |config| config.max_batch_size = 0,
            ExpectedError::BatchSize(0),
            "max batch size must be between 1 and 500 events, got 0",
        ),
        (
            "batch larger than gateway limit",
            |config| config.max_batch_size = 501,
            ExpectedError::BatchSize(501),
            "max batch size must be between 1 and 500 events, got 501",
        ),
        (
            "zero flush interval",
            |config| config.flush_interval = Duration::ZERO,
            ExpectedError::FlushInterval,
            "flush interval must be greater than zero",
        ),
        (
            "zero rpc timeout",
            |config| config.rpc_timeout = Duration::ZERO,
            ExpectedError::RpcTimeout,
            "rpc timeout must be greater than zero",
        ),
        (
            "zero initial retry",
            |config| config.retry_initial = Duration::ZERO,
            ExpectedError::RetryInitial,
            "initial retry delay must be greater than zero",
        ),
        (
            "retry maximum below initial retry",
            |config| config.retry_max = config.retry_initial - Duration::from_millis(1),
            ExpectedError::RetryRange,
            "maximum retry delay must be greater than or equal to the initial retry delay",
        ),
    ];

    for (name, mutate, expected, expected_message) in cases {
        let mut config = ClientConfig::default();
        mutate(&mut config);

        let error = match PulseClient::connect(config) {
            Ok(_) => panic!("{name} should be rejected"),
            Err(error) => error,
        };

        let correct_variant = match (&error, expected) {
            (Error::InvalidQueueCapacity, ExpectedError::QueueCapacity)
            | (Error::InvalidFlushInterval, ExpectedError::FlushInterval)
            | (Error::InvalidRpcTimeout, ExpectedError::RpcTimeout)
            | (Error::InvalidRetryInitial, ExpectedError::RetryInitial)
            | (Error::InvalidRetryRange, ExpectedError::RetryRange) => true,
            (Error::InvalidMaxBatchSize(actual), ExpectedError::BatchSize(expected)) => {
                *actual == expected
            }
            _ => false,
        };
        assert!(correct_variant, "{name} returned {error:?}");
        assert_eq!(error.to_string(), expected_message, "{name}");
    }
}
