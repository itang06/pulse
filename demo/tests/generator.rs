use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};

use pulse_demo::config::LoadConfig;
use pulse_demo::generator::{checked_deadline, CumulativePacer, EventGenerator, RunSummary};
use pulse_sdk::MetricsSnapshot;

fn event_signature(
    event: pulse_sdk::Telemetry,
) -> (
    SystemTime,
    String,
    String,
    u64,
    u32,
    Option<String>,
    HashMap<String, String>,
) {
    (
        event.event_time,
        event.service_name,
        event.route,
        event.latency_us,
        event.status_code,
        event.trace_id,
        event.attributes,
    )
}

#[test]
fn deterministic_seed_produces_repeatable_events() {
    let timestamp = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut first = EventGenerator::new(42, 12).expect("valid generator");
    let mut second = EventGenerator::new(42, 12).expect("valid generator");

    let first_events = (0..100)
        .map(|_| event_signature(first.next_event(timestamp)))
        .collect::<Vec<_>>();
    let second_events = (0..100)
        .map(|_| event_signature(second.next_event(timestamp)))
        .collect::<Vec<_>>();

    assert_eq!(first_events, second_events);
}

#[test]
fn generated_routes_are_normalized_and_bounded() {
    let timestamp = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let mut generator = EventGenerator::new(7, 8).expect("valid generator");

    for _ in 0..1_000 {
        let event = generator.next_event(timestamp);
        assert!(event.route.starts_with("/v1/route-"));
        assert!(!event.route.contains('?'));
        assert!(!event.route.contains("//"));
        let route_number = event
            .route
            .strip_prefix("/v1/route-")
            .expect("normalized route prefix")
            .parse::<usize>()
            .expect("numeric route bucket");
        assert!(route_number < 8);
    }
}

#[test]
fn pacer_emits_requested_count_without_accumulating_drift() {
    let mut pacer = CumulativePacer::new(1_000).expect("valid rate");

    // The scheduler woke late at 40 ms. The cumulative target makes that tick
    // catch up 30 events instead of permanently losing three 10 ms windows.
    assert_eq!(pacer.due_at(Duration::from_millis(10)), 10);
    assert_eq!(pacer.due_at(Duration::from_millis(40)), 30);
    assert_eq!(pacer.due_at(Duration::from_millis(100)), 60);
    assert_eq!(pacer.due_at(Duration::from_secs(1)), 900);
    assert_eq!(pacer.emitted(), 1_000);
}

#[test]
fn summary_uses_acknowledged_events_and_total_wall_time() {
    let metrics = MetricsSnapshot {
        attempted: 1_000,
        queued: 1_000,
        dropped_full: 2,
        dropped_closed: 3,
        invalid_events: 0,
        acknowledged: 900,
        batch_retries: 2,
        permanent_failures: 0,
        queue_depth: 0,
    };

    let summary = RunSummary::from_metrics(metrics, 1_000, 1_000, Duration::from_secs_f64(2.5));

    assert_eq!(summary.expected_attempted, 1_000);
    assert_eq!(summary.acknowledged, 900);
    assert_eq!(summary.dropped, 5);
    assert_eq!(summary.elapsed_seconds, 2.5);
    assert_eq!(summary.acknowledged_events_per_second, 360.0);
    assert!(!summary.is_valid());
}

#[test]
fn summary_rejects_a_short_run_even_when_queued_events_are_acknowledged() {
    let metrics = MetricsSnapshot {
        attempted: 99,
        queued: 99,
        acknowledged: 99,
        ..MetricsSnapshot::default()
    };
    let summary = RunSummary::from_metrics(metrics, 100, 100, Duration::from_secs(1));

    assert!(!summary.is_valid());
}

#[test]
fn config_uses_explicit_defaults_when_environment_is_empty() {
    let config = LoadConfig::from_values(std::iter::empty::<(&str, &str)>()).expect("defaults");

    assert_eq!(config.gateway_addr, "http://localhost:50051");
    assert_eq!(config.events_per_second, 1_000);
    assert_eq!(config.duration_seconds, 60);
    assert_eq!(config.expected_attempted, 60_000);
    assert_eq!(config.route_count, 8);
    assert_eq!(config.random_seed, 42);
}

#[test]
fn config_reads_all_supported_environment_values() {
    let config = LoadConfig::from_values([
        ("PULSE_GATEWAY_ADDR", "http://gateway:50051"),
        ("PULSE_EVENTS_PER_SECOND", "250"),
        ("PULSE_DURATION_SECONDS", "12"),
        ("PULSE_ROUTE_COUNT", "5"),
        ("PULSE_RANDOM_SEED", "991"),
    ])
    .expect("valid configuration");

    assert_eq!(config.gateway_addr, "http://gateway:50051");
    assert_eq!(config.events_per_second, 250);
    assert_eq!(config.duration_seconds, 12);
    assert_eq!(config.route_count, 5);
    assert_eq!(config.random_seed, 991);
}

#[test]
fn config_rejects_zero_rate_and_route_count() {
    assert!(LoadConfig::from_values([("PULSE_EVENTS_PER_SECOND", "0")]).is_err());
    assert!(LoadConfig::from_values([("PULSE_ROUTE_COUNT", "0")]).is_err());
}

#[test]
fn config_rejects_attempt_count_overflow() {
    assert!(LoadConfig::from_values([
        ("PULSE_EVENTS_PER_SECOND", "18446744073709551615"),
        ("PULSE_DURATION_SECONDS", "2"),
    ])
    .is_err());
}

#[test]
fn monotonic_deadline_rejects_duration_overflow() {
    assert!(checked_deadline(tokio::time::Instant::now(), Duration::from_secs(u64::MAX),).is_err());
}
