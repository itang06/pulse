use std::time::{Duration, SystemTime};

use pulse_sdk::{MetricsSnapshot, Telemetry};
use rand::{rngs::StdRng, RngExt, SeedableRng};
use tokio::time::Instant;

/// Reproduces all generated telemetry fields for the same seed, route count,
/// and supplied event-time sequence. SDK-assigned transport event IDs remain
/// unique and nondeterministic; deterministic runs must supply the same clock values.
pub struct EventGenerator {
    rng: StdRng,
    route_count: usize,
    sequence: u64,
}

impl EventGenerator {
    pub fn new(seed: u64, route_count: usize) -> anyhow::Result<Self> {
        anyhow::ensure!(route_count > 0, "route count must be greater than zero");
        Ok(Self {
            rng: StdRng::seed_from_u64(seed),
            route_count,
            sequence: 0,
        })
    }

    pub fn next_event(&mut self, event_time: SystemTime) -> Telemetry {
        let route_number = self.rng.random_range(0..self.route_count);
        let service_number = self.rng.random_range(0..3);
        let baseline = [8_000_u64, 35_000, 120_000][service_number];
        let latency_us = baseline + self.rng.random_range(0..baseline / 2 + 1);
        let status_code = if self.rng.random_range(0..100) == 0 {
            500
        } else {
            200
        };
        let sequence = self.sequence;
        self.sequence += 1;

        Telemetry {
            event_time,
            service_name: format!("pulse-demo-service-{}", service_number + 1),
            route: format!("/v1/route-{route_number}"),
            latency_us,
            status_code,
            trace_id: Some(format!("demo-{sequence:016x}")),
            attributes: Default::default(),
        }
    }
}

/// A cumulative 10 ms pacer that repays any missed emissions on later ticks.
pub struct CumulativePacer {
    rate: u64,
    emitted: u64,
}

impl CumulativePacer {
    pub fn new(rate: u64) -> anyhow::Result<Self> {
        anyhow::ensure!(rate > 0, "rate must be greater than zero");
        Ok(Self { rate, emitted: 0 })
    }

    pub fn due_at(&mut self, elapsed: Duration) -> u64 {
        let elapsed_nanos = elapsed.as_nanos();
        let target = (elapsed_nanos.saturating_mul(self.rate as u128) / 1_000_000_000)
            .min(u64::MAX as u128) as u64;
        let due = target.saturating_sub(self.emitted);
        self.emitted += due;
        due
    }

    pub fn emitted(&self) -> u64 {
        self.emitted
    }
}

pub fn checked_deadline(start: Instant, duration: Duration) -> anyhow::Result<Instant> {
    start
        .checked_add(duration)
        .ok_or_else(|| anyhow::anyhow!("configured duration exceeds the monotonic clock range"))
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RunSummary {
    pub expected_attempted: u64,
    pub attempted: u64,
    pub queued: u64,
    pub invalid: u64,
    pub dropped_full: u64,
    pub dropped_closed: u64,
    pub dropped: u64,
    pub acknowledged: u64,
    pub retries: u64,
    pub permanent_failures: u64,
    pub configured_rate: u64,
    pub elapsed_seconds: f64,
    pub acknowledged_events_per_second: f64,
}

impl RunSummary {
    pub fn from_metrics(
        metrics: MetricsSnapshot,
        expected_attempted: u64,
        configured_rate: u64,
        elapsed: Duration,
    ) -> Self {
        let elapsed_seconds = elapsed.as_secs_f64();
        Self {
            expected_attempted,
            attempted: metrics.attempted,
            queued: metrics.queued,
            invalid: metrics.invalid_events,
            dropped_full: metrics.dropped_full,
            dropped_closed: metrics.dropped_closed,
            dropped: metrics.dropped_full.saturating_add(metrics.dropped_closed),
            acknowledged: metrics.acknowledged,
            retries: metrics.batch_retries,
            permanent_failures: metrics.permanent_failures,
            configured_rate,
            elapsed_seconds,
            acknowledged_events_per_second: if elapsed_seconds > 0.0 {
                metrics.acknowledged as f64 / elapsed_seconds
            } else {
                0.0
            },
        }
    }

    pub fn is_valid(&self) -> bool {
        self.attempted == self.expected_attempted
            && self.invalid == 0
            && self.dropped_full == 0
            && self.dropped_closed == 0
            && self.permanent_failures == 0
            && self.acknowledged == self.queued
    }
}
