use std::time::{Duration, SystemTime};

use anyhow::Context;
use pulse_demo::{
    config::LoadConfig,
    generator::{checked_deadline, CumulativePacer, EventGenerator, RunSummary},
};
use pulse_sdk::{ClientConfig, PulseClient};
use tokio::time::{interval_at, Instant, MissedTickBehavior};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = LoadConfig::from_env().context("reading load generator configuration")?;
    let client = PulseClient::connect(ClientConfig {
        endpoint: config.gateway_addr.clone(),
        ..ClientConfig::default()
    })
    .context("creating SDK client")?;
    let mut generator = EventGenerator::new(config.random_seed, config.route_count)?;
    let mut pacer = CumulativePacer::new(config.events_per_second)?;

    let started = Instant::now();
    let duration = Duration::from_secs(config.duration_seconds);
    let deadline = checked_deadline(started, duration)?;
    let mut ticker = interval_at(
        started + Duration::from_millis(10),
        Duration::from_millis(10),
    );
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    while Instant::now() < deadline {
        ticker.tick().await;
        let elapsed = started.elapsed().min(duration);
        emit_due(&client, &mut generator, &mut pacer, elapsed);
    }
    // Account for the exact requested duration even when it is not a 10 ms boundary.
    emit_due(&client, &mut generator, &mut pacer, duration);

    let flush_result = client.flush().await;
    let summary = RunSummary::from_metrics(
        client.metrics(),
        config.expected_attempted,
        config.events_per_second,
        started.elapsed(),
    );
    println!(
        "{}",
        serde_json::json!({
            "attempted": summary.attempted,
            "expected_attempted": summary.expected_attempted,
            "queued": summary.queued,
            "invalid": summary.invalid,
            "dropped_full": summary.dropped_full,
            "dropped_closed": summary.dropped_closed,
            "dropped": summary.dropped,
            "acknowledged": summary.acknowledged,
            "retries": summary.retries,
            "permanent_failures": summary.permanent_failures,
            "configured_rate": summary.configured_rate,
            "elapsed_seconds": summary.elapsed_seconds,
            "acknowledged_events_per_second": summary.acknowledged_events_per_second,
        })
    );

    flush_result.context("flushing queued telemetry")?;
    anyhow::ensure!(
        summary.is_valid(),
        "load run did not fully acknowledge all queued telemetry"
    );
    Ok(())
}

fn emit_due(
    client: &PulseClient,
    generator: &mut EventGenerator,
    pacer: &mut CumulativePacer,
    elapsed: Duration,
) {
    let event_time = SystemTime::now();
    for _ in 0..pacer.due_at(elapsed) {
        let _ = client.emit(generator.next_event(event_time));
    }
}
