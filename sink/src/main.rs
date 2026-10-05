//! Pulse's idempotent TimescaleDB persistence sink.

use std::{future::Future, time::Duration};

use anyhow::{Context, Result};
use pulse_sink::{
    db::{self, DbFailure, PersistOutcome},
    failpoint::SinkFailpoint,
    kafka,
    metrics::{self, Metrics},
    model::ValidatedEvent,
    pipeline::{self, Database, PipelineConfig, PipelineObserver},
};
use sqlx::postgres::PgPool;
use tokio::sync::watch;
use tracing::{error, info, warn};

struct PgDatabase(PgPool);

impl Database for PgDatabase {
    fn persist_batch<'a>(
        &'a self,
        events: &'a [ValidatedEvent],
    ) -> impl Future<Output = std::result::Result<PersistOutcome, DbFailure>> + Send + 'a {
        db::persist_batch(&self.0, events)
    }
}

struct SinkObserver {
    metrics: Metrics,
    failpoint: SinkFailpoint,
}

impl PipelineObserver for SinkObserver {
    fn source_record_consumed(&self) {
        self.metrics.source_record_consumed();
    }

    fn database_committed_before_ack(&self, outcome: &PersistOutcome) {
        self.metrics.observe_persisted(outcome);
        if self.failpoint == SinkFailpoint::AfterDbCommitBeforeOffsetCommit {
            use std::io::Write;

            let marker = serde_json::json!({
                "event": "FAILPOINT_REACHED",
                "failpoint": SinkFailpoint::NAME,
                "stage": "after_db_commit_before_offset_commit",
                "inserted_count": outcome.inserted_ids.len(),
                "conflict_count": outcome.conflict_count,
                "exit_code": 86,
            });
            let mut stdout = std::io::stdout().lock();
            let _ = writeln!(stdout, "{marker}");
            let _ = stdout.flush();
            std::process::exit(86);
        }
    }

    fn database_retry(&self) {
        self.metrics.database_retry();
    }

    fn dlq_record_published(&self) {
        self.metrics.dlq_record_published();
    }

    fn batch_finished(&self, records: usize, duration: Duration) {
        self.metrics.batch_finished(records, duration);
    }
}

fn env_or(key: &str, fallback: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| fallback.to_owned())
}

fn env_usize(key: &str, fallback: usize) -> Result<usize> {
    match std::env::var(key) {
        Ok(value) => value.parse().with_context(|| format!("parsing {key}")),
        Err(_) => Ok(fallback),
    }
}

fn config_from_env() -> Result<PipelineConfig> {
    let mut config = PipelineConfig::default();
    config.batch.max_records = env_usize("PULSE_SINK_BATCH_MAX_RECORDS", config.batch.max_records)?;
    config.batch.max_wait = Duration::from_millis(env_usize(
        "PULSE_SINK_BATCH_MAX_WAIT_MS",
        config.batch.max_wait.as_millis() as usize,
    )? as u64);
    config.retry.max_attempts = env_usize("PULSE_SINK_DB_MAX_ATTEMPTS", config.retry.max_attempts)?;
    config.retry.max_backoff = Duration::from_millis(env_usize(
        "PULSE_SINK_DB_MAX_BACKOFF_MS",
        config.retry.max_backoff.as_millis() as usize,
    )? as u64);
    config.retry.max_backoff_budget = Duration::from_millis(env_usize(
        "PULSE_SINK_DB_BACKOFF_BUDGET_MS",
        config.retry.max_backoff_budget.as_millis() as usize,
    )? as u64);
    config.validate().context("validating sink configuration")
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = config_from_env()?;
    let failpoint_value = std::env::var_os(SinkFailpoint::ENV)
        .map(|value| {
            value
                .into_string()
                .map_err(|_| anyhow::anyhow!("{} must be valid UTF-8", SinkFailpoint::ENV))
        })
        .transpose()?;
    let failpoint = SinkFailpoint::parse(failpoint_value.as_deref())?;
    info!(
        max_records = config.batch.max_records,
        max_wait_ms = config.batch.max_wait.as_millis(),
        db_max_attempts = config.retry.max_attempts,
        db_max_backoff_ms = config.retry.max_backoff.as_millis(),
        db_backoff_budget_ms = config.retry.max_backoff_budget.as_millis(),
        "validated sink pipeline configuration"
    );

    let brokers = env_or("PULSE_KAFKA_BROKERS", "localhost:9092");
    let group_id = env_or("PULSE_CONSUMER_GROUP", "pulse-sink");
    let metrics_addr = env_or("PULSE_METRICS_ADDR", "127.0.0.1:9466");
    let database_url =
        std::env::var("PULSE_DATABASE_URL").context("PULSE_DATABASE_URL must be set")?;

    let metrics = Metrics::new();
    let observer = SinkObserver {
        metrics: metrics.clone(),
        failpoint,
    };
    let listener = tokio::net::TcpListener::bind(&metrics_addr)
        .await
        .with_context(|| format!("binding metrics server on {metrics_addr}"))?;
    tokio::spawn(metrics::serve(listener, metrics.clone()));
    info!(metrics_addr, "metrics server listening");

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&database_url)
        .await
        .context("connecting to timescaledb")?;
    sqlx::query("SELECT 1")
        .execute(&pool)
        .await
        .context("pinging timescaledb")?;
    db::verify_schema(&pool)
        .await
        .inspect_err(|error| error!(%error, "sink database schema verification failed"))?;
    info!("connected to TimescaleDB and verified sink schema");

    let (mut receiver, mut committer, publisher) = kafka::create(&brokers, &group_id)?;
    info!(
        brokers,
        group_id,
        topic = pipeline::SOURCE_TOPIC,
        "connected to Kafka"
    );

    metrics.set_healthy(true);
    receiver.start_lag_monitor(metrics.clone());

    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                info!("shutdown signal received");
                let _ = shutdown_tx.send(true);
            }
            Err(error) => error!(%error, "failed to listen for shutdown signal"),
        }
    });

    let database = PgDatabase(pool.clone());
    loop {
        match pipeline::process_next_batch_observed(
            &mut receiver,
            &database,
            publisher.clone(),
            &mut committer,
            &mut shutdown_rx,
            config,
            &observer,
        )
        .await
        {
            Ok(true) => {}
            Ok(false) => break,
            Err(pipeline::PipelineError::Retryable(failure)) => {
                warn!(%failure, "retryable sink batch failed; exiting so Kafka can replay it");
                return Err(failure.context("sink batch left uncommitted and replayable"));
            }
            Err(pipeline::PipelineError::Fatal(failure)) => {
                error!(%failure, "fatal sink failure");
                metrics.set_healthy(false);
                return Err(failure);
            }
        }
    }

    metrics.set_healthy(false);
    info!("sink shut down cleanly");
    pool.close().await;
    Ok(())
}
