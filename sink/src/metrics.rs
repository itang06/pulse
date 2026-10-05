//! Prometheus metrics and HTTP endpoint for the sink process.

use axum::{extract::State, routing::get, Router};
use prometheus::{Encoder, Histogram, HistogramOpts, IntCounter, IntGauge, Registry, TextEncoder};
use std::time::Duration;

use crate::{db::PersistOutcome, pipeline::PipelineObserver};

#[derive(Clone)]
pub struct Metrics {
    pub registry: Registry,
    records_consumed: IntCounter,
    events_persisted: IntCounter,
    conflicts_skipped: IntCounter,
    database_retries: IntCounter,
    dlq_published: IntCounter,
    batch_size: Histogram,
    batch_duration: Histogram,
    consumer_lag: IntGauge,
    healthy: IntGauge,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();
        let records_consumed = counter(
            "pulse_sink_records_consumed_total",
            "Owned source records fetched from Kafka by the sink",
        );
        let events_persisted = counter(
            "pulse_sink_events_persisted_total",
            "Rows inserted into TimescaleDB after a committed batch transaction",
        );
        let conflicts_skipped = counter(
            "pulse_sink_conflicts_skipped_total",
            "Events skipped because their receipt already existed after a committed transaction",
        );
        let database_retries = counter(
            "pulse_sink_database_retries_total",
            "Retryable database failures followed by another persistence attempt",
        );
        let dlq_published = counter(
            "pulse_sink_dlq_published_total",
            "Dead-letter records acknowledged by Kafka",
        );
        let batch_size = histogram(
            "pulse_sink_batch_size",
            "Owned source records in a batch through its final pipeline outcome",
            prometheus::exponential_buckets(1.0, 2.0, 10).expect("valid bucket config"),
        );
        let batch_duration = histogram(
            "pulse_sink_batch_duration_seconds",
            "Seconds from the first owned source record through the final pipeline outcome",
            prometheus::exponential_buckets(0.001, 2.0, 16).expect("valid bucket config"),
        );
        let consumer_lag = IntGauge::new(
            "pulse_sink_consumer_lag_records_estimate",
            "Estimated total lag from committed offsets for current assignment; -1 means unavailable",
        )
        .expect("valid metric definition");
        consumer_lag.set(-1);
        let healthy = IntGauge::new(
            "pulse_sink_healthy",
            "Whether startup checks completed and no fatal sink failure has occurred",
        )
        .expect("valid metric definition");

        for collector in [
            Box::new(records_consumed.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(events_persisted.clone()),
            Box::new(conflicts_skipped.clone()),
            Box::new(database_retries.clone()),
            Box::new(dlq_published.clone()),
            Box::new(batch_size.clone()),
            Box::new(batch_duration.clone()),
            Box::new(consumer_lag.clone()),
            Box::new(healthy.clone()),
        ] {
            registry.register(collector).expect("register sink metric");
        }

        Self {
            registry,
            records_consumed,
            events_persisted,
            conflicts_skipped,
            database_retries,
            dlq_published,
            batch_size,
            batch_duration,
            consumer_lag,
            healthy,
        }
    }

    pub fn set_healthy(&self, healthy: bool) {
        self.healthy.set(i64::from(healthy));
    }

    pub fn set_consumer_lag_estimate(&self, records: Option<i64>) {
        self.consumer_lag
            .set(records.map_or(-1, |records| records.max(0)));
    }

    pub fn observe_persisted(&self, outcome: &PersistOutcome) {
        self.events_persisted
            .inc_by(outcome.inserted_ids.len() as u64);
        self.conflicts_skipped.inc_by(outcome.conflict_count);
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl PipelineObserver for Metrics {
    fn source_record_consumed(&self) {
        self.records_consumed.inc();
    }

    fn persisted(&self, outcome: &PersistOutcome) {
        self.observe_persisted(outcome);
    }

    fn database_retry(&self) {
        self.database_retries.inc();
    }

    fn dlq_record_published(&self) {
        self.dlq_published.inc();
    }

    fn batch_finished(&self, records: usize, duration: Duration) {
        self.batch_size.observe(records as f64);
        self.batch_duration.observe(duration.as_secs_f64());
    }
}

fn counter(name: &str, help: &str) -> IntCounter {
    IntCounter::new(name, help).expect("valid metric definition")
}

fn histogram(name: &str, help: &str, buckets: Vec<f64>) -> Histogram {
    Histogram::with_opts(HistogramOpts::new(name, help).buckets(buckets))
        .expect("valid metric definition")
}

pub async fn serve(listener: tokio::net::TcpListener, metrics: Metrics) {
    let app = Router::new()
        .route("/metrics", get(render))
        .with_state(metrics);
    if let Err(err) = axum::serve(listener, app).await {
        tracing::error!(?err, "metrics server failed");
    }
}

async fn render(State(metrics): State<Metrics>) -> String {
    let mut buf = Vec::new();
    if let Err(err) = TextEncoder::new().encode(&metrics.registry.gather(), &mut buf) {
        tracing::error!(?err, "encoding metrics");
    }
    String::from_utf8(buf).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::Metrics;
    use prometheus::Encoder;

    #[test]
    fn registry_exposes_all_sink_metrics_without_labels() {
        let metrics = Metrics::new();
        let families = metrics.registry.gather();
        let names = families
            .iter()
            .map(|family| family.name())
            .collect::<Vec<_>>();

        for name in [
            "pulse_sink_records_consumed_total",
            "pulse_sink_events_persisted_total",
            "pulse_sink_conflicts_skipped_total",
            "pulse_sink_database_retries_total",
            "pulse_sink_dlq_published_total",
            "pulse_sink_batch_size",
            "pulse_sink_batch_duration_seconds",
            "pulse_sink_consumer_lag_records_estimate",
            "pulse_sink_healthy",
        ] {
            assert!(names.contains(&name), "missing metric {name}");
        }
        assert!(families.iter().all(|family| family
            .get_metric()
            .iter()
            .all(|metric| metric.get_label().is_empty())));

        let mut output = Vec::new();
        Encoder::encode(&prometheus::TextEncoder::new(), &families, &mut output).unwrap();
        let exposition = String::from_utf8(output).unwrap();
        assert!(exposition.contains("# HELP pulse_sink_consumer_lag_records_estimate"));
        assert!(exposition.contains("-1 means unavailable"));
        assert!(exposition.contains("committed offsets"));
    }

    #[tokio::test]
    async fn metrics_scrape_returns_prometheus_text() {
        let body = super::render(axum::extract::State(Metrics::new())).await;
        assert!(body.contains("# TYPE pulse_sink_records_consumed_total counter"));
        assert!(body.contains("pulse_sink_healthy 0"));
        assert!(body.contains("pulse_sink_consumer_lag_records_estimate -1"));
    }

    #[test]
    fn unavailable_lag_resets_gauge_after_a_previous_estimate() {
        let metrics = Metrics::new();
        metrics.set_consumer_lag_estimate(Some(7));
        metrics.set_consumer_lag_estimate(None);
        let family = metrics
            .registry
            .gather()
            .into_iter()
            .find(|family| family.name() == "pulse_sink_consumer_lag_records_estimate")
            .unwrap();
        assert_eq!(family.get_metric()[0].get_gauge().value(), -1.0);
    }
}
