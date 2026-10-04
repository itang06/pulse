//! Prometheus metrics for the processor. One place owns every exported
//! series so the /metrics surface stays reviewable.

use axum::{extract::State, routing::get, Router};
use prometheus::{Encoder, Histogram, HistogramOpts, IntCounter, IntGauge, Registry, TextEncoder};

use crate::transform::TransformOutput;

#[derive(Clone)]
pub struct Metrics {
    pub registry: Registry,
    pub events_consumed: IntCounter,
    pub dlq_events: IntCounter,
    pub events_enriched: IntCounter,
    pub anomalies: IntCounter,
    pub active_state_keys: IntGauge,
    pub ttl_evictions: IntCounter,
    pub capacity_evictions: IntCounter,
    pub warmup_suppressions: IntCounter,
    pub anomaly_score: Histogram,
    pub processing_latency_seconds: Histogram,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();
        let dlq_events = IntCounter::new(
            "pulse_processor_dlq_events_total",
            "Events routed to telemetry.dlq",
        )
        .expect("valid metric definition");
        let events_consumed = counter(
            "pulse_processor_events_consumed_total",
            "Raw events consumed",
        );
        let events_enriched = counter(
            "pulse_processor_events_enriched_total",
            "Enriched events whose Kafka delivery was acknowledged",
        );
        let anomalies = counter(
            "pulse_processor_anomalies_total",
            "Acknowledged enriched events classified as anomalous",
        );
        let ttl_evictions = counter(
            "pulse_processor_state_ttl_evictions_total",
            "Detector keys removed by TTL",
        );
        let capacity_evictions = counter(
            "pulse_processor_state_capacity_evictions_total",
            "Detector keys removed by capacity",
        );
        let warmup_suppressions = counter(
            "pulse_processor_warmup_suppressions_total",
            "Verdicts suppressed during detector warm-up",
        );
        let active_state_keys = IntGauge::new(
            "pulse_processor_active_state_keys",
            "Current active detector keys",
        )
        .expect("valid metric definition");
        let anomaly_score = Histogram::with_opts(HistogramOpts::new(
            "pulse_processor_anomaly_score",
            "Anomaly score distribution",
        ))
        .expect("valid metric definition");
        let processing_latency_seconds = Histogram::with_opts(HistogramOpts::new(
            "pulse_processor_processing_latency_seconds",
            "Record transformation latency in seconds",
        ))
        .expect("valid metric definition");
        for metric in [
            Box::new(dlq_events.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(events_consumed.clone()),
            Box::new(events_enriched.clone()),
            Box::new(anomalies.clone()),
            Box::new(ttl_evictions.clone()),
            Box::new(capacity_evictions.clone()),
            Box::new(warmup_suppressions.clone()),
            Box::new(active_state_keys.clone()),
            Box::new(anomaly_score.clone()),
            Box::new(processing_latency_seconds.clone()),
        ] {
            registry
                .register(metric)
                .expect("register processor metric");
        }
        Self {
            registry,
            events_consumed,
            dlq_events,
            events_enriched,
            anomalies,
            active_state_keys,
            ttl_evictions,
            capacity_evictions,
            warmup_suppressions,
            anomaly_score,
            processing_latency_seconds,
        }
    }

    pub fn record_transform(&self, output: &TransformOutput, elapsed_seconds: f64) {
        self.events_consumed.inc();
        self.processing_latency_seconds
            .observe(elapsed_seconds.max(0.0));
        match output {
            TransformOutput::DeadLetter { .. } | TransformOutput::RetryableError { .. } => {}
            TransformOutput::Enriched { stats, .. } => {
                self.active_state_keys.set(stats.active_keys as i64);
                self.ttl_evictions.inc_by(stats.ttl_evictions as u64);
                self.capacity_evictions
                    .inc_by(stats.capacity_evictions as u64);
                if stats.warmup_suppressed {
                    self.warmup_suppressions.inc();
                }
                self.anomaly_score.observe(stats.anomaly_score);
            }
        }
    }

    pub fn record_delivery_success(&self, output: &TransformOutput) {
        match output {
            TransformOutput::Enriched { stats, .. } => {
                self.events_enriched.inc();
                if stats.anomaly {
                    self.anomalies.inc();
                }
            }
            TransformOutput::DeadLetter { .. } => self.dlq_events.inc(),
            TransformOutput::RetryableError { .. } => {}
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

fn counter(name: &str, help: &str) -> IntCounter {
    IntCounter::new(name, help).expect("valid metric definition")
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
    let encoder = TextEncoder::new();
    if let Err(err) = encoder.encode(&metrics.registry.gather(), &mut buf) {
        tracing::error!(?err, "encoding metrics");
    }
    String::from_utf8(buf).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        detector::DetectorConfig,
        state::{DetectorStateStore, StateStoreConfig},
        transform::{transform, RawRecord},
    };
    use prost::Message;
    use pulse_proto::pulse::v1::TelemetryEvent;
    use std::time::Instant;

    #[test]
    fn transform_metrics_register_without_cardinality_labels_and_increment() {
        let metrics = Metrics::new();
        let mut store = DetectorStateStore::new(StateStoreConfig {
            detector: DetectorConfig {
                warmup_observations: 1,
                ..Default::default()
            },
            ..Default::default()
        })
        .unwrap();
        let e = TelemetryEvent {
            event_id: "550e8400-e29b-41d4-a716-446655440000".into(),
            event_time: Some(prost_types::Timestamp {
                seconds: 1,
                nanos: 0,
            }),
            service_name: "svc".into(),
            route: "/r".into(),
            latency_us: 10,
            ..Default::default()
        };
        let output = transform(
            RawRecord {
                payload: e.encode_to_vec(),
                key: b"svc\0/r".into(),
                topic: "raw".into(),
                partition: 0,
                offset: 0,
                now: Instant::now(),
                wall_time: prost_types::Timestamp {
                    seconds: 2,
                    nanos: 0,
                },
            },
            &mut store,
        );
        metrics.record_transform(&output, 0.001);
        assert_eq!(metrics.events_consumed.get(), 1);
        assert_eq!(metrics.events_enriched.get(), 0);
        assert_eq!(metrics.dlq_events.get(), 0);
        assert_eq!(metrics.anomalies.get(), 0);
        assert_eq!(metrics.active_state_keys.get(), 1);
        assert_eq!(metrics.processing_latency_seconds.get_sample_count(), 1);
        assert!(metrics.registry.gather().iter().all(|family| family
            .get_metric()
            .iter()
            .all(|metric| metric.get_label().is_empty())));
        metrics.record_delivery_success(&output);
        assert_eq!(metrics.events_enriched.get(), 1);
        let anomaly = TransformOutput::Enriched {
            payload: Vec::new(),
            key: b"svc\0/r".to_vec(),
            stats: crate::transform::TransformStats {
                anomaly: true,
                ..Default::default()
            },
        };
        metrics.record_transform(&anomaly, 0.001);
        assert_eq!(metrics.anomalies.get(), 0);
        metrics.record_delivery_success(&anomaly);
        assert_eq!(metrics.events_enriched.get(), 2);
        assert_eq!(metrics.anomalies.get(), 1);
        let dlq = TransformOutput::DeadLetter {
            payload: Vec::new(),
        };
        metrics.record_transform(&dlq, 0.001);
        assert_eq!(metrics.dlq_events.get(), 0);
        metrics.record_delivery_success(&dlq);
        assert_eq!(metrics.dlq_events.get(), 1);
        let retryable = TransformOutput::RetryableError { reason: "retry" };
        metrics.record_transform(&retryable, 0.001);
        metrics.record_delivery_success(&retryable);
        assert_eq!(metrics.events_enriched.get(), 2);
        assert_eq!(metrics.dlq_events.get(), 1);
        assert_eq!(metrics.anomalies.get(), 1);
    }
}
