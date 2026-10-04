use std::time::Instant;

use prost::Message;
use pulse_proto::pulse::v1::{DeadLetterRecord, EnrichedTelemetry, FailureStage, TelemetryEvent};

use crate::state::DetectorStateStore;

#[derive(Debug)]
pub struct RawRecord {
    pub payload: Vec<u8>,
    pub key: Vec<u8>,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub now: Instant,
    pub wall_time: prost_types::Timestamp,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct TransformStats {
    pub active_keys: usize,
    pub ttl_evictions: usize,
    pub capacity_evictions: usize,
    pub warmup_suppressed: bool,
    pub anomaly: bool,
    pub anomaly_score: f64,
}

#[derive(Debug)]
pub enum TransformOutput {
    Enriched {
        payload: Vec<u8>,
        key: Vec<u8>,
        stats: TransformStats,
    },
    DeadLetter {
        payload: Vec<u8>,
    },
    RetryableError {
        reason: &'static str,
    },
}

pub fn transform(record: RawRecord, store: &mut DetectorStateStore) -> TransformOutput {
    if !valid_timestamp(&record.wall_time) {
        return TransformOutput::RetryableError {
            reason: "processing timestamp is invalid",
        };
    }
    let decoded = TelemetryEvent::decode(record.payload.as_slice());
    let event = match decoded {
        Ok(event) => event,
        Err(error) => {
            return dead_letter(
                record,
                None,
                "decode",
                &format!("invalid protobuf: {error}"),
            )
        }
    };
    let validation = validate(&event, &record.key);
    if let Err((category, reason)) = validation {
        let event_id = valid_uuid(&event.event_id).then(|| event.event_id.clone());
        return dead_letter(record, event_id, category, reason);
    }
    let processed_at = record.wall_time;
    let outcome = match store.observe_at(
        &event.service_name,
        &event.route,
        event.latency_us as f64,
        record.now,
    ) {
        Ok(outcome) => outcome,
        Err(reason) => return TransformOutput::RetryableError { reason },
    };
    let enriched = EnrichedTelemetry {
        event: Some(event),
        ewma_mean_us: outcome.score.previous_mean_us.unwrap_or_default(),
        ewma_stddev_us: outcome.score.previous_stddev_us.unwrap_or_default(),
        anomaly_score: outcome.score.score,
        is_anomaly: outcome.score.anomaly,
        processed_at: Some(processed_at),
    };
    TransformOutput::Enriched {
        payload: enriched.encode_to_vec(),
        key: record.key,
        stats: TransformStats {
            active_keys: outcome.active_key_count,
            ttl_evictions: outcome.evictions.ttl,
            capacity_evictions: outcome.evictions.capacity,
            warmup_suppressed: outcome.warmup_suppressed,
            anomaly: outcome.score.anomaly,
            anomaly_score: outcome.score.score,
        },
    }
}

fn validate(event: &TelemetryEvent, key: &[u8]) -> Result<(), (&'static str, &'static str)> {
    if !valid_uuid(&event.event_id) {
        return Err(("event_id", "event_id must be a UUID"));
    }
    if !event.event_time.as_ref().is_some_and(valid_timestamp) {
        return Err(("event_time", "event_time is missing or invalid"));
    }
    if event.service_name.is_empty() {
        return Err(("service_name", "service_name must not be empty"));
    }
    if event.service_name.contains('\0') {
        return Err(("service_name", "service_name must not contain NUL"));
    }
    if event.route.is_empty() {
        return Err(("route", "route must not be empty"));
    }
    if event.route.contains('\0') {
        return Err(("route", "route must not contain NUL"));
    }
    if event.route.len() > 256 {
        return Err(("route", "route exceeds 256 bytes"));
    }
    if event.attributes.len() > 16 {
        return Err(("attributes", "attributes exceed 16 entries"));
    }
    if event
        .attributes
        .iter()
        .any(|(key, value)| key.len() > 64 || value.len() > 256)
    {
        return Err((
            "attributes",
            "attribute key or value exceeds its byte limit",
        ));
    }
    let expected = format!("{}\0{}", event.service_name, event.route);
    if key != expected.as_bytes() {
        return Err(("key", "source key does not match service_name and route"));
    }
    Ok(())
}

fn valid_uuid(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 36
        && [8, 13, 18, 23].into_iter().all(|i| bytes[i] == b'-')
        && bytes
            .iter()
            .enumerate()
            .all(|(i, b)| [8, 13, 18, 23].contains(&i) || b.is_ascii_hexdigit())
}

fn valid_timestamp(timestamp: &prost_types::Timestamp) -> bool {
    (-62_135_596_800..=253_402_300_799).contains(&timestamp.seconds)
        && (0..1_000_000_000).contains(&timestamp.nanos)
}

fn dead_letter(
    record: RawRecord,
    event_id: Option<String>,
    category: &str,
    reason: &str,
) -> TransformOutput {
    let failed_at = record.wall_time;
    let dlq = DeadLetterRecord {
        original_payload: record.payload,
        stage: FailureStage::Processor as i32,
        source_topic: record.topic,
        source_partition: record.partition,
        source_offset: record.offset,
        error_category: category.to_owned(),
        reason: reason.to_owned(),
        failed_at: Some(failed_at),
        event_id: event_id.unwrap_or_default(),
    };
    TransformOutput::DeadLetter {
        payload: dlq.encode_to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        detector::DetectorConfig,
        state::{DetectorStateStore, StateStoreConfig},
    };
    use std::time::{Duration, Instant};

    const ID: &str = "550e8400-e29b-41d4-a716-446655440000";
    fn store() -> DetectorStateStore {
        DetectorStateStore::new(StateStoreConfig {
            detector: DetectorConfig {
                warmup_observations: 2,
                ..Default::default()
            },
            ..Default::default()
        })
        .unwrap()
    }
    fn event() -> TelemetryEvent {
        TelemetryEvent {
            event_id: ID.into(),
            event_time: Some(prost_types::Timestamp {
                seconds: 1,
                nanos: 2,
            }),
            service_name: "svc".into(),
            route: "/r".into(),
            latency_us: 10,
            ..Default::default()
        }
    }
    fn record(payload: Vec<u8>, key: &[u8], now: Instant) -> RawRecord {
        RawRecord {
            payload,
            key: key.into(),
            topic: "telemetry.raw".into(),
            partition: 2,
            offset: 19,
            now,
            wall_time: prost_types::Timestamp {
                seconds: 20,
                nanos: 30,
            },
        }
    }
    fn dlq(output: TransformOutput) -> DeadLetterRecord {
        match output {
            TransformOutput::DeadLetter { payload, .. } => {
                DeadLetterRecord::decode(payload.as_slice()).unwrap()
            }
            _ => panic!("expected DLQ"),
        }
    }

    #[test]
    fn malformed_protobuf_preserves_bytes_and_source_and_has_no_event_id() {
        let raw = vec![0xff];
        let output = transform(
            record(raw.clone(), b"svc\0/r", Instant::now()),
            &mut store(),
        );
        let dlq = dlq(output);
        assert_eq!(dlq.original_payload, raw);
        assert_eq!(dlq.stage, FailureStage::Processor as i32);
        assert_eq!(
            (
                dlq.source_topic.as_str(),
                dlq.source_partition,
                dlq.source_offset
            ),
            ("telemetry.raw", 2, 19)
        );
        assert!(dlq.event_id.is_empty());
    }

    #[test]
    fn invalid_event_id_is_not_copied_to_dlq() {
        let mut event = event();
        event.event_id = "not-an-id".into();
        assert!(dlq(transform(
            record(event.encode_to_vec(), b"svc\0/r", Instant::now()),
            &mut store()
        ))
        .event_id
        .is_empty());
    }

    #[test]
    fn valid_event_is_enriched_with_previous_baseline_and_stable_key() {
        let mut store = store();
        let now = Instant::now();
        let a = transform(record(event().encode_to_vec(), b"svc\0/r", now), &mut store);
        let TransformOutput::Enriched {
            payload,
            key,
            stats,
        } = a
        else {
            panic!("expected enriched")
        };
        assert_eq!(key, b"svc\0/r");
        assert!(stats.warmup_suppressed);
        let out = EnrichedTelemetry::decode(payload.as_slice()).unwrap();
        assert_eq!(out.ewma_mean_us, 0.0);
        assert_eq!(out.processed_at.unwrap().seconds, 20);
        let mut next = event();
        next.latency_us = 20;
        let b = transform(
            record(
                next.encode_to_vec(),
                b"svc\0/r",
                now + Duration::from_secs(1),
            ),
            &mut store,
        );
        let TransformOutput::Enriched { payload, stats, .. } = b else {
            panic!("expected enriched")
        };
        assert!(stats.warmup_suppressed);
        assert_eq!(
            EnrichedTelemetry::decode(payload.as_slice())
                .unwrap()
                .ewma_mean_us,
            10.0
        );
    }

    #[test]
    fn key_mismatch_and_missing_fields_do_not_mutate_store() {
        let mut store = store();
        let now = Instant::now();
        let mut e = event();
        e.route.clear();
        assert!(
            dlq(transform(
                record(e.encode_to_vec(), b"svc\0/r", now),
                &mut store
            ))
            .event_id
                == ID
        );
        assert_eq!(store.active_key_count(), 0);
        assert!(
            dlq(transform(
                record(event().encode_to_vec(), b"wrong", now),
                &mut store
            ))
            .error_category
                == "key"
        );
        assert_eq!(store.active_key_count(), 0);
    }

    #[test]
    fn out_of_order_monotonic_time_is_retryable_and_does_not_dlq() {
        let mut store = store();
        let start = Instant::now();
        transform(
            record(event().encode_to_vec(), b"svc\0/r", start),
            &mut store,
        );
        let output = transform(
            record(
                event().encode_to_vec(),
                b"svc\0/r",
                start - Duration::from_secs(1),
            ),
            &mut store,
        );
        assert!(matches!(output, TransformOutput::RetryableError { .. }));
        assert_eq!(store.active_key_count(), 1);
    }

    #[test]
    fn forbidden_nuls_and_gateway_attribute_bounds_dlq_without_mutating_state() {
        let now = Instant::now();
        let invalid = [
            ("service_name", {
                let mut e = event();
                e.service_name.push('\0');
                e
            }),
            ("route", {
                let mut e = event();
                e.route.push('\0');
                e
            }),
            ("attributes", {
                let mut e = event();
                e.attributes.insert("k".into(), "v".into());
                for i in 0..16 {
                    e.attributes.insert(format!("k{i}"), "v".into());
                }
                e
            }),
            ("attributes", {
                let mut e = event();
                e.attributes.insert("k".repeat(65), "v".into());
                e
            }),
            ("attributes", {
                let mut e = event();
                e.attributes.insert("k".into(), "v".repeat(257));
                e
            }),
        ];
        let mut store = store();
        for (category, e) in invalid {
            let key = format!("{}\0{}", e.service_name, e.route);
            let output = transform(record(e.encode_to_vec(), key.as_bytes(), now), &mut store);
            assert_eq!(dlq(output).error_category, category);
            assert_eq!(store.active_key_count(), 0);
        }
    }

    #[test]
    fn old_structurally_valid_event_survives_kafka_backlog() {
        // Gateway ingress owns freshness checks. Rechecking here would DLQ valid Kafka backlog.
        let mut e = event();
        e.event_time = Some(prost_types::Timestamp {
            seconds: 1,
            nanos: 0,
        });
        let mut raw = record(e.encode_to_vec(), b"svc\0/r", Instant::now());
        raw.wall_time = prost_types::Timestamp {
            seconds: 1_800_000_000,
            nanos: 0,
        };
        let result = transform(raw, &mut store());
        assert!(matches!(result, TransformOutput::Enriched { .. }));
    }
}
