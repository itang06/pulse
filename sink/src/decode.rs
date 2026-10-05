use prost::Message;
use prost_types::Timestamp;
use pulse_proto::pulse::v1::{DeadLetterRecord, EnrichedTelemetry, FailureStage};
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::model::{DecodedRecord, SourceRecord, UtcTimestamp, ValidatedEvent};

const TIMESTAMP_MIN_SECONDS: i64 = -62_135_596_800;
const TIMESTAMP_MAX_SECONDS: i64 = 253_402_300_799;
const MAX_ROUTE_BYTES: usize = 256;
const MAX_ATTRIBUTE_COUNT: usize = 16;
const MAX_ATTRIBUTE_KEY_BYTES: usize = 64;
const MAX_ATTRIBUTE_VALUE_BYTES: usize = 256;

/// Decode and validate one owned Kafka payload without performing I/O.
///
/// The categories are a bounded vocabulary (`decode`, `validation`,
/// `timestamp`) so DLQ consumers can aggregate failures safely.
pub fn decode_record(source: SourceRecord, failed_at: Timestamp) -> DecodedRecord {
    let enriched = match EnrichedTelemetry::decode(source.payload()) {
        Ok(enriched) => enriched,
        Err(_) => {
            return dead_letter(
                source,
                failed_at,
                "decode",
                "invalid enriched protobuf payload",
                "",
            )
        }
    };

    let Some(event) = enriched.event else {
        return dead_letter(
            source,
            failed_at,
            "validation",
            "enriched event is missing",
            "",
        );
    };
    let safe_event_id = parse_event_id(&event.event_id);
    let event_id = safe_event_id.map_or_else(String::new, |id| id.to_string());

    let event_time = match event.event_time.as_ref().and_then(utc_timestamp) {
        Some(timestamp) => timestamp,
        None => {
            return dead_letter(
                source,
                failed_at,
                "timestamp",
                "event_time is missing or outside the supported UTC range",
                &event_id,
            )
        }
    };
    let processed_at = match enriched.processed_at.as_ref().and_then(utc_timestamp) {
        Some(timestamp) => timestamp,
        None => {
            return dead_letter(
                source,
                failed_at,
                "timestamp",
                "processed_at is missing or outside the supported UTC range",
                &event_id,
            )
        }
    };

    let validation = validate_fields(
        &event,
        enriched.ewma_mean_us,
        enriched.ewma_stddev_us,
        enriched.anomaly_score,
        safe_event_id,
    );
    if let Err(reason) = validation {
        return dead_letter(source, failed_at, "validation", reason, &event_id);
    }

    let attributes = Value::Object(
        event
            .attributes
            .into_iter()
            .map(|(key, value)| (key, Value::String(value)))
            .collect::<Map<_, _>>(),
    );
    DecodedRecord::Valid(ValidatedEvent {
        event_id: safe_event_id.expect("validated UUID"),
        event_time,
        service_name: event.service_name,
        route: event.route,
        latency_us: event.latency_us as i64,
        status_code: event.status_code as i32,
        trace_id: event.trace_id,
        attributes,
        ewma_mean_us: enriched.ewma_mean_us,
        ewma_stddev_us: enriched.ewma_stddev_us,
        anomaly_score: enriched.anomaly_score,
        is_anomaly: enriched.is_anomaly,
        processed_at,
    })
}

fn validate_fields(
    event: &pulse_proto::pulse::v1::TelemetryEvent,
    ewma_mean_us: f64,
    ewma_stddev_us: f64,
    anomaly_score: f64,
    event_id: Option<Uuid>,
) -> Result<(), &'static str> {
    if event_id.is_none() {
        return Err("event_id must be a canonical UUID");
    }
    if event.service_name.is_empty() || contains_nul(&event.service_name) {
        return Err("service_name must be nonempty and contain no NUL bytes");
    }
    if event.route.is_empty() || contains_nul(&event.route) {
        return Err("route must be nonempty and contain no NUL bytes");
    }
    if event.route.len() > MAX_ROUTE_BYTES {
        return Err("route exceeds 256 UTF-8 bytes");
    }
    if event.trace_id.contains('\0') {
        return Err("trace_id must not contain NUL bytes");
    }
    if event.attributes.len() > MAX_ATTRIBUTE_COUNT {
        return Err("attributes exceed 16 entries");
    }
    for (key, value) in &event.attributes {
        if contains_nul(key) || contains_nul(value) {
            return Err("attribute keys and values must not contain NUL bytes");
        }
        if key.len() > MAX_ATTRIBUTE_KEY_BYTES || value.len() > MAX_ATTRIBUTE_VALUE_BYTES {
            return Err("attribute key or value exceeds the processor contract byte limit");
        }
    }
    if event.latency_us > i64::MAX as u64 {
        return Err("latency_us exceeds PostgreSQL BIGINT range");
    }
    if event.status_code > i32::MAX as u32 {
        return Err("status_code exceeds PostgreSQL INTEGER range");
    }
    if !valid_detector_value(ewma_mean_us)
        || !valid_detector_value(ewma_stddev_us)
        || !valid_detector_value(anomaly_score)
    {
        return Err("detector values must be finite and nonnegative");
    }
    Ok(())
}

fn parse_event_id(value: &str) -> Option<Uuid> {
    let bytes = value.as_bytes();
    let canonical_shape = bytes.len() == 36
        && [8, 13, 18, 23]
            .into_iter()
            .all(|index| bytes[index] == b'-')
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| [8, 13, 18, 23].contains(&index) || byte.is_ascii_hexdigit());
    canonical_shape
        .then(|| Uuid::parse_str(value).ok())
        .flatten()
}

fn utc_timestamp(value: &Timestamp) -> Option<UtcTimestamp> {
    if !(TIMESTAMP_MIN_SECONDS..=TIMESTAMP_MAX_SECONDS).contains(&value.seconds)
        || !(0..1_000_000_000).contains(&value.nanos)
    {
        return None;
    }
    Some(UtcTimestamp {
        seconds: value.seconds,
        nanos: value.nanos,
    })
}

fn valid_detector_value(value: f64) -> bool {
    value.is_finite() && value >= 0.0
}

fn contains_nul(value: &str) -> bool {
    value.contains('\0')
}

fn dead_letter(
    source: SourceRecord,
    failed_at: Timestamp,
    category: &str,
    reason: &str,
    event_id: &str,
) -> DecodedRecord {
    let (original_payload, source_topic, source_partition, source_offset) = source.into_parts();
    let record = DeadLetterRecord {
        original_payload,
        stage: FailureStage::Sink as i32,
        source_topic,
        source_partition,
        source_offset,
        error_category: category.to_owned(),
        reason: reason.to_owned(),
        failed_at: Some(failed_at),
        event_id: event_id.to_owned(),
    };
    DecodedRecord::DeadLetter(record.encode_to_vec())
}
