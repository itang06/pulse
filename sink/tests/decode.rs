use prost::Message;
use prost_types::Timestamp;
use pulse_proto::pulse::v1::{DeadLetterRecord, EnrichedTelemetry, FailureStage, TelemetryEvent};
use pulse_sink::{
    decode::decode_record,
    model::{DecodedRecord, SourceMetadataError, SourceRecord, UtcTimestamp},
};
use serde_json::json;
use std::collections::HashMap;
use uuid::Uuid;

const ID: &str = "550e8400-e29b-41d4-a716-446655440000";

fn source(payload: Vec<u8>) -> SourceRecord {
    SourceRecord::try_new(payload, "telemetry.enriched", 3, 42).unwrap()
}

fn event() -> TelemetryEvent {
    TelemetryEvent {
        event_id: ID.into(),
        event_time: Some(Timestamp {
            seconds: 1_700_000_000,
            nanos: 123_000_000,
        }),
        service_name: "api".into(),
        route: "/users".into(),
        latency_us: 1_000,
        status_code: 200,
        trace_id: "trace-1".into(),
        attributes: HashMap::from([("region".into(), "west".into())]),
    }
}

fn enriched(event: Option<TelemetryEvent>) -> EnrichedTelemetry {
    EnrichedTelemetry {
        event,
        ewma_mean_us: 100.0,
        ewma_stddev_us: 20.0,
        anomaly_score: 0.5,
        is_anomaly: false,
        processed_at: Some(Timestamp {
            seconds: 1_700_000_001,
            nanos: 456_000_000,
        }),
    }
}

fn decode(input: EnrichedTelemetry) -> DecodedRecord {
    decode_record(
        source(input.encode_to_vec()),
        Timestamp {
            seconds: 1_700_000_002,
            nanos: 0,
        },
    )
}

fn expect_dlq(record: DecodedRecord, category: &str, event_id: &str) -> DeadLetterRecord {
    let DecodedRecord::DeadLetter(payload) = record else {
        panic!("expected dead letter")
    };
    let dlq = DeadLetterRecord::decode(payload.as_slice()).unwrap();
    assert_eq!(dlq.stage, FailureStage::Sink as i32);
    assert_eq!(dlq.error_category, category);
    assert_eq!(dlq.event_id, event_id);
    assert_eq!(
        (
            dlq.source_topic.as_str(),
            dlq.source_partition,
            dlq.source_offset
        ),
        ("telemetry.enriched", 3, 42)
    );
    assert_eq!(
        dlq.failed_at,
        Some(Timestamp {
            seconds: 1_700_000_002,
            nanos: 0
        })
    );
    dlq
}

#[test]
fn valid_enriched_record_becomes_owned_sql_ready_event() {
    let DecodedRecord::Valid(event) = decode(enriched(Some(event()))) else {
        panic!("expected valid event")
    };
    assert_eq!(event.event_id, Uuid::parse_str(ID).unwrap());
    assert_eq!(
        event.event_time,
        UtcTimestamp {
            seconds: 1_700_000_000,
            nanos: 123_000_000
        }
    );
    assert_eq!(
        event.processed_at,
        UtcTimestamp {
            seconds: 1_700_000_001,
            nanos: 456_000_000
        }
    );
    assert_eq!(event.attributes, json!({"region": "west"}));
    assert_eq!((event.latency_us, event.status_code), (1_000, 200));
}

#[test]
fn malformed_protobuf_retains_original_payload_and_source_without_event_id() {
    let raw = vec![0xff, 0x00];
    let src = source(raw.clone());
    let decoded = decode_record(
        src,
        Timestamp {
            seconds: 1_700_000_002,
            nanos: 0,
        },
    );
    let dlq = expect_dlq(decoded, "decode", "");
    assert_eq!(dlq.original_payload, raw);
}

#[test]
fn missing_event_is_validation_dlq() {
    expect_dlq(decode(enriched(None)), "validation", "");
}

#[test]
fn malformed_uuid_is_not_copied_to_dlq() {
    let mut e = event();
    e.event_id = "not-a-uuid".into();
    expect_dlq(decode(enriched(Some(e))), "validation", "");
}

#[test]
fn missing_or_out_of_range_timestamps_are_dlq() {
    let mut missing_event_time = event();
    missing_event_time.event_time = None;
    expect_dlq(decode(enriched(Some(missing_event_time))), "timestamp", ID);
    let mut e = enriched(Some(event()));
    e.processed_at = None;
    expect_dlq(decode(e), "timestamp", ID);
    for ts in [
        Timestamp {
            seconds: 1,
            nanos: -1,
        },
        Timestamp {
            seconds: 1,
            nanos: 1_000_000_000,
        },
        Timestamp {
            seconds: -62_135_596_801,
            nanos: 0,
        },
        Timestamp {
            seconds: 253_402_300_800,
            nanos: 0,
        },
    ] {
        let mut e = event();
        e.event_time = Some(ts);
        expect_dlq(decode(enriched(Some(e))), "timestamp", ID);
    }
    for ts in [
        Timestamp {
            seconds: 1,
            nanos: -1,
        },
        Timestamp {
            seconds: 1,
            nanos: 1_000_000_000,
        },
        Timestamp {
            seconds: -62_135_596_801,
            nanos: 0,
        },
        Timestamp {
            seconds: 253_402_300_800,
            nanos: 0,
        },
    ] {
        let mut input = enriched(Some(event()));
        input.processed_at = Some(ts);
        expect_dlq(decode(input), "timestamp", ID);
    }
}

#[test]
fn required_text_and_postgres_text_nul_safety_are_enforced() {
    for (service, route) in [
        ("", "/r"),
        ("svc", ""),
        ("svc\0x", "/r"),
        ("svc", "/r\0x"),
        ("svc", &"é".repeat(129)),
    ] {
        let mut e = event();
        e.service_name = service.into();
        e.route = route.into();
        expect_dlq(decode(enriched(Some(e))), "validation", ID);
    }
    let mut e = event();
    e.trace_id = "bad\0trace".into();
    expect_dlq(decode(enriched(Some(e))), "validation", ID);
    for (key, value) in [("bad\0key", "v"), ("key", "bad\0value")] {
        let mut e = event();
        e.attributes = HashMap::from([(key.into(), value.into())]);
        expect_dlq(decode(enriched(Some(e))), "validation", ID);
    }
}

#[test]
fn sql_integer_ranges_and_detector_values_are_enforced() {
    let mut e = event();
    e.latency_us = i64::MAX as u64 + 1;
    expect_dlq(decode(enriched(Some(e))), "validation", ID);
    let mut e = event();
    e.status_code = i32::MAX as u32 + 1;
    expect_dlq(decode(enriched(Some(e))), "validation", ID);
    for invalid in [-1.0, f64::INFINITY, f64::NAN] {
        for field in 0..3 {
            let mut input = enriched(Some(event()));
            match field {
                0 => input.ewma_mean_us = invalid,
                1 => input.ewma_stddev_us = invalid,
                _ => input.anomaly_score = invalid,
            }
            expect_dlq(decode(input), "validation", ID);
        }
    }
}

#[test]
fn source_metadata_and_safely_parsed_event_id_are_preserved_on_validation_dlq() {
    let mut e = event();
    e.route.clear();
    let dlq = expect_dlq(decode(enriched(Some(e))), "validation", ID);
    assert!(!dlq.reason.is_empty());
}

#[test]
fn invalid_kafka_metadata_is_rejected_before_the_decoder_can_return_a_dlq() {
    for (topic, partition, offset, expected_error) in [
        ("", 3, 42, SourceMetadataError::EmptyTopic),
        ("bad\0topic", 3, 42, SourceMetadataError::TopicContainsNul),
        (
            "telemetry.enriched",
            -1,
            42,
            SourceMetadataError::NegativePartition,
        ),
        (
            "telemetry.enriched",
            3,
            -1,
            SourceMetadataError::NegativeOffset,
        ),
    ] {
        let result = SourceRecord::try_new(
            enriched(Some(event())).encode_to_vec(),
            topic,
            partition,
            offset,
        );
        assert_eq!(result.unwrap_err(), expected_error);
    }
}

#[test]
fn valid_source_record_exposes_immutable_metadata_for_dlq_envelopes() {
    let raw = vec![0xff];
    let src = SourceRecord::try_new(raw.clone(), "topic", 2, 19).unwrap();
    assert_eq!(src.payload(), raw);
    assert_eq!(src.topic(), "topic");
    assert_eq!(src.partition(), 2);
    assert_eq!(src.offset(), 19);
}
