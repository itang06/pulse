use serde_json::Value;
use uuid::Uuid;

/// Invalid metadata supplied by a Kafka adapter while constructing a source record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceMetadataError {
    EmptyTopic,
    TopicContainsNul,
    NegativePartition,
    NegativeOffset,
}

impl std::fmt::Display for SourceMetadataError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::EmptyTopic => "Kafka source topic must not be empty",
            Self::TopicContainsNul => "Kafka source topic must not contain NUL",
            Self::NegativePartition => "Kafka source partition must be nonnegative",
            Self::NegativeOffset => "Kafka source offset must be nonnegative",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for SourceMetadataError {}

/// Kafka data copied into owned memory before any asynchronous work.
///
/// Metadata can only enter this model after it passes basic Kafka invariants,
/// so the payload decoder never mistakes adapter/transport faults for poison
/// input that should be published to the DLQ.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceRecord {
    payload: Vec<u8>,
    topic: String,
    partition: i32,
    offset: i64,
}

impl SourceRecord {
    pub fn try_new(
        payload: Vec<u8>,
        topic: impl Into<String>,
        partition: i32,
        offset: i64,
    ) -> Result<Self, SourceMetadataError> {
        let topic = topic.into();
        if topic.is_empty() {
            return Err(SourceMetadataError::EmptyTopic);
        }
        if topic.contains('\0') {
            return Err(SourceMetadataError::TopicContainsNul);
        }
        if partition < 0 {
            return Err(SourceMetadataError::NegativePartition);
        }
        if offset < 0 {
            return Err(SourceMetadataError::NegativeOffset);
        }
        Ok(Self {
            payload,
            topic,
            partition,
            offset,
        })
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn partition(&self) -> i32 {
        self.partition
    }

    pub fn offset(&self) -> i64 {
        self.offset
    }

    pub fn into_parts(self) -> (Vec<u8>, String, i32, i64) {
        (self.payload, self.topic, self.partition, self.offset)
    }
}

/// A protobuf UTC instant represented as seconds and fractional nanoseconds
/// from the Unix epoch. The decoder checks protobuf's year 0001..9999 range.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UtcTimestamp {
    pub seconds: i64,
    pub nanos: i32,
}

/// A validated enriched event ready for database binding.
#[derive(Clone, Debug, PartialEq)]
pub struct ValidatedEvent {
    pub event_id: Uuid,
    pub event_time: UtcTimestamp,
    pub service_name: String,
    pub route: String,
    pub latency_us: i64,
    pub status_code: i32,
    pub trace_id: String,
    pub attributes: Value,
    pub ewma_mean_us: f64,
    pub ewma_stddev_us: f64,
    pub anomaly_score: f64,
    pub is_anomaly: bool,
    pub processed_at: UtcTimestamp,
}

/// The decoder has no infrastructure-error variant: only malformed or
/// permanently invalid source input can become a dead letter.
#[derive(Clone, Debug, PartialEq)]
pub enum DecodedRecord {
    Valid(ValidatedEvent),
    DeadLetter(Vec<u8>),
}
