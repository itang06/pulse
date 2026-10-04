use std::time::Duration;

use crate::Error;

/// Runtime settings for the telemetry client and its background exporter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientConfig {
    /// Unary gRPC gateway endpoint.
    pub endpoint: String,
    /// Maximum number of commands retained in memory before new events drop.
    pub queue_capacity: usize,
    /// Maximum events in one export request.
    pub max_batch_size: usize,
    /// Maximum time a partial batch waits before export.
    pub flush_interval: Duration,
    /// Deadline for one unary export attempt.
    pub rpc_timeout: Duration,
    /// Initial delay before retrying a transient export failure.
    pub retry_initial: Duration,
    /// Maximum delay between transient export retries.
    pub retry_max: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:50051".to_owned(),
            queue_capacity: 65_536,
            max_batch_size: 500,
            flush_interval: Duration::from_millis(5),
            rpc_timeout: Duration::from_secs(2),
            retry_initial: Duration::from_millis(50),
            retry_max: Duration::from_secs(5),
        }
    }
}

impl ClientConfig {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        if self.queue_capacity == 0 {
            return Err(Error::InvalidQueueCapacity);
        }
        if !(1..=500).contains(&self.max_batch_size) {
            return Err(Error::InvalidMaxBatchSize(self.max_batch_size));
        }
        if self.flush_interval.is_zero() {
            return Err(Error::InvalidFlushInterval);
        }
        if self.rpc_timeout.is_zero() {
            return Err(Error::InvalidRpcTimeout);
        }
        if self.retry_initial.is_zero() {
            return Err(Error::InvalidRetryInitial);
        }
        if self.retry_max < self.retry_initial {
            return Err(Error::InvalidRetryRange);
        }

        Ok(())
    }
}
