use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Point-in-time view of the SDK's local delivery counters.
///
/// Counters are intentionally read without a global lock. During concurrent
/// emission, different fields can therefore represent adjacent instants.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub attempted: u64,
    pub queued: u64,
    pub invalid_events: u64,
    pub dropped_full: u64,
    pub dropped_closed: u64,
    pub acknowledged: u64,
    pub batch_retries: u64,
    pub permanent_failures: u64,
    pub queue_depth: usize,
}

#[derive(Debug, Default)]
pub(crate) struct Metrics {
    pub(crate) attempted: AtomicU64,
    pub(crate) queued: AtomicU64,
    pub(crate) invalid_events: AtomicU64,
    pub(crate) dropped_full: AtomicU64,
    pub(crate) dropped_closed: AtomicU64,
    pub(crate) acknowledged: AtomicU64,
    pub(crate) batch_retries: AtomicU64,
    pub(crate) permanent_failures: AtomicU64,
    pub(crate) queue_depth: AtomicUsize,
}

impl Metrics {
    pub(crate) fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            attempted: self.attempted.load(Ordering::Relaxed),
            queued: self.queued.load(Ordering::Relaxed),
            invalid_events: self.invalid_events.load(Ordering::Relaxed),
            dropped_full: self.dropped_full.load(Ordering::Relaxed),
            dropped_closed: self.dropped_closed.load(Ordering::Relaxed),
            acknowledged: self.acknowledged.load(Ordering::Relaxed),
            batch_retries: self.batch_retries.load(Ordering::Relaxed),
            permanent_failures: self.permanent_failures.load(Ordering::Relaxed),
            queue_depth: self.queue_depth.load(Ordering::Relaxed),
        }
    }
}
