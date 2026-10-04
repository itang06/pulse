//! Bounded keyed ownership for EWMA detector state.
//!
//! The linked map is ordered least- to most-recently-used. Expired entries
//! therefore form a prefix and can be swept without scanning live keys.

use std::time::{Duration, Instant};

use hashlink::LinkedHashMap;

use crate::detector::{DetectorConfig, EwmaDetector, ScoreResult};

pub const DEFAULT_MAX_KEYS: usize = 10_000;
pub const DEFAULT_INACTIVE_TTL: Duration = Duration::from_secs(15 * 60);

#[derive(Clone, Copy, Debug)]
pub struct StateStoreConfig {
    pub max_keys: usize,
    pub inactive_ttl: Duration,
    pub detector: DetectorConfig,
}

impl Default for StateStoreConfig {
    fn default() -> Self {
        Self {
            max_keys: DEFAULT_MAX_KEYS,
            inactive_ttl: DEFAULT_INACTIVE_TTL,
            detector: DetectorConfig::default(),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DetectorKey {
    pub service_name: String,
    pub route: String,
}

struct KeyState {
    detector: EwmaDetector,
    last_seen: Instant,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EvictionCounts {
    pub ttl: usize,
    pub capacity: usize,
}

impl EvictionCounts {
    pub fn total(self) -> usize {
        self.ttl + self.capacity
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct StoreOutcome {
    pub score: ScoreResult,
    pub active_key_count: usize,
    pub evictions: EvictionCounts,
    /// True when this observation did not have enough prior samples for a
    /// verdict. This is useful to account for warm-up suppression in metrics.
    pub warmup_suppressed: bool,
}

/// Sole owner of all keyed detector state.
pub struct DetectorStateStore {
    config: StateStoreConfig,
    keys: LinkedHashMap<DetectorKey, KeyState>,
    last_successful_observation: Option<Instant>,
}

impl DetectorStateStore {
    pub fn new(config: StateStoreConfig) -> Result<Self, &'static str> {
        if config.max_keys == 0 {
            return Err("maximum key count must be greater than zero");
        }
        if config.inactive_ttl.is_zero() {
            return Err("inactive TTL must be greater than zero");
        }
        // Validate detector settings once when constructing the store.
        EwmaDetector::new(config.detector)?;
        Ok(Self {
            config,
            keys: LinkedHashMap::new(),
            last_successful_observation: None,
        })
    }

    pub fn active_key_count(&self) -> usize {
        self.keys.len()
    }

    /// Discards every detector baseline on an assignment epoch change,
    /// including keys for partitions that remain assigned.
    pub fn clear(&mut self) {
        self.keys.clear();
        self.last_successful_observation = None;
    }

    /// Score one observation at a caller-supplied monotonic time.
    ///
    /// Failed observations are transactional with respect to the store: they
    /// neither create/touch a key nor run TTL/capacity eviction.
    pub fn observe_at(
        &mut self,
        service_name: impl Into<String>,
        route: impl Into<String>,
        value_us: f64,
        now: Instant,
    ) -> Result<StoreOutcome, &'static str> {
        if self
            .last_successful_observation
            .is_some_and(|last_successful| now < last_successful)
        {
            return Err("observation time must not move backwards");
        }

        // Reject obvious invalid values before any possible store mutation.
        if !value_us.is_finite() {
            return Err("observation must be finite");
        }

        let key = DetectorKey {
            service_name: service_name.into(),
            route: route.into(),
        };
        let is_active = self.keys.get(&key).is_some_and(|state| {
            now.saturating_duration_since(state.last_seen) < self.config.inactive_ttl
        });

        // Score before recency/eviction bookkeeping. EwmaDetector leaves its
        // baseline unchanged on error, so an overflowing sample is also inert.
        let (score, new_detector) = if is_active {
            (
                self.keys
                    .get_mut(&key)
                    .expect("active key exists")
                    .detector
                    .score(value_us)?,
                None,
            )
        } else {
            let mut detector = EwmaDetector::new(self.config.detector)?;
            let score = detector.score(value_us)?;
            (score, Some(detector))
        };

        let mut evictions = self.evict_expired(now);
        if is_active {
            let state = self
                .keys
                .to_back(&key)
                .expect("active key survived TTL sweep");
            state.last_seen = now;
        } else {
            if self.keys.len() == self.config.max_keys {
                self.keys.pop_front();
                evictions.capacity += 1;
            }
            self.keys.insert(
                key,
                KeyState {
                    detector: new_detector.expect("new key has a fresh detector"),
                    last_seen: now,
                },
            );
        }

        self.last_successful_observation = Some(now);
        Ok(StoreOutcome {
            score,
            active_key_count: self.keys.len(),
            evictions,
            warmup_suppressed: !score.warmed_up,
        })
    }

    fn evict_expired(&mut self, now: Instant) -> EvictionCounts {
        let mut evictions = EvictionCounts::default();
        loop {
            let expired = self.keys.front().is_some_and(|(_, state)| {
                now.saturating_duration_since(state.last_seen) >= self.config.inactive_ttl
            });
            if !expired {
                break;
            }
            self.keys.pop_front();
            evictions.ttl += 1;
        }
        evictions
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::detector::DetectorConfig;
    use std::time::{Duration, Instant};

    fn store(max_keys: usize, ttl: Duration, warmup_observations: usize) -> DetectorStateStore {
        DetectorStateStore::new(StateStoreConfig {
            max_keys,
            inactive_ttl: ttl,
            detector: DetectorConfig {
                warmup_observations,
                ..DetectorConfig::default()
            },
        })
        .unwrap()
    }

    fn observe(
        store: &mut DetectorStateStore,
        service: &str,
        route: &str,
        value: f64,
        now: Instant,
    ) -> StoreOutcome {
        store.observe_at(service, route, value, now).unwrap()
    }

    #[test]
    fn same_service_and_route_share_a_baseline_but_other_keys_are_isolated() {
        let mut store = store(10, Duration::from_secs(60), 1);
        let now = Instant::now();
        observe(&mut store, "api", "/a", 10.0, now);
        observe(&mut store, "api", "/b", 20.0, now);
        let same = observe(&mut store, "api", "/a", 12.0, now);
        let other = observe(&mut store, "api", "/b", 22.0, now);
        assert_eq!(same.score.previous_mean_us, Some(10.0));
        assert_eq!(other.score.previous_mean_us, Some(20.0));
        assert_eq!(same.active_key_count, 2);
    }

    #[test]
    fn clearing_discards_every_baseline_and_time_watermark() {
        let mut store = store(10, Duration::from_secs(60), 1);
        let now = Instant::now();
        observe(&mut store, "svc", "/a", 10.0, now);
        observe(&mut store, "svc", "/b", 20.0, now + Duration::from_secs(1));
        assert_eq!(store.active_key_count(), 2);

        store.clear();

        assert_eq!(store.active_key_count(), 0);
        let reset = observe(&mut store, "svc", "/a", 99.0, now);
        assert_eq!(reset.score.observation_count, 1);
        assert_eq!(reset.score.previous_mean_us, None);
    }

    #[test]
    fn least_recently_used_key_is_evicted_and_recent_access_refreshes_recency() {
        let mut store = store(2, Duration::from_secs(60), 10);
        let now = Instant::now();
        observe(&mut store, "svc", "/a", 1.0, now);
        observe(&mut store, "svc", "/b", 1.0, now);
        observe(&mut store, "svc", "/a", 2.0, now);
        let inserted = observe(&mut store, "svc", "/c", 1.0, now);
        assert_eq!(inserted.evictions.capacity, 1);
        let preserved = observe(&mut store, "svc", "/a", 3.0, now);
        assert_eq!(preserved.score.observation_count, 3);
        let reset = observe(&mut store, "svc", "/b", 4.0, now);
        assert_eq!(reset.score.observation_count, 1);
        assert_eq!(reset.score.previous_mean_us, None);
        assert_eq!(reset.evictions.capacity, 1);
    }

    #[test]
    fn ttl_expires_at_the_boundary_and_reintroduced_key_starts_fresh() {
        let ttl = Duration::from_secs(15);
        let start = Instant::now();
        let mut store = store(10, ttl, 10);
        observe(&mut store, "svc", "/a", 1.0, start);
        let outcome = observe(&mut store, "svc", "/b", 2.0, start + ttl);
        assert_eq!(outcome.evictions.ttl, 1);
        let reintroduced = observe(&mut store, "svc", "/a", 9.0, start + ttl);
        assert_eq!(reintroduced.score.observation_count, 1);
        assert_eq!(reintroduced.score.previous_mean_us, None);
    }

    #[test]
    fn successful_access_refreshes_last_seen_for_ttl() {
        let ttl = Duration::from_secs(15);
        let start = Instant::now();
        let mut store = store(10, ttl, 10);
        observe(&mut store, "svc", "/a", 1.0, start);
        observe(
            &mut store,
            "svc",
            "/a",
            2.0,
            start + Duration::from_secs(10),
        );
        let before_boundary = observe(
            &mut store,
            "svc",
            "/a",
            3.0,
            start + Duration::from_secs(24),
        );
        assert_eq!(before_boundary.score.observation_count, 3);
        let at_boundary = observe(
            &mut store,
            "svc",
            "/b",
            1.0,
            start + Duration::from_secs(39),
        );
        assert_eq!(at_boundary.evictions.ttl, 1);
    }

    #[test]
    fn backward_time_is_rejected_without_breaking_ttl_prefix_order() {
        let start = Instant::now();
        let mut store = store(2, Duration::from_secs(15), 10);
        observe(&mut store, "svc", "/a", 1.0, start);
        observe(
            &mut store,
            "svc",
            "/b",
            1.0,
            start + Duration::from_secs(10),
        );

        assert!(store
            .observe_at("svc", "/a", 2.0, start + Duration::from_secs(5))
            .is_err());
        assert_eq!(store.active_key_count(), 2);

        let at_twenty = observe(
            &mut store,
            "svc",
            "/c",
            1.0,
            start + Duration::from_secs(20),
        );
        assert_eq!(at_twenty.evictions.ttl, 1);
        assert_eq!(at_twenty.evictions.capacity, 0);
        assert_eq!(at_twenty.active_key_count, 2);

        let at_twenty_six = observe(
            &mut store,
            "svc",
            "/d",
            1.0,
            start + Duration::from_secs(26),
        );
        assert_eq!(at_twenty_six.evictions.ttl, 1);
        assert_eq!(at_twenty_six.evictions.capacity, 0);
        assert_eq!(at_twenty_six.active_key_count, 2);
    }

    #[test]
    fn failed_later_score_does_not_advance_the_time_watermark() {
        let start = Instant::now();
        let mut store = store(2, Duration::from_secs(60), 10);
        observe(&mut store, "svc", "/a", 1.0, start);
        assert!(store
            .observe_at("svc", "/a", f64::MAX, start + Duration::from_secs(10))
            .is_err());

        let earlier_but_still_monotonic_success =
            observe(&mut store, "svc", "/b", 1.0, start + Duration::from_secs(5));
        assert_eq!(earlier_but_still_monotonic_success.active_key_count, 2);
        assert_eq!(earlier_but_still_monotonic_success.evictions.total(), 0);
    }

    #[test]
    fn capacity_is_never_exceeded_and_eviction_restarts_warmup() {
        let now = Instant::now();
        let mut store = store(2, Duration::from_secs(60), 2);
        observe(&mut store, "svc", "/a", 1.0, now);
        let warmup = observe(&mut store, "svc", "/a", 2.0, now);
        assert!(warmup.warmup_suppressed);
        observe(&mut store, "svc", "/b", 1.0, now);
        let eviction = observe(&mut store, "svc", "/c", 1.0, now);
        assert_eq!(eviction.active_key_count, 2);
        assert_eq!(eviction.evictions.total(), 1);
        let reintroduced = observe(&mut store, "svc", "/a", 5.0, now);
        assert_eq!(reintroduced.score.observation_count, 1);
        assert!(reintroduced.warmup_suppressed);
        assert!(reintroduced.active_key_count <= 2);
    }

    #[test]
    fn invalid_observation_does_not_create_refresh_or_evict_state() {
        let mut store = store(1, Duration::from_secs(60), 10);
        let start = Instant::now();
        observe(&mut store, "svc", "/a", 1.0, start);
        assert!(store
            .observe_at("svc", "/b", f64::NAN, start + Duration::from_secs(2))
            .is_err());
        assert_eq!(store.active_key_count(), 1);
        let existing = observe(&mut store, "svc", "/a", 2.0, start + Duration::from_secs(2));
        assert_eq!(existing.score.observation_count, 2);
        assert_eq!(existing.evictions.total(), 0);
        assert_eq!(existing.active_key_count, 1);
        assert!(store
            .observe_at("svc", "/a", f64::MAX, start + Duration::from_secs(3))
            .is_err());
        assert_eq!(store.active_key_count(), 1);
        let still_existing = observe(&mut store, "svc", "/a", 3.0, start + Duration::from_secs(3));
        assert_eq!(still_existing.score.observation_count, 3);
    }

    #[test]
    fn configuration_rejects_zero_capacity_or_ttl() {
        for config in [
            StateStoreConfig {
                max_keys: 0,
                inactive_ttl: Duration::from_secs(1),
                detector: DetectorConfig::default(),
            },
            StateStoreConfig {
                max_keys: 1,
                inactive_ttl: Duration::ZERO,
                detector: DetectorConfig::default(),
            },
        ] {
            assert!(DetectorStateStore::new(config).is_err());
        }
    }
}
