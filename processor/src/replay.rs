//! Deterministic offline anomaly replay using the production keyed detector store.
//! Replay runs immediately because all evaluation timing uses synthetic event time.

use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use anyhow::{ensure, Result};
use rand::{rngs::StdRng, RngExt, SeedableRng};
use serde::{Deserialize, Serialize};

use crate::state::{DetectorStateStore, StateStoreConfig};

pub const DEFAULT_SEED: u64 = 20261005;
pub const DEFAULT_EVENT_COUNT: usize = 100_000;
pub const DEFAULT_WINDOW_COUNT: usize = 25;
const KEY_COUNT: usize = 10;
const FIRST_INJECTION_INDEX: usize = 10_000;
const INJECTION_INTERVAL: usize = 3_000;
const WINDOW_DURATION_MS: u64 = 1_000;
/// Maximum accepted label-span duration, tied to the generated one-second injection window.
const MAX_LABEL_WINDOW_DURATION_MS: u64 = WINDOW_DURATION_MS;
const DETECTION_DEADLINE_MS: u64 = 5_000;
const ANOMALY_LATENCY_US: u64 = 60_000;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReplayEvent {
    pub event_time_ms: u64,
    pub service_name: String,
    pub route: String,
    pub latency_us: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AnomalyWindow {
    pub service_name: String,
    pub route: String,
    pub injected_at_ms: u64,
    pub window_end_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReplayManifest {
    pub seed: u64,
    pub events: Vec<ReplayEvent>,
    pub windows: Vec<AnomalyWindow>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
pub struct DetectorSettings {
    pub alpha: f64,
    pub warmup_observations: usize,
    pub threshold: f64,
    pub stddev_floor_us: f64,
    pub max_keys: usize,
    pub inactive_ttl_secs: u64,
}

impl Default for DetectorSettings {
    fn default() -> Self {
        let config = StateStoreConfig::default();
        Self {
            alpha: config.detector.alpha,
            warmup_observations: config.detector.warmup_observations,
            threshold: config.detector.threshold,
            stddev_floor_us: config.detector.stddev_floor_us,
            max_keys: config.max_keys,
            inactive_ttl_secs: config.inactive_ttl.as_secs(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WindowResult {
    pub service_name: String,
    pub route: String,
    pub injected_at_ms: u64,
    pub detected_at_ms: Option<u64>,
    pub detection_latency_ms: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct FalsePositive {
    pub event_time_ms: u64,
    pub service_name: String,
    pub route: String,
    pub latency_us: u64,
    pub score: f64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ReplayReport {
    pub seed: u64,
    pub event_count: usize,
    pub anomaly_window_count: usize,
    pub detector: DetectorSettings,
    pub window_results: Vec<WindowResult>,
    pub missed_windows: usize,
    pub max_detection_latency_ms: Option<u64>,
    pub false_positive_count: usize,
    pub false_positive_events: Vec<FalsePositive>,
    #[serde(rename = "pass")]
    pub passed: bool,
}

/// Generates a monotonic one-millisecond event-time stream with 25 labeled ramps.
/// The event payloads intentionally contain no anomaly label; labels live in `windows`.
pub fn generate_manifest(seed: u64, event_count: usize) -> ReplayManifest {
    let keys = (0..KEY_COUNT)
        .map(|index| {
            (
                format!("pulse-service-{}", index / 2 + 1),
                format!("/v1/resource-{}/{{id}}", index % 5 + 1),
            )
        })
        .collect::<Vec<_>>();

    let mut windows = Vec::with_capacity(DEFAULT_WINDOW_COUNT);
    let mut ramp_samples = HashMap::with_capacity(DEFAULT_WINDOW_COUNT * 101);
    for window_index in 0..DEFAULT_WINDOW_COUNT {
        let event_index =
            FIRST_INJECTION_INDEX + window_index * INJECTION_INTERVAL + window_index % KEY_COUNT;
        let key_index = event_index % KEY_COUNT;
        let (service_name, route) = &keys[key_index];
        let injected_at_ms = event_index as u64;
        windows.push(AnomalyWindow {
            service_name: service_name.clone(),
            route: route.clone(),
            injected_at_ms,
            window_end_ms: injected_at_ms + WINDOW_DURATION_MS,
        });
        for sample in 0..=WINDOW_DURATION_MS / KEY_COUNT as u64 {
            let sample_index = event_index + sample as usize * KEY_COUNT;
            ramp_samples.insert(sample_index, sample as usize);
        }
    }

    let mut rng = StdRng::seed_from_u64(seed);
    let events = (0..event_count)
        .map(|event_index| {
            let (service_name, route) = &keys[event_index % KEY_COUNT];
            let baseline_us = 10_000 + rng.random_range(0..=500);
            let latency_us = match ramp_samples.get(&event_index) {
                Some(sample) if *sample < 80 => baseline_us + (*sample as u64 * 250),
                Some(_) => ANOMALY_LATENCY_US,
                None => baseline_us,
            };
            ReplayEvent {
                event_time_ms: event_index as u64,
                service_name: service_name.clone(),
                route: route.clone(),
                latency_us,
            }
        })
        .collect();

    ReplayManifest {
        seed,
        events,
        windows,
    }
}

/// Replays every event through the same `DetectorStateStore` used by the live processor.
pub fn evaluate(manifest: &ReplayManifest) -> Result<ReplayReport> {
    ensure!(
        manifest
            .events
            .windows(2)
            .all(|pair| pair[0].event_time_ms < pair[1].event_time_ms),
        "replay event timestamps must be monotonic"
    );
    validate_windows(manifest)?;
    let mut store = DetectorStateStore::new(StateStoreConfig::default())
        .map_err(|error| anyhow::anyhow!("invalid detector state config: {error}"))?;
    let origin = Instant::now();
    let mut verdicts = Vec::new();

    for event in &manifest.events {
        let now = origin
            .checked_add(Duration::from_millis(event.event_time_ms))
            .ok_or_else(|| anyhow::anyhow!("event time exceeds monotonic clock range"))?;
        let outcome = store
            .observe_at(
                &event.service_name,
                &event.route,
                event.latency_us as f64,
                now,
            )
            .map_err(|error| anyhow::anyhow!("scoring replay event failed: {error}"))?;
        if outcome.score.anomaly {
            verdicts.push((verdicts.len(), event, outcome.score.score));
        }
    }

    let mut consumed_verdicts = HashSet::new();
    let mut window_results = Vec::with_capacity(manifest.windows.len());
    for window in &manifest.windows {
        let detection = verdicts.iter().find(|(verdict_index, event, _)| {
            !consumed_verdicts.contains(verdict_index)
                && event.service_name == window.service_name
                && event.route == window.route
                && event.event_time_ms >= window.injected_at_ms
                && event.event_time_ms
                    <= window.injected_at_ms.saturating_add(DETECTION_DEADLINE_MS)
        });
        if let Some((verdict_index, _, _)) = detection {
            consumed_verdicts.insert(*verdict_index);
        }
        let detected_at_ms = detection.map(|(_, event, _)| event.event_time_ms);
        window_results.push(WindowResult {
            service_name: window.service_name.clone(),
            route: window.route.clone(),
            injected_at_ms: window.injected_at_ms,
            detected_at_ms,
            detection_latency_ms: detected_at_ms
                .map(|detected| detected.saturating_sub(window.injected_at_ms)),
        });
    }

    let false_positive_events = verdicts
        .iter()
        .filter(|(verdict_index, event, _)| {
            !manifest.windows.iter().any(|window| {
                event.service_name == window.service_name
                    && event.route == window.route
                    && event.event_time_ms >= window.injected_at_ms
                    && event.event_time_ms <= window.window_end_ms
            }) && !consumed_verdicts.contains(verdict_index)
        })
        .map(|(_, event, score)| FalsePositive {
            event_time_ms: event.event_time_ms,
            service_name: event.service_name.clone(),
            route: event.route.clone(),
            latency_us: event.latency_us,
            score: *score,
        })
        .collect::<Vec<_>>();
    let latencies = window_results
        .iter()
        .filter_map(|window| window.detection_latency_ms)
        .collect::<Vec<_>>();
    let missed_windows = window_results
        .iter()
        .filter(|window| window.detected_at_ms.is_none())
        .count();
    let max_detection_latency_ms = latencies.iter().copied().max();

    let passed = manifest.events.len() >= DEFAULT_EVENT_COUNT
        && manifest.windows.len() >= DEFAULT_WINDOW_COUNT
        && missed_windows == 0
        && max_detection_latency_ms.is_some_and(|latency| latency <= DETECTION_DEADLINE_MS);
    Ok(ReplayReport {
        seed: manifest.seed,
        event_count: manifest.events.len(),
        anomaly_window_count: manifest.windows.len(),
        detector: DetectorSettings::default(),
        window_results,
        missed_windows,
        max_detection_latency_ms,
        false_positive_count: false_positive_events.len(),
        false_positive_events,
        passed,
    })
}

fn validate_windows(manifest: &ReplayManifest) -> Result<()> {
    let mut identities = HashSet::with_capacity(manifest.windows.len());
    for window in &manifest.windows {
        ensure!(
            window.window_end_ms >= window.injected_at_ms,
            "anomaly window end must not precede injection"
        );
        ensure!(
            window.window_end_ms.saturating_sub(window.injected_at_ms)
                <= MAX_LABEL_WINDOW_DURATION_MS,
            "anomaly label span must not exceed the generated one-second duration"
        );
        ensure!(
            identities.insert((
                window.service_name.as_str(),
                window.route.as_str(),
                window.injected_at_ms,
            )),
            "anomaly window identity must be unique"
        );
        ensure!(
            manifest.events.iter().any(|event| {
                event.event_time_ms == window.injected_at_ms
                    && event.service_name == window.service_name
                    && event.route == window.route
            }),
            "each anomaly window must have a matching event at injection time"
        );
    }

    let mut ordered_windows = manifest.windows.iter().collect::<Vec<_>>();
    ordered_windows.sort_by(|left, right| {
        (&left.service_name, &left.route, left.injected_at_ms).cmp(&(
            &right.service_name,
            &right.route,
            right.injected_at_ms,
        ))
    });
    for pair in ordered_windows.windows(2) {
        let (left, right) = (pair[0], pair[1]);
        if left.service_name == right.service_name && left.route == right.route {
            ensure!(
                right.injected_at_ms > left.window_end_ms,
                "label spans for the same key must not overlap"
            );
            ensure!(
                right.injected_at_ms > left.injected_at_ms.saturating_add(DETECTION_DEADLINE_MS),
                "detection intervals for the same key must not overlap"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        evaluate, generate_manifest, AnomalyWindow, ReplayEvent, ReplayManifest,
        DEFAULT_EVENT_COUNT, DEFAULT_WINDOW_COUNT,
    };
    use std::collections::HashSet;

    #[test]
    fn default_replay_is_deterministic_and_detects_every_labeled_window() {
        let manifest = generate_manifest(42, DEFAULT_EVENT_COUNT);
        let same_manifest = generate_manifest(42, DEFAULT_EVENT_COUNT);
        assert_eq!(manifest, same_manifest);
        assert!(manifest.events.len() >= 100_000);
        assert_eq!(manifest.windows.len(), 25);
        assert_eq!(manifest.windows.len(), DEFAULT_WINDOW_COUNT);

        let keys = manifest
            .events
            .iter()
            .map(|event| (event.service_name.as_str(), event.route.as_str()))
            .collect::<HashSet<_>>();
        assert!(!keys.is_empty());
        assert!(keys.len() <= 32, "replay keys should remain bounded");
        assert!(keys.iter().all(|(service, route)| {
            !service.is_empty()
                && service
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
                && route.starts_with('/')
                && route.bytes().all(|byte| {
                    byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || byte == b'/'
                        || byte == b'-'
                        || byte == b'{'
                        || byte == b'}'
                })
        }));

        let report = evaluate(&manifest).unwrap();
        let same_report = evaluate(&same_manifest).unwrap();
        assert_eq!(report, same_report);
        assert_eq!(report.window_results.len(), 25);
        assert!(report
            .window_results
            .iter()
            .all(|window| window.detected_at_ms.is_some()));
        let detected_times = report
            .window_results
            .iter()
            .filter_map(|window| window.detected_at_ms)
            .collect::<HashSet<_>>();
        assert_eq!(detected_times.len(), DEFAULT_WINDOW_COUNT);
        assert_eq!(report.missed_windows, 0);
        assert!(report.passed);
        let latencies = report
            .window_results
            .iter()
            .filter_map(|window| window.detection_latency_ms)
            .collect::<Vec<_>>();
        assert_eq!(latencies.len(), DEFAULT_WINDOW_COUNT);
        assert!(latencies
            .iter()
            .all(|latency| *latency > 0 && *latency <= 5_000));
        assert!(report.max_detection_latency_ms.unwrap() <= 5_000);
        assert_eq!(report.false_positive_count, 0);
        assert!(report.false_positive_events.is_empty());
    }

    #[test]
    fn false_positive_verdicts_are_included_in_the_report() {
        let mut manifest = generate_manifest(7, DEFAULT_EVENT_COUNT);
        manifest.events.push(ReplayEvent {
            event_time_ms: DEFAULT_EVENT_COUNT as u64 + 1,
            service_name: "pulse-service-1".into(),
            route: "/v1/resource-1/{id}".into(),
            latency_us: 60_000,
        });

        let report = evaluate(&manifest).unwrap();
        assert_eq!(report.false_positive_count, 1);
        assert_eq!(report.false_positive_events.len(), 1);
        assert_eq!(report.false_positive_events[0].event_time_ms, 100_001);
        assert!(
            report.passed,
            "false positives are reported, not an exit failure"
        );
    }

    #[test]
    fn detection_can_arrive_two_seconds_after_the_injection_window_ends() {
        let events = (0..=3_000)
            .map(|time| {
                event(
                    time,
                    "svc-a",
                    "/a",
                    if time == 3_000 { 60_000 } else { 10_000 },
                )
            })
            .collect();
        let manifest = ReplayManifest {
            seed: 1,
            events,
            windows: vec![window("svc-a", "/a", 1_000, 1_500)],
        };

        let report = evaluate(&manifest).unwrap();
        assert_eq!(report.window_results[0].detected_at_ms, Some(3_000));
        assert_eq!(report.window_results[0].detection_latency_ms, Some(2_000));
    }

    #[test]
    fn unrelated_key_verdict_inside_a_window_is_a_false_positive() {
        let events = (0..=300)
            .map(|time| {
                let (service, latency) = if time == 251 {
                    ("svc-b", 60_000)
                } else if time % 2 == 0 {
                    ("svc-a", 10_000)
                } else {
                    ("svc-b", 10_000)
                };
                event(time, service, "/r", latency)
            })
            .collect();
        let manifest = ReplayManifest {
            seed: 2,
            events,
            windows: vec![window("svc-a", "/r", 200, 300)],
        };

        let report = evaluate(&manifest).unwrap();
        assert_eq!(report.false_positive_count, 1);
        assert_eq!(report.false_positive_events[0].event_time_ms, 251);
    }

    #[test]
    fn duplicate_or_overlapping_key_windows_are_rejected() {
        let events: Vec<ReplayEvent> = (0..=10_000)
            .map(|time| event(time, "svc-a", "/a", 10_000))
            .collect();
        let first = window("svc-a", "/a", 1_000, 2_000);
        let duplicate = ReplayManifest {
            seed: 3,
            events: events.clone(),
            windows: vec![first.clone(), first],
        };
        assert!(evaluate(&duplicate).is_err());

        let overlapping = ReplayManifest {
            seed: 3,
            events,
            windows: vec![
                window("svc-a", "/a", 1_000, 2_000),
                window("svc-a", "/a", 3_000, 4_000),
            ],
        };
        assert!(evaluate(&overlapping).is_err());
    }

    #[test]
    fn overlapping_same_key_label_spans_are_rejected_even_when_detection_intervals_do_not_overlap()
    {
        let events: Vec<ReplayEvent> = (0..=12_000)
            .map(|time| event(time, "svc-a", "/a", 10_000))
            .collect();
        let manifest = ReplayManifest {
            seed: 4,
            events,
            windows: vec![
                window("svc-a", "/a", 1_000, 8_000),
                window("svc-a", "/a", 7_000, 8_000),
            ],
        };

        assert!(evaluate(&manifest).is_err());
    }

    #[test]
    fn label_spans_cannot_exceed_the_generated_one_second_duration() {
        let events: Vec<ReplayEvent> = (0..=2_001)
            .map(|time| event(time, "svc-a", "/a", 10_000))
            .collect();
        let manifest = ReplayManifest {
            seed: 5,
            events,
            windows: vec![window("svc-a", "/a", 1_000, 2_001)],
        };

        assert!(evaluate(&manifest).is_err());
    }

    #[test]
    fn undersized_event_or_window_counts_cannot_pass() {
        let short_stream = generate_manifest(5, DEFAULT_EVENT_COUNT - 1);
        assert!(!evaluate(&short_stream).unwrap().passed);

        let mut few_windows = generate_manifest(5, DEFAULT_EVENT_COUNT);
        few_windows.windows.pop();
        assert!(!evaluate(&few_windows).unwrap().passed);
    }

    fn event(time: u64, service: &str, route: &str, latency_us: u64) -> ReplayEvent {
        ReplayEvent {
            event_time_ms: time,
            service_name: service.into(),
            route: route.into(),
            latency_us,
        }
    }

    fn window(
        service: &str,
        route: &str,
        injected_at_ms: u64,
        window_end_ms: u64,
    ) -> AnomalyWindow {
        AnomalyWindow {
            service_name: service.into(),
            route: route.into(),
            injected_at_ms,
            window_end_ms,
        }
    }
}
