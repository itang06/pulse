//! Pure per-key EWMA latency scoring. State eviction belongs to the caller.

/// Default settings for the EWMA detector.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DetectorConfig {
    pub alpha: f64,
    pub warmup_observations: usize,
    pub threshold: f64,
    pub stddev_floor_us: f64,
}

impl Default for DetectorConfig {
    fn default() -> Self {
        Self {
            alpha: 0.1,
            warmup_observations: 100,
            threshold: 3.0,
            stddev_floor_us: 1_000.0,
        }
    }
}

impl DetectorConfig {
    fn validate(self) -> Result<(), &'static str> {
        if !self.alpha.is_finite() || self.alpha <= 0.0 || self.alpha >= 1.0 {
            return Err("alpha must be finite and strictly between 0 and 1");
        }
        if self.warmup_observations == 0 {
            return Err("warm-up observations must be greater than zero");
        }
        if !self.threshold.is_finite() || self.threshold <= 0.0 {
            return Err("threshold must be finite and greater than zero");
        }
        if !self.stddev_floor_us.is_finite() || self.stddev_floor_us <= 0.0 {
            return Err("standard-deviation floor must be finite and greater than zero");
        }
        Ok(())
    }
}

/// Outcome for one observation. Baseline fields describe the state before
/// this observation was incorporated; both are absent for the first sample.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ScoreResult {
    pub previous_mean_us: Option<f64>,
    pub previous_stddev_us: Option<f64>,
    pub score: f64,
    /// Number of samples in the baseline after this observation is applied.
    pub observation_count: usize,
    /// Whether enough prior samples existed to make a verdict for this sample.
    pub warmed_up: bool,
    pub anomaly: bool,
}

#[derive(Clone, Copy, Debug, Default)]
struct StreamState {
    mean_us: f64,
    variance_us2: f64,
    observation_count: usize,
}

/// EWMA state for one stream. A bounded state store owns key-to-detector mapping.
pub struct EwmaDetector {
    config: DetectorConfig,
    state: StreamState,
}

impl EwmaDetector {
    pub fn new(config: DetectorConfig) -> Result<Self, &'static str> {
        config.validate()?;
        Ok(Self {
            config,
            state: StreamState::default(),
        })
    }

    /// Scores against this stream's previous baseline, then incorporates the sample.
    pub fn score(&mut self, value_us: f64) -> Result<ScoreResult, &'static str> {
        if !value_us.is_finite() {
            return Err("observation must be finite");
        }

        let state = &mut self.state;
        let previous_mean_us = (state.observation_count > 0).then_some(state.mean_us);
        let previous_stddev_us = (state.observation_count > 0).then_some(state.variance_us2.sqrt());
        let warmed_up = state.observation_count >= self.config.warmup_observations;
        let score = match previous_mean_us {
            Some(mean) => {
                (value_us - mean).max(0.0)
                    / previous_stddev_us
                        .unwrap_or_default()
                        .max(self.config.stddev_floor_us)
            }
            None => 0.0,
        };
        if !score.is_finite() {
            return Err("observation would produce a non-finite score");
        }
        let anomaly = warmed_up && score >= self.config.threshold;

        let next = if state.observation_count == 0 {
            StreamState {
                mean_us: value_us,
                variance_us2: 0.0,
                observation_count: 1,
            }
        } else {
            let delta = value_us - state.mean_us;
            let alpha = self.config.alpha;
            // EWMA population variance recurrence: v' = (1-a) * (v + a*d^2),
            // where d is the residual from the prior mean. This stays aligned
            // with the mean update and starts at zero variance for the seed.
            let variance_us2 = (1.0 - alpha) * (state.variance_us2 + alpha * delta * delta);
            let mean_us = state.mean_us + alpha * delta;
            if !variance_us2.is_finite() || !mean_us.is_finite() {
                return Err("observation would overflow EWMA state");
            }
            StreamState {
                mean_us,
                variance_us2,
                observation_count: state.observation_count.saturating_add(1),
            }
        };
        *state = next;

        Ok(ScoreResult {
            previous_mean_us,
            previous_stddev_us,
            score,
            observation_count: next.observation_count,
            warmed_up,
            anomaly,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_has_the_documented_starting_values() {
        let config = DetectorConfig::default();
        assert_eq!(config.alpha, 0.1);
        assert_eq!(config.warmup_observations, 100);
        assert_eq!(config.threshold, 3.0);
        assert_eq!(config.stddev_floor_us, 1_000.0);
    }

    #[test]
    fn first_observation_seeds_baseline_without_scoring_against_itself() {
        let mut detector = EwmaDetector::new(config(0.5, 1, 3.0, 1.0)).unwrap();
        let result = detector.score(10.0).unwrap();
        assert_eq!(result.previous_mean_us, None);
        assert_eq!(result.previous_stddev_us, None);
        assert_eq!(result.score, 0.0);
        assert_eq!(result.observation_count, 1);
        assert!(!result.warmed_up);
        assert!(!result.anomaly);
    }

    #[test]
    fn scores_against_previous_baseline_before_updating_mean_and_variance() {
        let mut detector = EwmaDetector::new(config(0.5, 1, 3.0, 1.0)).unwrap();
        detector.score(10.0).unwrap();
        let second = detector.score(14.0).unwrap();
        assert_eq!(second.previous_mean_us, Some(10.0));
        assert_eq!(second.previous_stddev_us, Some(0.0));
        assert_eq!(second.score, 4.0);
        assert_eq!(second.observation_count, 2);
        assert!(second.warmed_up);
        assert!(second.anomaly);

        let third = detector.score(12.0).unwrap();
        assert_eq!(third.previous_mean_us, Some(12.0));
        assert_eq!(third.previous_stddev_us, Some(2.0));
        assert_eq!(third.score, 0.0);
    }

    #[test]
    fn standard_deviation_floor_bounds_the_score_denominator() {
        let mut detector = EwmaDetector::new(config(0.5, 1, 3.0, 2.0)).unwrap();
        detector.score(10.0).unwrap();
        let result = detector.score(16.0).unwrap();
        assert_eq!(result.previous_stddev_us, Some(0.0));
        assert_eq!(result.score, 3.0);
        assert!(result.anomaly);
    }

    #[test]
    fn warmup_suppresses_exactly_the_configured_number_of_observations() {
        let mut detector = EwmaDetector::new(config(0.1, 3, 0.5, 1.0)).unwrap();
        for value in [10.0, 10.0, 10.0] {
            let result = detector.score(value).unwrap();
            assert!(!result.warmed_up);
            assert!(!result.anomaly);
        }
        let result = detector.score(20.0).unwrap();
        assert!(result.warmed_up);
        assert!(result.anomaly);
        assert_eq!(result.observation_count, 4);
    }

    #[test]
    fn negative_residual_has_zero_score() {
        let mut detector = EwmaDetector::new(config(0.5, 1, 0.1, 1.0)).unwrap();
        detector.score(10.0).unwrap();
        let result = detector.score(5.0).unwrap();
        assert_eq!(result.score, 0.0);
        assert!(!result.anomaly);
    }

    #[test]
    fn detector_instances_keep_independent_baselines() {
        let mut first = EwmaDetector::new(config(0.5, 1, 3.0, 1.0)).unwrap();
        let mut second = EwmaDetector::new(config(0.5, 1, 3.0, 1.0)).unwrap();
        first.score(10.0).unwrap();
        let result = second.score(20.0).unwrap();
        assert_eq!(result.previous_mean_us, None);
        assert_eq!(result.observation_count, 1);
    }

    #[test]
    fn rejects_invalid_configurations_and_observations() {
        for alpha in [0.0, 1.0, -0.1, 1.1, f64::NAN, f64::INFINITY] {
            assert!(EwmaDetector::new(config(alpha, 1, 3.0, 1.0)).is_err());
        }
        for threshold in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            assert!(EwmaDetector::new(config(0.1, 1, threshold, 1.0)).is_err());
        }
        for floor in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            assert!(EwmaDetector::new(config(0.1, 1, 3.0, floor)).is_err());
        }
        assert!(EwmaDetector::new(config(0.1, 0, 3.0, 1.0)).is_err());

        let mut detector = EwmaDetector::new(config(0.1, 1, 3.0, 1.0)).unwrap();
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(detector.score(value).is_err());
        }
    }

    #[test]
    fn rejects_non_finite_score_without_updating_baseline() {
        let mut detector = EwmaDetector::new(config(0.1, 1, 3.0, f64::MIN_POSITIVE)).unwrap();
        detector.score(1.0).unwrap();
        assert!(detector.score(f64::MAX).is_err());
        let result = detector.score(1.0).unwrap();
        assert_eq!(result.previous_mean_us, Some(1.0));
        assert_eq!(result.observation_count, 2);
    }

    fn config(
        alpha: f64,
        warmup_observations: usize,
        threshold: f64,
        stddev_floor_us: f64,
    ) -> DetectorConfig {
        DetectorConfig {
            alpha,
            warmup_observations,
            threshold,
            stddev_floor_us,
        }
    }
}
