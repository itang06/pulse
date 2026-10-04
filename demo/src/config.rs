use std::{collections::HashMap, env};

/// Runtime settings for the bounded ingress workload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadConfig {
    pub gateway_addr: String,
    pub events_per_second: u64,
    pub duration_seconds: u64,
    pub expected_attempted: u64,
    pub route_count: usize,
    pub random_seed: u64,
}

impl Default for LoadConfig {
    fn default() -> Self {
        Self {
            gateway_addr: "http://localhost:50051".to_owned(),
            events_per_second: 1_000,
            duration_seconds: 60,
            expected_attempted: 60_000,
            route_count: 8,
            random_seed: 42,
        }
    }
}

impl LoadConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        Self::from_values(env::vars())
    }

    pub fn from_values<K, V>(values: impl IntoIterator<Item = (K, V)>) -> anyhow::Result<Self>
    where
        K: Into<String>,
        V: Into<String>,
    {
        let values = values
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect::<HashMap<_, _>>();
        let defaults = Self::default();
        let gateway_addr = values
            .get("PULSE_GATEWAY_ADDR")
            .cloned()
            .unwrap_or(defaults.gateway_addr);
        let events_per_second = parse_value(
            &values,
            "PULSE_EVENTS_PER_SECOND",
            defaults.events_per_second,
        )?;
        let duration_seconds =
            parse_value(&values, "PULSE_DURATION_SECONDS", defaults.duration_seconds)?;
        let route_count = parse_value(&values, "PULSE_ROUTE_COUNT", defaults.route_count)?;
        let random_seed = parse_value(&values, "PULSE_RANDOM_SEED", defaults.random_seed)?;

        anyhow::ensure!(
            events_per_second > 0,
            "PULSE_EVENTS_PER_SECOND must be greater than zero"
        );
        anyhow::ensure!(
            duration_seconds > 0,
            "PULSE_DURATION_SECONDS must be greater than zero"
        );
        let expected_attempted = events_per_second
            .checked_mul(duration_seconds)
            .ok_or_else(|| anyhow::anyhow!("configured event count overflows u64"))?;
        anyhow::ensure!(
            route_count > 0,
            "PULSE_ROUTE_COUNT must be greater than zero"
        );
        anyhow::ensure!(
            !gateway_addr.trim().is_empty(),
            "PULSE_GATEWAY_ADDR must not be empty"
        );

        Ok(Self {
            gateway_addr,
            events_per_second,
            duration_seconds,
            expected_attempted,
            route_count,
            random_seed,
        })
    }
}

fn parse_value<T: std::str::FromStr>(
    values: &HashMap<String, String>,
    key: &str,
    default: T,
) -> anyhow::Result<T>
where
    T::Err: std::fmt::Display,
{
    values
        .get(key)
        .map(|value| {
            value
                .parse()
                .map_err(|error| anyhow::anyhow!("invalid {key}: {error}"))
        })
        .unwrap_or(Ok(default))
}
