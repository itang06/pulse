use std::collections::HashSet;

use anyhow::{Context, Result};
use sqlx::{postgres::PgPool, query_builder::QueryBuilder, Error, Postgres, Row};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::model::{UtcTimestamp, ValidatedEvent};

/// Maximum number of valid events accepted by one persistence transaction.
pub const MAX_BATCH_SIZE: usize = 500;

const SCHEMA_CHECK_QUERY: &str = r#"
WITH expected_receipt_columns(column_name, data_type) AS (
    VALUES
        ('event_id', 'uuid'),
        ('first_seen_at', 'timestamp with time zone')
), expected_event_columns(column_name, data_type) AS (
    VALUES
        ('event_id', 'uuid'),
        ('event_time', 'timestamp with time zone'),
        ('service_name', 'text'),
        ('route', 'text'),
        ('latency_us', 'bigint'),
        ('status_code', 'integer'),
        ('trace_id', 'text'),
        ('attributes', 'jsonb'),
        ('ewma_mean_us', 'double precision'),
        ('ewma_stddev_us', 'double precision'),
        ('anomaly_score', 'double precision'),
        ('is_anomaly', 'boolean'),
        ('processed_at', 'timestamp with time zone')
)
SELECT
    EXISTS (
        SELECT 1
        FROM pg_class c
        WHERE c.oid = to_regclass('event_receipts') AND c.relkind = 'r'
    )
    AND NOT EXISTS (
        SELECT 1
        FROM expected_receipt_columns expected
        WHERE NOT EXISTS (
            SELECT 1
            FROM information_schema.columns actual
            WHERE actual.table_schema = current_schema()
              AND actual.table_name = 'event_receipts'
              AND actual.column_name = expected.column_name
              AND actual.data_type = expected.data_type
              AND actual.is_nullable = 'NO'
        )
    )
    AND EXISTS (
        SELECT 1
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON kcu.constraint_schema = tc.constraint_schema
         AND kcu.constraint_name = tc.constraint_name
         AND kcu.table_name = tc.table_name
        WHERE tc.constraint_schema = current_schema()
          AND tc.table_name = 'event_receipts'
          AND tc.constraint_type = 'PRIMARY KEY'
          AND kcu.column_name = 'event_id'
          AND (
              SELECT count(*)
              FROM information_schema.key_column_usage kcu2
              WHERE kcu2.constraint_schema = tc.constraint_schema
                AND kcu2.constraint_name = tc.constraint_name
                AND kcu2.table_name = tc.table_name
          ) = 1
    )
    AND EXISTS (
        SELECT 1
        FROM pg_class c
        WHERE c.oid = to_regclass('telemetry_events') AND c.relkind = 'r'
    )
    AND NOT EXISTS (
        SELECT 1
        FROM expected_event_columns expected
        WHERE NOT EXISTS (
            SELECT 1
            FROM information_schema.columns actual
            WHERE actual.table_schema = current_schema()
              AND actual.table_name = 'telemetry_events'
              AND actual.column_name = expected.column_name
              AND actual.data_type = expected.data_type
              AND actual.is_nullable = 'NO'
        )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM timescaledb_information.hypertables
        WHERE hypertable_schema = current_schema()
          AND hypertable_name = 'event_receipts'
    )
    AND EXISTS (
        SELECT 1
        FROM timescaledb_information.hypertables
        WHERE hypertable_schema = current_schema()
          AND hypertable_name = 'telemetry_events'
    )
    AS schema_ready
"#;

/// Verify the runtime persistence contract before consuming Kafka records.
pub async fn verify_schema(pool: &PgPool) -> Result<()> {
    let row = sqlx::query(SCHEMA_CHECK_QUERY)
        .fetch_one(pool)
        .await
        .context("checking sink schema and TimescaleDB hypertable")?;
    let ready: bool = row
        .try_get("schema_ready")
        .context("reading schema check")?;
    if !ready {
        anyhow::bail!(
            "sink schema is incomplete; apply deploy/migrations before starting pulse-sink"
        );
    }
    Ok(())
}

/// The IDs whose receipt and telemetry rows were committed by this call.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistOutcome {
    pub inserted_ids: HashSet<Uuid>,
    /// Duplicate attempts include IDs already present before this call and
    /// repeated IDs later in this input slice.
    pub conflict_count: u64,
}

/// Database failures are kept separate from poison input. Only explicitly
/// transient failures should be retried by the caller.
#[derive(Debug)]
pub enum DbFailure {
    Retryable(Error),
    Fatal(Error),
}

impl DbFailure {
    pub fn error(&self) -> &Error {
        match self {
            Self::Retryable(error) | Self::Fatal(error) => error,
        }
    }

    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Retryable(_))
    }
}

/// Persist receipt claims and only their matching events in one transaction.
/// Repeated IDs are collapsed in input order, so the earliest payload wins.
pub async fn persist_batch(
    pool: &PgPool,
    events: &[ValidatedEvent],
) -> Result<PersistOutcome, DbFailure> {
    if events.len() > MAX_BATCH_SIZE {
        return Err(DbFailure::Fatal(Error::InvalidArgument(format!(
            "persist_batch accepts at most {MAX_BATCH_SIZE} events; got {}",
            events.len()
        ))));
    }
    if events.is_empty() {
        return Ok(PersistOutcome {
            inserted_ids: HashSet::new(),
            conflict_count: 0,
        });
    }

    let mut seen = HashSet::with_capacity(events.len());
    let mut unique = Vec::with_capacity(events.len());
    for event in events {
        if seen.insert(event.event_id) {
            unique.push(event);
        }
    }
    let duplicate_attempts = events.len() - unique.len();

    let mut transaction = pool.begin().await.map_err(DbFailure::from)?;
    let first_seen_at = OffsetDateTime::now_utc();
    let mut receipt_query =
        QueryBuilder::<Postgres>::new("INSERT INTO event_receipts (event_id, first_seen_at) ");
    receipt_query.push_values(unique.iter(), |mut row, event| {
        row.push_bind(event.event_id).push_bind(first_seen_at);
    });
    receipt_query.push(" ON CONFLICT (event_id) DO NOTHING RETURNING event_id");
    let receipt_rows = receipt_query
        .build()
        .fetch_all(&mut *transaction)
        .await
        .map_err(DbFailure::from)?;
    let mut inserted_ids = HashSet::with_capacity(receipt_rows.len());
    for row in receipt_rows {
        inserted_ids.insert(
            row.try_get::<Uuid, _>("event_id")
                .map_err(DbFailure::from)?,
        );
    }

    let new_events = unique
        .into_iter()
        .filter(|event| inserted_ids.contains(&event.event_id))
        .collect::<Vec<_>>();
    if !new_events.is_empty() {
        let bound_events = new_events
            .iter()
            .map(|event| {
                Ok((
                    *event,
                    to_offset_datetime(event.event_time)?,
                    to_offset_datetime(event.processed_at)?,
                ))
            })
            .collect::<Result<Vec<_>, DbFailure>>()?;
        let mut event_query = QueryBuilder::<Postgres>::new(
            "INSERT INTO telemetry_events (event_id, event_time, service_name, route, latency_us, status_code, trace_id, attributes, ewma_mean_us, ewma_stddev_us, anomaly_score, is_anomaly, processed_at) ",
        );
        event_query.push_values(
            bound_events.iter(),
            |mut row, (event, event_time, processed_at)| {
                row.push_bind(event.event_id)
                    .push_bind(*event_time)
                    .push_bind(&event.service_name)
                    .push_bind(&event.route)
                    .push_bind(event.latency_us)
                    .push_bind(event.status_code)
                    .push_bind(&event.trace_id)
                    .push_bind(event.attributes.clone())
                    .push_bind(event.ewma_mean_us)
                    .push_bind(event.ewma_stddev_us)
                    .push_bind(event.anomaly_score)
                    .push_bind(event.is_anomaly)
                    .push_bind(*processed_at);
            },
        );
        event_query
            .build()
            .execute(&mut *transaction)
            .await
            .map_err(DbFailure::from)?;
    }

    transaction.commit().await.map_err(DbFailure::from)?;
    let conflict_count = (seen.len() - inserted_ids.len() + duplicate_attempts) as u64;
    Ok(PersistOutcome {
        inserted_ids,
        conflict_count,
    })
}

fn to_offset_datetime(timestamp: UtcTimestamp) -> Result<OffsetDateTime, DbFailure> {
    let error = |message: &str| DbFailure::Fatal(Error::Protocol(message.to_owned()));
    let instant = OffsetDateTime::from_unix_timestamp(timestamp.seconds)
        .map_err(|_| error("validated timestamp is outside SQLx time range"))?;
    instant
        .replace_nanosecond(timestamp.nanos as u32)
        .map_err(|_| error("validated timestamp has invalid nanoseconds"))
}

impl From<Error> for DbFailure {
    fn from(error: Error) -> Self {
        if is_retryable_error(&error) {
            Self::Retryable(error)
        } else {
            Self::Fatal(error)
        }
    }
}

fn is_retryable_error(error: &Error) -> bool {
    match error {
        Error::Io(_) | Error::PoolTimedOut | Error::WorkerCrashed | Error::BeginFailed => true,
        Error::Database(database_error) => database_error
            .code()
            .as_deref()
            .is_some_and(is_retryable_sqlstate),
        _ => false,
    }
}

fn is_retryable_sqlstate(code: &str) -> bool {
    code.starts_with("08")
        || matches!(code, "40001" | "40P01" | "57P01" | "57P02" | "57P03")
        || code.starts_with("53")
}

#[cfg(test)]
mod tests {
    use super::{is_retryable_sqlstate, persist_batch, DbFailure, MAX_BATCH_SIZE};
    use crate::model::{UtcTimestamp, ValidatedEvent};
    use serde_json::json;
    use sqlx::{postgres::PgPoolOptions, Error};
    use uuid::Uuid;

    #[tokio::test]
    async fn rejects_batches_over_the_public_limit_before_acquiring_a_connection() {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://localhost/pulse_test")
            .expect("create lazy test pool");
        pool.close().await;
        let event = ValidatedEvent {
            event_id: Uuid::from_u128(1),
            event_time: UtcTimestamp {
                seconds: 1_725_000_000,
                nanos: 0,
            },
            service_name: "service".into(),
            route: "/".into(),
            latency_us: 1,
            status_code: 200,
            trace_id: "trace".into(),
            attributes: json!({}),
            ewma_mean_us: 0.0,
            ewma_stddev_us: 0.0,
            anomaly_score: 0.0,
            is_anomaly: false,
            processed_at: UtcTimestamp {
                seconds: 1_725_000_000,
                nanos: 0,
            },
        };
        let events = vec![event; MAX_BATCH_SIZE + 1];

        let failure = persist_batch(&pool, &events)
            .await
            .expect_err("oversized input must be rejected before pool access");
        assert!(matches!(
            failure,
            DbFailure::Fatal(Error::InvalidArgument(_))
        ));
    }

    #[test]
    fn classifies_connection_and_pool_failures_as_retryable() {
        for error in [
            Error::Io(std::io::Error::other("connection reset")),
            Error::PoolTimedOut,
            Error::WorkerCrashed,
            Error::BeginFailed,
        ] {
            assert!(
                DbFailure::from(error).is_retryable(),
                "connection and pool errors should be retryable"
            );
        }
    }

    #[test]
    fn classifies_deliberate_pool_shutdown_as_fatal() {
        assert!(!DbFailure::from(Error::PoolClosed).is_retryable());
    }

    #[test]
    fn classifies_unrecognized_sqlx_errors_as_fatal() {
        assert!(
            !DbFailure::from(Error::InvalidArgument("bad query argument".into())).is_retryable()
        );
    }

    #[test]
    fn classifies_known_transient_sqlstates_as_retryable() {
        for code in [
            "40001", "40P01", "08006", "53300", "57P01", "57P02", "57P03",
        ] {
            assert!(is_retryable_sqlstate(code), "{code} should be retryable");
        }
    }

    #[test]
    fn classifies_contract_and_unknown_sqlstates_as_fatal() {
        for code in [
            "42P01", "42703", "42883", "3F000", "23514", "57P04", "99999",
        ] {
            assert!(!is_retryable_sqlstate(code), "{code} should be fatal");
        }
    }
}
