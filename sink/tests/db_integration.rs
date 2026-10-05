use std::{
    collections::HashSet,
    env,
    sync::atomic::{AtomicU64, Ordering},
};

use pulse_sink::{
    db::{persist_batch, verify_schema},
    model::{UtcTimestamp, ValidatedEvent},
};
use serde_json::json;
use sqlx::{postgres::PgPoolOptions, PgPool};
use uuid::Uuid;

const MIGRATION: &str = include_str!("../../deploy/migrations/0002_sink.sql");
static NEXT_SCHEMA_ID: AtomicU64 = AtomicU64::new(1);

async fn test_pool() -> Option<(PgPool, String)> {
    let Ok(database_url) = env::var("PULSE_TEST_DATABASE_URL") else {
        eprintln!("SKIP TimescaleDB integration case: PULSE_TEST_DATABASE_URL is unset; set it to an isolated TimescaleDB database URL and rerun with --nocapture to execute and report these cases");
        return None;
    };

    let admin = match PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
    {
        Ok(pool) => pool,
        Err(error) => {
            panic!("PULSE_TEST_DATABASE_URL is set but the database is unavailable: {error}")
        }
    };
    let schema = format!(
        "sink_test_{}_{}",
        std::process::id(),
        NEXT_SCHEMA_ID.fetch_add(1, Ordering::Relaxed)
    );
    // The identifier contains only a fixed prefix, the process ID, and a decimal counter.
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .expect("create isolated test schema");

    let search_path = format!("{schema},public");
    let options = database_url
        .parse::<sqlx::postgres::PgConnectOptions>()
        .expect("parse test database URL")
        .options([("search_path", search_path.as_str())]);
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await
        .expect("connect to isolated test schema");
    sqlx::raw_sql(MIGRATION)
        .execute(&pool)
        .await
        .expect("apply sink migration to isolated test schema");
    admin.close().await;
    Some((pool, schema))
}

#[tokio::test]
async fn schema_verification_accepts_the_sink_migration() {
    let Some((pool, schema)) = test_pool().await else {
        return;
    };

    verify_schema(&pool)
        .await
        .expect("sink migration satisfies startup schema contract");
    cleanup(pool, &schema).await;
}

#[tokio::test]
async fn schema_verification_rejects_receipts_without_event_id_primary_key() {
    let Some((pool, schema)) = test_pool().await else {
        return;
    };
    sqlx::query("ALTER TABLE event_receipts DROP CONSTRAINT event_receipts_pkey")
        .execute(&pool)
        .await
        .expect("remove receipt primary key");

    assert!(verify_schema(&pool).await.is_err());
    cleanup(pool, &schema).await;
}

async fn cleanup(pool: PgPool, schema: &str) {
    pool.close().await;
    let database_url = env::var("PULSE_TEST_DATABASE_URL").expect("URL remains configured");
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("reconnect to test database for cleanup");
    // `schema` was generated locally above and cannot contain SQL syntax.
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .expect("drop isolated test schema");
    admin.close().await;
}

fn event(event_id: Uuid, service_name: &str, latency_us: i64) -> ValidatedEvent {
    ValidatedEvent {
        event_id,
        event_time: UtcTimestamp {
            seconds: 1_725_000_000,
            nanos: 123_456_789,
        },
        service_name: service_name.to_owned(),
        route: "/items".to_owned(),
        latency_us,
        status_code: 200,
        trace_id: "trace-1".to_owned(),
        attributes: json!({"region": "test"}),
        ewma_mean_us: 10.0,
        ewma_stddev_us: 2.0,
        anomaly_score: 0.5,
        is_anomaly: false,
        processed_at: UtcTimestamp {
            seconds: 1_725_000_001,
            nanos: 987_654_321,
        },
    }
}

async fn counts(pool: &PgPool) -> (i64, i64) {
    let receipts: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM event_receipts")
        .fetch_one(pool)
        .await
        .expect("count receipts");
    let events: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM telemetry_events")
        .fetch_one(pool)
        .await
        .expect("count events");
    (receipts.0, events.0)
}

#[tokio::test]
async fn persists_once_and_replay_is_a_receipt_conflict() {
    let Some((pool, schema)) = test_pool().await else {
        return;
    };
    let input = event(Uuid::from_u128(1), "first-service", 111);

    let first = persist_batch(&pool, std::slice::from_ref(&input))
        .await
        .expect("insert first event");
    assert_eq!(first.inserted_ids, HashSet::from([input.event_id]));
    assert_eq!(first.conflict_count, 0);

    let replay = persist_batch(&pool, &[event(input.event_id, "replay-service", 999)])
        .await
        .expect("replay existing event");
    assert!(replay.inserted_ids.is_empty());
    assert_eq!(replay.conflict_count, 1);
    assert_eq!(counts(&pool).await, (1, 1));
    let stored: (String, i64) =
        sqlx::query_as("SELECT service_name, latency_us FROM telemetry_events")
            .fetch_one(&pool)
            .await
            .expect("read first payload");
    assert_eq!(stored, ("first-service".to_owned(), 111));
    cleanup(pool, &schema).await;
}

#[tokio::test]
async fn batch_duplicates_are_counted_and_first_payload_wins() {
    let Some((pool, schema)) = test_pool().await else {
        return;
    };
    let id = Uuid::from_u128(2);
    let first = event(id, "winner", 10);
    let outcome = persist_batch(&pool, &[first.clone(), first, event(id, "conflicting", 30)])
        .await
        .expect("persist batch with duplicate IDs");

    assert_eq!(outcome.inserted_ids, HashSet::from([id]));
    assert_eq!(outcome.conflict_count, 2);
    assert_eq!(counts(&pool).await, (1, 1));
    let stored: (String, i64) =
        sqlx::query_as("SELECT service_name, latency_us FROM telemetry_events")
            .fetch_one(&pool)
            .await
            .expect("read winner payload");
    assert_eq!(stored, ("winner".to_owned(), 10));
    cleanup(pool, &schema).await;
}

#[tokio::test]
async fn concurrent_batches_racing_for_one_id_insert_exactly_one_row() {
    let Some((pool, schema)) = test_pool().await else {
        return;
    };
    let id = Uuid::from_u128(3);
    let first_pool = pool.clone();
    let second_pool = pool.clone();
    let first =
        tokio::spawn(async move { persist_batch(&first_pool, &[event(id, "race-a", 1)]).await });
    let second =
        tokio::spawn(async move { persist_batch(&second_pool, &[event(id, "race-b", 2)]).await });
    let (first, second) = tokio::join!(first, second);
    let outcomes = [
        first.expect("first task").expect("first transaction"),
        second.expect("second task").expect("second transaction"),
    ];

    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| outcome.inserted_ids.contains(&id))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .map(|outcome| outcome.conflict_count)
            .sum::<u64>(),
        1
    );
    assert_eq!(counts(&pool).await, (1, 1));
    cleanup(pool, &schema).await;
}

#[tokio::test]
async fn empty_batch_is_a_noop() {
    let Some((pool, schema)) = test_pool().await else {
        return;
    };
    let outcome = persist_batch(&pool, &[]).await.expect("empty batch");
    assert!(outcome.inserted_ids.is_empty());
    assert_eq!(outcome.conflict_count, 0);
    assert_eq!(counts(&pool).await, (0, 0));
    cleanup(pool, &schema).await;
}

#[tokio::test]
async fn event_insert_failure_rolls_back_receipt_and_valid_replay_can_succeed() {
    let Some((pool, schema)) = test_pool().await else {
        return;
    };
    sqlx::query("ALTER TABLE telemetry_events ADD CONSTRAINT test_reject_tripwire CHECK (service_name <> 'tripwire')")
        .execute(&pool).await.expect("install failure trigger constraint");
    let id = Uuid::from_u128(4);
    let failure = persist_batch(&pool, &[event(id, "tripwire", 10)]).await;
    assert!(
        failure.is_err(),
        "telemetry constraint should reject the insert"
    );
    assert_eq!(
        counts(&pool).await,
        (0, 0),
        "receipt must roll back with event failure"
    );

    let retry = persist_batch(&pool, &[event(id, "recovered", 10)])
        .await
        .expect("valid replay after rollback");
    assert_eq!(retry.inserted_ids, HashSet::from([id]));
    assert_eq!(retry.conflict_count, 0);
    assert_eq!(counts(&pool).await, (1, 1));
    cleanup(pool, &schema).await;
}
