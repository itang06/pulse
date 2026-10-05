use std::fs;

const MIGRATION_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../deploy/migrations/0002_sink.sql"
);

fn strip_line_comments(sql: &str) -> String {
    sql.lines()
        .map(|line| line.split_once("--").map_or(line, |(code, _)| code))
        .collect::<Vec<_>>()
        .join("\n")
}

fn table_body<'a>(sql: &'a str, table: &str) -> &'a str {
    let marker = format!("create table if not exists {table}");
    let start = sql
        .find(&marker)
        .unwrap_or_else(|| panic!("missing table definition: {table}"));
    let open = start
        + marker.len()
        + sql[start + marker.len()..]
            .find('(')
            .expect("table body starts with (");

    let mut depth = 0;
    for (offset, character) in sql[open..].char_indices() {
        match character {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return &sql[open + 1..open + offset];
                }
            }
            _ => {}
        }
    }
    panic!("unterminated table definition: {table}");
}

fn telemetry_has_event_id_uniqueness(body: &str) -> bool {
    body.split(',').any(|definition| {
        let definition = definition.split_whitespace().collect::<Vec<_>>().join(" ");
        let definition = definition.to_ascii_lowercase();
        if definition.starts_with("event_id ") {
            definition.contains(" unique") || definition.contains(" primary key")
        } else {
            (definition.starts_with("unique ") || definition.starts_with("primary key "))
                && definition.contains("event_id")
        }
    })
}

#[test]
fn sink_migration_defines_receipts_and_non_unique_event_hypertable() {
    let migration = fs::read_to_string(MIGRATION_PATH)
        .expect("deploy/migrations/0002_sink.sql must define the sink schema");
    let normalized = strip_line_comments(&migration).to_ascii_lowercase();

    assert!(normalized.contains("create table if not exists event_receipts"));
    let receipt_body = table_body(&normalized, "event_receipts");
    assert!(receipt_body.contains("event_id uuid primary key"));
    assert!(receipt_body.contains("first_seen_at timestamptz not null"));
    assert!(!normalized.contains("create_hypertable('event_receipts'"));

    let event_body = table_body(&normalized, "telemetry_events");

    for column in [
        "event_id uuid not null",
        "event_time timestamptz not null",
        "service_name text not null",
        "route text not null",
        "latency_us bigint not null",
        "status_code integer not null",
        "trace_id text not null",
        "attributes jsonb not null",
        "ewma_mean_us double precision not null",
        "ewma_stddev_us double precision not null",
        "anomaly_score double precision not null",
        "is_anomaly boolean not null",
        "processed_at timestamptz not null",
    ] {
        assert!(
            event_body.contains(column),
            "missing column definition: {column}"
        );
    }

    assert!(normalized
        .contains("create_hypertable('telemetry_events', 'event_time', if_not_exists => true)"));
    assert!(normalized.contains("(service_name, route, event_time desc)"));
    assert!(
        !telemetry_has_event_id_uniqueness(event_body),
        "event_id must not be UNIQUE or PRIMARY KEY in the telemetry_events hypertable"
    );
}

#[test]
fn event_id_uniqueness_check_detects_inline_and_table_constraints() {
    for definition in [
        "event_id UUID NOT NULL UNIQUE",
        "event_id UUID PRIMARY KEY",
        "UNIQUE (event_id)",
        "PRIMARY KEY (event_id)",
    ] {
        assert!(
            telemetry_has_event_id_uniqueness(definition),
            "failed to detect event_id uniqueness in: {definition}"
        );
    }

    let commented_constraint = strip_line_comments("event_id UUID NOT NULL, -- UNIQUE\n");
    assert!(!telemetry_has_event_id_uniqueness(&commented_constraint));
}
