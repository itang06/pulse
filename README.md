# Pulse

Pulse is a small telemetry pipeline built to explore event ingestion, stream
processing, and reliable database writes. A Rust SDK sends batches through a Go
gRPC gateway into Kafka. A Rust processor enriches valid events and routes
malformed records to a dead-letter topic. A sink writes enriched events to
TimescaleDB.

```text
Instrumented service
        │
        ▼
Rust SDK ── gRPC/Protobuf ── Go gateway
                                  │
                                  ▼
                           Kafka: telemetry.raw
                                  │
                           Rust processor
                           ├── telemetry.enriched ── TimescaleDB sink
                           └── telemetry.dlq
```

The local Compose stack also includes Prometheus and Grafana.

## Components

| Directory | Purpose |
| --- | --- |
| `proto/` | Protobuf service and event schemas |
| `sdk/` | Rust client with a bounded queue, batching, retries, and flush |
| `gateway/` | Go gRPC service that validates batches and waits for Kafka delivery acknowledgements |
| `demo/` | Deterministic synthetic load generator |
| `processor/` | Rust Kafka consumer with bounded EWMA state, enrichment, and dead-letter routing |
| `sink/` | Kafka consumer that persists enriched events to TimescaleDB |
| `deploy/` | Local Kafka, TimescaleDB, Prometheus, and Grafana stack |

## Getting started

Prerequisites: Go ≥ 1.26, Rust stable via rustup, Docker, `buf`, `cmake`,
`jq`, and `curl`.

```sh
make up        # start the local stack
make proto     # lint schemas and generate Go code
make build     # build the gateway and Rust workspace
make test      # run repository tests
```

Use `make down` to stop the stack. `make clean` also removes its data volumes.

## Delivery behavior

The gateway validates a whole batch before publishing it and acknowledges the
RPC only after Kafka reports successful delivery for every record. If Kafka
stores a record but the RPC response is lost, the SDK cannot distinguish that
from a failed publish and may retry the batch. In that ambiguous failure window,
duplicates are possible.

The processor scores each valid event against the previous EWMA baseline before
updating it. State is keyed by `(service_name, route)` and held in a bounded
LRU; a restart or Kafka assignment change clears the in-memory state. The
processor waits for output delivery before synchronously committing source
offsets, so failures can cause records to be replayed. Output deduplication is
not implemented.

The sink stores each event UUID in an `event_receipts` table in the same
transaction as the corresponding `telemetry_events` insert. The receipt
primary key makes database effects idempotent across Kafka replays. This is
at-least-once Kafka processing with idempotent database writes, not exactly-once
Kafka processing. Receipts are retained indefinitely in this MVP.

## Checks

```sh
make down
make verify-ingress
make verify-processor
make replay-processor
make verify-sink
```

Run `make down` first so the checks can start and stop the Compose services they
need without colliding with an existing stack. The commands check acknowledged
ingress, processor routing and replay, and sink recovery after database commits.
Run `make replay-processor` to inspect the deterministic replay output.
