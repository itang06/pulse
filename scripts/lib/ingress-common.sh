#!/usr/bin/env bash

ingress_die() {
  printf 'ingress harness: %s\n' "$*" >&2
  exit 1
}

ingress_error() {
  printf 'ingress harness: %s\n' "$*" >&2
}

assert_positive_integer() {
  local name="$1" value="$2" normalized
  normalized="$(normalize_positive_integer "$name" "$value")" || return 1
  printf '%s\n' "$normalized"
}

normalize_positive_integer() {
  local LC_ALL=C name="$1" value="$2" max=9223372036854775807
  [[ "$value" =~ ^[0-9]+$ ]] || { ingress_error "$name must be a positive base-10 integer (got '$value')"; return 1; }
  while [[ ${#value} -gt 1 && "${value:0:1}" == 0 ]]; do value="${value:1}"; done
  [[ "$value" != 0 ]] || { ingress_error "$name must be greater than zero"; return 1; }
  if [[ ${#value} -gt ${#max} || ( ${#value} -eq ${#max} && "$value" > "$max" ) ]]; then
    ingress_error "$name exceeds the signed 64-bit integer limit"
    return 1
  fi
  printf '%s\n' "$value"
}

calculate_expected_events() {
  local rate duration max=9223372036854775807
  rate="$(normalize_positive_integer event_rate "$1")" || return 1
  duration="$(normalize_positive_integer duration "$2")" || return 1
  if (( rate > max / duration )); then
    ingress_die 'event rate multiplied by duration exceeds the signed 64-bit integer limit'
  fi
  printf '%s\n' "$((rate * duration))"
}

assert_expected_event_count() {
  [[ $# -eq 3 ]] || ingress_die 'expected event count check needs rate, duration, and expected count'
  local actual
  actual="$(calculate_expected_events "$1" "$2")" || return 1
  [[ "$actual" == "$3" ]] || ingress_die "calculated event count $actual does not match expected $3"
}

assert_kafka_stopped() {
  [[ -z "$1" ]] || ingress_die 'Kafka is already running in this Compose project; stop it and rerun so offset deltas are attributable'
}

ingress_run_locked() {
  [[ "${PULSE_INGRESS_LOCK_HELD:-0}" == 1 ]] && return 0
  local lock_file="${TMPDIR:-/tmp}/pulse-ingress-harness.lock"
  if command -v lockf >/dev/null 2>&1; then
    PULSE_INGRESS_LOCK_HELD=1 exec lockf -t 0 "$lock_file" "$@"
  elif command -v flock >/dev/null 2>&1; then
    PULSE_INGRESS_LOCK_HELD=1 exec flock -n "$lock_file" "$@"
  fi
  ingress_error 'lockf or flock is required to serialize local Pulse Kafka harnesses'
  return 1
}

parse_raw_group_status() {
  local status="$1" expected_partitions="$2" expected_group="$3"
  [[ "$expected_partitions" =~ ^[1-9][0-9]*$ ]] || return 1
  awk -v expected="$expected_partitions" -v expected_group="$expected_group" '
    $1 == "GROUP" && $2 == "TOPIC" {
      if ($3 != "PARTITION" || $4 != "CURRENT-OFFSET" || $5 != "LOG-END-OFFSET" || $6 != "LAG") bad = 1
      else header = 1
      next
    }
    header && $2 == "telemetry.raw" {
      partition = $3
      if ($1 != expected_group || partition !~ /^[0-9]+$/ || partition >= expected || seen[partition]++) {
        bad = 1
        next
      }
      if ($4 == "-" && $5 == "0" && $6 == "-") {
        current = 0
        end = 0
        lag = 0
      } else if ($4 ~ /^[0-9]+$/ && $5 ~ /^[0-9]+$/ && $6 ~ /^[0-9]+$/) {
        current = $4 + 0
        end = $5 + 0
        lag = $6 + 0
      } else {
        bad = 1
        next
      }
      if (current != end || lag != 0) bad = 1
      committed_total += current
      end_total += end
      lag_total += lag
      rows++
    }
    END {
      if (!header || bad || rows != expected) exit 1
      for (i = 0; i < expected; i++) if (!seen[i]) exit 1
      printf "%.0f %.0f %.0f %d\n", committed_total, end_total, lag_total, rows
    }
  ' <<<"$status"
}

query_compose_kafka() {
  local output
  if output="$("${COMPOSE[@]}" ps --status running -q kafka 2>/dev/null)"; then
    printf '%s\n' "$output"
  else
    ingress_error 'failed to query Kafka status for this Compose project'
    return 1
  fi
}

stop_owned_kafka() {
  [[ -n "${OWNED_KAFKA_ID:-}" ]] || return 0
  local identity inspected_id project service running
  identity="$(inspect_compose_container "$OWNED_KAFKA_ID")" || return 1
  read -r inspected_id project service running <<<"$identity"
  if [[ "$inspected_id" != "$OWNED_KAFKA_ID" || "$project" != "$COMPOSE_PROJECT_NAME" || "$service" != kafka ]]; then
    ingress_error 'Kafka container identity changed; refusing to stop an unverified container'
    return 1
  fi
  [[ "$running" == true ]] || return 0
  docker stop "$OWNED_KAFKA_ID"
}

inspect_compose_container() {
  docker inspect --format '{{.Id}} {{index .Config.Labels "com.docker.compose.project"}} {{index .Config.Labels "com.docker.compose.service"}} {{.State.Running}}' "$1"
}

assert_summary_valid() {
  local summary="$1" expected="$2"
  command -v jq >/dev/null 2>&1 || ingress_die 'jq is required to validate the final SDK summary'
  printf '%s\n' "$summary" | jq -e \
    --argjson expected "$expected" \
    'type == "object" and
     .attempted == $expected and .expected_attempted == $expected and
     .invalid == 0 and .dropped == 0 and .dropped_full == 0 and .dropped_closed == 0 and
     .permanent_failures == 0 and .acknowledged == $expected and .queued == $expected' \
    >/dev/null || ingress_die 'SDK final summary does not show exact, loss-free acknowledgement'
}

ingress_init() {
  ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
  COMPOSE=(docker compose --env-file "$ROOT_DIR/deploy/.env.example" -f "$ROOT_DIR/deploy/docker-compose.yml")
  RUN_ID="$(date -u '+%Y%m%dT%H%M%SZ')-$$"
  ARTIFACT_DIR="${ARTIFACT_DIR:-$ROOT_DIR/artifacts/runs/ingress/$RUN_ID}"
  mkdir -p "$ARTIFACT_DIR"
  OWNED_KAFKA_ID=''
  GATEWAY_PID=''
  PROCESSOR_PID=''
  TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/pulse-ingress.XXXXXX")"
  cleanup_ingress() {
    local exit_code=$?
    trap - EXIT INT TERM
    if [[ -n "$GATEWAY_PID" ]] && kill -0 "$GATEWAY_PID" 2>/dev/null; then
      kill "$GATEWAY_PID" 2>/dev/null || true
      wait "$GATEWAY_PID" 2>/dev/null || true
    fi
    if [[ -n "$PROCESSOR_PID" ]] && kill -0 "$PROCESSOR_PID" 2>/dev/null; then
      kill "$PROCESSOR_PID" 2>/dev/null || true
      wait "$PROCESSOR_PID" 2>/dev/null || true
    fi
    stop_owned_kafka >/dev/null 2>&1 || true
    rm -rf "$TMP_DIR"
    exit "$exit_code"
  }
  trap cleanup_ingress EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
}

wait_for_processor() {
  local attempt pid="$1" metrics_url="$2" metric="$3" log_file="$4"
  for attempt in $(seq 1 60); do
    if ! kill -0 "$pid" 2>/dev/null; then
      cat "$log_file" >&2
      ingress_die 'processor exited before readiness'
    fi
    if curl --silent --fail "$metrics_url" 2>/dev/null | grep -q "$metric"; then
      return 0
    fi
    sleep 1
  done
  ingress_die 'processor readiness timed out after 60 seconds'
}

start_ingress_dependencies() {
  command -v docker >/dev/null 2>&1 || ingress_die 'docker is required'
  command -v jq >/dev/null 2>&1 || ingress_die 'jq is required'
  local before_id container_id identity inspected_id project service running
  COMPOSE_PROJECT_NAME="$("${COMPOSE[@]}" config --format json | jq -er '.name')" || ingress_die 'cannot determine Compose project name'
  before_id="$(query_compose_kafka)" || ingress_die 'cannot determine whether Kafka is already running'
  assert_kafka_stopped "$before_id"
  "${COMPOSE[@]}" up -d --wait kafka || ingress_die 'Kafka did not become ready; cleanup will not stop a container without verified ownership'
  container_id="$(query_compose_kafka)" || ingress_die 'cannot identify the started Kafka container'
  [[ "$container_id" =~ ^[[:xdigit:]]{64}$ ]] || ingress_die 'Compose did not return exactly one Kafka container ID; refusing to stop an unverified container'
  identity="$(inspect_compose_container "$container_id")" || ingress_die 'cannot inspect the started Kafka container'
  read -r inspected_id project service running <<<"$identity"
  [[ "$inspected_id" == "$container_id" && "$project" == "$COMPOSE_PROJECT_NAME" && "$service" == kafka && "$running" == true ]] \
    || ingress_die 'started Kafka container identity did not match this Compose project; refusing to stop an unverified container'
  OWNED_KAFKA_ID="$container_id"
  printf '%s\n' "$OWNED_KAFKA_ID" >"$ARTIFACT_DIR/kafka-container-id.txt"
  "${COMPOSE[@]}" run --rm kafka-init >"$ARTIFACT_DIR/kafka-init.log" 2>&1 || ingress_die 'Kafka topic initialization failed'
  "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic telemetry.raw \
    >"$ARTIFACT_DIR/topic.txt" || ingress_die 'telemetry.raw is not ready'
}

build_ingress_processes() {
  command -v cargo >/dev/null 2>&1 || ingress_die 'cargo is required'
  command -v go >/dev/null 2>&1 || ingress_die 'go is required'
  (cd "$ROOT_DIR" && cargo build -p pulse-demo) >"$ARTIFACT_DIR/cargo-build.log" 2>&1 || ingress_die 'failed to build the Rust load generator'
  (cd "$ROOT_DIR/gateway" && go build -o "$TMP_DIR/pulse-gateway" ./cmd/gateway) >"$ARTIFACT_DIR/go-build.log" 2>&1 || ingress_die 'failed to build the Go gateway'
}

start_gateway() {
  PULSE_GRPC_ADDR=127.0.0.1:50051 PULSE_METRICS_ADDR=127.0.0.1:19464 \
    PULSE_KAFKA_BROKERS=localhost:9092 "$TMP_DIR/pulse-gateway" \
    >"$ARTIFACT_DIR/gateway.log" 2>&1 &
  GATEWAY_PID=$!
  local attempt
  for attempt in $(seq 1 60); do
    if ! kill -0 "$GATEWAY_PID" 2>/dev/null; then
      cat "$ARTIFACT_DIR/gateway.log" >&2
      ingress_die 'gateway exited before readiness'
    fi
    if curl --silent --fail http://127.0.0.1:19464/metrics 2>/dev/null | grep -q pulse_gateway_events_received_total; then
      return 0
    fi
    sleep 1
  done
  ingress_die 'gateway readiness timed out after 60 seconds'
}

kafka_raw_offsets() {
  local time="$1" output
  output="$("${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic telemetry.raw --time "$time")" || ingress_die 'could not read telemetry.raw offsets'
  printf '%s\n' "$output" | awk -F: 'NF >= 3 { total += $NF; seen++ } END { if (seen == 0) exit 1; print total }'
}

record_verification_metadata() {
  local os cpu cores memory partitions
  os="$(uname -srm)"
  case "$(uname -s)" in
    Darwin) cpu="$(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m)"; cores="$(sysctl -n hw.logicalcpu 2>/dev/null || printf unknown)"; memory="$(sysctl -n hw.memsize 2>/dev/null || printf unknown)" ;;
    Linux) cpu="$(awk -F: '/model name/ {sub(/^[ \t]+/, "", $2); print $2; exit}' /proc/cpuinfo 2>/dev/null || uname -m)"; cores="$(getconf _NPROCESSORS_ONLN 2>/dev/null || printf unknown)"; memory="$(awk '/MemTotal/ {print $2 " kB"}' /proc/meminfo 2>/dev/null || printf unknown)" ;;
    *) cpu="$(uname -m)"; cores=unknown; memory=unknown ;;
  esac
  partitions="$(sed -n 's/.*PartitionCount: \([0-9][0-9]*\).*/\1/p' "$ARTIFACT_DIR/topic.txt" | head -n 1)"
  [[ "$partitions" =~ ^[1-9][0-9]*$ ]] || ingress_die 'could not determine telemetry.raw partition count'
  jq -n --arg os "$os" --arg cpu "$cpu" --arg cores "$cores" --arg memory "$memory" \
    --argjson partitions "$partitions" \
    '{host:{os:$os,cpu_model:$cpu,logical_cores:$cores,memory:$memory},topology:{client_to_gateway:"host gRPC at 127.0.0.1:50051",gateway_to_kafka:"single local Kafka broker at localhost:9092",topic:"telemetry.raw",partitions:$partitions},sdk:{queue_capacity:65536,max_batch_size:500,flush_interval_ms:5,rpc_timeout_seconds:2,retry_initial_ms:50,retry_max_seconds:5},gateway:{grpc_addr:"127.0.0.1:50051",metrics_addr:"127.0.0.1:19464",max_batch_events:500,producer_max_buffered_records:4000,producer_max_buffered_bytes:67108864,required_acks:"all",producer_linger_ms:5,compression:"lz4"}}' \
    >"$ARTIFACT_DIR/config.json"
}

run_ingress_verification() {
  local rate="$1" duration="$2" seed="$3" output_file="$4" expected before after summary acknowledged
  expected="$(calculate_expected_events "$rate" "$duration")"
  before="$(kafka_raw_offsets -1)"
  if ! PULSE_EVENTS_PER_SECOND="$rate" PULSE_DURATION_SECONDS="$duration" PULSE_RANDOM_SEED="$seed" \
    PULSE_ROUTE_COUNT=8 PULSE_GATEWAY_ADDR=http://127.0.0.1:50051 \
    "$ROOT_DIR/target/debug/pulse-demo" >"$output_file" 2>"${output_file%.json}.stderr"; then
    ingress_die "load generator failed; see ${output_file%.json}.stderr"
  fi
  summary="$(tail -n 1 "$output_file")"
  assert_summary_valid "$summary" "$expected"
  acknowledged="$(jq -er '.acknowledged' <<<"$summary")" || ingress_die 'SDK summary has no acknowledged event count'
  after="$(kafka_raw_offsets -1)"
  local delta=$((after - before))
  [[ "$delta" -eq "$acknowledged" ]] || ingress_die "telemetry.raw offset delta $delta does not equal acknowledged event count $acknowledged"
  jq --argjson before "$before" --argjson after "$after" --argjson delta "$delta" \
    '. + {kafka_offset_before:$before,kafka_offset_after:$after,kafka_offset_delta:$delta}' \
    <<<"$summary" >"${output_file%.json}.validated.json" || ingress_die 'could not write the validated verification summary'
}
