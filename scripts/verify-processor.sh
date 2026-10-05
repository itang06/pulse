#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/ingress-common.sh"
[[ $# -eq 0 ]] || ingress_die 'usage: verify-processor.sh'
ingress_run_locked "$0" "$@"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROCESSOR_RUN_ID="$(date -u '+%Y%m%dT%H%M%SZ')-$$"
ARTIFACT_DIR="${ARTIFACT_DIR:-$ROOT_DIR/artifacts/runs/processor/verify-$PROCESSOR_RUN_ID}"
ingress_init
start_ingress_dependencies

command -v awk >/dev/null 2>&1 || ingress_die 'awk is required'
command -v curl >/dev/null 2>&1 || ingress_die 'curl is required'
command -v jq >/dev/null 2>&1 || ingress_die 'jq is required'

(cd "$ROOT_DIR" && cargo build -p pulse-demo --bin pulse-demo) >"$ARTIFACT_DIR/cargo-build-demo.log" 2>&1 || ingress_die 'failed to build pulse-demo'
(cd "$ROOT_DIR" && cargo build -p pulse-processor --bin pulse-processor --bin inspect-records) >"$ARTIFACT_DIR/cargo-build-processor.log" 2>&1 || ingress_die 'failed to build pulse-processor and inspect-records'
CARGO_TARGET_PATH="$(cd "$ROOT_DIR" && cargo metadata --no-deps --format-version 1 | jq -er '.target_directory')" \
  || ingress_die 'could not resolve Cargo target directory'
[[ -x "$CARGO_TARGET_PATH/debug/pulse-demo" && -x "$CARGO_TARGET_PATH/debug/pulse-processor" && -x "$CARGO_TARGET_PATH/debug/inspect-records" ]] \
  || ingress_die 'Cargo build did not produce each required executable'
(cd "$ROOT_DIR/gateway" && go build -o "$TMP_DIR/pulse-gateway" ./cmd/gateway) >"$ARTIFACT_DIR/go-build.log" 2>&1 || ingress_die 'failed to build the gateway'

PULSE_GRPC_ADDR=127.0.0.1:50051 PULSE_METRICS_ADDR=127.0.0.1:19464 \
  PULSE_KAFKA_BROKERS=127.0.0.1:9092 "$TMP_DIR/pulse-gateway" \
  >"$ARTIFACT_DIR/gateway.log" 2>&1 &
GATEWAY_PID=$!
for attempt in $(seq 1 60); do
  if ! kill -0 "$GATEWAY_PID" 2>/dev/null; then
    cat "$ARTIFACT_DIR/gateway.log" >&2
    ingress_die 'gateway exited before readiness'
  fi
  if curl --silent --fail http://127.0.0.1:19464/metrics 2>/dev/null | grep -q pulse_gateway_events_received_total; then break; fi
  [[ "$attempt" -lt 60 ]] || ingress_die 'gateway readiness timed out after 60 seconds'
  sleep 1
done

GROUP_ID="pulse-processor-verify-$RUN_ID"
PULSE_KAFKA_BROKERS=127.0.0.1:9092 PULSE_CONSUMER_GROUP="$GROUP_ID" \
  PULSE_METRICS_ADDR=127.0.0.1:19465 "$CARGO_TARGET_PATH/debug/pulse-processor" \
  >"$ARTIFACT_DIR/processor.log" 2>&1 &
PROCESSOR_PID=$!
wait_for_processor "$PROCESSOR_PID" http://127.0.0.1:19465/metrics pulse_processor_events_consumed_total "$ARTIFACT_DIR/processor.log"
assert_processor_alive() {
  if ! kill -0 "$PROCESSOR_PID" 2>/dev/null || ! curl --silent --fail http://127.0.0.1:19465/metrics >/dev/null 2>&1; then
    cat "$ARTIFACT_DIR/processor.log" >&2
    ingress_die 'processor is not alive at the verification checkpoint'
  fi
}

kafka_partition_offsets() {
  local topic="$1" output
  output="$("${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic "$topic" --time -1)" || ingress_die "could not read $topic partition offsets"
  printf '%s\n' "$output" | awk -F: 'NF >= 3 { print $2, $3 }' | sort -n -k1,1
}

sum_partition_offsets() {
  awk '{ if ($1 !~ /^[0-9]+$/ || $2 !~ /^[0-9]+$/) bad=1; total += $2; seen++ } END { if (bad || seen == 0) exit 1; printf "%.0f\n", total }' "$1"
}

partition_offset() {
  awk -v wanted="$2" '$1 == wanted { value=$2; seen++ } END { if (seen != 1) exit 1; print value }' "$1"
}

inspect_topic_range() {
  local kind="$1" before_file="$2" after_file="$3" output_file="$4" partition start end
  : >"$output_file"
  while read -r partition start; do
    end="$(partition_offset "$after_file" "$partition")" || ingress_die "missing $kind end offset for partition $partition"
    PULSE_KAFKA_BROKERS=127.0.0.1:9092 "$CARGO_TARGET_PATH/debug/inspect-records" "$kind" "$partition" "$start" "$end" \
      >>"$output_file" || ingress_die "failed to inspect exact $kind range for partition $partition"
  done <"$before_file"
}

group_status() {
  "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --describe --group "$GROUP_ID" 2>/dev/null
}

RAW_PARTITION_COUNT="$(sed -n 's/.*PartitionCount: \([0-9][0-9]*\).*/\1/p' "$ARTIFACT_DIR/topic.txt" | head -n 1)"
[[ "$RAW_PARTITION_COUNT" =~ ^[1-9][0-9]*$ ]] || ingress_die 'could not determine raw topic partition count'

# The processor starts with earliest offsets. Drain any old raw backlog before
# establishing per-run output baselines, so previous records cannot skew deltas.
wait_for_group_caught_up() {
  local snapshot_path="$1" deadline=$((SECONDS + 120)) status parsed
  while (( SECONDS < deadline )); do
    status="$(group_status || true)"
    parsed="$(parse_raw_group_status "$status" "$RAW_PARTITION_COUNT" "$GROUP_ID" 2>/dev/null || true)"
    if [[ -n "$parsed" ]]; then
      printf '%s\n' "$status" >"$snapshot_path"
      return 0
    fi
    if ! kill -0 "$PROCESSOR_PID" 2>/dev/null; then cat "$ARTIFACT_DIR/processor.log" >&2; ingress_die 'processor exited while consuming'; fi
    sleep 1
  done
  ingress_die 'processor did not commit through the current raw log end within 120 seconds'
}

PRE_RUN_GROUP_STATUS="$ARTIFACT_DIR/consumer-group-before.txt"
FINAL_GROUP_STATUS="$ARTIFACT_DIR/consumer-group-final.txt"
wait_for_group_caught_up "$PRE_RUN_GROUP_STATUS"
kafka_partition_offsets telemetry.raw >"$ARTIFACT_DIR/raw-offsets-before.txt"
kafka_partition_offsets telemetry.enriched >"$ARTIFACT_DIR/enriched-offsets-before.txt"
kafka_partition_offsets telemetry.dlq >"$ARTIFACT_DIR/dlq-offsets-before.txt"
raw_before="$(sum_partition_offsets "$ARTIFACT_DIR/raw-offsets-before.txt")" || ingress_die 'invalid raw partition offsets'
enriched_before="$(sum_partition_offsets "$ARTIFACT_DIR/enriched-offsets-before.txt")" || ingress_die 'invalid enriched partition offsets'
dlq_before="$(sum_partition_offsets "$ARTIFACT_DIR/dlq-offsets-before.txt")" || ingress_die 'invalid DLQ partition offsets'
read -r committed_before end_before lag_before partitions_before < <(parse_raw_group_status "$(<"$PRE_RUN_GROUP_STATUS")" "$RAW_PARTITION_COUNT" "$GROUP_ID") \
  || ingress_die 'could not read pre-run committed raw offsets'
[[ "$committed_before" == "$raw_before" && "$end_before" == "$raw_before" && "$partitions_before" == "$RAW_PARTITION_COUNT" ]] \
  || ingress_die 'consumer group pre-run committed offsets did not match every raw partition log end'

PULSE_EVENTS_PER_SECOND=10 PULSE_DURATION_SECONDS=2 PULSE_RANDOM_SEED=42 \
  PULSE_ROUTE_COUNT=8 PULSE_GATEWAY_ADDR=http://127.0.0.1:50051 \
  "$CARGO_TARGET_PATH/debug/pulse-demo" >"$ARTIFACT_DIR/loadgen.jsonl" 2>"$ARTIFACT_DIR/loadgen.stderr" \
  || ingress_die 'load generator failed; see loadgen.stderr'
valid_count="$(tail -n 1 "$ARTIFACT_DIR/loadgen.jsonl" | jq -er '.acknowledged')" \
  || ingress_die 'could not read valid-event acknowledgement count'
assert_summary_valid "$(tail -n 1 "$ARTIFACT_DIR/loadgen.jsonl")" 20
[[ "$valid_count" -eq 20 ]] || ingress_die "deterministic load generator acknowledged $valid_count events; expected 20"

# This is a single intentionally invalid Protobuf value, written directly to
# the raw topic after the gateway batch has been acknowledged.
printf 'pulse-malformed-key:not-a-protobuf-record\n' | \
  "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic telemetry.raw --property parse.key=true \
    --property key.separator=: >"$ARTIFACT_DIR/malformed-producer.log" 2>&1 \
  || ingress_die 'failed to inject the malformed raw record'

raw_after_expected=$((raw_before + valid_count + 1))
deadline=$((SECONDS + 120))
while (( SECONDS < deadline )); do
  kafka_partition_offsets telemetry.raw >"$ARTIFACT_DIR/raw-offsets-after.txt"
  kafka_partition_offsets telemetry.enriched >"$ARTIFACT_DIR/enriched-offsets-after.txt"
  kafka_partition_offsets telemetry.dlq >"$ARTIFACT_DIR/dlq-offsets-after.txt"
  raw_after="$(sum_partition_offsets "$ARTIFACT_DIR/raw-offsets-after.txt")" || ingress_die 'invalid raw partition offsets'
  enriched_after="$(sum_partition_offsets "$ARTIFACT_DIR/enriched-offsets-after.txt")" || ingress_die 'invalid enriched partition offsets'
  dlq_after="$(sum_partition_offsets "$ARTIFACT_DIR/dlq-offsets-after.txt")" || ingress_die 'invalid DLQ partition offsets'
  if (( raw_after >= raw_after_expected && enriched_after >= enriched_before + valid_count && dlq_after >= dlq_before + 1 )); then
    wait_for_group_caught_up "$FINAL_GROUP_STATUS"
    status="$(<"$FINAL_GROUP_STATUS")"
    parsed="$(parse_raw_group_status "$status" "$RAW_PARTITION_COUNT" "$GROUP_ID" 2>/dev/null || true)"
    read -r committed_after end_after lag partitions_after <<<"$parsed"
    if [[ "$committed_after" == "$raw_after" && "$end_after" == "$raw_after" && "$lag" == 0 && "$partitions_after" == "$RAW_PARTITION_COUNT" ]]; then
      break
    fi
  fi
  if ! kill -0 "$PROCESSOR_PID" 2>/dev/null; then cat "$ARTIFACT_DIR/processor.log" >&2; ingress_die 'processor exited during verification'; fi
  sleep 1
done
(( SECONDS < deadline )) || ingress_die 'timed out waiting for enriched, DLQ, and committed-offset evidence'

raw_delta=$((raw_after - raw_before))
enriched_delta=$((enriched_after - enriched_before))
dlq_delta=$((dlq_after - dlq_before))
[[ "$raw_delta" -eq $((valid_count + 1)) ]] || ingress_die "raw delta $raw_delta did not equal $valid_count valid records plus one malformed record"
[[ "$enriched_delta" -eq "$valid_count" ]] || ingress_die "enriched delta $enriched_delta did not equal valid event count $valid_count"
[[ "$dlq_delta" -eq 1 ]] || ingress_die "DLQ delta $dlq_delta did not equal the single malformed record"
[[ "$raw_delta" -eq $((enriched_delta + dlq_delta)) ]] || ingress_die 'raw offset delta does not equal enriched plus DLQ deltas'

inspect_topic_range raw "$ARTIFACT_DIR/raw-offsets-before.txt" "$ARTIFACT_DIR/raw-offsets-after.txt" "$ARTIFACT_DIR/raw-records.jsonl"
inspect_topic_range enriched "$ARTIFACT_DIR/enriched-offsets-before.txt" "$ARTIFACT_DIR/enriched-offsets-after.txt" "$ARTIFACT_DIR/enriched-records.jsonl"
inspect_topic_range dlq "$ARTIFACT_DIR/dlq-offsets-before.txt" "$ARTIFACT_DIR/dlq-offsets-after.txt" "$ARTIFACT_DIR/dlq-records.jsonl"
for ((sequence = 0; sequence < valid_count; sequence++)); do printf 'demo-%016x\n' "$sequence"; done | sort >"$ARTIFACT_DIR/expected-trace-ids.txt"
jq -sr '[.[] | select(.kind == "valid") | .trace_id] | sort[]' "$ARTIFACT_DIR/raw-records.jsonl" >"$ARTIFACT_DIR/raw-trace-ids.txt"
jq -sr '[.[] | select(.kind == "enriched") | .trace_id] | sort[]' "$ARTIFACT_DIR/enriched-records.jsonl" >"$ARTIFACT_DIR/enriched-trace-ids.txt"
cmp -s "$ARTIFACT_DIR/expected-trace-ids.txt" "$ARTIFACT_DIR/raw-trace-ids.txt" || ingress_die 'raw offset range did not contain exactly the deterministic load generator trace IDs'
cmp -s "$ARTIFACT_DIR/expected-trace-ids.txt" "$ARTIFACT_DIR/enriched-trace-ids.txt" || ingress_die 'enriched offset range did not contain exactly the deterministic load generator trace IDs'
jq -sr '[.[] | select(.kind == "valid") | [.trace_id, .event_id] | @tsv] | sort[]' "$ARTIFACT_DIR/raw-records.jsonl" >"$ARTIFACT_DIR/raw-trace-event-pairs.tsv"
jq -sr '[.[] | select(.kind == "enriched") | [.trace_id, .event_id] | @tsv] | sort[]' "$ARTIFACT_DIR/enriched-records.jsonl" >"$ARTIFACT_DIR/enriched-trace-event-pairs.tsv"
cmp -s "$ARTIFACT_DIR/raw-trace-event-pairs.tsv" "$ARTIFACT_DIR/enriched-trace-event-pairs.tsv" || ingress_die 'enriched (trace_id,event_id) pairs did not match raw valid event pairs'
malformed_raw="$(jq -sc '[.[] | select(.kind == "malformed")] | if length == 1 then .[0] else error("expected one malformed raw record") end' "$ARTIFACT_DIR/raw-records.jsonl")" \
  || ingress_die 'the measured raw range did not contain exactly one malformed Protobuf record'
malformed_partition="$(jq -er '.partition' <<<"$malformed_raw")"
malformed_offset="$(jq -er '.offset' <<<"$malformed_raw")"
malformed_payload_hex="$(jq -er '.payload_hex' <<<"$malformed_raw")"
malformed_key_hex="$(jq -er '.key_hex' <<<"$malformed_raw")"
expected_payload_hex=6e6f742d612d70726f746f6275662d7265636f7264
expected_key_hex=70756c73652d6d616c666f726d65642d6b6579
[[ "$malformed_payload_hex" == "$expected_payload_hex" && "$malformed_key_hex" == "$expected_key_hex" ]] \
  || ingress_die 'measured malformed raw record did not match its deliberate key and payload markers'
jq -se --argjson partition "$malformed_partition" --argjson offset "$malformed_offset" --arg payload "$malformed_payload_hex" \
  'length == 1 and .[0].stage == 1 and .[0].source_topic == "telemetry.raw" and .[0].source_partition == $partition and .[0].source_offset == $offset and .[0].error_category == "decode" and (.[0].reason | length) > 0 and .[0].original_payload_hex == $payload' \
  "$ARTIFACT_DIR/dlq-records.jsonl" >/dev/null || ingress_die 'DLQ payload or source metadata did not match the malformed raw record'
assert_processor_alive

jq -n --arg run_id "$RUN_ID" --arg group_id "$GROUP_ID" --arg kafka_container_id "$OWNED_KAFKA_ID" \
  --arg cargo_target_dir "$CARGO_TARGET_PATH" \
  --argjson valid_events "$valid_count" --argjson raw_before "$raw_before" --argjson raw_after "$raw_after" \
  --argjson enriched_before "$enriched_before" --argjson enriched_after "$enriched_after" \
  --argjson dlq_before "$dlq_before" --argjson dlq_after "$dlq_after" \
  --argjson raw_delta "$raw_delta" --argjson enriched_delta "$enriched_delta" --argjson dlq_delta "$dlq_delta" \
  --argjson committed_offset "$committed_after" --argjson lag "$lag" --argjson partitions "$partitions_after" \
  '{run_id:$run_id,consumer_group:$group_id,kafka_container_id:$kafka_container_id,build_evidence:{cargo_target_dir:$cargo_target_dir,executables_built:["pulse-demo","pulse-processor","inspect-records"]},valid_events_acknowledged:$valid_events,malformed_raw_records:1,offsets:{raw:{before:$raw_before,after:$raw_after,delta:$raw_delta},enriched:{before:$enriched_before,after:$enriched_after,delta:$enriched_delta},dlq:{before:$dlq_before,after:$dlq_after,delta:$dlq_delta}},consumer_group_evidence:{raw_committed_offset:$committed_offset,raw_log_end_offset:$raw_after,raw_lag:$lag,partitions:$partitions},identity_checks:{deterministic_trace_ids_match:true,trace_event_pairs_match:true,deliberate_malformed_marker_matches:true,malformed_payload_and_dlq_source_match:true},checks:{raw_delta_equals_enriched_plus_dlq:($raw_delta == $enriched_delta + $dlq_delta),valid_events_enriched:($enriched_delta == $valid_events),malformed_record_dead_lettered:($dlq_delta == 1),consumer_caught_up:($lag == 0 and $committed_offset == $raw_after)}}' \
  >"$ARTIFACT_DIR/verification.json" || ingress_die 'failed to write verification report'
jq -e '(.checks | to_entries | all(.value == true)) and (.identity_checks | to_entries | all(.value == true))' \
  "$ARTIFACT_DIR/verification.json" >/dev/null || ingress_die 'verification report contains a failed check'
assert_processor_alive
printf 'Processor verification passed. Evidence: %s\n' "$ARTIFACT_DIR/verification.json"
