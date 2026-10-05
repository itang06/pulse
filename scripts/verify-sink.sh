#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPT_DIR/lib/ingress-common.sh"

[[ $# -le 2 ]] || ingress_die 'usage: verify-sink.sh [COUNT [CRASHES]]'
COUNT="$(normalize_positive_integer count "${1:-50000}")" || ingress_die 'invalid count'
CRASHES="$(normalize_positive_integer crashes "${2:-3}")" || ingress_die 'invalid crash count'
(( COUNT <= 281474976710655 )) || ingress_die 'count exceeds the deterministic 12-hex-digit event ID range'

ingress_run_locked "$0" "$@"
RUN_ID="$(date -u '+%Y%m%dt%H%M%Sz')-$$"
PROJECT="pulse-sink-$RUN_ID"
ARTIFACT_DIR="${ARTIFACT_DIR:-$ROOT_DIR/artifacts/runs/sink/$RUN_ID}"
COMPOSE=(docker compose --project-name "$PROJECT" --env-file "$ROOT_DIR/deploy/.env.example" -f "$ROOT_DIR/deploy/docker-compose.yml")

command -v docker >/dev/null 2>&1 || ingress_die 'docker is required'
command -v curl >/dev/null 2>&1 || ingress_die 'curl is required'
command -v jq >/dev/null 2>&1 || ingress_die 'jq is required'
command -v lsof >/dev/null 2>&1 || ingress_die 'lsof is required to refuse conflicting host services'
[[ -f "$ROOT_DIR/deploy/.env.example" ]] || ingress_die 'deploy/.env.example is required'
[[ -n "$(docker info --format '{{.ServerVersion}}' 2>/dev/null)" ]] || ingress_die 'Docker daemon is unavailable'

# Compose fixes these loopback ports for local host clients. Refuse rather than
# attach the run to a developer stack or another process.
for port in 9092 5432; do
  if lsof -nP -iTCP:"$port" -sTCP:LISTEN -t >/dev/null 2>&1; then
    ingress_die "127.0.0.1:$port is already bound; stop the conflicting service and retry"
  fi
done
for kind in container volume network; do
  case "$kind" in
    container) existing="$(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT")" ;;
    volume) existing="$(docker volume ls -q --filter "label=com.docker.compose.project=$PROJECT")" ;;
    network) existing="$(docker network ls -q --filter "label=com.docker.compose.project=$PROJECT")" ;;
  esac
  [[ -z "$existing" ]] || ingress_die "unique Compose project already has $kind resources: $PROJECT"
done

# No cleanup trap is active until the unique project is proven empty. From this
# point onward, every resource carrying this project label belongs to this run.
mkdir -p "$ARTIFACT_DIR"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/pulse-sink.XXXXXX")"
SINK_PID=''
OWNED_CONTAINER_IDS=()
OWNED_VOLUME_NAMES=()
OWNED_NETWORK_IDS=()
cleanup() {
  local exit_code=$? id project service name label_project
  trap - EXIT INT TERM
  if [[ -n "$SINK_PID" ]] && kill -0 "$SINK_PID" 2>/dev/null; then
    kill "$SINK_PID" 2>/dev/null || true
    wait "$SINK_PID" 2>/dev/null || true
  fi
  while IFS= read -r id; do
    [[ -n "$id" ]] || continue
    read -r project service <<<"$(docker inspect --format '{{index .Config.Labels "com.docker.compose.project"}} {{index .Config.Labels "com.docker.compose.service"}}' "$id" 2>/dev/null || true)"
    if [[ "$project" == "$PROJECT" && -n "$service" ]]; then OWNED_CONTAINER_IDS+=("$id"); else ingress_error "cleanup skipped unverified container $id"; fi
  done < <(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT" 2>/dev/null || true)
  while IFS= read -r name; do
    [[ -n "$name" ]] || continue
    label_project="$(docker volume inspect --format '{{index .Labels "com.docker.compose.project"}}' "$name" 2>/dev/null || true)"
    if [[ "$label_project" == "$PROJECT" && "$name" == "$PROJECT"* ]]; then OWNED_VOLUME_NAMES+=("$name"); else ingress_error "cleanup skipped unverified volume $name"; fi
  done < <(docker volume ls -q --filter "label=com.docker.compose.project=$PROJECT" 2>/dev/null || true)
  while IFS= read -r id; do
    [[ -n "$id" ]] || continue
    label_project="$(docker network inspect --format '{{index .Labels "com.docker.compose.project"}}' "$id" 2>/dev/null || true)"
    if [[ "$label_project" == "$PROJECT" ]]; then OWNED_NETWORK_IDS+=("$id"); else ingress_error "cleanup skipped unverified network $id"; fi
  done < <(docker network ls -q --filter "label=com.docker.compose.project=$PROJECT" 2>/dev/null || true)
  if ((${#OWNED_CONTAINER_IDS[@]})); then docker rm -f "${OWNED_CONTAINER_IDS[@]}" >/dev/null 2>&1 || ingress_error 'some verified containers could not be removed'; fi
  if ((${#OWNED_VOLUME_NAMES[@]})); then docker volume rm "${OWNED_VOLUME_NAMES[@]}" >/dev/null 2>&1 || ingress_error 'some verified volumes could not be removed'; fi
  if ((${#OWNED_NETWORK_IDS[@]})); then docker network rm "${OWNED_NETWORK_IDS[@]}" >/dev/null 2>&1 || ingress_error 'some verified networks could not be removed'; fi
  rm -rf "$TMP_DIR"
  printf '%s\n' "$exit_code" >"$ARTIFACT_DIR/exit-code.txt" 2>/dev/null || true
  exit "$exit_code"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

example_value() {
  awk -v wanted="$1" '
    {
      separator = index($0, "=")
      if (separator > 0 && substr($0, 1, separator - 1) == wanted) {
        print substr($0, separator + 1)
        found = 1
        exit
      }
    }
    END { if (!found) exit 1 }
  ' "$ROOT_DIR/deploy/.env.example"
}
resolve_compose_value() {
  local key="$1" value
  if value="$(printenv "$key" 2>/dev/null)" && [[ -n "$value" ]]; then
    printf '%s' "$value"
  else
    example_value "$key"
  fi
}
POSTGRES_USER="$(resolve_compose_value POSTGRES_USER)" || ingress_die 'POSTGRES_USER is absent from environment and deploy/.env.example'
POSTGRES_PASSWORD="$(resolve_compose_value POSTGRES_PASSWORD)" || ingress_die 'POSTGRES_PASSWORD is absent from environment and deploy/.env.example'
POSTGRES_DB="$(resolve_compose_value POSTGRES_DB)" || ingress_die 'POSTGRES_DB is absent from environment and deploy/.env.example'
[[ -n "$POSTGRES_USER" && -n "$POSTGRES_PASSWORD" && -n "$POSTGRES_DB" ]] || ingress_die 'PostgreSQL environment values must be nonempty'
export POSTGRES_USER POSTGRES_PASSWORD POSTGRES_DB

{
  printf '{"run_id":"%s","compose_project":"%s","count":%s,"crashes":%s}\n' "$RUN_ID" "$PROJECT" "$COUNT" "$CRASHES"
  uname -a
  docker version --format 'docker_client={{.Client.Version}} docker_server={{.Server.Version}}'
  rustc --version
  cargo --version
} >"$ARTIFACT_DIR/host.txt" 2>&1
"${COMPOSE[@]}" config --format json \
  | jq '(.services |= with_entries(if (.value.environment | type) == "object" then .value.environment |= with_entries(if (.key | test("PASSWORD|TOKEN|SECRET|CREDENTIAL|PRIVATE"; "i")) then .value = "<redacted>" else . end) else . end))' \
  >"$ARTIFACT_DIR/compose-config.redacted.json"
"${COMPOSE[@]}" config --images >"$ARTIFACT_DIR/images.txt"

(cd "$ROOT_DIR" && cargo build -p pulse-sink --bin pulse-sink --bin publish-enriched) >"$ARTIFACT_DIR/cargo-build.log" 2>&1 \
  || ingress_die 'sink binaries failed to build; see cargo-build.log'
CARGO_TARGET_PATH="$(cd "$ROOT_DIR" && cargo metadata --no-deps --format-version 1 | jq -er '.target_directory')" \
  || ingress_die 'could not resolve Cargo target directory'
SINK_BIN="$CARGO_TARGET_PATH/debug/pulse-sink"
PRODUCER_BIN="$CARGO_TARGET_PATH/debug/publish-enriched"
[[ -x "$SINK_BIN" && -x "$PRODUCER_BIN" ]] || ingress_die 'Cargo did not produce both sink executables'

GIT_HEAD="$(git -C "$ROOT_DIR" rev-parse HEAD)"
git -C "$ROOT_DIR" status --porcelain=v1 --untracked-files=all >"$ARTIFACT_DIR/git-status.txt"
GIT_DIFF_SHA256="$(git -C "$ROOT_DIR" diff --binary HEAD | shasum -a 256 | awk '{print $1}')"
SINK_SHA256="$(shasum -a 256 "$SINK_BIN" | awk '{print $1}')"
PRODUCER_SHA256="$(shasum -a 256 "$PRODUCER_BIN" | awk '{print $1}')"
SOURCE_MANIFEST_PATH='source-manifest.jsonl'
if ! "$SCRIPT_DIR/lib/source-manifest.sh" "$ROOT_DIR" "$ARTIFACT_DIR/$SOURCE_MANIFEST_PATH" "$TMP_DIR"; then
  ingress_die 'source manifest could not hash every tracked and non-ignored workspace path'
fi
SOURCE_MANIFEST_SHA256="$(shasum -a 256 "$ARTIFACT_DIR/$SOURCE_MANIFEST_PATH" | awk '{print $1}')"
jq -n --arg head "$GIT_HEAD" --arg dirty_diff_sha256 "$GIT_DIFF_SHA256" \
  --arg sink_binary_sha256 "$SINK_SHA256" --arg producer_binary_sha256 "$PRODUCER_SHA256" \
  --arg status_file git-status.txt --arg manifest_path "$SOURCE_MANIFEST_PATH" \
  --arg manifest_sha256 "$SOURCE_MANIFEST_SHA256" \
  '{git_head:$head,dirty_diff_sha256:$dirty_diff_sha256,dirty_status_file:$status_file,source_manifest:{path:$manifest_path,sha256:$manifest_sha256},binaries:{pulse_sink_sha256:$sink_binary_sha256,publish_enriched_sha256:$producer_binary_sha256}}' \
  >"$ARTIFACT_DIR/source-provenance.json"

"${COMPOSE[@]}" up -d --wait kafka timescaledb >"$ARTIFACT_DIR/compose-up.log" 2>&1 \
  || ingress_die 'Kafka or TimescaleDB did not become healthy; see compose-up.log'
"${COMPOSE[@]}" run --rm kafka-init >"$ARTIFACT_DIR/kafka-init.log" 2>&1 \
  || ingress_die 'Kafka topic initialization failed; see kafka-init.log'
for migration in "$ROOT_DIR"/deploy/migrations/*.sql; do
  name="$(basename "$migration")"
  "${COMPOSE[@]}" exec -T timescaledb psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" <"$migration" \
    >"$ARTIFACT_DIR/$name.log" 2>&1 || ingress_die "migration $name failed"
done

get_topic_offsets() {
  local topic="$1"
  "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic "$topic" --time -1 \
    | awk -F: 'NF >= 3 {print $2, $3}' | sort -n -k1,1
}
sum_offsets() {
  local expected="${2:-3}"
  awk -v expected="$expected" '{ if ($1 !~ /^[0-9]+$/ || $2 !~ /^[0-9]+$/) bad=1; total += $2; rows++ } END { if (bad || rows != expected) exit 1; printf "%.0f\n", total }' "$1"
}
get_group_status() {
  "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --describe --group "$GROUP_ID" 2>/dev/null
}
parse_group_offsets() {
  awk -v group="$GROUP_ID" '
    $1 == "GROUP" && $2 == "TOPIC" {header=1; next}
    header && $1 == group && $2 == "telemetry.enriched" {
      p=$3; c=$4; e=$5; l=$6
      if (p !~ /^[0-9]+$/ || p >= 3 || seen[p]++) bad=1
      if (c == "-") c=0
      if (l == "-") l=e-c
      if (c !~ /^[0-9]+$/ || e !~ /^[0-9]+$/ || l !~ /^[0-9]+$/) bad=1
      printf "%s %s %s %s\n", p, c, e, l
      rows++
    }
    END { if (!header || bad || rows != 3) exit 1 }
  ' | sort -n -k1,1
}

kafka_init_id="$("${COMPOSE[@]}" ps -aq kafka-init)"
kafka_id="$("${COMPOSE[@]}" ps -q kafka)"
db_id="$("${COMPOSE[@]}" ps -q timescaledb)"
[[ -n "$kafka_id" && -n "$db_id" ]] || ingress_die 'could not resolve owned Kafka and TimescaleDB container IDs'
for id in "$kafka_id" "$db_id"; do
  read -r owner service <<<"$(docker inspect --format '{{index .Config.Labels "com.docker.compose.project"}} {{index .Config.Labels "com.docker.compose.service"}}' "$id")"
  [[ "$owner" == "$PROJECT" && ( "$service" == kafka || "$service" == timescaledb ) ]] || ingress_die "container $id did not carry expected project/service labels"
done
printf '%s\n' "project=$PROJECT" "kafka_container_id=$kafka_id" "timescaledb_container_id=$db_id" "kafka_init_container_id=${kafka_init_id:-removed-on-success}" >"$ARTIFACT_DIR/owned-resources.txt"
for id in "$kafka_id" "$db_id"; do
  docker inspect --format 'container={{.Name}} image={{.Config.Image}} image_id={{.Image}}' "$id"
done >"$ARTIFACT_DIR/images-runtime.txt"

GROUP_ID="pulse-sink-verify-$RUN_ID"
METRICS_PORT=$((19000 + $$ % 1000))
if lsof -nP -iTCP:"$METRICS_PORT" -sTCP:LISTEN -t >/dev/null 2>&1; then ingress_die "metrics port $METRICS_PORT is already bound"; fi
export PULSE_KAFKA_BROKERS=127.0.0.1:9092
export PULSE_CONSUMER_GROUP="$GROUP_ID"
export PULSE_METRICS_ADDR="127.0.0.1:$METRICS_PORT"
encode_uri_component() {
  jq -nr --arg value "$1" '$value | @uri'
}
PULSE_DATABASE_URL="postgres://$(encode_uri_component "$POSTGRES_USER"):$(encode_uri_component "$POSTGRES_PASSWORD")@127.0.0.1:5432/$(encode_uri_component "$POSTGRES_DB")"
export PULSE_DATABASE_URL

get_topic_offsets telemetry.enriched >"$ARTIFACT_DIR/enriched-offsets-before.txt"
get_topic_offsets telemetry.dlq >"$ARTIFACT_DIR/dlq-offsets-before.txt"
enriched_before="$(sum_offsets "$ARTIFACT_DIR/enriched-offsets-before.txt")" || ingress_die 'invalid initial enriched offsets'
dlq_before="$(sum_offsets "$ARTIFACT_DIR/dlq-offsets-before.txt" 1)" || ingress_die 'invalid initial DLQ offsets'
"$PRODUCER_BIN" "$COUNT" >"$ARTIFACT_DIR/producer-summary.json" 2>"$ARTIFACT_DIR/producer.log" \
  || ingress_die 'deterministic producer failed; see producer.log'
jq -e --argjson count "$COUNT" '.topic == "telemetry.enriched" and .requested == $count and .acknowledged == $count and ([.partition_distribution[]] | add) == $count' \
  "$ARTIFACT_DIR/producer-summary.json" >/dev/null || ingress_die 'producer summary did not confirm every send'
get_topic_offsets telemetry.enriched >"$ARTIFACT_DIR/enriched-offsets-published.txt"
enriched_published="$(sum_offsets "$ARTIFACT_DIR/enriched-offsets-published.txt")" || ingress_die 'invalid published enriched offsets'
enriched_published_delta=$((enriched_published - enriched_before))

# Seed this unique group's three source offsets at the beginning explicitly;
# every failpoint run must demonstrate that these values stay unchanged.
"${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --reset-offsets --to-earliest \
  --group "$GROUP_ID" --topic telemetry.enriched --execute \
  >"$ARTIFACT_DIR/group-offset-seed.txt" 2>&1 \
  || ingress_die 'could not seed the unique sink group at the earliest source offsets'

group_snapshot() {
  local destination="$1" raw parsed deadline=$((SECONDS + 60))
  while (( SECONDS < deadline )); do
    raw="$(get_group_status || true)"
    if parsed="$(printf '%s\n' "$raw" | parse_group_offsets 2>/dev/null)"; then
      printf '%s\n' "$raw" >"$destination"
      printf '%s\n' "$parsed"
      return 0
    fi
    sleep 1
  done
  ingress_die "consumer group did not expose all three enriched partitions: $destination"
}
CONFIRMED_CRASHES=0
for ((crash=1; crash<=CRASHES; crash++)); do
  before="$ARTIFACT_DIR/crash-$crash-offsets-before.txt"
  after="$ARTIFACT_DIR/crash-$crash-offsets-after.txt"
  group_snapshot "$before" >"$before.parsed"
  PULSE_SINK_FAILPOINT=after_db_commit_before_offset_commit "$SINK_BIN" >"$ARTIFACT_DIR/crash-$crash.log" 2>&1 &
  SINK_PID=$!
  deadline=$((SECONDS + 120))
  while kill -0 "$SINK_PID" 2>/dev/null && (( SECONDS < deadline )); do sleep 1; done
  if kill -0 "$SINK_PID" 2>/dev/null; then
    kill "$SINK_PID" 2>/dev/null || true
    wait "$SINK_PID" 2>/dev/null || true
    SINK_PID=''
    ingress_die "failpoint crash $crash timed out"
  fi
  set +e
  wait "$SINK_PID"
  status=$?
  set -e
  SINK_PID=''
  [[ "$status" -eq 86 ]] || ingress_die "failpoint crash $crash exited $status, expected 86"
  jq -se --arg name after_db_commit_before_offset_commit --argjson code 86 \
    'length == 1 and .[0].event == "FAILPOINT_REACHED" and .[0].failpoint == $name and .[0].exit_code == $code' \
    "$ARTIFACT_DIR/crash-$crash.log" >/dev/null || ingress_die "crash $crash did not emit exactly one flushed failpoint marker"
  group_snapshot "$after" >"$after.parsed"
  awk '{print $1, $2}' "$before.parsed" >"$TMP_DIR/before-committed"
  awk '{print $1, $2}' "$after.parsed" >"$TMP_DIR/after-committed"
  cmp -s "$TMP_DIR/before-committed" "$TMP_DIR/after-committed" \
    || ingress_die "consumer group committed offsets advanced during failpoint crash $crash"
  awk '{lag += $4} END { if (lag <= 0) exit 1 }' "$after.parsed" \
    || ingress_die "no uncommitted lag remained after failpoint crash $crash"
  CONFIRMED_CRASHES=$((CONFIRMED_CRASHES + 1))
done

unset PULSE_SINK_FAILPOINT
"$SINK_BIN" >"$ARTIFACT_DIR/final-sink.log" 2>&1 &
SINK_PID=$!
deadline=$((SECONDS + 900))
while (( SECONDS < deadline )); do
  if ! kill -0 "$SINK_PID" 2>/dev/null; then cat "$ARTIFACT_DIR/final-sink.log" >&2; ingress_die 'final sink exited before draining the source topic'; fi
  status="$(get_group_status || true)"
  parsed="$(printf '%s\n' "$status" | parse_group_offsets 2>/dev/null || true)"
  if [[ -n "$parsed" ]] && awk '{sum += $4} END {exit !(sum == 0)}' <<<"$parsed"; then
    printf '%s\n' "$status" >"$ARTIFACT_DIR/consumer-group-final.txt"
    printf '%s\n' "$parsed" >"$ARTIFACT_DIR/consumer-group-final-parsed.txt"
    final_aggregate_lag="$(awk '{sum += $4} END {printf "%.0f", sum}' <<<"$parsed")"
    break
  fi
  sleep 1
done
(( SECONDS < deadline )) || ingress_die 'final sink did not reach consumer-group lag zero within 15 minutes'

curl --silent --show-error --fail "http://127.0.0.1:$METRICS_PORT/metrics" >"$ARTIFACT_DIR/final-sink-metrics.prom" \
  || ingress_die 'could not scrape final sink metrics before stopping it'
conflicts="$(awk '$1 == "pulse_sink_conflicts_skipped_total" {print $2}' "$ARTIFACT_DIR/final-sink-metrics.prom")"
CONFLICT_METRIC_PRESENT=0
if [[ "$conflicts" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  CONFLICT_METRIC_PRESENT=1
else
  conflicts=0
fi

get_topic_offsets telemetry.enriched >"$ARTIFACT_DIR/enriched-offsets-final.txt"
get_topic_offsets telemetry.dlq >"$ARTIFACT_DIR/dlq-offsets-final.txt"
enriched_final="$(sum_offsets "$ARTIFACT_DIR/enriched-offsets-final.txt")" || ingress_die 'invalid final enriched offsets'
dlq_final="$(sum_offsets "$ARTIFACT_DIR/dlq-offsets-final.txt" 1)" || ingress_die 'invalid final DLQ offsets'
enriched_final_delta=$((enriched_final - enriched_before))
dlq_final_delta=$((dlq_final - dlq_before))

: >"$ARTIFACT_DIR/partition-checks.tsv"
for partition in 0 1 2; do
  source_before="$(awk -v p="$partition" '$1 == p {print $2}' "$ARTIFACT_DIR/enriched-offsets-before.txt")"
  source_published="$(awk -v p="$partition" '$1 == p {print $2}' "$ARTIFACT_DIR/enriched-offsets-published.txt")"
  committed="$(awk -v p="$partition" '$1 == p {print $2}' "$ARTIFACT_DIR/consumer-group-final-parsed.txt")"
  log_end="$(awk -v p="$partition" '$1 == p {print $3}' "$ARTIFACT_DIR/consumer-group-final-parsed.txt")"
  lag="$(awk -v p="$partition" '$1 == p {print $4}' "$ARTIFACT_DIR/consumer-group-final-parsed.txt")"
  expected_records="$(jq -er ".partition_distribution[$partition]" "$ARTIFACT_DIR/producer-summary.json")"
  source_delta=$((source_published - source_before))
  committed_delta=$((committed - source_before))
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$partition" "$expected_records" "$source_before" "$source_delta" "$committed" "$committed_delta" "$log_end" "$lag" \
    >>"$ARTIFACT_DIR/partition-checks.tsv"
done
jq -Rn '[inputs | split("\t") | {partition:(.[0]|tonumber),producer_expected:(.[1]|tonumber),starting_offset:(.[2]|tonumber),source_delta:(.[3]|tonumber),committed_offset:(.[4]|tonumber),committed_delta:(.[5]|tonumber),log_end_offset:(.[6]|tonumber),lag:(.[7]|tonumber)}]' \
  <"$ARTIFACT_DIR/partition-checks.tsv" >"$ARTIFACT_DIR/partition-checks.json"

"${COMPOSE[@]}" exec -T timescaledb psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At -F ' ' \
  >"$ARTIFACT_DIR/database-counts.txt" 2>"$ARTIFACT_DIR/database-check.log" <<SQL
WITH expected AS (
  SELECT ('00000000-0000-4000-8000-' || lpad(to_hex(n), 12, '0'))::uuid AS event_id
  FROM generate_series(0, $((COUNT - 1))) AS series(n)
), counts AS (
  SELECT
    (SELECT count(*) FROM event_receipts) AS receipts,
    (SELECT count(*) FROM telemetry_events) AS events,
    (SELECT count(DISTINCT event_id) FROM telemetry_events) AS distinct_events,
    (SELECT count(*) FROM expected e LEFT JOIN event_receipts r USING (event_id) WHERE r.event_id IS NULL) AS missing_receipts,
    (SELECT count(*) FROM expected e LEFT JOIN telemetry_events t USING (event_id) WHERE t.event_id IS NULL) AS missing_events
)
SELECT receipts, events, distinct_events, missing_receipts, missing_events FROM counts;
SQL
read -r receipts events distinct_events missing_receipts missing_events <"$ARTIFACT_DIR/database-counts.txt"

jq -n --slurpfile partitions "$ARTIFACT_DIR/partition-checks.json" \
  --arg run_id "$RUN_ID" --arg project "$PROJECT" --arg group "$GROUP_ID" \
  --argjson count "$COUNT" --argjson requested_crashes "$CRASHES" --argjson confirmed_crashes "$CONFIRMED_CRASHES" \
  --argjson conflicts "$conflicts" --argjson conflict_metric_present "$CONFLICT_METRIC_PRESENT" \
  --argjson enriched_before "$enriched_before" --argjson enriched_published "$enriched_published" \
  --argjson enriched_final "$enriched_final" --argjson enriched_published_delta "$enriched_published_delta" \
  --argjson enriched_final_delta "$enriched_final_delta" --argjson dlq_before "$dlq_before" --argjson dlq_final "$dlq_final" \
  --argjson dlq_final_delta "$dlq_final_delta" --argjson aggregate_lag "$final_aggregate_lag" \
  --argjson receipts "$receipts" --argjson events "$events" --argjson distinct_events "$distinct_events" \
  --argjson missing_receipts "$missing_receipts" --argjson missing_events "$missing_events" \
  '{run_id:$run_id,compose_project:$project,consumer_group:$group,requested_events:$count,failpoint_crashes:{requested:$requested_crashes,confirmed:$confirmed_crashes},final_conflicts_skipped:$conflicts,source_offsets:{enriched:{before:$enriched_before,published_end:$enriched_published,published_delta:$enriched_published_delta,final_end:$enriched_final,final_delta:$enriched_final_delta},dlq:{before:$dlq_before,after:$dlq_final,delta:$dlq_final_delta}},consumer_group_offsets:{aggregate_lag:$aggregate_lag,partitions:$partitions[0]},database:{receipts:$receipts,events:$events,distinct_event_ids:$distinct_events,missing_receipts:$missing_receipts,missing_events:$missing_events},checks:{source_delta_matches_count:($enriched_published_delta==$count and $enriched_final_delta==$count),dlq_empty:($dlq_final_delta==0),database_exact:($receipts==$count and $events==$count and $distinct_events==$count),all_expected_ids_present:($missing_receipts==0 and $missing_events==0),conflict_metric_present:($conflict_metric_present==1),conflict_metric_positive:($conflicts>0),failpoint_crashes_confirmed:($confirmed_crashes==$requested_crashes),per_partition_source_delta_matches_producer:(all($partitions[0][];.source_delta==.producer_expected)),per_partition_committed_delta_matches_producer:(all($partitions[0][];.committed_delta==.producer_expected)),final_committed_offsets_match_log_ends:(all($partitions[0][];.committed_offset==.log_end_offset)),per_partition_lag_zero:(all($partitions[0][];.lag==0)),aggregate_lag_zero:($aggregate_lag==0),group_lag_zero:($aggregate_lag==0 and all($partitions[0][];.lag==0))}}' \
  >"$ARTIFACT_DIR/verification.json"
jq -e '.checks | to_entries | all(.value == true)' "$ARTIFACT_DIR/verification.json" >/dev/null \
  || ingress_die 'verification.json contains a failed check'

"${COMPOSE[@]}" logs --no-color kafka timescaledb >"$ARTIFACT_DIR/services.log" 2>&1 || true
printf 'Sink verification passed. Evidence: %s\n' "$ARTIFACT_DIR/verification.json"
