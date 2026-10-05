#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ARTIFACT_ROOT="$ROOT_DIR/artifacts/runs/processor"
RUN_ID="$(date -u '+%Y%m%dT%H%M%SZ')-$$"
REPORT="$ARTIFACT_ROOT/replay-$RUN_ID.json"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/pulse-replay.XXXXXX")"
cleanup() { rm -rf "$TMP_DIR"; }
trap cleanup EXIT

command -v cargo >/dev/null 2>&1 || { printf 'processor replay: cargo is required\n' >&2; exit 1; }
command -v jq >/dev/null 2>&1 || { printf 'processor replay: jq is required\n' >&2; exit 1; }
mkdir -p "$ARTIFACT_ROOT"
(cd "$ROOT_DIR" && cargo build -p pulse-processor --bin replay) >"$TMP_DIR/build.log" 2>&1 || {
  cat "$TMP_DIR/build.log" >&2
  printf 'processor replay: build failed\n' >&2
  exit 1
}
if ! "$ROOT_DIR/target/debug/replay" >"$TMP_DIR/replay.json"; then
  printf 'processor replay: evaluator failed; see %s\n' "$TMP_DIR/replay.json" >&2
  exit 1
fi
jq --arg generated_at "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
  --arg run_id "$RUN_ID" '. + {generated_at_utc:$generated_at,run_id:$run_id}' \
  "$TMP_DIR/replay.json" >"$REPORT" || { printf 'processor replay: invalid JSON report\n' >&2; exit 1; }
jq -e '.pass == true and .event_count >= 100000 and .anomaly_window_count >= 25 and .missed_windows == 0 and .false_positive_count == 0' \
  "$REPORT" >/dev/null || { printf 'processor replay: report did not meet the evaluator criteria: %s\n' "$REPORT" >&2; exit 1; }
printf 'Processor replay passed. Evidence: %s\n' "$REPORT"
