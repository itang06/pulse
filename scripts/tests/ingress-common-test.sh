#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=../lib/ingress-common.sh
source "$ROOT_DIR/scripts/lib/ingress-common.sh"

TEMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/pulse-ingress-common-test.XXXXXX")"
trap 'rm -rf "$TEMP_DIR"' EXIT

assert_rejected() {
  if ( "$@" ) >/dev/null 2>&1; then
    printf 'expected rejection: %s\n' "$*" >&2
    exit 1
  fi
}

[[ "$(calculate_expected_events 1000 10)" -eq 10000 ]]
assert_rejected calculate_expected_events 3037000500 3037000500
assert_rejected calculate_expected_events 9223372036854775808 1

LOSSLESS_SUMMARY='{"attempted":10,"expected_attempted":10,"invalid":0,"dropped":0,"dropped_full":0,"dropped_closed":0,"permanent_failures":0,"acknowledged":10,"queued":10}'
assert_summary_valid "$LOSSLESS_SUMMARY" 10
assert_rejected assert_summary_valid '{"attempted":10,"expected_attempted":10,"invalid":0,"dropped":1,"dropped_full":1,"dropped_closed":0,"permanent_failures":0,"acknowledged":9,"queued":10}' 10

assert_kafka_stopped ''
assert_rejected assert_kafka_stopped 'container-id'
COMPOSE=(true)
[[ -z "$(query_compose_kafka)" ]]
COMPOSE=(false)
assert_rejected query_compose_kafka
COMPOSE_PROJECT_NAME=pulse
OWNED_KAFKA_ID=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
inspect_compose_container() { printf '%s pulse kafka true\n' "$1"; }
docker() { printf '%s ' "$@"; }
owned_stop="$(stop_owned_kafka)"
[[ "$owned_stop" == 'stop aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa ' ]]
inspect_compose_container() { printf '%s another-project kafka true\n' "$1"; }
assert_rejected stop_owned_kafka
inspect_compose_container() { printf '%s pulse kafka false\n' "$1"; }
[[ -z "$(stop_owned_kafka)" ]]
OWNED_KAFKA_ID=''
[[ -z "$(stop_owned_kafka)" ]]

printf 'ingress common checks passed\n'
