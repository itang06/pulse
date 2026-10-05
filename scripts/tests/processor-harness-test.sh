#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=../lib/ingress-common.sh
source "$ROOT_DIR/scripts/lib/ingress-common.sh"
if [[ -z "${PULSE_INGRESS_LOCK_HELD:-}" ]]; then
  ingress_run_locked "$0" "$@"
  exit $?
fi

assert_rejected() {
  if ( "$@" ) >/dev/null 2>&1; then
    printf 'expected rejection: %s\n' "$*" >&2
    exit 1
  fi
}

GROUP_STATUS='
GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID
pulse-test telemetry.raw 0 4 4 0 member /host client
pulse-test telemetry.raw 1 8 8 0 member /host client
pulse-test telemetry.raw 2 3 3 0 member /host client'
[[ "$(parse_raw_group_status "$GROUP_STATUS" 3 pulse-test)" == '15 15 0 3' ]]
EMPTY_GROUP_STATUS='
GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID
pulse-test telemetry.raw 0 - 0 - member /host client
pulse-test telemetry.raw 1 - 0 - member /host client
pulse-test telemetry.raw 2 - 0 - member /host client'
[[ "$(parse_raw_group_status "$EMPTY_GROUP_STATUS" 3 pulse-test)" == '0 0 0 3' ]]

assert_rejected parse_raw_group_status "$GROUP_STATUS" 2 pulse-test
assert_rejected parse_raw_group_status "${GROUP_STATUS/4 4 0/4 5 1}" 3 pulse-test
assert_rejected parse_raw_group_status "${GROUP_STATUS/CURRENT-OFFSET/OLD-OFFSET}" 3 pulse-test
assert_rejected parse_raw_group_status "${GROUP_STATUS/telemetry.raw 2/telemetry.other 2}" 3 pulse-test
run_nested_harness() {
  unset PULSE_INGRESS_LOCK_HELD
  ingress_run_locked /bin/true
}
assert_rejected run_nested_harness

printf 'processor harness checks passed\n'
