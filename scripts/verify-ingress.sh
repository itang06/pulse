#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/ingress-common.sh"
[[ $# -eq 0 ]] || ingress_die 'usage: verify-ingress.sh'
ingress_run_locked "$0" "$@"
ingress_init
start_ingress_dependencies
build_ingress_processes
start_gateway
record_verification_metadata

run_ingress_verification 1000 10 42 "$ARTIFACT_DIR/verification.json"
mv "$ARTIFACT_DIR/verification.validated.json" "$ARTIFACT_DIR/verification.json"
printf 'Ingress verification passed. Evidence: %s\n' "$ARTIFACT_DIR/verification.json"
