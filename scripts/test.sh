#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# coldplane CLI — test runner
#
# Usage:
#   ./scripts/test.sh                 # unit tests only (default)
#   ./scripts/test.sh unit            # unit tests only
#   ./scripts/test.sh local           # integration tests against Docker databases
#   ./scripts/test.sh all             # unit + local
#
# Prerequisites (local):
#   - docker compose up -d (PostgreSQL on :5432, MySQL on :3306)
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

export GOTOOLCHAIN=auto

log()  { printf "\033[1;36m==>\033[0m %s\n" "$*"; }
ok()   { printf "\033[1;32m  ✓\033[0m %s\n" "$*"; }
fail() { printf "\033[1;31m  ✗\033[0m %s\n" "$*"; exit 1; }

FAILURES=0

run_unit_tests() {
    log "Running unit tests"

    if go test -v -count=1 ./...; then
        ok "unit tests passed"
    else
        fail "unit tests failed"
    fi
}

run_local_tests() {
    log "Running integration tests against Docker databases"

    if COLDPLANE_LOCAL_TEST=1 \
        go test -tags integration -v -timeout 10m -count=1 -run TestLocal ./integration/; then
        ok "local integration tests passed"
    else
        fail "local integration tests failed"
    fi
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

TARGET="${1:-unit}"

case "$TARGET" in
    unit)
        run_unit_tests
        ;;
    local)
        run_local_tests
        ;;
    all)
        run_unit_tests
        run_local_tests
        ;;
    *)
        echo "Usage: $0 [unit|local|all]"
        exit 1
        ;;
esac

echo ""
ok "All tests passed"
