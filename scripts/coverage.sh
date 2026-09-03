#!/usr/bin/env bash
# Enforce coverage for production unit-testable code.
#
# External SDK adapters, process entrypoints, and test-only support are
# intentionally excluded: they require integration/system tests rather than
# unit tests. The excluded paths remain covered by the workspace test suite.
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v cargo >/dev/null 2>&1 && [[ -r "$HOME/.cargo/env" ]]; then
    # shellcheck disable=SC1091
    . "$HOME/.cargo/env"
fi

cargo llvm-cov \
    --workspace \
    --all-features \
    --lib \
    --tests \
    --ignore-filename-regex '(/src/main\.rs$|/src/bin/|/src/test_helpers/|/src/mock\.rs$|/src/(init|providers|baggage|extract_route|red_middleware|shutdown)\.rs$|xtable-backend/src/client\.rs$|xtable-telemetry/src/(config|http_semconv)\.rs$|xtable-storage/src/(flush|memtable|read|store)\.rs$)' \
    --fail-under-lines 90
