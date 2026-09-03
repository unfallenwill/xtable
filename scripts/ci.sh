#!/usr/bin/env bash
# xtable full-pipeline check.
#
# Use after each iteration (agent/loop or local edit) to verify the whole
# workspace still compiles, lints, tests, and that the structured HTTP layer
# answers correctly end-to-end.
#
# Exit codes:
#   0  every step passed
#   1  a step failed (caller should stop and inspect)
#  77  a step was skipped (--skip flag)
#
# Flags:
#   --skip <step>        skip one step: fmt | build | clippy | test | smoke
#   --include-ignored    also run #[ignore]'d tests in the smoke step
#                        (these are gated on Task 4; they may fail by design)
#   -h | --help          show this help
#
# Output is plain text with a clear section header per step so an agent loop
# can grep PASS/FAIL and step boundaries.

set -euo pipefail

cd "$(dirname "$0")/.."

# Make cargo discoverable when rustup was installed without --no-modify-path.
# If cargo is already on PATH this is a no-op.
if ! command -v cargo >/dev/null 2>&1; then
    if [[ -r "$HOME/.cargo/env" ]]; then
        # shellcheck disable=SC1091
        . "$HOME/.cargo/env"
    fi
fi

# ---- args ------------------------------------------------------------------

SKIP=""
INCLUDE_IGNORED=0
usage() {
    sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
}
# Manual while-shift so `--skip <value>` actually consumes two args.
# (`for arg in "$@"` snapshots, so shifting inside it doesn't change iteration.)
while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip)            shift; SKIP="${1:-}"; [[ $# -gt 0 ]] && shift || true ;;
        --skip=*)          SKIP="${1#--skip=}"; shift ;;
        --include-ignored) INCLUDE_IGNORED=1; shift ;;
        -h|--help)         usage ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

# ---- helpers ---------------------------------------------------------------

BAR="──────────────────────────────────────────────────────────────────"
ok()    { printf "✓ %s\n" "$*"; }
fail()  { printf "✗ %s\n" "$*" >&2; exit 1; }
note()  { printf "  · %s\n" "$*"; }
should_skip() { [[ "$SKIP" == "$1" ]]; }

# Run a step; print a header, time it, and return the step's exit code.
#
# Args: <label> <step-fn> [<step-fn> ...]
#   label   short name for --skip matching (fmt | build | clippy | test | smoke)
#   step-fn bash function that wraps the actual cargo invocation
#
# Uses `if`/`else` instead of `set +e`/`set -e` because bash propagates `set -e`
# across function boundaries: a child function that re-enables set -e before
# returning will cause the caller's next command to abort when set -e is on.
# Wrapping the step call in an `if` makes the failure inert for set -e and
# lets us capture the real exit code.
run_step() {
    local label="$1"; shift
    if should_skip "$label"; then
        printf "\n%s\n── STEP: %s (skipped) ──\n%s\n" "$BAR" "$label" "$BAR"
        note "skipped via --skip=$SKIP"
        return 77
    fi
    printf "\n%s\n── STEP: %s ──\n%s\n" "$BAR" "$label" "$BAR"
    local start=$SECONDS
    local rc=0
    if "$@"; then
        rc=0
    else
        rc=$?
    fi
    local dt=$((SECONDS - start))
    if [[ $rc -eq 0 ]]; then
        ok "$label passed (${dt}s)"
        return 0
    fi
    printf "✗ %s failed after %ds (exit=%s)\n" "$label" "$dt" "$rc" >&2
    return "$rc"
}

# ---- steps -----------------------------------------------------------------

step_fmt() {
    # --check exits non-zero if any file would be reformatted.
    cargo fmt --all -- --check
}

step_build() {
    cargo build --workspace --all-targets --all-features
}

step_clippy() {
    # -D warnings turns warnings into hard errors so we don't regress.
    cargo clippy --workspace --all-targets --all-features -- -D warnings
}

step_test() {
    cargo test --workspace --all-features
}

step_smoke() {
    # Structured-data-space HTTP smoke. Targets the integration tests under
    # xtable-server that hit the real axum router (no socket, but full
    # middleware stack: auth → router → state → storage → backend dummy).
    local extra=()
    if [[ $INCLUDE_IGNORED -eq 1 ]]; then
        extra+=(--ignored)
        note "including #[ignore]'d tests (Task 4 gated)"
    fi
    cargo test -p xtable-server --test structured_http "${extra[@]}"
}

# ---- dispatch --------------------------------------------------------------

overall_start=$SECONDS
results=()

run_and_record() {
    # Run every step regardless of failure so the summary is complete.
    # Args: <label> <step-fn> ...
    local label="$1"; shift
    local rc=0
    if run_step "$label" "$@"; then
        rc=0
    else
        rc=$?
    fi
    results+=("$rc $label")
}

run_and_record fmt    step_fmt
run_and_record build  step_build
run_and_record clippy step_clippy
run_and_record test   step_test
run_and_record smoke  step_smoke

# ---- summary ---------------------------------------------------------------

total=$((SECONDS - overall_start))
printf "\n%s\n── SUMMARY ──\n%s\n" "$BAR" "$BAR"
fails=0
skips=0
for r in "${results[@]}"; do
    rc="${r%% *}"
    name="${r#* }"
    case "$rc" in
        0)  printf "  PASS  %s\n" "$name" ;;
        77) printf "  SKIP  %s\n" "$name"; skips=$((skips+1)) ;;
        *)  printf "  FAIL  %s (rc=%s)\n" "$name" "$rc"; fails=$((fails+1)) ;;
    esac
done
printf "\nxtable full-pipeline check: %d pass, %d fail, %d skip (total %ds)\n" \
    "$((5 - fails - skips))" "$fails" "$skips" "$total"

if [[ $fails -gt 0 ]]; then
    exit 1
fi
exit 0
