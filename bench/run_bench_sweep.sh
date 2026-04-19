#!/usr/bin/env bash
# bench/run_bench_sweep.sh — Run pg_accumulator benchmark at 1x and 10x scales

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BENCH_SQL_TEMPLATE="$SCRIPT_DIR/sql/bench.sql"
BENCH_DIR="$SCRIPT_DIR/sql"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.bench.yml"

usage() {
    cat <<EOF
Usage: $(basename "$0") [--no-build]

Runs the benchmark suite twice:
  1. baseline (1x)
  2. scaled by 10x

Options:
  --no-build   Skip Docker image rebuild
  --help       Show this help
EOF
}

replace_counts() {
    local scale=$1
    local dest=$2

    python3 - "$scale" "$BENCH_SQL_TEMPLATE" "$dest" <<'PY'
import re
import sys
from pathlib import Path
scale = int(sys.argv[1])
template_path = Path(sys.argv[2])
dest_path = Path(sys.argv[3])
text = template_path.read_text()
replacements = {
    r"N\s+constant int := 5000;": f"N       constant int := {5000*scale};",
    r"N_batches\s+constant int := 500;": f"N_batches  constant int := {500*scale};",
    r"N_batches\s+constant int := 100;": f"N_batches constant int := {100*scale};",
    r"N_batches\s+constant int := 10;": f"N_batches  constant int := {10*scale};",
    r"N\s+constant int := 2000;": f"N       constant int := {2000*scale};",
    r"N\s+constant int := 1000;": f"N       constant int := {1000*scale};",
    r"N\s+constant int := 5000;": f"N       constant int := {5000*scale};",
}
for pattern, repl in replacements.items():
    text = re.sub(pattern, repl, text)
text = text.replace('Scenario 1/8  register_post() single inserts (5 000 ops)...',
                    f'Scenario 1/8  register_post() single inserts ({5000*scale:,} ops)...')
text = text.replace('Scenario 2/8  register_post() batch 10 (500 × 10 = 5 000 items)...',
                    f'Scenario 2/8  register_post() batch 10 ({500*scale} × 10 = {5000*scale:,} items)...')
text = text.replace('Scenario 3/8  register_post() batch 100 (100 × 100 = 10 000 items)...',
                    f'Scenario 3/8  register_post() batch 100 ({100*scale} × 100 = {10000*scale:,} items)...')
text = text.replace('Scenario 4/8  register_post() batch 1000 (10 × 1000 = 10 000 items)...',
                    f'Scenario 4/8  register_post() batch 1000 ({10*scale} × 1000 = {10000*scale:,} items)...')
text = text.replace('Scenario 5/8  balance_cache direct read (2 000 point lookups)...',
                    f'Scenario 5/8  balance_cache direct read ({2000*scale:,} point lookups)...')
text = text.replace('Scenario 6/8  b_std_balance() per-register function (1 000 calls)...',
                    f'Scenario 6/8  b_std_balance() per-register function ({1000*scale:,} calls)...')
text = text.replace('Scenario 7/8  register_post() high_write delta buffer (5 000 ops)...',
                    f'Scenario 7/8  register_post() high_write delta buffer ({5000*scale:,} ops)...')
text = text.replace('Scenario 8/8  register_unpost() (1 000 cancellations)...',
                    f'Scenario 8/8  register_unpost() ({1000*scale:,} cancellations)...')
if scale != 1:
    text = text.replace('b_std', f'b_std_{scale}')
    text = text.replace('b_hw', f'b_hw_{scale}')
dest_path.write_text(text)
PY
}

run_scale() {
    local scale=$1
    local sql_file="$BENCH_DIR/bench_scale_${scale}.sql"
    local sql_name

    echo "Generating benchmark SQL for scale ${scale}x..."
    replace_counts "$scale" "$sql_file"
    sql_name="$(basename "$sql_file")"
    docker compose -f "$COMPOSE_FILE" run --rm bench-runner bash -lc \
        "psql -P pager=off -v ON_ERROR_STOP=1 -h postgres-bench -U bench -d accumulator_bench -f /bench/sql/${sql_name}"
}

BUILD_FLAG="--build"
for arg in "$@"; do
    case "$arg" in
        --no-build) BUILD_FLAG="" ;;
        --help|-h) usage; exit 0 ;;
        *) echo "Unknown option: $arg" >&2; usage; exit 1 ;;
    esac
done

cd "$REPO_ROOT"

echo "Starting benchmark database..."
docker compose -f "$COMPOSE_FILE" down --volumes --remove-orphans 2>/dev/null || true
if [[ -n "$BUILD_FLAG" ]]; then
    docker compose -f "$COMPOSE_FILE" build
fi

docker compose -f "$COMPOSE_FILE" up -d postgres-bench

echo "Waiting for PostgreSQL to become ready..."
# Wait for postgres service healthcheck
until docker compose -f "$COMPOSE_FILE" exec -T postgres-bench pg_isready -U bench -d accumulator_bench >/dev/null 2>&1; do
    sleep 2
done

for scale in 1 10; do
    run_scale "$scale"
done

echo "Benchmark sweep complete. Shutting down containers..."
docker compose -f "$COMPOSE_FILE" down --volumes 2>/dev/null || true

echo "Done."
