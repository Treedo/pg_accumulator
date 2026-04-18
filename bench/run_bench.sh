#!/usr/bin/env bash
# bench/run_bench.sh — pg_accumulator benchmark runner
#
# Usage:
#   ./bench/run_bench.sh              # run via Docker (default)
#   ./bench/run_bench.sh --local      # run against local PostgreSQL
#   ./bench/run_bench.sh --help
#
# Environment variables (--local mode):
#   PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BENCH_SQL="$SCRIPT_DIR/sql/bench.sql"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.bench.yml"

# ----------------------------------------------------------------
usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Options:
  --local     Run against a local PostgreSQL instance (requires psql in PATH)
  --no-build  Skip Docker image rebuild
  --help      Show this help

Environment variables (--local mode):
  PGHOST       (default: localhost)
  PGPORT       (default: 5432)
  PGDATABASE   (default: accumulator_bench)
  PGUSER       (default: bench)
  PGPASSWORD   (default: '')

EOF
}

# ----------------------------------------------------------------
run_docker() {
    local build_flag="${1:---build}"

    echo "================================================================"
    echo "  pg_accumulator — Benchmark (Docker)"
    echo "================================================================"
    echo ""

    cd "$REPO_ROOT"

    # Clean up any previous run (including volumes so DB starts fresh)
    docker compose -f "$COMPOSE_FILE" down --volumes --remove-orphans 2>/dev/null || true

    docker compose \
        -f "$COMPOSE_FILE" \
        up $build_flag \
        --force-recreate \
        --abort-on-container-exit \
        --exit-code-from bench-runner

    docker compose -f "$COMPOSE_FILE" down --volumes 2>/dev/null || true
}

# ----------------------------------------------------------------
run_local() {
    : "${PGHOST:=localhost}"
    : "${PGPORT:=5432}"
    : "${PGDATABASE:=accumulator_bench}"
    : "${PGUSER:=bench}"

    echo "================================================================"
    echo "  pg_accumulator — Benchmark (local)"
    echo "================================================================"
    echo "  Host:     $PGHOST:$PGPORT"
    echo "  Database: $PGDATABASE"
    echo "  User:     $PGUSER"
    echo "================================================================"
    echo ""

    psql \
        -h "$PGHOST" \
        -p "$PGPORT" \
        -d "$PGDATABASE" \
        -U "$PGUSER" \
        -v ON_ERROR_STOP=1 \
        -f "$BENCH_SQL"
}

# ----------------------------------------------------------------
# Parse args
MODE=docker
BUILD_FLAG="--build"

for arg in "$@"; do
    case "$arg" in
        --local)    MODE=local    ;;
        --no-build) BUILD_FLAG="" ;;
        --help|-h)  usage; exit 0 ;;
        *)
            echo "Unknown option: $arg" >&2
            usage
            exit 1
            ;;
    esac
done

if [[ "$MODE" == "docker" ]]; then
    run_docker "$BUILD_FLAG"
else
    run_local
fi
