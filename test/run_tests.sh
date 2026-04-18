#!/bin/bash
# Run all pgTAP tests against a local or Docker PostgreSQL instance
# Usage:
#   ./test/run_tests.sh                    # Use docker-compose
#   ./test/run_tests.sh --local            # Use local PostgreSQL
#   PGHOST=... PGPORT=... ./test/run_tests.sh --local

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

if [[ "${1:-}" == "--local" ]]; then
    echo "=== Running tests against local PostgreSQL ==="
    
    # Defaults for local mode
    export PGHOST="${PGHOST:-localhost}"
    export PGPORT="${PGPORT:-5432}"
    export PGDATABASE="${PGDATABASE:-accumulator_test}"
    export PGUSER="${PGUSER:-$(whoami)}"
    
    # Create test database if needed
    psql -d postgres -tc "SELECT 1 FROM pg_database WHERE datname = '$PGDATABASE'" | grep -q 1 || \
        createdb "$PGDATABASE"
    
    # Install pgTAP if needed
    psql -c "CREATE EXTENSION IF NOT EXISTS pgtap;" 2>/dev/null || {
        echo "pgTAP not installed. Install with: pgxn install pgtap"
        exit 1
    }
    
    # Run setup scripts
    echo "--- Running setup scripts ---"
    for f in "$SCRIPT_DIR"/setup/*.sql; do
        [ -f "$f" ] && psql -f "$f"
    done
    
    # Run tests
    echo "--- Running pgTAP tests ---"
    pg_prove --verbose --recurse "$SCRIPT_DIR/sql/"
else
    echo "=== Running tests in Docker ==="
    cd "$PROJECT_DIR"
    docker compose -f docker/docker-compose.test.yml up \
        --build \
        --abort-on-container-exit \
        --exit-code-from test-runner
    
    docker compose -f docker/docker-compose.test.yml down -v
fi
