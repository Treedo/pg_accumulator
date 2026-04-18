#!/bin/bash
set -e

echo "=== pg_accumulator: Initializing extension ==="

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Create the accum schema
    CREATE SCHEMA IF NOT EXISTS accum;
    
    -- Will be replaced with CREATE EXTENSION when built
    -- CREATE EXTENSION pg_accumulator;
    
    \echo 'pg_accumulator: Schema initialized'
EOSQL
