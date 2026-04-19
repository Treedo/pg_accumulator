-- test/setup/00-test-schema.sql
-- Bootstrap the accum schema and pgTAP for testing
-- This file runs BEFORE any test files

CREATE EXTENSION IF NOT EXISTS pgtap;

-- Try to load the compiled extension first (available in Docker builds).
-- If the extension is installed, this creates the accum schema and all functions.
-- The remaining setup files (01, 02) use CREATE IF NOT EXISTS / CREATE OR REPLACE
-- and act as a fallback for local testing without the C extension.
DO $$
BEGIN
    CREATE EXTENSION IF NOT EXISTS pg_accumulator;
    RAISE NOTICE 'pg_accumulator extension loaded — using compiled version';
EXCEPTION WHEN OTHERS THEN
    CREATE SCHEMA IF NOT EXISTS accum;
    RAISE NOTICE 'pg_accumulator extension not available — using emulated SQL functions';
END;
$$;
