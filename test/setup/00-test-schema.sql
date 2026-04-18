-- test/setup/00-test-schema.sql
-- Bootstrap the accum schema and pgTAP for testing
-- This file runs BEFORE any test files

CREATE EXTENSION IF NOT EXISTS pgtap;
CREATE SCHEMA IF NOT EXISTS accum;

-- ============================================================
-- EMULATED EXTENSION FUNCTIONS (prototype layer)
-- These SQL functions emulate the C extension API.
-- They will be replaced by CREATE EXTENSION pg_accumulator
-- once the C code is built. Tests remain the same.
-- ============================================================
