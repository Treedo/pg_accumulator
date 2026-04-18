-- sql/00_schema.sql
-- Core schema creation for pg_accumulator extension

CREATE SCHEMA IF NOT EXISTS @extschema@;

COMMENT ON SCHEMA @extschema@ IS
    'pg_accumulator — accumulation registers for PostgreSQL (balance & turnover tracking)';
