-- sql/00_schema.sql
-- Core schema creation for pg_accumulator extension
--
-- NOTE: the schema itself is created automatically by PostgreSQL
-- via the 'schema = accum' directive in pg_accumulator.control,
-- so no CREATE SCHEMA statement is needed here.

COMMENT ON SCHEMA @extschema@ IS
    'pg_accumulator — accumulation registers for PostgreSQL (balance & turnover tracking)';
