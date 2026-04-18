-- sql/00_schema.sql
-- Core schema creation for pg_accumulator extension

-- Create the accumulator schema
-- Note: schema name is configurable via pg_accumulator.schema GUC

DO $$
BEGIN
    -- The schema name comes from the extension's CREATE EXTENSION ... SCHEMA
    -- For now, we use 'accum' as default
    CREATE SCHEMA IF NOT EXISTS accum;
END;
$$;
