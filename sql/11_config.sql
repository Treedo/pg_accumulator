-- sql/11_config.sql
-- Configuration and GUC parameters
-- Documents the GUC parameters registered by the C module (_PG_init).
-- These are set via postgresql.conf or ALTER SYSTEM / SET.

-- ============================================================
-- GUC Parameter Reference (registered in src/core/pg_accumulator.c)
-- ============================================================
--
-- pg_accumulator.background_workers  (int, PGC_POSTMASTER)
--   Number of background maintenance workers.
--   Default: 1, Range: 0..8
--   Set to 0 to disable background maintenance entirely.
--   Requires restart to change.
--
-- pg_accumulator.maintenance_interval  (int ms, PGC_SIGHUP)
--   Interval between partition maintenance and stats collection runs.
--   Default: 3600000 (1 hour), Range: 1000..86400000
--   Reload with: SELECT pg_reload_conf();
--
-- pg_accumulator.delta_merge_interval  (int ms, PGC_SIGHUP)
--   Interval between delta buffer merge cycles.
--   Default: 5000 (5 seconds), Range: 100..3600000
--
-- pg_accumulator.delta_merge_delay  (int ms, PGC_SIGHUP)
--   Minimum age of a delta row before it is eligible for merge.
--   Default: 2000 (2 seconds), Range: 0..3600000
--
-- pg_accumulator.delta_merge_batch_size  (int, PGC_SIGHUP)
--   Maximum number of delta rows consumed per merge cycle.
--   Default: 10000, Range: 100..1000000
--
-- pg_accumulator.partitions_ahead  (int, PGC_SIGHUP)
--   Number of future partitions to create ahead of current date.
--   Default: 3, Range: 0..24
--
-- pg_accumulator.schema  (string, PGC_SUSET)
--   Schema name used by the extension.
--   Default: 'accum'

-- ============================================================
-- Helper view: current runtime values of pg_accumulator GUCs
-- ============================================================
CREATE OR REPLACE VIEW @extschema@._config AS
SELECT
    current_setting('pg_accumulator.background_workers',  true) AS background_workers,
    current_setting('pg_accumulator.maintenance_interval', true) AS maintenance_interval,
    current_setting('pg_accumulator.delta_merge_interval', true) AS delta_merge_interval,
    current_setting('pg_accumulator.delta_merge_delay',   true) AS delta_merge_delay,
    current_setting('pg_accumulator.delta_merge_batch_size', true) AS delta_merge_batch_size,
    current_setting('pg_accumulator.partitions_ahead',    true) AS partitions_ahead,
    current_setting('pg_accumulator.schema',              true) AS schema;

COMMENT ON VIEW @extschema@._config IS
    'Current runtime values of pg_accumulator GUC parameters';

-- ============================================================
-- _maintenance_status: Report background worker activity
-- Returns information about running maintenance workers.
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@._maintenance_status()
RETURNS TABLE(
    pid        int,
    worker_name text,
    state      text,
    query      text,
    started_at timestamptz
)
LANGUAGE sql STABLE AS $$
    SELECT
        pid,
        backend_type AS worker_name,
        state,
        query,
        backend_start AS started_at
    FROM pg_stat_activity
    WHERE backend_type LIKE 'pg_accumulator%'
    ORDER BY pid;
$$;

COMMENT ON FUNCTION @extschema@._maintenance_status() IS
    'List running pg_accumulator background maintenance workers';

-- ============================================================
-- _force_delta_merge: Manually trigger delta merge for all
-- high_write registers (useful for testing and maintenance)
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@._force_delta_merge(
    p_max_age     interval DEFAULT interval '0 seconds',
    p_batch_size  int DEFAULT 1000000
) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    total int;
BEGIN
    total := @extschema@._delta_merge(p_max_age, p_batch_size);
    RETURN total;
END;
$$;

COMMENT ON FUNCTION @extschema@._force_delta_merge(interval, int) IS
    'Manually trigger delta merge for all high_write registers (bypasses age filter by default)';
