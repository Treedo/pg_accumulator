-- bench/sql/bench.sql
-- pg_accumulator Benchmark Suite
--
-- Measures throughput of core API operations:
--   1. register_post() — single inserts
--   2. register_post() — batch 10 items
--   3. register_post() — batch 100 items
--   4. register_post() — batch 1000 items
--   5. balance read (balance_cache SELECT)
--   6. balance() generic function
--   7. register_post() — high_write mode (delta buffer)
--   8. register_unpost() — cancel movements
--
-- Usage: psql -f bench/sql/bench.sql
-- ================================================================

\echo ''
\echo '================================================================'
\echo '  pg_accumulator — Benchmark Suite'
\echo '================================================================'
\echo ''

-- ================================================================
-- Initialise
-- ================================================================
CREATE EXTENSION IF NOT EXISTS pg_accumulator;

-- Results collection table
CREATE TEMP TABLE bench_results (
    ord         serial,
    scenario    text,
    iterations  integer,
    total_ms    numeric(12,2),
    avg_ms      numeric(10,4),
    ops_per_sec numeric(12,1)
);

-- Convenience helper used by every DO block
CREATE OR REPLACE FUNCTION pg_temp.bench_record(
    p_scenario  text,
    p_n         integer,
    p_start     timestamptz,
    p_end       timestamptz
) RETURNS void LANGUAGE sql AS $$
    INSERT INTO bench_results(scenario, iterations, total_ms, avg_ms, ops_per_sec)
    SELECT
        p_scenario,
        p_n,
        round(extract(epoch from (p_end - p_start)) * 1000,  2),
        round(extract(epoch from (p_end - p_start)) * 1000 / p_n, 4),
        round(p_n / extract(epoch from (p_end - p_start)), 1);
$$;

-- ================================================================
-- Test registers
-- ================================================================
\echo 'Setting up benchmark registers...'

SELECT accum.register_create(
    name       := 'b_std_10',
    dimensions := '{"warehouse": "int", "product": "int"}',
    resources  := '{"quantity": "numeric(18,4)", "amount": "numeric(18,2)"}',
    kind       := 'balance'
);

SELECT accum.register_create(
    name       := 'b_hw_10',
    dimensions := '{"warehouse": "int", "product": "int"}',
    resources  := '{"quantity": "numeric(18,4)", "amount": "numeric(18,2)"}',
    kind       := 'balance',
    high_write := true
);

\echo ''

-- ================================================================
-- SCENARIO 1: register_post() — single inserts
-- ================================================================
\echo 'Scenario 1/8  register_post() single inserts (50,000 ops)...'

DO $$
DECLARE
    t_start timestamptz;
    t_end   timestamptz;
    N       constant int := 50000;
    i       int;
BEGIN
    t_start := clock_timestamp();
    FOR i IN 1..N LOOP
        PERFORM accum.register_post('b_std_10', jsonb_build_object(
            'recorder', 'bench:s1:' || i,
            'period',   '2026-01-15',
            'warehouse', (i % 10) + 1,
            'product',   (i % 50) + 1,
            'quantity',  1,
            'amount',    10.00
        ));
    END LOOP;
    t_end := clock_timestamp();
    PERFORM pg_temp.bench_record('1. register_post() single insert', N, t_start, t_end);
END;
$$;

-- ================================================================
-- SCENARIO 2: register_post() — batch 10
-- ================================================================
\echo 'Scenario 2/8  register_post() batch 10 (5000 × 10 = 50,000 items)...'

DO $$
DECLARE
    t_start    timestamptz;
    t_end      timestamptz;
    N_batches  constant int := 5000;
    BATCH_SZ   constant int := 10;
    i          int;
    batch      jsonb;
BEGIN
    t_start := clock_timestamp();
    FOR i IN 1..N_batches LOOP
        SELECT jsonb_agg(jsonb_build_object(
            'recorder',  'bench:s2:' || i || ':' || j,
            'period',    '2026-02-01',
            'warehouse', (j % 10) + 1,
            'product',   (j % 50) + 1,
            'quantity',  1,
            'amount',    5.00
        )) INTO batch
        FROM generate_series(1, BATCH_SZ) AS j;

        PERFORM accum.register_post('b_std_10', batch);
    END LOOP;
    t_end := clock_timestamp();
    PERFORM pg_temp.bench_record(
        '2. register_post() batch 10  (500 batches)',
        N_batches * BATCH_SZ, t_start, t_end
    );
END;
$$;

-- ================================================================
-- SCENARIO 3: register_post() — batch 100
-- ================================================================
\echo 'Scenario 3/8  register_post() batch 100 (1000 × 100 = 100,000 items)...'

DO $$
DECLARE
    t_start   timestamptz;
    t_end     timestamptz;
    N_batches constant int := 1000;
    BATCH_SZ  constant int := 100;
    i         int;
    batch     jsonb;
BEGIN
    t_start := clock_timestamp();
    FOR i IN 1..N_batches LOOP
        SELECT jsonb_agg(jsonb_build_object(
            'recorder',  'bench:s3:' || i || ':' || j,
            'period',    '2026-03-01',
            'warehouse', (j % 10) + 1,
            'product',   (j % 50) + 1,
            'quantity',  1,
            'amount',    5.00
        )) INTO batch
        FROM generate_series(1, BATCH_SZ) AS j;

        PERFORM accum.register_post('b_std_10', batch);
    END LOOP;
    t_end := clock_timestamp();
    PERFORM pg_temp.bench_record(
        '3. register_post() batch 100 (100 batches)',
        N_batches * BATCH_SZ, t_start, t_end
    );
END;
$$;

-- ================================================================
-- SCENARIO 4: register_post() — batch 1000
-- ================================================================
\echo 'Scenario 4/8  register_post() batch 1000 (100 × 1000 = 100,000 items)...'

DO $$
DECLARE
    t_start   timestamptz;
    t_end     timestamptz;
    N_batches  constant int := 100;
    BATCH_SZ  constant int := 1000;
    i         int;
    batch     jsonb;
BEGIN
    t_start := clock_timestamp();
    FOR i IN 1..N_batches LOOP
        SELECT jsonb_agg(jsonb_build_object(
            'recorder',  'bench:s4:' || i || ':' || j,
            'period',    '2026-04-01',
            'warehouse', (j % 10) + 1,
            'product',   (j % 50) + 1,
            'quantity',  1,
            'amount',    5.00
        )) INTO batch
        FROM generate_series(1, BATCH_SZ) AS j;

        PERFORM accum.register_post('b_std_10', batch);
    END LOOP;
    t_end := clock_timestamp();
    PERFORM pg_temp.bench_record(
        '4. register_post() batch 1000 (10 batches)',
        N_batches * BATCH_SZ, t_start, t_end
    );
END;
$$;

-- ================================================================
-- SCENARIO 5: balance_cache direct read (point lookup)
-- ================================================================
\echo 'Scenario 5/8  balance_cache direct read (20,000 point lookups)...'

DO $$
DECLARE
    t_start timestamptz;
    t_end   timestamptz;
    N       constant int := 20000;
    i       int;
    dummy   numeric;
BEGIN
    t_start := clock_timestamp();
    FOR i IN 1..N LOOP
        SELECT quantity INTO dummy
        FROM accum.b_std_10_balance_cache
        WHERE warehouse = (i % 10) + 1
          AND product   = (i % 50) + 1
        LIMIT 1;
    END LOOP;
    t_end := clock_timestamp();
    PERFORM pg_temp.bench_record('5. balance_cache direct SELECT', N, t_start, t_end);
END;
$$;

-- ================================================================
-- SCENARIO 6: b_std_10_balance() per-register function
-- ================================================================
\echo 'Scenario 6/8  b_std_10_balance() per-register function (10,000 calls)...'

DO $$
DECLARE
    t_start timestamptz;
    t_end   timestamptz;
    N       constant int := 10000;
    i       int;
    dummy   jsonb;
BEGIN
    t_start := clock_timestamp();
    FOR i IN 1..N LOOP
        SELECT accum.b_std_10_balance(
            dimensions := jsonb_build_object(
                'warehouse', (i % 10) + 1,
                'product',   (i % 50) + 1
            )
        ) INTO dummy;
    END LOOP;
    t_end := clock_timestamp();
    PERFORM pg_temp.bench_record('6. b_std_10_balance() per-register fn', N, t_start, t_end);
END;
$$;

-- ================================================================
-- SCENARIO 7: register_post() high_write mode (delta buffer)
-- ================================================================
\echo 'Scenario 7/8  register_post() high_write delta buffer (50,000 ops)...'

DO $$
DECLARE
    t_start timestamptz;
    t_end   timestamptz;
    N       constant int := 50000;
    i       int;
BEGIN
    t_start := clock_timestamp();
    FOR i IN 1..N LOOP
        PERFORM accum.register_post('b_hw_10', jsonb_build_object(
            'recorder', 'bench:s7:' || i,
            'period',   '2026-01-15',
            'warehouse', (i % 10) + 1,
            'product',   (i % 50) + 1,
            'quantity',  1,
            'amount',    10.00
        ));
    END LOOP;
    t_end := clock_timestamp();
    PERFORM pg_temp.bench_record('7. register_post() high_write mode', N, t_start, t_end);
END;
$$;

-- ================================================================
-- SCENARIO 8: register_unpost() — cancel movements
-- ================================================================
\echo 'Scenario 8/8  register_unpost() (10,000 cancellations)...'

DO $$
DECLARE
    t_start   timestamptz;
    t_end     timestamptz;
    N       constant int := 10000;
    i         int;
    rec_id    text;
BEGIN
    -- Pre-seed 1 000 movements dedicated to unpost testing
    FOR i IN 1..N LOOP
        PERFORM accum.register_post('b_std_10', jsonb_build_object(
            'recorder', 'bench:s8:' || i,
            'period',   '2026-05-01',
            'warehouse', (i % 10) + 1,
            'product',   (i % 50) + 1,
            'quantity',  1,
            'amount',    1.00
        ));
    END LOOP;

    -- Now time the unpost
    t_start := clock_timestamp();
    FOR i IN 1..N LOOP
        PERFORM accum.register_unpost('b_std_10', 'bench:s8:' || i);
    END LOOP;
    t_end := clock_timestamp();
    PERFORM pg_temp.bench_record('8. register_unpost()', N, t_start, t_end);
END;
$$;

-- ================================================================
-- RESULTS
-- ================================================================
\echo ''
\echo '================================================================'
\echo '  RESULTS'
\echo '================================================================'

\pset border 2
\pset format aligned

SELECT
    scenario                                   AS "Scenario",
    to_char(iterations, '999G999G999')         AS "Iterations",
    to_char(total_ms,   'FM99999990.00') || ' ms'  AS "Total",
    to_char(avg_ms,     'FM9990.0000')   || ' ms'  AS "Avg/op",
    to_char(ops_per_sec,'FM999G999G990.0')          AS "ops/sec"
FROM bench_results
ORDER BY ord;

\echo ''
\echo '================================================================'
\echo '  DATA VOLUME'
\echo '================================================================'

SELECT
    relname                                      AS "Table",
    to_char(n_live_tup, '999G999G999')           AS "Live rows",
    pg_size_pretty(pg_total_relation_size(
        'accum.' || relname))                    AS "Total size"
FROM pg_stat_user_tables
WHERE schemaname = 'accum'
  AND relname LIKE 'b_%'
ORDER BY relname;

\echo ''
