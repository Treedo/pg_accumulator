-- test/sql/20_maintenance.sql
-- Tests for maintenance module: register_verify, register_rebuild_totals, register_rebuild_cache, register_stats

BEGIN;
SELECT plan(28);

-- ============================================================
-- Setup: create a balance register with data
-- ============================================================
SELECT accum.register_create(
    name       := 'maint',
    dimensions := '{"warehouse": "int", "product": "int"}',
    resources  := '{"quantity": "numeric(18,4)", "amount": "numeric(18,2)"}',
    kind       := 'balance'
);

-- Post test data
SELECT accum.register_post('maint', '[
    {"recorder":"doc:1","period":"2026-01-15","warehouse":1,"product":100,"quantity":50,"amount":500},
    {"recorder":"doc:2","period":"2026-01-20","warehouse":1,"product":101,"quantity":30,"amount":300},
    {"recorder":"doc:3","period":"2026-02-10","warehouse":2,"product":100,"quantity":20,"amount":200},
    {"recorder":"doc:4","period":"2026-03-05","warehouse":1,"product":100,"quantity":10,"amount":100}
]');

-- ============================================================
-- TEST 1: verify on consistent register → all OK
-- ============================================================
SELECT is(
    (SELECT count(*) FROM accum.register_verify('maint') WHERE status != 'OK'),
    0::bigint,
    'verify on consistent register should return all OK'
);

-- ============================================================
-- TEST 2: verify returns balance_cache checks
-- ============================================================
SELECT ok(
    (SELECT count(*) > 0 FROM accum.register_verify('maint') WHERE check_type = 'balance_cache'),
    'verify should include balance_cache checks'
);

-- ============================================================
-- TEST 3: verify returns totals_month checks
-- ============================================================
SELECT ok(
    (SELECT count(*) > 0 FROM accum.register_verify('maint') WHERE check_type = 'totals_month'),
    'verify should include totals_month checks'
);

-- ============================================================
-- TEST 4: verify returns totals_year checks
-- ============================================================
SELECT ok(
    (SELECT count(*) > 0 FROM accum.register_verify('maint') WHERE check_type = 'totals_year'),
    'verify should include totals_year checks'
);

-- ============================================================
-- TEST 5: Introduce MISMATCH in balance_cache → verify detects it
-- ============================================================
UPDATE accum.maint_balance_cache
SET quantity = quantity + 999
WHERE dim_hash = (SELECT dim_hash FROM accum.maint_balance_cache LIMIT 1);

SELECT ok(
    (SELECT count(*) > 0 FROM accum.register_verify('maint')
     WHERE check_type = 'balance_cache' AND status = 'MISMATCH'),
    'verify should detect MISMATCH after corrupting balance_cache'
);

-- Restore for further tests
UPDATE accum.maint_balance_cache bc
SET quantity = agg.quantity
FROM (
    SELECT dim_hash, SUM(quantity) AS quantity
    FROM accum.maint_movements
    GROUP BY dim_hash
) agg
WHERE bc.dim_hash = agg.dim_hash;

-- ============================================================
-- TEST 6: Introduce MISMATCH in totals_month → verify detects it
-- ============================================================
UPDATE accum.maint_totals_month
SET quantity = quantity + 777
WHERE dim_hash = (SELECT dim_hash FROM accum.maint_totals_month LIMIT 1);

SELECT ok(
    (SELECT count(*) > 0 FROM accum.register_verify('maint')
     WHERE check_type = 'totals_month' AND status = 'MISMATCH'),
    'verify should detect MISMATCH in totals_month'
);

-- ============================================================
-- TEST 7: register_rebuild_totals → totals recalculated correctly
-- ============================================================
SELECT ok(
    accum.register_rebuild_totals('maint') > 0,
    'rebuild_totals should return positive count'
);

-- ============================================================
-- TEST 8: After rebuild_totals → verify all OK
-- ============================================================
SELECT is(
    (SELECT count(*) FROM accum.register_verify('maint')
     WHERE check_type IN ('totals_month', 'totals_year') AND status != 'OK'),
    0::bigint,
    'After rebuild_totals, all totals checks should be OK'
);

-- ============================================================
-- TEST 9: register_rebuild_cache (full) → cache recalculated
-- ============================================================
-- First corrupt cache
UPDATE accum.maint_balance_cache SET quantity = 0, amount = 0;

SELECT ok(
    accum.register_rebuild_cache('maint') > 0,
    'rebuild_cache should return positive count'
);

-- ============================================================
-- TEST 10: After rebuild_cache → verify cache OK
-- ============================================================
SELECT is(
    (SELECT count(*) FROM accum.register_verify('maint')
     WHERE check_type = 'balance_cache' AND status != 'OK'),
    0::bigint,
    'After rebuild_cache, balance_cache checks should be OK'
);

-- ============================================================
-- TEST 11: rebuild_cache partial (specific dim_hash)
-- ============================================================
-- Get a dim_hash and corrupt it
DO $$
DECLARE
    v_hash bigint;
BEGIN
    SELECT dim_hash INTO v_hash FROM accum.maint_balance_cache LIMIT 1;
    UPDATE accum.maint_balance_cache SET quantity = -9999 WHERE dim_hash = v_hash;
END;
$$;

-- Rebuild only that dim_hash
SELECT ok(
    accum.register_rebuild_cache('maint',
        (SELECT dim_hash FROM accum.maint_balance_cache WHERE quantity = -9999)) = 1,
    'Partial rebuild_cache should rebuild exactly 1 row'
);

-- ============================================================
-- TEST 12: After partial rebuild → verify all OK
-- ============================================================
SELECT is(
    (SELECT count(*) FROM accum.register_verify('maint')
     WHERE check_type = 'balance_cache' AND status != 'OK'),
    0::bigint,
    'After partial rebuild_cache, all balance_cache checks should be OK'
);

-- ============================================================
-- TEST 13: register_stats returns valid structure
-- ============================================================
SELECT ok(
    accum.register_stats('maint') ? 'movements_count',
    'stats should contain movements_count'
);

SELECT ok(
    accum.register_stats('maint') ? 'partitions_count',
    'stats should contain partitions_count'
);

SELECT ok(
    accum.register_stats('maint') ? 'cache_rows',
    'stats should contain cache_rows'
);

SELECT ok(
    accum.register_stats('maint') ? 'totals_month_rows',
    'stats should contain totals_month_rows'
);

SELECT ok(
    accum.register_stats('maint') ? 'totals_year_rows',
    'stats should contain totals_year_rows'
);

SELECT ok(
    accum.register_stats('maint') ? 'table_sizes',
    'stats should contain table_sizes'
);

-- ============================================================
-- TEST 14: stats returns correct movement count
-- ============================================================
SELECT is(
    (accum.register_stats('maint')->>'movements_count')::int,
    4,
    'stats should report 4 movements'
);

-- ============================================================
-- TEST 15: stats returns correct cache row count
-- ============================================================
SELECT is(
    (accum.register_stats('maint')->>'cache_rows')::int,
    (SELECT count(*)::int FROM accum.maint_balance_cache),
    'stats cache_rows should match actual count'
);

-- ============================================================
-- TEST 16: verify + rebuild circle: verify MISMATCH → rebuild → verify OK
-- ============================================================
-- Corrupt both totals and cache
UPDATE accum.maint_balance_cache SET quantity = -1111;
UPDATE accum.maint_totals_month SET quantity = -2222;

-- Verify detects issues
SELECT ok(
    (SELECT count(*) > 0 FROM accum.register_verify('maint') WHERE status != 'OK'),
    'verify should detect issues after corruption'
);

-- Rebuild totals first, then cache
SELECT accum.register_rebuild_totals('maint');
SELECT accum.register_rebuild_cache('maint');

-- Verify all OK now
SELECT is(
    (SELECT count(*) FROM accum.register_verify('maint') WHERE status != 'OK'),
    0::bigint,
    'After full rebuild cycle, verify should report all OK'
);

-- ============================================================
-- TEST 17: rebuild on empty register
-- ============================================================
SELECT accum.register_create(
    name       := 'maint_empty',
    dimensions := '{"x": "int"}',
    resources  := '{"val": "numeric"}',
    kind       := 'balance'
);

SELECT is(
    accum.register_rebuild_totals('maint_empty'),
    0,
    'rebuild_totals on empty register should return 0'
);

SELECT is(
    accum.register_rebuild_cache('maint_empty'),
    0,
    'rebuild_cache on empty register should return 0'
);

-- ============================================================
-- TEST 18: verify on empty register → all OK (no rows)
-- ============================================================
SELECT is(
    (SELECT count(*) FROM accum.register_verify('maint_empty') WHERE status != 'OK'),
    0::bigint,
    'verify on empty register should have no mismatches'
);

-- ============================================================
-- TEST 19: stats on empty register → zeros
-- ============================================================
SELECT is(
    (accum.register_stats('maint_empty')->>'movements_count')::int,
    0,
    'stats on empty register should report 0 movements'
);

-- ============================================================
-- TEST 20: rebuild_cache fails on turnover register
-- ============================================================
SELECT accum.register_create(
    name       := 'maint_turnover',
    dimensions := '{"x": "int"}',
    resources  := '{"val": "numeric"}',
    kind       := 'turnover'
);

SELECT throws_ok(
    $$SELECT accum.register_rebuild_cache('maint_turnover')$$,
    NULL,
    NULL,
    'rebuild_cache should fail on turnover register'
);

-- ============================================================
-- TEST 21: verify on nonexistent register → error
-- ============================================================
SELECT throws_ok(
    $$SELECT * FROM accum.register_verify('nonexistent')$$,
    NULL,
    NULL,
    'verify should fail on nonexistent register'
);

-- ============================================================
-- Cleanup
-- ============================================================
SELECT accum.register_drop('maint', true);
SELECT accum.register_drop('maint_empty', true);
SELECT accum.register_drop('maint_turnover', true);

SELECT * FROM finish();
ROLLBACK;
