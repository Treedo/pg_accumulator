-- test/sql/06_register_drop.sql
-- Tests for register_drop() — removing registers

BEGIN;
SELECT plan(12);

-- ============================================================
-- Setup: create two test registers
-- ============================================================
SELECT accum.register_create(
    name       := 'drop_test',
    dimensions := '{"dim1": "int"}',
    resources  := '{"res1": "numeric"}',
    kind       := 'balance'
);

SELECT accum.register_create(
    name       := 'drop_test_turnover',
    dimensions := '{"dim1": "int"}',
    resources  := '{"res1": "numeric"}',
    kind       := 'turnover'
);

-- ============================================================
-- TEST: Drop empty register without force
-- ============================================================
SELECT lives_ok(
    $$SELECT accum.register_drop('drop_test_turnover')$$,
    'Drop empty register without force should succeed'
);

SELECT hasnt_table('accum', 'drop_test_turnover_movements',
    'Movements table should be removed');
SELECT hasnt_table('accum', 'drop_test_turnover_totals_month',
    'Totals month should be removed');

SELECT is(
    (SELECT count(*)::int FROM accum._registers WHERE name = 'drop_test_turnover'),
    0,
    'Register should be removed from registry'
);

-- ============================================================
-- TEST: Drop register with data requires force
-- ============================================================
SELECT accum.register_post('drop_test', '{
    "recorder":"doc:1","period":"2026-04-18","dim1":1,"res1":100
}');

SELECT throws_ok(
    $$SELECT accum.register_drop('drop_test')$$,
    NULL,
    NULL,
    'Drop register with data should require force'
);

-- ============================================================
-- TEST: Drop register with data using force
-- ============================================================
SELECT lives_ok(
    $$SELECT accum.register_drop('drop_test', force := true)$$,
    'Drop with force should succeed even with data'
);

SELECT hasnt_table('accum', 'drop_test_movements',
    'Movements table should be gone');
SELECT hasnt_table('accum', 'drop_test_balance_cache',
    'Balance cache should be gone');
SELECT hasnt_table('accum', 'drop_test_totals_month',
    'Totals month should be gone');
SELECT hasnt_table('accum', 'drop_test_totals_year',
    'Totals year should be gone');

SELECT is(
    (SELECT count(*)::int FROM accum._registers WHERE name = 'drop_test'),
    0,
    'Register should be removed from registry'
);

-- ============================================================
-- TEST: Drop nonexistent register
-- ============================================================
SELECT throws_ok(
    $$SELECT accum.register_drop('nonexistent')$$,
    NULL,
    NULL,
    'Drop nonexistent register should raise error'
);

SELECT * FROM finish();
ROLLBACK;
