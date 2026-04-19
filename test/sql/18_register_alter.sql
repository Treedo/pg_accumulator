-- test/sql/18_register_alter.sql
-- Tests for register_alter() — modifying existing registers

BEGIN;
SELECT plan(28);

-- ============================================================
-- Setup: create a balance register with data
-- ============================================================
SELECT accum.register_create(
    name       := 'alter_test_x',
    dimensions := '{"warehouse": "int", "product": "int"}',
    resources  := '{"quantity": "numeric(18,4)"}',
    kind       := 'balance'
);

-- Post some initial data
SELECT accum.register_post('alter_test_x', '[
    {"recorder":"doc:1","period":"2026-04-01","warehouse":1,"product":100,"quantity":50},
    {"recorder":"doc:2","period":"2026-04-02","warehouse":1,"product":101,"quantity":30},
    {"recorder":"doc:3","period":"2026-04-03","warehouse":2,"product":100,"quantity":20}
]');

-- ============================================================
-- TEST: Add a new resource (no recalculation needed)
-- ============================================================
SELECT lives_ok(
    $$SELECT accum.register_alter(
        p_name        := 'alter_test_x',
        add_resources := '{"amount": "numeric(18,2)"}'
    )$$,
    'Adding a resource should succeed'
);

-- Verify column exists
SELECT has_column('accum', 'alter_test_x_movements', 'amount',
    'Movements should have new amount column');
SELECT has_column('accum', 'alter_test_x_totals_month', 'amount',
    'Totals month should have new amount column');
SELECT has_column('accum', 'alter_test_x_totals_year', 'amount',
    'Totals year should have new amount column');
SELECT has_column('accum', 'alter_test_x_balance_cache', 'amount',
    'Balance cache should have new amount column');

-- Verify metadata updated
SELECT is(
    (SELECT accum.register_info('alter_test_x')->'resources'->>'amount'),
    'numeric(18,2)',
    'Metadata should contain new resource amount'
);

-- Verify existing data still intact
SELECT is(
    (SELECT count(*)::int FROM accum.alter_test_x_movements),
    3,
    'Movements should still have 3 rows after add_resources'
);

-- ============================================================
-- TEST: Post data with new resource
-- ============================================================
SELECT lives_ok(
    $$SELECT accum.register_post('alter_test_x', '{
        "recorder":"doc:4","period":"2026-04-04",
        "warehouse":1,"product":100,"quantity":10,"amount":99.99
    }')$$,
    'Posting with new resource should succeed'
);

SELECT is(
    (SELECT count(*)::int FROM accum.alter_test_x_movements),
    4,
    'Should have 4 movements after new post'
);

-- ============================================================
-- TEST: Add a new dimension (triggers recalculation)
-- ============================================================
SELECT lives_ok(
    $$SELECT accum.register_alter(
        p_name         := 'alter_test_x',
        add_dimensions := '{"location": "text"}'
    )$$,
    'Adding a dimension should succeed'
);

SELECT has_column('accum', 'alter_test_x_movements', 'location',
    'Movements should have new location column');
SELECT has_column('accum', 'alter_test_x_totals_month', 'location',
    'Totals month should have new location column');
SELECT has_column('accum', 'alter_test_x_balance_cache', 'location',
    'Balance cache should have new location column');

-- Verify metadata updated
SELECT is(
    (SELECT accum.register_info('alter_test_x')->'dimensions'->>'location'),
    'text',
    'Metadata should contain new dimension location'
);

-- Verify movements count preserved
SELECT is(
    (SELECT count(*)::int FROM accum.alter_test_x_movements),
    4,
    'Movements should still have 4 rows after add_dimensions'
);

-- Verify totals were rebuilt
SELECT is(
    (SELECT count(*)::int FROM accum.alter_test_x_totals_month WHERE dim_hash IS NOT NULL),
    (SELECT count(*)::int FROM accum.alter_test_x_totals_month),
    'All totals_month rows should have non-null dim_hash after rebuild'
);

-- ============================================================
-- TEST: Duplicate dimension rejected
-- ============================================================
SELECT throws_ok(
    $$SELECT accum.register_alter(
        p_name         := 'alter_test_x',
        add_dimensions := '{"warehouse": "int"}'
    )$$,
    NULL,
    NULL,
    'Adding existing dimension should be rejected'
);

-- ============================================================
-- TEST: Duplicate resource rejected
-- ============================================================
SELECT throws_ok(
    $$SELECT accum.register_alter(
        p_name        := 'alter_test_x',
        add_resources := '{"quantity": "numeric"}'
    )$$,
    NULL,
    NULL,
    'Adding existing resource should be rejected'
);

-- ============================================================
-- TEST: Alter nonexistent register
-- ============================================================
SELECT throws_ok(
    $$SELECT accum.register_alter(
        p_name        := 'nonexistent',
        add_resources := '{"x": "int"}'
    )$$,
    NULL,
    NULL,
    'Altering nonexistent register should raise error'
);

-- ============================================================
-- TEST: Toggle high_write mode ON
-- ============================================================
SELECT lives_ok(
    $$SELECT accum.register_alter(
        p_name     := 'alter_test_x',
        high_write := true
    )$$,
    'Enabling high_write should succeed'
);

SELECT has_table('accum', 'alter_test_x_balance_cache_delta',
    'Delta buffer table should be created');

SELECT is(
    (SELECT (accum.register_info('alter_test_x')->>'high_write')::boolean),
    true,
    'Metadata should show high_write=true'
);

-- ============================================================
-- TEST: Toggle high_write mode OFF
-- ============================================================
SELECT lives_ok(
    $$SELECT accum.register_alter(
        p_name     := 'alter_test_x',
        high_write := false
    )$$,
    'Disabling high_write should succeed'
);

SELECT hasnt_table('accum', 'alter_test_x_balance_cache_delta',
    'Delta buffer table should be removed');

SELECT is(
    (SELECT (accum.register_info('alter_test_x')->>'high_write')::boolean),
    false,
    'Metadata should show high_write=false'
);

-- ============================================================
-- TEST: Alter turnover register (no balance_cache)
-- ============================================================
SELECT accum.register_create(
    name       := 'alter_turnover',
    dimensions := '{"dim1": "int"}',
    resources  := '{"res1": "numeric"}',
    kind       := 'turnover'
);

SELECT lives_ok(
    $$SELECT accum.register_alter(
        p_name        := 'alter_turnover',
        add_resources := '{"res2": "numeric"}'
    )$$,
    'Adding resource to turnover register should succeed'
);

SELECT has_column('accum', 'alter_turnover_movements', 'res2',
    'Turnover movements should have new resource');
SELECT hasnt_table('accum', 'alter_turnover_balance_cache',
    'Turnover should still not have balance_cache');

-- ============================================================
-- Cleanup
-- ============================================================
SELECT accum.register_drop('alter_test_x', force := true);
SELECT accum.register_drop('alter_turnover', force := true);

SELECT * FROM finish();
ROLLBACK;
