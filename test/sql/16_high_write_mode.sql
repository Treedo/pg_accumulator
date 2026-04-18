-- test/sql/16_high_write_mode.sql
-- Tests for high_write mode (delta buffer)

BEGIN;
SELECT plan(8);

-- ============================================================
-- TEST: Create register with high_write=true
-- ============================================================
SELECT lives_ok(
    $$SELECT accum.register_create(
        name       := 'hw',
        dimensions := '{"page": "text"}',
        resources  := '{"views": "int"}',
        kind       := 'balance',
        high_write := true
    )$$,
    'Create high_write register should succeed'
);

-- ============================================================
-- TEST: Delta buffer table created
-- ============================================================
SELECT has_table('accum', 'hw_balance_cache_delta',
    'Delta buffer table should exist for high_write register');

SELECT has_column('accum', 'hw_balance_cache_delta', 'dim_hash',
    'Delta should have dim_hash');
SELECT has_column('accum', 'hw_balance_cache_delta', 'views',
    'Delta should have views resource column');
SELECT has_column('accum', 'hw_balance_cache_delta', 'created_at',
    'Delta should have created_at');

-- ============================================================
-- TEST: Post to high_write register → delta buffer populated
-- ============================================================
SELECT accum.register_post('hw', '{
    "recorder":"v:1","period":"2026-04-18","page":"/home","views":1
}');

SELECT is(
    (SELECT count(*)::int FROM accum.hw_balance_cache_delta),
    1,
    'Delta buffer should have 1 entry after post'
);

-- ============================================================
-- TEST: Multiple posts → multiple deltas
-- ============================================================
SELECT accum.register_post('hw', '[
    {"recorder":"v:2","period":"2026-04-18","page":"/home","views":1},
    {"recorder":"v:3","period":"2026-04-18","page":"/home","views":1},
    {"recorder":"v:4","period":"2026-04-18","page":"/about","views":1}
]');

SELECT is(
    (SELECT count(*)::int FROM accum.hw_balance_cache_delta),
    4,
    'Delta buffer should have 4 entries after batch post'
);

-- ============================================================
-- TEST: Totals still work in high_write mode
-- ============================================================
SELECT is(
    (SELECT views FROM accum.hw_totals_month
     WHERE page='/home' AND period='2026-04-01'::date),
    3::int,
    'Totals should still aggregate correctly in high_write mode'
);

-- Cleanup
SELECT accum.register_drop('hw', force := true);

SELECT * FROM finish();
ROLLBACK;
