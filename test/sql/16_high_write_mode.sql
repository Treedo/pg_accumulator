-- test/sql/16_high_write_mode.sql
-- Tests for high_write mode (delta buffer)

BEGIN;
SELECT plan(26);

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

-- ============================================================
-- TEST: Balance cache has seed rows with zero values
-- ============================================================
SELECT is(
    (SELECT count(*)::int FROM accum.hw_balance_cache),
    2,
    'Balance cache should have 2 seed rows (zeroed resources)'
);

SELECT is(
    (SELECT views FROM accum.hw_balance_cache WHERE page='/home'),
    0,
    'Seed row for /home should have views=0 before merge'
);

-- ============================================================
-- TEST: Delta-aware balance read (cache + pending deltas)
-- ============================================================
SELECT is(
    (SELECT (accum.hw_balance('{"page":"/home"}'::jsonb))->>'views'),
    '3',
    'Balance read should include pending deltas for /home'
);

SELECT is(
    (SELECT (accum.hw_balance('{"page":"/about"}'::jsonb))->>'views'),
    '1',
    'Balance read should include pending deltas for /about'
);

-- ============================================================
-- TEST: _delta_count returns pending count
-- ============================================================
SELECT is(
    accum._delta_count('hw'),
    4::bigint,
    'Delta count should be 4'
);

-- ============================================================
-- TEST: Delta merge (flush all with age=0)
-- ============================================================
SELECT is(
    accum._delta_flush_register('hw'),
    2,
    'Delta flush should update 2 dim_hash rows in balance cache'
);

-- After flush: delta buffer empty
SELECT is(
    (SELECT count(*)::int FROM accum.hw_balance_cache_delta),
    0,
    'Delta buffer should be empty after flush'
);

-- After flush: balance cache populated
SELECT is(
    (SELECT views FROM accum.hw_balance_cache WHERE page='/home'),
    3,
    'Balance cache should have merged value 3 for /home after flush'
);

SELECT is(
    (SELECT views FROM accum.hw_balance_cache WHERE page='/about'),
    1,
    'Balance cache should have merged value 1 for /about after flush'
);

-- ============================================================
-- TEST: Post more after flush → new deltas
-- ============================================================
SELECT accum.register_post('hw', '{
    "recorder":"v:5","period":"2026-04-18","page":"/home","views":5
}');

SELECT is(
    (SELECT count(*)::int FROM accum.hw_balance_cache_delta),
    1,
    'New delta after flush should appear in buffer'
);

-- Balance should combine cache + new delta
SELECT is(
    (SELECT (accum.hw_balance('{"page":"/home"}'::jsonb))->>'views'),
    '8',
    'Balance should be 3 (cache) + 5 (delta) = 8'
);

-- ============================================================
-- TEST: Delta merge with age filter (recent deltas not merged)
-- ============================================================
-- Set created_at to 10 seconds ago for the existing delta
UPDATE accum.hw_balance_cache_delta SET created_at = now() - interval '10 seconds';

-- Add a fresh delta
SELECT accum.register_post('hw', '{
    "recorder":"v:6","period":"2026-04-18","page":"/home","views":2
}');

-- Merge only deltas older than 5 seconds
SELECT is(
    accum._delta_merge_register('hw', interval '5 seconds', 10000),
    1,
    'Age-filtered merge should update 1 cache row'
);

-- Old delta merged, new delta remains
SELECT is(
    (SELECT count(*)::int FROM accum.hw_balance_cache_delta),
    1,
    'Recent delta should remain after age-filtered merge'
);

SELECT is(
    (SELECT views FROM accum.hw_balance_cache WHERE page='/home'),
    8,
    'Cache should have 3+5=8 after partial merge'
);

-- Balance includes remaining delta
SELECT is(
    (SELECT (accum.hw_balance('{"page":"/home"}'::jsonb))->>'views'),
    '10',
    'Balance should be 8 (cache) + 2 (remaining delta) = 10'
);

-- ============================================================
-- TEST: _delta_merge for all registers
-- ============================================================
-- Flush remaining
SELECT accum._delta_flush_register('hw');

SELECT accum.register_post('hw', '[
    {"recorder":"v:7","period":"2026-04-18","page":"/home","views":1},
    {"recorder":"v:8","period":"2026-04-18","page":"/about","views":1}
]');

-- Age them
UPDATE accum.hw_balance_cache_delta SET created_at = now() - interval '10 seconds';

SELECT is(
    (SELECT accum._delta_merge(interval '5 seconds', 10000) > 0),
    true,
    'Global delta merge should process deltas'
);

SELECT is(
    (SELECT count(*)::int FROM accum.hw_balance_cache_delta),
    0,
    'All deltas should be merged after global merge'
);

-- ============================================================
-- TEST: _delta_count for non-high_write register returns 0
-- ============================================================
SELECT is(
    accum._delta_count('hw'),
    0::bigint,
    'Delta count should be 0 after full merge'
);

-- Cleanup
SELECT accum.register_drop('hw', force := true);

SELECT * FROM finish();
ROLLBACK;
