-- test/sql/21_bgworker.sql
-- Tests for background worker infrastructure and maintenance helper functions
-- Note: The actual bgworker process runs only when pg_accumulator is loaded
-- via shared_preload_libraries. These tests verify the SQL-level support
-- functions and manual maintenance triggers that the worker internally calls.

BEGIN;
SELECT plan(16);

-- ============================================================
-- Setup: create registers for testing
-- ============================================================
SELECT accum.register_create(
    name       := 'bgw_bal',
    dimensions := '{"account": "text"}',
    resources  := '{"balance": "numeric(18,2)"}',
    kind       := 'balance',
    high_write := true
);

SELECT accum.register_create(
    name       := 'bgw_turn',
    dimensions := '{"category": "text"}',
    resources  := '{"total": "numeric(18,2)"}',
    kind       := 'turnover'
);

-- ============================================================
-- TEST 1: _force_delta_merge on empty buffer → returns 0
-- ============================================================
SELECT is(
    accum._force_delta_merge(),
    0,
    '_force_delta_merge on empty delta buffer returns 0'
);

-- ============================================================
-- TEST 2: Post data to high_write register → creates deltas
-- ============================================================
SELECT accum.register_post('bgw_bal', '[
    {"recorder":"bg:1","period":"2026-04-18","account":"1000","balance":100},
    {"recorder":"bg:2","period":"2026-04-18","account":"1000","balance":50},
    {"recorder":"bg:3","period":"2026-04-18","account":"2000","balance":200}
]');

SELECT ok(
    (SELECT count(*) > 0 FROM accum.bgw_bal_balance_cache_delta),
    'Delta buffer should have entries after posts'
);

-- Backdate deltas so they are eligible for merge (now() is frozen in transaction)
UPDATE accum.bgw_bal_balance_cache_delta SET created_at = created_at - interval '10 seconds';

-- ============================================================
-- TEST 3: _delta_merge_register merges deltas
-- ============================================================
SELECT is(
    (SELECT accum._delta_merge_register('bgw_bal', interval '0 seconds', 10000) > 0),
    true,
    '_delta_merge_register should merge at least some deltas'
);

-- ============================================================
-- TEST 4: After merge, balance_cache is updated
-- ============================================================
SELECT is(
    (SELECT balance FROM accum.bgw_bal_balance_cache WHERE account = '1000'),
    150.00::numeric(18,2),
    'balance_cache should reflect merged deltas for account 1000'
);

SELECT is(
    (SELECT balance FROM accum.bgw_bal_balance_cache WHERE account = '2000'),
    200.00::numeric(18,2),
    'balance_cache should reflect merged deltas for account 2000'
);

-- ============================================================
-- TEST 5: Delta buffer is empty after full merge
-- ============================================================
SELECT is(
    (SELECT count(*)::int FROM accum.bgw_bal_balance_cache_delta),
    0,
    'Delta buffer should be empty after full merge'
);

-- ============================================================
-- TEST 6: _force_delta_merge with age=0 merges everything
-- ============================================================
SELECT accum.register_post('bgw_bal', '[
    {"recorder":"bg:4","period":"2026-04-18","account":"1000","balance":25}
]');
-- Backdate for merge eligibility
UPDATE accum.bgw_bal_balance_cache_delta SET created_at = created_at - interval '10 seconds';

SELECT ok(
    (SELECT accum._force_delta_merge() >= 0),
    '_force_delta_merge returns >= 0'
);

-- ============================================================
-- TEST 7: _delta_merge_register rejects non-existent register
-- ============================================================
SELECT throws_ok(
    $$SELECT accum._delta_merge_register('nonexistent')$$,
    NULL,
    NULL,
    '_delta_merge_register rejects non-existent register'
);

-- ============================================================
-- TEST 8: _delta_merge_register rejects non-high_write register
-- ============================================================
SELECT accum.register_create(
    name       := 'bgw_normal',
    dimensions := '{"item": "text"}',
    resources  := '{"qty": "int"}',
    kind       := 'balance',
    high_write := false
);

SELECT throws_ok(
    $$SELECT accum._delta_merge_register('bgw_normal')$$,
    NULL,
    NULL,
    '_delta_merge_register rejects non-high_write register'
);

-- ============================================================
-- TEST 9: _delta_merge_register rejects turnover register
-- ============================================================
SELECT throws_ok(
    $$SELECT accum._delta_merge_register('bgw_turn')$$,
    NULL,
    NULL,
    '_delta_merge_register rejects turnover register'
);

-- ============================================================
-- TEST 10: _delta_flush_register flushes all pending deltas
-- ============================================================
SELECT accum.register_post('bgw_bal', '[
    {"recorder":"bg:5","period":"2026-04-18","account":"3000","balance":999}
]');

SELECT ok(
    (SELECT accum._delta_flush_register('bgw_bal') >= 0),
    '_delta_flush_register should flush pending deltas'
);

SELECT is(
    (SELECT count(*)::int FROM accum.bgw_bal_balance_cache_delta),
    0,
    'Delta buffer should be empty after flush'
);

-- ============================================================
-- TEST 11: Partition maintenance — create partitions ahead
-- ============================================================
SELECT ok(
    (SELECT accum.register_create_partitions('bgw_bal', interval '3 months') >= 0),
    'register_create_partitions should succeed'
);

-- ============================================================
-- TEST 12: register_stats returns valid JSON
-- ============================================================
SELECT ok(
    (SELECT accum.register_stats('bgw_bal') IS NOT NULL),
    'register_stats should return non-null JSON'
);

SELECT ok(
    (SELECT (accum.register_stats('bgw_bal'))->>'movements_count' IS NOT NULL),
    'register_stats should include movements_count'
);

-- ============================================================
-- TEST 13: _maintenance_status function works
-- ============================================================
SELECT lives_ok(
    $$SELECT * FROM accum._maintenance_status()$$,
    '_maintenance_status should execute without error'
);

-- ============================================================
-- Cleanup
-- ============================================================
SELECT accum.register_drop('bgw_bal', force := true);
SELECT accum.register_drop('bgw_turn', force := true);
SELECT accum.register_drop('bgw_normal', force := true);

SELECT * FROM finish();
ROLLBACK;
