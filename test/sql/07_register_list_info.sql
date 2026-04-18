-- test/sql/07_register_list_info.sql
-- Tests for register_list() and register_info()

BEGIN;
SELECT plan(17);

-- Setup: create test registers
SELECT accum.register_create(
    name       := 'reg_a',
    dimensions := '{"warehouse": "int", "product": "int"}',
    resources  := '{"quantity": "numeric"}',
    kind       := 'balance'
);

SELECT accum.register_create(
    name       := 'reg_b',
    dimensions := '{"customer": "int"}',
    resources  := '{"total": "numeric", "count": "int"}',
    kind       := 'turnover'
);

-- ============================================================
-- TEST: register_list returns all registers
-- ============================================================
SELECT is(
    (SELECT count(*)::int FROM accum.register_list()),
    2,
    'register_list should return 2 registers'
);

SELECT is(
    (SELECT name FROM accum.register_list() WHERE name = 'reg_a'),
    'reg_a',
    'reg_a should be in the list'
);

SELECT is(
    (SELECT kind FROM accum.register_list() WHERE name = 'reg_b'),
    'turnover',
    'reg_b should be turnover kind'
);

SELECT is(
    (SELECT dimensions FROM accum.register_list() WHERE name = 'reg_a'),
    2,
    'reg_a should have 2 dimensions'
);

SELECT is(
    (SELECT resources FROM accum.register_list() WHERE name = 'reg_b'),
    2,
    'reg_b should have 2 resources'
);

-- ============================================================
-- TEST: register_list movements_count
-- ============================================================
SELECT is(
    (SELECT movements_count FROM accum.register_list() WHERE name = 'reg_a'),
    0::bigint,
    'reg_a should have 0 movements initially'
);

-- Add some movements and check count
SELECT accum.register_post('reg_a', '[
    {"recorder":"doc:1","period":"2026-04-18","warehouse":1,"product":100,"quantity":10},
    {"recorder":"doc:2","period":"2026-04-18","warehouse":1,"product":101,"quantity":5}
]');

SELECT is(
    (SELECT movements_count FROM accum.register_list() WHERE name = 'reg_a'),
    2::bigint,
    'reg_a should have 2 movements after posting'
);

-- ============================================================
-- TEST: register_info returns correct details
-- ============================================================
SELECT is(
    (SELECT accum.register_info('reg_a')->>'name'),
    'reg_a',
    'Info name should match'
);

SELECT is(
    (SELECT accum.register_info('reg_a')->>'kind'),
    'balance',
    'Info kind should be balance'
);

SELECT is(
    (SELECT accum.register_info('reg_a')->'dimensions'->>'warehouse'),
    'int',
    'Info dimensions should contain warehouse:int'
);

SELECT is(
    (SELECT accum.register_info('reg_a')->'resources'->>'quantity'),
    'numeric',
    'Info resources should contain quantity:numeric'
);

SELECT is(
    (SELECT accum.register_info('reg_b')->>'kind'),
    'turnover',
    'Info for reg_b kind should be turnover'
);

-- ============================================================
-- TEST: register_info includes movements_count
-- ============================================================
SELECT is(
    (SELECT (accum.register_info('reg_a')->>'movements_count')::int),
    2,
    'Info should show 2 movements for reg_a'
);

-- ============================================================
-- TEST: register_info includes tables
-- ============================================================
SELECT is(
    (SELECT accum.register_info('reg_a')->'tables'->>'movements'),
    'reg_a_movements',
    'Info tables should include movements table name'
);

SELECT is(
    (SELECT accum.register_info('reg_a')->'tables'->>'balance_cache'),
    'reg_a_balance_cache',
    'Info tables should include balance_cache for balance register'
);

-- ============================================================
-- TEST: register_info for nonexistent
-- ============================================================
SELECT throws_ok(
    $$SELECT accum.register_info('nonexistent')$$,
    NULL,
    NULL,
    'register_info for nonexistent should raise error'
);

-- ============================================================
-- TEST: register_list after drop
-- ============================================================
SELECT accum.register_drop('reg_b');

SELECT is(
    (SELECT count(*)::int FROM accum.register_list()),
    1,
    'register_list should return 1 after dropping reg_b'
);

-- Cleanup
SELECT accum.register_drop('reg_a', force := true);

SELECT * FROM finish();
ROLLBACK;
