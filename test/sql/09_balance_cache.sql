-- test/sql/09_balance_cache.sql
-- Tests for balance_cache — direct SQL access, edge cases

BEGIN;
SELECT plan(12);

-- Setup
SELECT accum.register_create(
    name       := 'bc',
    dimensions := '{"account": "int", "currency": "text"}',
    resources  := '{"debit": "numeric(18,2)", "credit": "numeric(18,2)", "net": "numeric(18,2)"}',
    kind       := 'balance'
);

-- ============================================================
-- TEST: Empty register — no cache rows
-- ============================================================
SELECT is(
    (SELECT count(*)::int FROM accum.bc_balance_cache),
    0,
    'Empty register should have no cache rows'
);

-- ============================================================
-- TEST: First movement creates cache row
-- ============================================================
SELECT accum.register_post('bc', '{
    "recorder":"t:1","period":"2026-04-18",
    "account":1001,"currency":"UAH",
    "debit":0,"credit":5000,"net":-5000
}');

SELECT is(
    (SELECT count(*)::int FROM accum.bc_balance_cache),
    1,
    'First movement should create one cache row'
);

SELECT is(
    (SELECT net FROM accum.bc_balance_cache WHERE account=1001 AND currency='UAH'),
    -5000::numeric,
    'Net balance should be -5000'
);

-- ============================================================
-- TEST: Counter-movement
-- ============================================================
SELECT accum.register_post('bc', '{
    "recorder":"t:2","period":"2026-04-18",
    "account":1001,"currency":"UAH",
    "debit":3000,"credit":0,"net":3000
}');

SELECT is(
    (SELECT net FROM accum.bc_balance_cache WHERE account=1001 AND currency='UAH'),
    -2000::numeric,
    'Net should be -5000+3000=-2000'
);

-- ============================================================
-- TEST: Multiple currencies — separate cache rows
-- ============================================================
SELECT accum.register_post('bc', '{
    "recorder":"t:3","period":"2026-04-18",
    "account":1001,"currency":"USD",
    "debit":100,"credit":0,"net":100
}');

SELECT is(
    (SELECT count(*)::int FROM accum.bc_balance_cache WHERE account=1001),
    2,
    'Different currencies should create separate cache rows'
);

-- ============================================================
-- TEST: Direct SQL query on cache table
-- ============================================================
SELECT is(
    (SELECT currency FROM accum.bc_balance_cache
     WHERE account=1001 AND net > 0),
    'USD',
    'Direct SQL query should work on cache table'
);

-- ============================================================
-- TEST: last_movement_at is updated
-- ============================================================
SELECT isnt(
    (SELECT last_movement_at FROM accum.bc_balance_cache
     WHERE account=1001 AND currency='UAH'),
    NULL::timestamptz,
    'last_movement_at should be set'
);

-- ============================================================
-- TEST: version tracks updates
-- ============================================================
SELECT is(
    (SELECT version FROM accum.bc_balance_cache
     WHERE account=1001 AND currency='UAH'),
    2::bigint,
    'Version should be 2 after two movements'
);

-- ============================================================
-- TEST: Unpost back to zero
-- ============================================================
SELECT accum.register_unpost('bc', 't:1');
SELECT accum.register_unpost('bc', 't:2');

SELECT is(
    (SELECT net FROM accum.bc_balance_cache
     WHERE account=1001 AND currency='UAH'),
    0::numeric,
    'Net should be 0 after unposting all UAH movements'
);

-- ============================================================
-- TEST: Cache row persists even at zero balance
-- ============================================================
SELECT is(
    (SELECT count(*)::int FROM accum.bc_balance_cache
     WHERE account=1001 AND currency='UAH'),
    1,
    'Cache row should persist even when balance is 0'
);

-- ============================================================
-- TEST: Turnover register has no balance_cache
-- ============================================================
SELECT accum.register_create(
    name       := 'bc_turnover',
    dimensions := '{"x": "int"}',
    resources  := '{"y": "numeric"}',
    kind       := 'turnover'
);

SELECT hasnt_table('accum', 'bc_turnover_balance_cache',
    'Turnover register should not have balance_cache'
);

-- Cleanup
SELECT accum.register_drop('bc', force := true);
SELECT accum.register_drop('bc_turnover');

SELECT * FROM finish();
ROLLBACK;
