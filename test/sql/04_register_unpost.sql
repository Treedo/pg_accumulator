-- test/sql/04_register_unpost.sql
-- Tests for register_unpost() — cancelling movements

BEGIN;
SELECT plan(14);

-- Setup
SELECT accum.register_create(
    name       := 'inv',
    dimensions := '{"warehouse": "int", "product": "int"}',
    resources  := '{"quantity": "numeric(18,4)", "amount": "numeric(18,2)"}',
    kind       := 'balance'
);

-- Post some movements
SELECT accum.register_post('inv', '[
    {"recorder":"doc:1","period":"2026-04-18","warehouse":1,"product":42,"quantity":100,"amount":5000},
    {"recorder":"doc:1","period":"2026-04-18","warehouse":1,"product":43,"quantity":50,"amount":2000}
]');

SELECT accum.register_post('inv', '{
    "recorder":"doc:2","period":"2026-04-18","warehouse":1,"product":42,"quantity":30,"amount":1500
}');

-- Verify initial state
SELECT is(
    (SELECT count(*)::int FROM accum.inv_movements),
    3,
    'Should have 3 movements initially'
);

SELECT is(
    (SELECT quantity FROM accum.inv_balance_cache WHERE warehouse=1 AND product=42),
    130::numeric,
    'Initial balance for wh=1,prod=42 should be 100+30=130'
);

-- ============================================================
-- TEST: Unpost returns count of deleted movements
-- ============================================================
SELECT is(
    accum.register_unpost('inv', 'doc:1'),
    2,
    'Unpost should return 2 (deleted 2 movements)'
);

-- ============================================================
-- TEST: Movements deleted
-- ============================================================
SELECT is(
    (SELECT count(*)::int FROM accum.inv_movements WHERE recorder = 'doc:1'),
    0,
    'Movements for doc:1 should be deleted'
);

SELECT is(
    (SELECT count(*)::int FROM accum.inv_movements),
    1,
    'Only doc:2 movement should remain'
);

-- ============================================================
-- TEST: Balance cache updated after unpost
-- ============================================================
SELECT is(
    (SELECT quantity FROM accum.inv_balance_cache WHERE warehouse=1 AND product=42),
    30::numeric,
    'Balance for wh=1,prod=42 should be 30 after unpost (only doc:2 remains)'
);

-- ============================================================
-- TEST: Product 43 balance should be 0 after unpost
-- ============================================================
SELECT is(
    (SELECT quantity FROM accum.inv_balance_cache WHERE warehouse=1 AND product=43),
    0::numeric,
    'Balance for wh=1,prod=43 should be 0 after unpost'
);

-- ============================================================
-- TEST: Totals updated after unpost
-- ============================================================
SELECT is(
    (SELECT quantity FROM accum.inv_totals_month
     WHERE warehouse=1 AND product=42 AND period='2026-04-01'::date),
    30::numeric,
    'Totals month should reflect only remaining movements'
);

SELECT is(
    (SELECT quantity FROM accum.inv_totals_year
     WHERE warehouse=1 AND product=42 AND period='2026-01-01'::date),
    30::numeric,
    'Totals year should reflect only remaining movements'
);

-- ============================================================
-- TEST: Unpost non-existent recorder returns 0
-- ============================================================
SELECT is(
    accum.register_unpost('inv', 'nonexistent:99'),
    0,
    'Unpost of nonexistent recorder should return 0'
);

-- ============================================================
-- TEST: Unpost all remaining movements
-- ============================================================
SELECT is(
    accum.register_unpost('inv', 'doc:2'),
    1,
    'Unpost doc:2 should return 1'
);

SELECT is(
    (SELECT count(*)::int FROM accum.inv_movements),
    0,
    'No movements should remain'
);

SELECT is(
    (SELECT quantity FROM accum.inv_balance_cache WHERE warehouse=1 AND product=42),
    0::numeric,
    'Balance should be 0 after all movements unposted'
);

-- ============================================================
-- TEST: Unpost nonexistent register
-- ============================================================
SELECT throws_ok(
    $$SELECT accum.register_unpost('nonexistent', 'doc:1')$$,
    NULL,
    NULL,
    'Unpost on nonexistent register should raise error'
);

-- Cleanup
SELECT accum.register_drop('inv', force := true);

SELECT * FROM finish();
ROLLBACK;
