-- test/sql/12_direct_insert.sql
-- Tests for direct INSERT into movements table (bypassing register_post)

BEGIN;
SELECT plan(8);

-- Setup
SELECT accum.register_create(
    name       := 'di',
    dimensions := '{"warehouse": "int", "product": "int"}',
    resources  := '{"quantity": "numeric", "amount": "numeric"}',
    kind       := 'balance'
);

-- ============================================================
-- TEST: Direct INSERT works and triggers fire
-- ============================================================
SELECT lives_ok(
    $$INSERT INTO accum.di_movements (recorder, period, warehouse, product, quantity, amount)
      VALUES ('direct:1', '2026-04-18', 1, 42, 100, 5000)$$,
    'Direct INSERT into movements should succeed'
);

-- ============================================================
-- TEST: dim_hash computed by BEFORE trigger
-- ============================================================
SELECT isnt(
    (SELECT dim_hash FROM accum.di_movements WHERE recorder='direct:1'),
    NULL::bigint,
    'dim_hash should be computed automatically'
);

-- ============================================================
-- TEST: Totals updated by AFTER trigger
-- ============================================================
SELECT is(
    (SELECT quantity FROM accum.di_totals_month
     WHERE warehouse=1 AND product=42 AND period='2026-04-01'::date),
    100::numeric,
    'Totals should be updated from direct INSERT'
);

-- ============================================================
-- TEST: Balance cache updated
-- ============================================================
SELECT is(
    (SELECT quantity FROM accum.di_balance_cache WHERE warehouse=1 AND product=42),
    100::numeric,
    'Balance cache should be updated from direct INSERT'
);

-- ============================================================
-- TEST: Batch direct INSERT
-- ============================================================
INSERT INTO accum.di_movements (recorder, period, warehouse, product, quantity, amount) VALUES
    ('batch:1', '2026-04-18', 1, 42, 50, 2500),
    ('batch:1', '2026-04-18', 1, 43, 200, 8000),
    ('batch:1', '2026-04-18', 2, 42, 30, 1500);

SELECT is(
    (SELECT count(*)::int FROM accum.di_movements),
    4,
    'Should have 4 movements total'
);

SELECT is(
    (SELECT quantity FROM accum.di_balance_cache WHERE warehouse=1 AND product=42),
    150::numeric,
    'Balance wh=1,prod=42 should be 100+50=150'
);

-- ============================================================
-- TEST: Direct DELETE works with reverse triggers
-- ============================================================
DELETE FROM accum.di_movements WHERE recorder='batch:1' AND product=43;

SELECT is(
    (SELECT count(*)::int FROM accum.di_balance_cache WHERE warehouse=1 AND product=43),
    1,
    'Cache row should still exist after delete'
);

SELECT is(
    (SELECT quantity FROM accum.di_balance_cache WHERE warehouse=1 AND product=43),
    0::numeric,
    'Balance should be 0 after deleting only movement'
);

-- Cleanup
SELECT accum.register_drop('di', force := true);

SELECT * FROM finish();
ROLLBACK;
