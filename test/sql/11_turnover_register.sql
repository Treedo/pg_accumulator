-- test/sql/11_turnover_register.sql
-- Tests for turnover-type registers (no balance_cache)

BEGIN;
SELECT plan(11);

-- ============================================================
-- Setup: create turnover register for sales
-- ============================================================
SELECT accum.register_create(
    name       := 'sales',
    dimensions := '{"product": "int", "region": "text"}',
    resources  := '{"sold_qty": "numeric", "revenue": "numeric(18,2)"}',
    kind       := 'turnover'
);

-- ============================================================
-- TEST: Infrastructure created correctly
-- ============================================================
SELECT has_table('accum', 'sales_movements', 'Movements table should exist');
SELECT has_table('accum', 'sales_totals_month', 'Totals month should exist');
SELECT has_table('accum', 'sales_totals_year', 'Totals year should exist');
SELECT hasnt_table('accum', 'sales_balance_cache', 'Balance cache should NOT exist');

-- ============================================================
-- TEST: Post turnover movements
-- ============================================================
SELECT is(
    accum.register_post('sales', '[
        {"recorder":"sale:1","period":"2026-04-10","product":1,"region":"north","sold_qty":10,"revenue":500},
        {"recorder":"sale:2","period":"2026-04-15","product":1,"region":"north","sold_qty":20,"revenue":1000},
        {"recorder":"sale:3","period":"2026-04-15","product":2,"region":"south","sold_qty":5,"revenue":250}
    ]'),
    3,
    'Should post 3 turnover movements'
);

-- ============================================================
-- TEST: Totals month aggregated
-- ============================================================
SELECT is(
    (SELECT sold_qty FROM accum.sales_totals_month
     WHERE product=1 AND region='north' AND period='2026-04-01'::date),
    30::numeric,
    'April north product 1 turnover should be 10+20=30'
);

SELECT is(
    (SELECT revenue FROM accum.sales_totals_month
     WHERE product=1 AND region='north' AND period='2026-04-01'::date),
    1500::numeric,
    'April north product 1 revenue should be 500+1000=1500'
);

-- ============================================================
-- TEST: Different dim_hash for different productxregion
-- ============================================================
SELECT is(
    (SELECT count(*)::int FROM accum.sales_totals_month),
    2,
    'Should have 2 totals_month rows (2 unique dim combos)'
);

-- ============================================================
-- TEST: Unpost works for turnover
-- ============================================================
SELECT accum.register_unpost('sales', 'sale:2');

SELECT is(
    (SELECT sold_qty FROM accum.sales_totals_month
     WHERE product=1 AND region='north' AND period='2026-04-01'::date),
    10::numeric,
    'After unpost sale:2, turnover should be 10'
);

-- ============================================================
-- TEST: Movements count after unpost
-- ============================================================
SELECT is(
    (SELECT count(*)::int FROM accum.sales_movements),
    2,
    'Should have 2 movements after unpost'
);

-- ============================================================
-- TEST: Year totals correct
-- ============================================================
SELECT is(
    (SELECT sold_qty FROM accum.sales_totals_year
     WHERE product=1 AND region='north' AND period='2026-01-01'::date),
    10::numeric,
    'Year totals should reflect unpost'
);

-- Cleanup
SELECT accum.register_drop('sales', force := true);

SELECT * FROM finish();
ROLLBACK;
