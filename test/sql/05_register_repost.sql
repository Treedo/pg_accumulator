-- test/sql/05_register_repost.sql
-- Tests for register_repost() — atomic re-posting

BEGIN;
SELECT plan(10);

-- Setup
SELECT accum.register_create(
    name       := 'inv',
    dimensions := '{"warehouse": "int", "product": "int"}',
    resources  := '{"quantity": "numeric(18,4)", "amount": "numeric(18,2)"}',
    kind       := 'balance'
);

-- Post initial movement
SELECT accum.register_post('inv', '{
    "recorder":  "order:1",
    "period":    "2026-04-18",
    "warehouse": 1,
    "product":   42,
    "quantity":  100,
    "amount":    5000
}');

-- Verify initial state
SELECT is(
    (SELECT quantity FROM accum.inv_balance_cache WHERE warehouse=1 AND product=42),
    100::numeric,
    'Initial balance should be 100'
);

-- ============================================================
-- TEST: Repost with corrected quantity
-- ============================================================
SELECT is(
    accum.register_repost('inv', 'order:1', '{
        "period":    "2026-04-18",
        "warehouse": 1,
        "product":   42,
        "quantity":  110,
        "amount":    5500
    }'),
    1,
    'Repost should return count of new movements'
);

-- ============================================================
-- TEST: Balance updated correctly
-- ============================================================
SELECT is(
    (SELECT quantity FROM accum.inv_balance_cache WHERE warehouse=1 AND product=42),
    110::numeric,
    'Balance should be 110 after repost (was 100, now 110)'
);

SELECT is(
    (SELECT amount FROM accum.inv_balance_cache WHERE warehouse=1 AND product=42),
    5500::numeric,
    'Amount should be 5500 after repost'
);

-- ============================================================
-- TEST: Only new movements exist
-- ============================================================
SELECT is(
    (SELECT count(*)::int FROM accum.inv_movements WHERE recorder = 'order:1'),
    1,
    'Should have exactly 1 movement for order:1 after repost'
);

SELECT is(
    (SELECT quantity FROM accum.inv_movements WHERE recorder = 'order:1'),
    110::numeric,
    'Movement quantity should be 110'
);

-- ============================================================
-- TEST: Repost with multiple new movements
-- ============================================================
SELECT is(
    accum.register_repost('inv', 'order:1', '[
        {"period":"2026-04-18","warehouse":1,"product":42,"quantity":50,"amount":2500},
        {"period":"2026-04-18","warehouse":2,"product":42,"quantity":60,"amount":3000}
    ]'),
    2,
    'Repost with multiple movements should return 2'
);

SELECT is(
    (SELECT quantity FROM accum.inv_balance_cache WHERE warehouse=1 AND product=42),
    50::numeric,
    'Warehouse 1 balance should be 50'
);

SELECT is(
    (SELECT quantity FROM accum.inv_balance_cache WHERE warehouse=2 AND product=42),
    60::numeric,
    'Warehouse 2 balance should be 60'
);

-- ============================================================
-- TEST: Totals correct after repost
-- ============================================================
SELECT is(
    (SELECT quantity FROM accum.inv_totals_month
     WHERE warehouse=1 AND product=42 AND period='2026-04-01'::date),
    50::numeric,
    'Totals month should reflect reposted value'
);

-- Cleanup
SELECT accum.register_drop('inv', force := true);

SELECT * FROM finish();
ROLLBACK;
