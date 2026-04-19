-- test/sql/03_register_post.sql
-- Tests for register_post() — writing movements

BEGIN;
SELECT plan(25);

-- Setup: create test register
SELECT accum.register_create(
    name       := 'inv',
    dimensions := '{"warehouse": "int", "product": "int"}',
    resources  := '{"quantity": "numeric(18,4)", "amount": "numeric(18,2)"}',
    kind       := 'balance'
);

-- ============================================================
-- TEST: Post a single movement
-- ============================================================
SELECT is(
    accum.register_post('inv', '{
        "recorder":  "purchase:1",
        "period":    "2026-04-18",
        "warehouse": 1,
        "product":   42,
        "quantity":  100,
        "amount":    5000.00
    }'),
    1,
    'register_post should return 1 for single movement'
);

-- ============================================================
-- TEST: Movement recorded in movements table
-- ============================================================
SELECT is(
    (SELECT count(*)::int FROM accum.inv_movements WHERE recorder = 'purchase:1'),
    1,
    'Movement should be recorded in movements table'
);

SELECT is(
    (SELECT warehouse FROM accum.inv_movements WHERE recorder = 'purchase:1'),
    1,
    'Warehouse dimension should be stored correctly'
);

SELECT is(
    (SELECT product FROM accum.inv_movements WHERE recorder = 'purchase:1'),
    42,
    'Product dimension should be stored correctly'
);

-- ============================================================
-- TEST: dim_hash computed by trigger
-- ============================================================
SELECT isnt(
    (SELECT dim_hash FROM accum.inv_movements WHERE recorder = 'purchase:1'),
    NULL::bigint,
    'dim_hash should be computed by trigger (not NULL)'
);

-- ============================================================
-- TEST: Totals updated by trigger
-- ============================================================
SELECT is(
    (SELECT quantity FROM accum.inv_totals_month
     WHERE dim_hash = (SELECT dim_hash FROM accum.inv_movements WHERE recorder = 'purchase:1' LIMIT 1)
       AND period = '2026-04-01'::date),
    100::numeric,
    'Totals month should be updated with quantity'
);

SELECT is(
    (SELECT amount FROM accum.inv_totals_month
     WHERE dim_hash = (SELECT dim_hash FROM accum.inv_movements WHERE recorder = 'purchase:1' LIMIT 1)
       AND period = '2026-04-01'::date),
    5000.00::numeric,
    'Totals month should be updated with amount'
);

SELECT is(
    (SELECT quantity FROM accum.inv_totals_year
     WHERE dim_hash = (SELECT dim_hash FROM accum.inv_movements WHERE recorder = 'purchase:1' LIMIT 1)
       AND period = '2026-01-01'::date),
    100::numeric,
    'Totals year should be updated'
);

-- ============================================================
-- TEST: Balance cache updated by trigger
-- ============================================================
SELECT is(
    (SELECT quantity FROM accum.inv_balance_cache
     WHERE dim_hash = (SELECT dim_hash FROM accum.inv_movements WHERE recorder = 'purchase:1' LIMIT 1)),
    100::numeric,
    'Balance cache quantity should be 100'
);

SELECT is(
    (SELECT amount FROM accum.inv_balance_cache
     WHERE dim_hash = (SELECT dim_hash FROM accum.inv_movements WHERE recorder = 'purchase:1' LIMIT 1)),
    5000.00::numeric,
    'Balance cache amount should be 5000'
);

-- ============================================================
-- TEST: Post batch movements
-- ============================================================
SELECT is(
    accum.register_post('inv', '[
        {
            "recorder": "sale:1",
            "period":   "2026-04-18",
            "warehouse": 1, "product": 42,
            "quantity": -10, "amount": -500.00
        },
        {
            "recorder": "sale:1",
            "period":   "2026-04-18",
            "warehouse": 2, "product": 42,
            "quantity": 10, "amount": 500.00
        }
    ]'),
    2,
    'Batch post should return count of movements'
);

-- ============================================================
-- TEST: Balance cache aggregated correctly after batch
-- ============================================================
SELECT is(
    (SELECT quantity FROM accum.inv_balance_cache
     WHERE warehouse = 1 AND product = 42),
    90::numeric,
    'Balance for warehouse=1,product=42 should be 100-10=90'
);

SELECT is(
    (SELECT quantity FROM accum.inv_balance_cache
     WHERE warehouse = 2 AND product = 42),
    10::numeric,
    'Balance for warehouse=2,product=42 should be 10'
);

-- ============================================================
-- TEST: Totals aggregated correctly
-- ============================================================
SELECT is(
    (SELECT quantity FROM accum.inv_totals_month
     WHERE warehouse = 1 AND product = 42
       AND period = '2026-04-01'::date),
    90::numeric,
    'Totals month for wh=1,prod=42 should be 100-10=90'
);

-- ============================================================
-- TEST: Multiple movements same dim_hash accumulate
-- ============================================================
SELECT accum.register_post('inv', '{
    "recorder":  "purchase:2",
    "period":    "2026-04-18",
    "warehouse": 1,
    "product":   42,
    "quantity":  50,
    "amount":    2500.00
}');

SELECT is(
    (SELECT quantity FROM accum.inv_balance_cache
     WHERE warehouse = 1 AND product = 42),
    140::numeric,
    'Balance should accumulate: 90+50=140'
);

SELECT is(
    (SELECT version FROM accum.inv_balance_cache
     WHERE warehouse = 1 AND product = 42),
    3::bigint,
    'Version counter should increment with each update'
);

-- ============================================================
-- TEST: Validation - missing recorder
-- ============================================================
SELECT throws_ok(
    $$SELECT accum.register_post('inv', '{"period":"2026-04-18","warehouse":1,"product":42,"quantity":1,"amount":1}')$$,
    NULL,
    NULL,
    'Missing recorder should raise error'
);

-- ============================================================
-- TEST: Validation - missing period
-- ============================================================
SELECT throws_ok(
    $$SELECT accum.register_post('inv', '{"recorder":"x","warehouse":1,"product":42,"quantity":1,"amount":1}')$$,
    NULL,
    NULL,
    'Missing period should raise error'
);

-- ============================================================
-- TEST: Validation - missing dimension
-- ============================================================
SELECT throws_ok(
    $$SELECT accum.register_post('inv', '{"recorder":"x","period":"2026-04-18","warehouse":1,"quantity":1,"amount":1}')$$,
    NULL,
    NULL,
    'Missing dimension "product" should raise error'
);

-- ============================================================
-- TEST: Validation - nonexistent register
-- ============================================================
SELECT throws_ok(
    $$SELECT accum.register_post('nonexistent', '{"recorder":"x","period":"2026-04-18"}')$$,
    NULL,
    NULL,
    'Post to nonexistent register should raise error'
);

-- ============================================================
-- TEST: Negative quantities (outgoing movement)
-- ============================================================
SELECT accum.register_post('inv', '{
    "recorder":  "writeoff:1",
    "period":    "2026-04-18",
    "warehouse": 1,
    "product":   42,
    "quantity":  -200,
    "amount":    -10000.00
}');

SELECT is(
    (SELECT quantity FROM accum.inv_balance_cache
     WHERE warehouse = 1 AND product = 42),
    -60::numeric,
    'Balance can go negative (no constraint by default): 140-200=-60'
);

-- ============================================================
-- TEST: Different periods go to different totals
-- ============================================================
SELECT accum.register_post('inv', '{
    "recorder": "march_purchase:1",
    "period":   "2026-03-15",
    "warehouse": 1,
    "product": 99,
    "quantity": 50,
    "amount":  1000
}');

SELECT is(
    (SELECT period FROM accum.inv_totals_month
     WHERE warehouse = 1 AND product = 99),
    '2026-03-01'::date,
    'March movement should create March totals'
);

SELECT is(
    (SELECT count(*)::int FROM accum.inv_totals_month WHERE warehouse = 1 AND product = 99),
    1,
    'Should have exactly one totals_month row for March wh=1,prod=99'
);

-- ============================================================
-- TEST: Resource defaults to 0 when not provided
-- ============================================================
SELECT accum.register_post('inv', '{
    "recorder": "partial:1",
    "period":   "2026-04-18",
    "warehouse": 3,
    "product": 1,
    "quantity": 10
}');

SELECT is(
    (SELECT amount FROM accum.inv_balance_cache WHERE warehouse = 3 AND product = 1),
    0::numeric,
    'Missing resource should default to 0'
);

-- ============================================================
-- TEST: Post with ISO-8601 period format
-- ============================================================
SELECT is(
    accum.register_post('inv', '{
        "recorder": "iso:1",
        "period":   "2026-04-18T14:30:00Z",
        "warehouse": 5,
        "product": 5,
        "quantity": 1,
        "amount": 1
    }'),
    1,
    'ISO-8601 period format should be accepted'
);

-- ============================================================
-- Cleanup
-- ============================================================
SELECT accum.register_drop('inv', force := true);

SELECT * FROM finish();
ROLLBACK;
