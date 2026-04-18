-- test/sql/10_correction_retroactive.sql
-- Tests for retroactive corrections (backdated movements)

BEGIN;
SELECT plan(12);

-- Setup: register with months of data
SELECT accum.register_create(
    name       := 'stock',
    dimensions := '{"warehouse": "int", "product": "int"}',
    resources  := '{"quantity": "numeric", "amount": "numeric"}',
    kind       := 'balance'
);

-- January data
SELECT accum.register_post('stock', '{
    "recorder":"jan:1","period":"2026-01-15","warehouse":1,"product":1,"quantity":100,"amount":1000
}');

-- February data
SELECT accum.register_post('stock', '{
    "recorder":"feb:1","period":"2026-02-10","warehouse":1,"product":1,"quantity":50,"amount":500
}');

-- March data
SELECT accum.register_post('stock', '{
    "recorder":"mar:1","period":"2026-03-20","warehouse":1,"product":1,"quantity":-30,"amount":-300
}');

-- April data
SELECT accum.register_post('stock', '{
    "recorder":"apr:1","period":"2026-04-05","warehouse":1,"product":1,"quantity":20,"amount":200
}');

-- ============================================================
-- TEST: Initial state correct
-- ============================================================
SELECT is(
    (SELECT quantity FROM accum.stock_balance_cache WHERE warehouse=1 AND product=1),
    140::numeric,
    'Initial balance: 100+50-30+20=140'
);

SELECT is(
    (SELECT quantity FROM accum.stock_totals_month
     WHERE warehouse=1 AND product=1 AND period='2026-01-01'::date),
    100::numeric,
    'January totals should be 100'
);

-- ============================================================
-- TEST: Correct January data (repost with different quantity)
-- ============================================================
SELECT accum.register_repost('stock', 'jan:1', '{
    "period":"2026-01-15","warehouse":1,"product":1,"quantity":110,"amount":1100
}');

-- ============================================================
-- TEST: January totals updated
-- ============================================================
SELECT is(
    (SELECT quantity FROM accum.stock_totals_month
     WHERE warehouse=1 AND product=1 AND period='2026-01-01'::date),
    110::numeric,
    'January totals should be corrected to 110'
);

-- ============================================================
-- TEST: February totals NOT affected
-- ============================================================
SELECT is(
    (SELECT quantity FROM accum.stock_totals_month
     WHERE warehouse=1 AND product=1 AND period='2026-02-01'::date),
    50::numeric,
    'February totals should be unchanged (50)'
);

-- ============================================================
-- TEST: March totals NOT affected
-- ============================================================
SELECT is(
    (SELECT quantity FROM accum.stock_totals_month
     WHERE warehouse=1 AND product=1 AND period='2026-03-01'::date),
    -30::numeric,
    'March totals should be unchanged (-30)'
);

-- ============================================================
-- TEST: April totals NOT affected
-- ============================================================
SELECT is(
    (SELECT quantity FROM accum.stock_totals_month
     WHERE warehouse=1 AND product=1 AND period='2026-04-01'::date),
    20::numeric,
    'April totals should be unchanged (20)'
);

-- ============================================================
-- TEST: Year totals updated
-- ============================================================
SELECT is(
    (SELECT quantity FROM accum.stock_totals_year
     WHERE warehouse=1 AND product=1 AND period='2026-01-01'::date),
    150::numeric,
    'Year 2026 totals should be 110+50-30+20=150'
);

-- ============================================================
-- TEST: Balance cache updated to new total
-- ============================================================
SELECT is(
    (SELECT quantity FROM accum.stock_balance_cache WHERE warehouse=1 AND product=1),
    150::numeric,
    'Balance cache should be 150 after correction'
);

-- ============================================================
-- TEST: Add a new backdated movement (February addition)
-- ============================================================
SELECT accum.register_post('stock', '{
    "recorder":"feb_correction:1","period":"2026-02-15","warehouse":1,"product":1,"quantity":5,"amount":50
}');

SELECT is(
    (SELECT quantity FROM accum.stock_totals_month
     WHERE warehouse=1 AND product=1 AND period='2026-02-01'::date),
    55::numeric,
    'February totals should now be 50+5=55'
);

SELECT is(
    (SELECT quantity FROM accum.stock_balance_cache WHERE warehouse=1 AND product=1),
    155::numeric,
    'Balance should be 155 after February correction'
);

-- ============================================================
-- TEST: Other months still unaffected
-- ============================================================
SELECT is(
    (SELECT quantity FROM accum.stock_totals_month
     WHERE warehouse=1 AND product=1 AND period='2026-03-01'::date),
    -30::numeric,
    'March still -30 after February correction'
);

SELECT is(
    (SELECT quantity FROM accum.stock_totals_month
     WHERE warehouse=1 AND product=1 AND period='2026-04-01'::date),
    20::numeric,
    'April still 20 after February correction'
);

-- Cleanup
SELECT accum.register_drop('stock', force := true);

SELECT * FROM finish();
ROLLBACK;
