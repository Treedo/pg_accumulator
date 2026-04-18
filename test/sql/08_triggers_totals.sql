-- test/sql/08_triggers_totals.sql
-- Tests for trigger chain: movements → totals → cache consistency

BEGIN;
SELECT plan(17);

-- Setup
SELECT accum.register_create(
    name       := 'trg',
    dimensions := '{"warehouse": "int", "product": "int"}',
    resources  := '{"quantity": "numeric", "amount": "numeric"}',
    kind       := 'balance'
);

-- ============================================================
-- TEST: INSERT creates totals_month entry
-- ============================================================
SELECT accum.register_post('trg', '{
    "recorder":"d:1","period":"2026-04-10","warehouse":1,"product":1,"quantity":100,"amount":1000
}');

SELECT is(
    (SELECT count(*)::int FROM accum.trg_totals_month),
    1,
    'One totals_month row should exist'
);

SELECT is(
    (SELECT period FROM accum.trg_totals_month LIMIT 1),
    '2026-04-01'::date,
    'Totals month period should be first day of month'
);

-- ============================================================
-- TEST: INSERT creates totals_year entry
-- ============================================================
SELECT is(
    (SELECT count(*)::int FROM accum.trg_totals_year),
    1,
    'One totals_year row should exist'
);

SELECT is(
    (SELECT period FROM accum.trg_totals_year LIMIT 1),
    '2026-01-01'::date,
    'Totals year period should be first day of year'
);

-- ============================================================
-- TEST: Multiple movements same dim+month → one totals row
-- ============================================================
SELECT accum.register_post('trg', '{
    "recorder":"d:2","period":"2026-04-15","warehouse":1,"product":1,"quantity":50,"amount":500
}');

SELECT is(
    (SELECT count(*)::int FROM accum.trg_totals_month
     WHERE warehouse=1 AND product=1 AND period='2026-04-01'::date),
    1,
    'Same dim+month should aggregate into one totals_month row'
);

SELECT is(
    (SELECT quantity FROM accum.trg_totals_month
     WHERE warehouse=1 AND product=1 AND period='2026-04-01'::date),
    150::numeric,
    'Totals month quantity should be 100+50=150'
);

-- ============================================================
-- TEST: Different month → separate totals row
-- ============================================================
SELECT accum.register_post('trg', '{
    "recorder":"d:3","period":"2026-03-10","warehouse":1,"product":1,"quantity":30,"amount":300
}');

SELECT is(
    (SELECT count(*)::int FROM accum.trg_totals_month
     WHERE warehouse=1 AND product=1),
    2,
    'Different months should create separate totals rows'
);

-- ============================================================
-- TEST: Year totals aggregate across months
-- ============================================================
SELECT is(
    (SELECT quantity FROM accum.trg_totals_year
     WHERE warehouse=1 AND product=1 AND period='2026-01-01'::date),
    180::numeric,
    'Year totals should aggregate: 100+50+30=180'
);

-- ============================================================
-- TEST: Different dim_hash → separate rows
-- ============================================================
SELECT accum.register_post('trg', '{
    "recorder":"d:4","period":"2026-04-10","warehouse":2,"product":1,"quantity":25,"amount":250
}');

SELECT is(
    (SELECT count(*)::int FROM accum.trg_balance_cache),
    2,
    'Two unique dim_hash combinations should exist in cache'
);

-- ============================================================
-- TEST: DELETE trigger reduces totals
-- ============================================================
DELETE FROM accum.trg_movements WHERE recorder = 'd:2';

SELECT is(
    (SELECT quantity FROM accum.trg_totals_month
     WHERE warehouse=1 AND product=1 AND period='2026-04-01'::date),
    100::numeric,
    'After delete d:2, April totals should be 100 (was 150)'
);

SELECT is(
    (SELECT quantity FROM accum.trg_totals_year
     WHERE warehouse=1 AND product=1 AND period='2026-01-01'::date),
    130::numeric,
    'After delete d:2, year totals should be 130 (was 180)'
);

-- ============================================================
-- TEST: Balance cache updated after DELETE
-- ============================================================
SELECT is(
    (SELECT quantity FROM accum.trg_balance_cache
     WHERE warehouse=1 AND product=1),
    130::numeric,
    'Cache should be 130 after delete'
);

-- ============================================================
-- TEST: Version increments on each change
-- ============================================================
SELECT is(
    (SELECT version FROM accum.trg_balance_cache WHERE warehouse=1 AND product=1) >= 2,
    true,
    'Version should be >= 2 after multiple operations'
);

-- ============================================================
-- TEST: dim_hash is consistent for same dimensions
-- ============================================================
SELECT accum.register_post('trg', '{
    "recorder":"d:5","period":"2026-04-20","warehouse":1,"product":1,"quantity":10,"amount":100
}');

SELECT is(
    (SELECT count(DISTINCT dim_hash)::int FROM accum.trg_movements
     WHERE warehouse=1 AND product=1),
    1,
    'Same dimensions should produce same dim_hash'
);

-- Different dimensions → different dim_hash
SELECT is(
    (SELECT count(DISTINCT dim_hash)::int FROM accum.trg_movements),
    2,
    'Different dimension combinations should have different dim_hash'
);

-- ============================================================
-- TEST: recorded_at is set automatically
-- ============================================================
SELECT isnt(
    (SELECT recorded_at FROM accum.trg_movements WHERE recorder = 'd:5'),
    NULL::timestamptz,
    'recorded_at should be set by trigger'
);

-- ============================================================
-- TEST: Totals and cache sum matches movements sum
-- ============================================================
SELECT is(
    (SELECT quantity FROM accum.trg_balance_cache WHERE warehouse=1 AND product=1),
    (SELECT sum(quantity) FROM accum.trg_movements WHERE warehouse=1 AND product=1)::numeric,
    'Balance cache should equal SUM of movements'
);

-- Cleanup
SELECT accum.register_drop('trg', force := true);

SELECT * FROM finish();
ROLLBACK;
