-- test/sql/22_consistency.sql
-- Comprehensive consistency tests for the aggregation chain:
-- movements → totals_day → totals_month → totals_year → balance_cache
-- Covers: multi-resource, cross-month, unpost/repost, turnover, high-write,
--         ORPHAN/MISSING detection, protection triggers on all derived tables.

BEGIN;
SELECT plan(42);

-- ============================================================
-- Setup: balance register with multi-month, multi-dimension data
-- ============================================================
SELECT accum.register_create(
    name       := 'con',
    dimensions := '{"warehouse": "int", "product": "int"}',
    resources  := '{"quantity": "numeric(18,4)", "amount": "numeric(18,2)"}',
    kind       := 'balance'
);

SELECT accum.register_post('con', '[
    {"recorder":"inv:1","period":"2026-01-10","warehouse":1,"product":100,"quantity":10,"amount":100},
    {"recorder":"inv:1","period":"2026-01-10","warehouse":1,"product":200,"quantity":5,"amount":50},
    {"recorder":"inv:2","period":"2026-01-25","warehouse":1,"product":100,"quantity":20,"amount":200},
    {"recorder":"inv:3","period":"2026-02-05","warehouse":1,"product":100,"quantity":7,"amount":70},
    {"recorder":"inv:4","period":"2026-02-15","warehouse":2,"product":100,"quantity":3,"amount":30},
    {"recorder":"inv:5","period":"2026-03-01","warehouse":1,"product":100,"quantity":15,"amount":150}
]');

-- ============================================================
-- 1. Full chain consistency after batch INSERT
-- ============================================================
SELECT is(
    (SELECT count(*) FROM accum.register_verify('con') WHERE status != 'OK'),
    0::bigint,
    'Full chain consistent after multi-month batch insert'
);

-- ============================================================
-- 2. totals_day aggregates per (dim_hash, period::date)
-- ============================================================
-- inv:1 and inv:2 are both wh:1/prod:100 in January, but different days
SELECT is(
    (SELECT count(*)::int FROM accum.con_totals_day
     WHERE warehouse=1 AND product=100),
    4,  -- Jan 10, Jan 25, Feb 5, Mar 1
    'totals_day: 4 distinct day entries for wh:1/prod:100'
);

SELECT is(
    (SELECT quantity FROM accum.con_totals_day
     WHERE warehouse=1 AND product=100 AND period='2026-01-10'::date),
    10.0000::numeric(18,4),
    'totals_day: Jan 10 should have quantity=10'
);

-- ============================================================
-- 3. totals_month correctly rolls up from day-level data
-- ============================================================
SELECT is(
    (SELECT quantity FROM accum.con_totals_month
     WHERE warehouse=1 AND product=100 AND period='2026-01-01'::date),
    30.0000::numeric(18,4),
    'totals_month: January wh:1/prod:100 = 10+20 = 30'
);

SELECT is(
    (SELECT amount FROM accum.con_totals_month
     WHERE warehouse=1 AND product=100 AND period='2026-02-01'::date),
    70.00::numeric(18,2),
    'totals_month: February wh:1/prod:100 amount = 70'
);

-- ============================================================
-- 4. totals_year correctly aggregates across months
-- ============================================================
SELECT is(
    (SELECT quantity FROM accum.con_totals_year
     WHERE warehouse=1 AND product=100 AND period='2026-01-01'::date),
    52.0000::numeric(18,4),
    'totals_year: wh:1/prod:100 year total = 10+20+7+15 = 52'
);

-- ============================================================
-- 5. balance_cache matches movements SUM for all dim_hashes
-- ============================================================
SELECT is(
    (SELECT count(*)::int FROM (
        SELECT m.dim_hash, SUM(m.quantity) AS mq, SUM(m.amount) AS ma,
               c.quantity AS cq, c.amount AS ca
        FROM accum.con_movements m
        JOIN accum.con_balance_cache c USING (dim_hash)
        GROUP BY m.dim_hash, c.quantity, c.amount
        HAVING SUM(m.quantity) != c.quantity OR SUM(m.amount) != c.amount
    ) mismatches),
    0,
    'balance_cache matches SUM(movements) for every dim_hash'
);

-- ============================================================
-- 6. Unpost → verify consistent (after rebuild to clean orphan zero-rows)
-- ============================================================
SELECT accum.register_unpost('con', 'inv:2');

-- Unpost leaves zeroed totals_day row for Jan 25 (orphan). This is expected
-- DELETE trigger behavior. Rebuild cleans up.
SELECT accum.register_rebuild_totals('con');

SELECT is(
    (SELECT count(*) FROM accum.register_verify('con') WHERE status != 'OK'),
    0::bigint,
    'Consistent after unpost + rebuild (inv:2 removed)'
);

-- Verify day-level was reduced
SELECT is(
    (SELECT quantity FROM accum.con_totals_day
     WHERE warehouse=1 AND product=100 AND period='2026-01-10'::date),
    10.0000::numeric(18,4),
    'totals_day: Jan 10 unchanged after unposting Jan 25 movement'
);

-- Verify month-level was reduced
SELECT is(
    (SELECT quantity FROM accum.con_totals_month
     WHERE warehouse=1 AND product=100 AND period='2026-01-01'::date),
    10.0000::numeric(18,4),
    'totals_month: January reduced to 10 after unpost of inv:2 (qty=20)'
);

-- ============================================================
-- 7. Repost → verify consistent
-- ============================================================
SELECT accum.register_repost('con', 'inv:5', '{
    "period":"2026-03-01","warehouse":1,"product":100,"quantity":25,"amount":250
}');

SELECT is(
    (SELECT count(*) FROM accum.register_verify('con') WHERE status != 'OK'),
    0::bigint,
    'Consistent after repost (inv:5 same day, changed qty 15→25)'
);

SELECT is(
    (SELECT quantity FROM accum.con_totals_day
     WHERE warehouse=1 AND product=100 AND period='2026-03-01'::date),
    25.0000::numeric(18,4),
    'totals_day: March reposted to 25'
);

-- ============================================================
-- 8. ORPHAN_IN_TOTALS detection (extra row in totals_day with no movements)
-- ============================================================
SET pg_accumulator.allow_internal = 'on';
INSERT INTO accum.con_totals_day (dim_hash, period, warehouse, product, quantity, amount)
VALUES (
    (SELECT dim_hash FROM accum.con_totals_day LIMIT 1),
    '2099-12-31'::date, 1, 100, 999, 999
);
RESET pg_accumulator.allow_internal;

SELECT ok(
    (SELECT count(*) > 0 FROM accum.register_verify('con')
     WHERE check_type = 'totals_day' AND status = 'ORPHAN_IN_TOTALS'),
    'verify detects ORPHAN_IN_TOTALS for extra totals_day row'
);

-- Rebuild to restore
SELECT accum.register_rebuild_totals('con');

SELECT is(
    (SELECT count(*) FROM accum.register_verify('con')
     WHERE check_type = 'totals_day' AND status != 'OK'),
    0::bigint,
    'rebuild_totals removes orphan — totals_day clean'
);

-- ============================================================
-- 9. MISSING_IN_TOTALS detection (delete row from totals_month)
-- ============================================================
SET pg_accumulator.allow_internal = 'on';
DELETE FROM accum.con_totals_month
WHERE period = '2026-01-01'::date AND warehouse = 1 AND product = 100;
RESET pg_accumulator.allow_internal;

SELECT ok(
    (SELECT count(*) > 0 FROM accum.register_verify('con')
     WHERE check_type = 'totals_month' AND status = 'MISSING_IN_TOTALS'),
    'verify detects MISSING_IN_TOTALS after deleting totals_month row'
);

SELECT accum.register_rebuild_totals('con');

SELECT is(
    (SELECT count(*) FROM accum.register_verify('con') WHERE status != 'OK'),
    0::bigint,
    'rebuild fixes MISSING_IN_TOTALS — all OK'
);

-- ============================================================
-- 10. totals_year MISMATCH detection
-- ============================================================
SET pg_accumulator.allow_internal = 'on';
UPDATE accum.con_totals_year SET amount = amount + 12345;
RESET pg_accumulator.allow_internal;

SELECT ok(
    (SELECT count(*) > 0 FROM accum.register_verify('con')
     WHERE check_type = 'totals_year' AND status = 'MISMATCH'),
    'verify detects MISMATCH in totals_year'
);

SELECT accum.register_rebuild_totals('con');

SELECT is(
    (SELECT count(*) FROM accum.register_verify('con')
     WHERE check_type = 'totals_year' AND status != 'OK'),
    0::bigint,
    'rebuild fixes totals_year MISMATCH'
);

-- ============================================================
-- 11. Multi-resource corruption: corrupt amount only
-- ============================================================
SET pg_accumulator.allow_internal = 'on';
UPDATE accum.con_balance_cache SET amount = amount - 9999;
RESET pg_accumulator.allow_internal;

SELECT ok(
    (SELECT count(*) > 0 FROM accum.register_verify('con')
     WHERE check_type = 'balance_cache' AND status = 'MISMATCH'),
    'verify detects amount-only corruption in balance_cache'
);

SELECT accum.register_rebuild_cache('con');

SELECT is(
    (SELECT count(*) FROM accum.register_verify('con') WHERE status != 'OK'),
    0::bigint,
    'rebuild_cache fixes amount-only corruption'
);

-- ============================================================
-- 12. Protection: block direct UPDATE on totals_year
-- ============================================================
SELECT throws_ok(
    $$UPDATE accum.con_totals_year SET quantity = 0$$,
    NULL,
    'Direct modification of derived table is not allowed. Use register_rebuild_totals() / register_rebuild_cache() for corrections.',
    'Direct UPDATE on totals_year blocked'
);

-- ============================================================
-- 13. Protection: block direct DELETE on balance_cache
-- ============================================================
SELECT throws_ok(
    $$DELETE FROM accum.con_balance_cache WHERE dim_hash = (SELECT dim_hash FROM accum.con_balance_cache LIMIT 1)$$,
    NULL,
    'Direct modification of derived table is not allowed. Use register_rebuild_totals() / register_rebuild_cache() for corrections.',
    'Direct DELETE on balance_cache blocked'
);

-- ============================================================
-- 14. Protection: block direct DELETE on totals_day
-- ============================================================
SELECT throws_ok(
    $$DELETE FROM accum.con_totals_day WHERE dim_hash = (SELECT dim_hash FROM accum.con_totals_day LIMIT 1)$$,
    NULL,
    'Direct modification of derived table is not allowed. Use register_rebuild_totals() / register_rebuild_cache() for corrections.',
    'Direct DELETE on totals_day blocked'
);

-- ============================================================
-- Cleanup balance register
-- ============================================================
SELECT accum.register_drop('con', true);

-- ============================================================
-- 15. Turnover register: consistency after inserts and deletes
-- ============================================================
SELECT accum.register_create(
    name       := 'con_turn',
    dimensions := '{"channel": "text"}',
    resources  := '{"revenue": "numeric(18,2)", "orders": "int"}',
    kind       := 'turnover'
);

SELECT accum.register_post('con_turn', '[
    {"recorder":"o:1","period":"2026-06-10","channel":"web","revenue":100,"orders":2},
    {"recorder":"o:2","period":"2026-06-15","channel":"web","revenue":250,"orders":5},
    {"recorder":"o:3","period":"2026-06-10","channel":"mobile","revenue":80,"orders":1},
    {"recorder":"o:4","period":"2026-07-01","channel":"web","revenue":300,"orders":3}
]');

SELECT is(
    (SELECT count(*) FROM accum.register_verify('con_turn') WHERE status != 'OK'),
    0::bigint,
    'Turnover register: consistent after batch insert'
);

-- Verify day-level
SELECT is(
    (SELECT revenue FROM accum.con_turn_totals_day
     WHERE channel='web' AND period='2026-06-10'::date),
    100.00::numeric(18,2),
    'Turnover totals_day: web Jun 10 revenue = 100'
);

-- Verify month-level
SELECT is(
    (SELECT orders::int FROM accum.con_turn_totals_month
     WHERE channel='web' AND period='2026-06-01'::date),
    7,
    'Turnover totals_month: web June orders = 2+5 = 7'
);

-- Unpost and rebuild (orphan zeroed rows cleaned by rebuild)
SELECT accum.register_unpost('con_turn', 'o:2');
SELECT accum.register_rebuild_totals('con_turn');

SELECT is(
    (SELECT count(*) FROM accum.register_verify('con_turn') WHERE status != 'OK'),
    0::bigint,
    'Turnover register: consistent after unpost + rebuild'
);

SELECT is(
    (SELECT revenue FROM accum.con_turn_totals_month
     WHERE channel='web' AND period='2026-06-01'::date),
    100.00::numeric(18,2),
    'Turnover month: web June revenue reduced to 100 after unpost'
);

-- Protection on turnover derived tables
SELECT throws_ok(
    $$INSERT INTO accum.con_turn_totals_day (dim_hash, period, channel, revenue, orders) VALUES (0, '2026-01-01', 'x', 0, 0)$$,
    NULL,
    'Direct modification of derived table is not allowed. Use register_rebuild_totals() / register_rebuild_cache() for corrections.',
    'Turnover: direct INSERT on totals_day blocked'
);

SELECT accum.register_drop('con_turn', true);

-- ============================================================
-- 16. High-write balance register: verify with delta buffer
-- ============================================================
SELECT accum.register_create(
    name       := 'con_hw',
    dimensions := '{"account": "text"}',
    resources  := '{"balance": "numeric(18,2)"}',
    kind       := 'balance',
    high_write := true
);

SELECT accum.register_post('con_hw', '[
    {"recorder":"t:1","period":"2026-04-01","account":"checking","balance":1000},
    {"recorder":"t:2","period":"2026-04-01","account":"checking","balance":500},
    {"recorder":"t:3","period":"2026-04-01","account":"savings","balance":2000}
]');

-- Verify with pending deltas (not yet merged)
SELECT is(
    (SELECT count(*) FROM accum.register_verify('con_hw') WHERE status != 'OK'),
    0::bigint,
    'High-write register: consistent with pending deltas'
);

-- Verify delta buffer has rows
SELECT ok(
    (SELECT count(*) > 0 FROM accum.con_hw_balance_cache_delta),
    'High-write: delta buffer has pending rows'
);

-- Flush deltas and re-verify
SELECT accum._delta_flush_register('con_hw');

SELECT is(
    (SELECT count(*) FROM accum.register_verify('con_hw') WHERE status != 'OK'),
    0::bigint,
    'High-write register: consistent after delta flush'
);

-- Verify cache values after flush
SELECT is(
    (SELECT balance FROM accum.con_hw_balance_cache WHERE account = 'checking'),
    1500.00::numeric(18,2),
    'High-write: merged cache for checking = 1000+500'
);

-- Totals chain is also correct
SELECT is(
    (SELECT balance FROM accum.con_hw_totals_day
     WHERE account='checking' AND period='2026-04-01'::date),
    1500.00::numeric(18,2),
    'High-write: totals_day consistent for checking'
);

-- Corrupt cache, rebuild, verify
SET pg_accumulator.allow_internal = 'on';
UPDATE accum.con_hw_balance_cache SET balance = -1;
RESET pg_accumulator.allow_internal;

SELECT ok(
    (SELECT count(*) > 0 FROM accum.register_verify('con_hw')
     WHERE check_type = 'balance_cache' AND status = 'MISMATCH'),
    'High-write: verify detects corrupted cache after flush'
);

SELECT accum.register_rebuild_cache('con_hw');

SELECT is(
    (SELECT count(*) FROM accum.register_verify('con_hw') WHERE status != 'OK'),
    0::bigint,
    'High-write: rebuild_cache restores consistency'
);

SELECT accum.register_drop('con_hw', true);

-- ============================================================
SELECT * FROM finish();
ROLLBACK;
