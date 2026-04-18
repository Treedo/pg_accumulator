-- test/sql/17_recorder_pattern.sql
-- Tests for recorder-based document pattern

BEGIN;
SELECT plan(10);

-- Setup
SELECT accum.register_create(
    name       := 'rec',
    dimensions := '{"warehouse": "int", "product": "int"}',
    resources  := '{"quantity": "numeric"}',
    kind       := 'balance'
);

-- ============================================================
-- TEST: Same recorder links multiple movements
-- ============================================================
SELECT accum.register_post('rec', '[
    {"recorder":"purchase_order:7001","period":"2026-04-18","warehouse":1,"product":1,"quantity":100},
    {"recorder":"purchase_order:7001","period":"2026-04-18","warehouse":1,"product":2,"quantity":50},
    {"recorder":"purchase_order:7001","period":"2026-04-18","warehouse":2,"product":1,"quantity":30}
]');

SELECT is(
    (SELECT count(*)::int FROM accum.rec_movements WHERE recorder='purchase_order:7001'),
    3,
    'All 3 movements should share the same recorder'
);

-- ============================================================
-- TEST: Unpost removes all movements for recorder
-- ============================================================
SELECT is(
    accum.register_unpost('rec', 'purchase_order:7001'),
    3,
    'Unpost should remove all 3 movements'
);

SELECT is(
    (SELECT count(*)::int FROM accum.rec_movements),
    0,
    'No movements should remain'
);

-- ============================================================
-- TEST: Different recorders are independent
-- ============================================================
SELECT accum.register_post('rec', '{
    "recorder":"doc_a:1","period":"2026-04-18","warehouse":1,"product":1,"quantity":10
}');
SELECT accum.register_post('rec', '{
    "recorder":"doc_b:1","period":"2026-04-18","warehouse":1,"product":1,"quantity":20
}');

SELECT is(
    (SELECT quantity FROM accum.rec_balance_cache WHERE warehouse=1 AND product=1),
    30::numeric,
    'Both recorders should contribute to balance'
);

-- Unpost only doc_a
SELECT accum.register_unpost('rec', 'doc_a:1');

SELECT is(
    (SELECT quantity FROM accum.rec_balance_cache WHERE warehouse=1 AND product=1),
    20::numeric,
    'After unpost doc_a, only doc_b remains'
);

SELECT is(
    (SELECT count(*)::int FROM accum.rec_movements WHERE recorder='doc_b:1'),
    1,
    'doc_b movements should be untouched'
);

-- ============================================================
-- TEST: Recorder with complex format
-- ============================================================
SELECT lives_ok(
    $$SELECT accum.register_post('rec', '{
        "recorder":"adjustment:manual:2026-04-18:admin",
        "period":"2026-04-18","warehouse":1,"product":1,"quantity":5
    }')$$,
    'Complex recorder format should work'
);

SELECT is(
    (SELECT recorder FROM accum.rec_movements
     WHERE recorder='adjustment:manual:2026-04-18:admin'),
    'adjustment:manual:2026-04-18:admin',
    'Complex recorder should be stored exactly'
);

-- ============================================================
-- TEST: Repost preserves recorder reference
-- ============================================================
SELECT accum.register_repost('rec', 'doc_b:1', '{
    "period":"2026-04-18","warehouse":1,"product":1,"quantity":25
}');

SELECT is(
    (SELECT recorder FROM accum.rec_movements
     WHERE quantity=25),
    'doc_b:1',
    'Reposted movement should have original recorder'
);

SELECT is(
    (SELECT count(*)::int FROM accum.rec_movements WHERE recorder='doc_b:1'),
    1,
    'Should have exactly 1 movement for reposted recorder'
);

-- Cleanup
SELECT accum.register_drop('rec', force := true);

SELECT * FROM finish();
ROLLBACK;
