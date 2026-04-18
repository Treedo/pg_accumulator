-- test/sql/14_end_to_end_warehouse.sql
-- End-to-end test: warehouse stock management scenario from README

BEGIN;
SELECT plan(10);

-- ============================================================
-- Create warehouse register (matches README example)
-- ============================================================
SELECT accum.register_create(
    name       := 'inventory',
    dimensions := '{"warehouse": "int", "product": "int"}',
    resources  := '{"quantity": "numeric(18,4)", "amount": "numeric(18,2)"}',
    kind       := 'balance'
);

-- ============================================================
-- Scenario: Receipt → Sale → Check balance → Correction → Verify
-- ============================================================

-- Step 1: Receive 100 units of product 42 at warehouse 1
SELECT accum.register_post('inventory', '{
    "recorder":  "receipt:1",
    "period":    "2026-04-18",
    "warehouse": 1,
    "product":   42,
    "quantity":  100,
    "amount":    5000.00
}');

SELECT is(
    (SELECT quantity FROM accum.inventory_balance_cache WHERE warehouse=1 AND product=42),
    100.0000::numeric,
    'Step 1: After receipt, balance should be 100'
);

-- Step 2: Sell 10 units from warehouse 1, transfer 10 to warehouse 2
SELECT accum.register_post('inventory', '[
    {"recorder":"sale:1","period":"2026-04-18","warehouse":1,"product":42,"quantity":-10,"amount":-500},
    {"recorder":"transfer:1","period":"2026-04-18","warehouse":1,"product":42,"quantity":-10,"amount":-500},
    {"recorder":"transfer:1","period":"2026-04-18","warehouse":2,"product":42,"quantity":10,"amount":500}
]');

SELECT is(
    (SELECT quantity FROM accum.inventory_balance_cache WHERE warehouse=1 AND product=42),
    80.0000::numeric,
    'Step 2: After sale+transfer, wh1 balance should be 100-10-10=80'
);

SELECT is(
    (SELECT quantity FROM accum.inventory_balance_cache WHERE warehouse=2 AND product=42),
    10.0000::numeric,
    'Step 2: Warehouse 2 should have 10 units'
);

-- Step 3: Receive another product
SELECT accum.register_post('inventory', '{
    "recorder":"receipt:2","period":"2026-04-18","warehouse":1,"product":99,"quantity":200,"amount":4000
}');

SELECT is(
    (SELECT count(*)::int FROM accum.inventory_balance_cache WHERE quantity != 0),
    3,
    'Step 3: Should have 3 non-zero balance rows'
);

-- Step 4: Cancel the sale (discovered error)
SELECT accum.register_unpost('inventory', 'sale:1');

SELECT is(
    (SELECT quantity FROM accum.inventory_balance_cache WHERE warehouse=1 AND product=42),
    90.0000::numeric,
    'Step 4: After cancel sale, wh1 balance should be 80+10=90'
);

-- Step 5: Correct the transfer amount (was 10, should be 15)
SELECT accum.register_repost('inventory', 'transfer:1', '[
    {"period":"2026-04-18","warehouse":1,"product":42,"quantity":-15,"amount":-750},
    {"period":"2026-04-18","warehouse":2,"product":42,"quantity":15,"amount":750}
]');

SELECT is(
    (SELECT quantity FROM accum.inventory_balance_cache WHERE warehouse=1 AND product=42),
    85.0000::numeric,
    'Step 5: After corrected transfer, wh1 should be 100-15=85'
);

SELECT is(
    (SELECT quantity FROM accum.inventory_balance_cache WHERE warehouse=2 AND product=42),
    15.0000::numeric,
    'Step 5: Warehouse 2 should now have 15'
);

-- Step 6: Verify totals consistency
SELECT is(
    (SELECT sum(quantity), sum(amount) FROM accum.inventory_movements WHERE warehouse=1 AND product=42),
    (SELECT quantity, amount FROM accum.inventory_balance_cache WHERE warehouse=1 AND product=42),
    'Step 6: Balance cache must equal SUM(movements) for wh1,prod42'
);

-- Step 7: Verify total product 42 across all warehouses
SELECT is(
    (SELECT sum(quantity) FROM accum.inventory_balance_cache WHERE product=42),
    100.0000::numeric,
    'Step 7: Total product 42 across all warehouses should be 100 (receipt only)'
);

-- Step 8: Verify grand total
SELECT is(
    (SELECT sum(quantity) FROM accum.inventory_balance_cache),
    300.0000::numeric,
    'Step 8: Grand total should be 100(prod42) + 200(prod99) = 300'
);

-- Cleanup
SELECT accum.register_drop('inventory', force := true);

SELECT * FROM finish();
ROLLBACK;
