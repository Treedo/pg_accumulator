-- test/sql/13_multiple_dimensions.sql
-- Tests for registers with various dimension types and counts

BEGIN;
SELECT plan(10);

-- ============================================================
-- TEST: Register with many dimensions
-- ============================================================
SELECT lives_ok(
    $$SELECT accum.register_create(
        name       := 'multi_dim',
        dimensions := '{
            "warehouse":  "int",
            "product":    "int",
            "lot_number": "text",
            "quality":    "text"
        }',
        resources  := '{
            "quantity": "numeric(18,4)",
            "weight":   "numeric(18,3)",
            "cost":     "numeric(18,2)"
        }',
        kind := 'balance'
    )$$,
    'Create register with 4 dimensions and 3 resources'
);

-- ============================================================
-- TEST: All dimension columns exist
-- ============================================================
SELECT has_column('accum', 'multi_dim_movements', 'warehouse', 'Should have warehouse');
SELECT has_column('accum', 'multi_dim_movements', 'product', 'Should have product');
SELECT has_column('accum', 'multi_dim_movements', 'lot_number', 'Should have lot_number');
SELECT has_column('accum', 'multi_dim_movements', 'quality', 'Should have quality');

-- ============================================================
-- TEST: Post with all dimensions
-- ============================================================
SELECT is(
    accum.register_post('multi_dim', '{
        "recorder":    "receipt:1",
        "period":      "2026-04-18",
        "warehouse":   1,
        "product":     42,
        "lot_number":  "LOT-A",
        "quality":     "grade_a",
        "quantity":    1000,
        "weight":      500.000,
        "cost":        25000.00
    }'),
    1,
    'Post with all dimensions should succeed'
);

-- ============================================================
-- TEST: Balance cache has all resources
-- ============================================================
SELECT is(
    (SELECT quantity FROM accum.multi_dim_balance_cache
     WHERE warehouse=1 AND product=42 AND lot_number='LOT-A'),
    1000::numeric,
    'Quantity should be 1000'
);

SELECT is(
    (SELECT weight FROM accum.multi_dim_balance_cache
     WHERE warehouse=1 AND product=42 AND lot_number='LOT-A'),
    500.000::numeric,
    'Weight should be 500'
);

SELECT is(
    (SELECT cost FROM accum.multi_dim_balance_cache
     WHERE warehouse=1 AND product=42 AND lot_number='LOT-A'),
    25000.00::numeric,
    'Cost should be 25000'
);

-- ============================================================
-- TEST: Different lot → different cache row
-- ============================================================
SELECT accum.register_post('multi_dim', '{
    "recorder": "receipt:2",
    "period":   "2026-04-18",
    "warehouse":1, "product":42, "lot_number":"LOT-B", "quality":"grade_a",
    "quantity":500, "weight":250, "cost":12500
}');

SELECT is(
    (SELECT count(*)::int FROM accum.multi_dim_balance_cache),
    2,
    'Different lots should create separate cache rows'
);

-- Cleanup
SELECT accum.register_drop('multi_dim', force := true);

SELECT * FROM finish();
ROLLBACK;
