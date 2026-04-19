-- test/sql/02_register_create.sql
-- Tests for register_create() — DDL generation, infrastructure creation

BEGIN;
SELECT plan(37);

-- ============================================================
-- TEST: Create a basic balance register
-- ============================================================
SELECT lives_ok(
    $$SELECT accum.register_create(
        name       := 'inventory',
        dimensions := '{"warehouse": "int", "product": "int"}',
        resources  := '{"quantity": "numeric(18,4)", "amount": "numeric(18,2)"}',
        kind       := 'balance'
    )$$,
    'register_create should succeed for basic balance register'
);

-- ============================================================
-- TEST: Register metadata saved
-- ============================================================
SELECT is(
    (SELECT kind FROM accum._registers WHERE name = 'inventory'),
    'balance',
    'Register kind should be saved as balance'
);

SELECT is(
    (SELECT dimensions->>'warehouse' FROM accum._registers WHERE name = 'inventory'),
    'int',
    'Dimension warehouse type should be saved'
);

SELECT is(
    (SELECT dimensions->>'product' FROM accum._registers WHERE name = 'inventory'),
    'int',
    'Dimension product type should be saved'
);

-- ============================================================
-- TEST: Movements table created
-- ============================================================
SELECT has_table('accum', 'inventory_movements', 'Movements table should exist');
SELECT has_column('accum', 'inventory_movements', 'id', 'Movements should have id');
SELECT has_column('accum', 'inventory_movements', 'recorder', 'Movements should have recorder');
SELECT has_column('accum', 'inventory_movements', 'period', 'Movements should have period');
SELECT has_column('accum', 'inventory_movements', 'dim_hash', 'Movements should have dim_hash');
SELECT has_column('accum', 'inventory_movements', 'warehouse', 'Movements should have warehouse dimension');
SELECT has_column('accum', 'inventory_movements', 'product', 'Movements should have product dimension');
SELECT has_column('accum', 'inventory_movements', 'quantity', 'Movements should have quantity resource');
SELECT has_column('accum', 'inventory_movements', 'amount', 'Movements should have amount resource');
SELECT has_column('accum', 'inventory_movements', 'movement_type', 'Movements should have movement_type');
SELECT has_column('accum', 'inventory_movements', 'recorded_at', 'Movements should have recorded_at');

-- ============================================================
-- TEST: Totals tables created
-- ============================================================
SELECT has_table('accum', 'inventory_totals_month', 'Totals month table should exist');
SELECT has_table('accum', 'inventory_totals_year', 'Totals year table should exist');

SELECT has_column('accum', 'inventory_totals_month', 'dim_hash', 'Totals month should have dim_hash');
SELECT has_column('accum', 'inventory_totals_month', 'period', 'Totals month should have period');
SELECT has_column('accum', 'inventory_totals_month', 'quantity', 'Totals month should have quantity');

-- ============================================================
-- TEST: Balance cache created (balance kind)
-- ============================================================
SELECT has_table('accum', 'inventory_balance_cache', 'Balance cache should exist for balance kind');
SELECT has_column('accum', 'inventory_balance_cache', 'dim_hash', 'Cache should have dim_hash');
SELECT has_column('accum', 'inventory_balance_cache', 'warehouse', 'Cache should have warehouse');
SELECT has_column('accum', 'inventory_balance_cache', 'product', 'Cache should have product');
SELECT has_column('accum', 'inventory_balance_cache', 'quantity', 'Cache should have quantity');
SELECT has_column('accum', 'inventory_balance_cache', 'version', 'Cache should have version');

-- ============================================================
-- TEST: Default partition created
-- ============================================================
SELECT has_table('accum', 'inventory_movements_default', 'Default partition should exist');

-- ============================================================
-- TEST: Hash function created
-- ============================================================
SELECT has_function('accum', '_hash_inventory', 'Hash function should exist');

-- ============================================================
-- TEST: Duplicate register name rejected
-- ============================================================
SELECT throws_ok(
    $$SELECT accum.register_create(
        name       := 'inventory',
        dimensions := '{"x": "int"}',
        resources  := '{"y": "numeric"}'
    )$$,
    NULL,
    NULL,
    'Duplicate register name should be rejected'
);

-- ============================================================
-- TEST: Invalid register name rejected
-- ============================================================
SELECT throws_ok(
    $$SELECT accum.register_create(
        name       := '123_bad',
        dimensions := '{"x": "int"}',
        resources  := '{"y": "numeric"}'
    )$$,
    NULL,
    NULL,
    'Name starting with digit should be rejected'
);

SELECT throws_ok(
    $$SELECT accum.register_create(
        name       := 'has spaces',
        dimensions := '{"x": "int"}',
        resources  := '{"y": "numeric"}'
    )$$,
    NULL,
    NULL,
    'Name with spaces should be rejected'
);

-- ============================================================
-- TEST: Empty dimensions rejected
-- ============================================================
SELECT throws_ok(
    $$SELECT accum.register_create(
        name       := 'empty_dim',
        dimensions := '{}',
        resources  := '{"y": "numeric"}'
    )$$,
    NULL,
    NULL,
    'Empty dimensions should be rejected'
);

-- ============================================================
-- TEST: Empty resources rejected
-- ============================================================
SELECT throws_ok(
    $$SELECT accum.register_create(
        name       := 'empty_res',
        dimensions := '{"x": "int"}',
        resources  := '{}'
    )$$,
    NULL,
    NULL,
    'Empty resources should be rejected'
);

-- ============================================================
-- TEST: Create a turnover register (no balance_cache)
-- ============================================================
SELECT lives_ok(
    $$SELECT accum.register_create(
        name       := 'sales_x',
        dimensions := '{"product": "int"}',
        resources  := '{"sold": "numeric"}',
        kind       := 'turnover'
    )$$,
    'Turnover register should be created'
);

SELECT hasnt_table('accum', 'sales_x_balance_cache',
    'Turnover register should NOT have balance_cache');

SELECT has_table('accum', 'sales_x_movements', 'Turnover should have movements');
SELECT has_table('accum', 'sales_x_totals_month', 'Turnover should have totals_month');

-- ============================================================
-- Cleanup
-- ============================================================
SELECT accum.register_drop('inventory', force := true);
SELECT accum.register_drop('sales_x', force := true);

SELECT * FROM finish();
ROLLBACK;
