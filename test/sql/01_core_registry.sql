-- test/sql/01_core_registry.sql
-- Tests for core module: schema, registry, validation

BEGIN;
SELECT plan(20);

-- ============================================================
-- TEST: Schema exists
-- ============================================================
SELECT has_schema('accum', 'Schema accum should exist');

-- ============================================================
-- TEST: Registry table exists with correct columns
-- ============================================================
SELECT has_table('accum', '_registers', 'Registry table should exist');
SELECT has_column('accum', '_registers', 'name', 'Registry should have name column');
SELECT has_column('accum', '_registers', 'kind', 'Registry should have kind column');
SELECT has_column('accum', '_registers', 'dimensions', 'Registry should have dimensions column');
SELECT has_column('accum', '_registers', 'resources', 'Registry should have resources column');
SELECT has_column('accum', '_registers', 'totals_period', 'Registry should have totals_period');
SELECT has_column('accum', '_registers', 'partition_by', 'Registry should have partition_by');
SELECT has_column('accum', '_registers', 'high_write', 'Registry should have high_write');
SELECT has_column('accum', '_registers', 'recorder_type', 'Registry should have recorder_type');
SELECT has_column('accum', '_registers', 'created_at', 'Registry should have created_at');

-- ============================================================
-- TEST: Registry table PK
-- ============================================================
SELECT col_is_pk('accum', '_registers', 'name', 'name should be PK');

-- ============================================================
-- TEST: Kind check constraint
-- ============================================================
SELECT throws_ok(
    $$INSERT INTO accum._registers (name, kind, dimensions, resources)
      VALUES ('test_bad_kind', 'invalid', '{"d":"int"}', '{"r":"numeric"}')$$,
    '23514',  -- check_violation
    NULL,
    'Invalid kind should be rejected by CHECK constraint'
);

-- ============================================================
-- TEST: totals_period check constraint
-- ============================================================
SELECT throws_ok(
    $$INSERT INTO accum._registers (name, kind, dimensions, resources, totals_period)
      VALUES ('test_bad_tp', 'balance', '{"d":"int"}', '{"r":"numeric"}', 'week')$$,
    '23514',
    NULL,
    'Invalid totals_period should be rejected'
);

-- ============================================================
-- TEST: partition_by check constraint
-- ============================================================
SELECT throws_ok(
    $$INSERT INTO accum._registers (name, kind, dimensions, resources, partition_by)
      VALUES ('test_bad_pb', 'balance', '{"d":"int"}', '{"r":"numeric"}', 'week')$$,
    '23514',
    NULL,
    'Invalid partition_by should be rejected'
);

-- ============================================================
-- TEST: Registry is initially empty
-- ============================================================
SELECT is_empty(
    'SELECT * FROM accum._registers',
    'Registry should be empty initially'
);

-- ============================================================
-- TEST: Valid record can be inserted
-- ============================================================
SELECT lives_ok(
    $$INSERT INTO accum._registers (name, kind, dimensions, resources)
      VALUES ('test_valid', 'balance', '{"warehouse":"int"}', '{"quantity":"numeric"}')$$,
    'Valid register record should be insertable'
);

SELECT is(
    (SELECT count(*)::int FROM accum._registers WHERE name = 'test_valid'),
    1,
    'Inserted record should be found'
);

-- ============================================================
-- TEST: Duplicate name rejected
-- ============================================================
SELECT throws_ok(
    $$INSERT INTO accum._registers (name, kind, dimensions, resources)
      VALUES ('test_valid', 'balance', '{"x":"int"}', '{"y":"numeric"}')$$,
    '23505',  -- unique_violation
    NULL,
    'Duplicate name should be rejected'
);

-- Cleanup
DELETE FROM accum._registers WHERE name = 'test_valid';

-- ============================================================
-- TEST: Default values
-- ============================================================
INSERT INTO accum._registers (name, kind, dimensions, resources)
VALUES ('test_defaults', 'balance', '{"d":"int"}', '{"r":"numeric"}');

SELECT is(
    (SELECT totals_period FROM accum._registers WHERE name = 'test_defaults'),
    'day',
    'Default totals_period should be day'
);

DELETE FROM accum._registers WHERE name = 'test_defaults';

SELECT * FROM finish();
ROLLBACK;
