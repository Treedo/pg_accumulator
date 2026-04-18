-- test/sql/01_core_registry.sql
-- Tests for core module: schema, registry, validation

BEGIN;
SELECT plan(60);

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

-- ============================================================
-- TEST: _validate_name
-- ============================================================
SELECT lives_ok(
    $$SELECT accum._validate_name('warehouse')$$,
    '_validate_name accepts valid name'
);

SELECT lives_ok(
    $$SELECT accum._validate_name('stock_v2')$$,
    '_validate_name accepts name with digits and underscores'
);

SELECT throws_ok(
    $$SELECT accum._validate_name('')$$,
    NULL, NULL,
    '_validate_name rejects empty string'
);

SELECT throws_ok(
    $$SELECT accum._validate_name(NULL)$$,
    NULL, NULL,
    '_validate_name rejects NULL'
);

SELECT throws_ok(
    $$SELECT accum._validate_name('2bad')$$,
    NULL, NULL,
    '_validate_name rejects name starting with digit'
);

SELECT throws_ok(
    $$SELECT accum._validate_name('Bad_Name')$$,
    NULL, NULL,
    '_validate_name rejects uppercase letters'
);

SELECT throws_ok(
    $$SELECT accum._validate_name('has-dash')$$,
    NULL, NULL,
    '_validate_name rejects dashes'
);

SELECT throws_ok(
    $$SELECT accum._validate_name(repeat('a', 49))$$,
    NULL, NULL,
    '_validate_name rejects names over 48 chars'
);

-- ============================================================
-- TEST: _validate_dimensions
-- ============================================================
SELECT lives_ok(
    $$SELECT accum._validate_dimensions('{"warehouse":"int"}'::jsonb)$$,
    '_validate_dimensions accepts valid dimensions'
);

SELECT throws_ok(
    $$SELECT accum._validate_dimensions('{}'::jsonb)$$,
    NULL, NULL,
    '_validate_dimensions rejects empty object'
);

SELECT throws_ok(
    $$SELECT accum._validate_dimensions(NULL)$$,
    NULL, NULL,
    '_validate_dimensions rejects NULL'
);

SELECT throws_ok(
    $$SELECT accum._validate_dimensions('{"Bad":"int"}'::jsonb)$$,
    NULL, NULL,
    '_validate_dimensions rejects uppercase dimension names'
);

-- ============================================================
-- TEST: _validate_resources
-- ============================================================
SELECT lives_ok(
    $$SELECT accum._validate_resources('{"quantity":"numeric"}'::jsonb)$$,
    '_validate_resources accepts valid resources'
);

SELECT throws_ok(
    $$SELECT accum._validate_resources('{}'::jsonb)$$,
    NULL, NULL,
    '_validate_resources rejects empty object'
);

-- ============================================================
-- TEST: _register_exists
-- ============================================================
SELECT is(
    accum._register_exists('nonexistent'),
    false,
    '_register_exists returns false for missing register'
);

-- ============================================================
-- TEST: _register_put + _register_exists + _register_get
-- ============================================================
SELECT lives_ok(
    $$SELECT accum._register_put('test_crud', 'balance', '{"wh":"int"}', '{"qty":"numeric"}')$$,
    '_register_put inserts a new register'
);

SELECT is(
    accum._register_exists('test_crud'),
    true,
    '_register_exists returns true after _register_put'
);

SELECT is(
    (accum._register_get('test_crud')).kind,
    'balance',
    '_register_get returns correct kind'
);

SELECT is(
    (accum._register_get('test_crud')).dimensions,
    '{"wh":"int"}'::jsonb,
    '_register_get returns correct dimensions'
);

-- ============================================================
-- TEST: _register_put upsert (update existing)
-- ============================================================
SELECT lives_ok(
    $$SELECT accum._register_put('test_crud', 'turnover', '{"acc":"int"}', '{"amount":"numeric"}')$$,
    '_register_put upserts existing register'
);

SELECT is(
    (accum._register_get('test_crud')).kind,
    'turnover',
    '_register_get reflects updated kind after upsert'
);

-- ============================================================
-- TEST: _register_list
-- ============================================================
SELECT accum._register_put('test_list_a', 'balance', '{"a":"int"}', '{"x":"numeric"}');
SELECT accum._register_put('test_list_b', 'turnover', '{"b":"int"}', '{"y":"numeric"}');

SELECT is(
    (SELECT count(*)::int FROM accum._register_list()
     WHERE name IN ('test_list_a', 'test_list_b')),
    2,
    '_register_list returns all inserted registers'
);

-- ============================================================
-- TEST: _register_delete
-- ============================================================
SELECT is(
    accum._register_delete('test_crud'),
    true,
    '_register_delete returns true for existing register'
);

SELECT is(
    accum._register_exists('test_crud'),
    false,
    '_register_exists returns false after _register_delete'
);

SELECT is(
    accum._register_delete('nonexistent'),
    false,
    '_register_delete returns false for missing register'
);

-- Cleanup
SELECT accum._register_delete('test_list_a');
SELECT accum._register_delete('test_list_b');

-- ============================================================
-- TEST: _generate_hash_function creates a callable function
-- ============================================================
SELECT lives_ok(
    $$SELECT accum._generate_hash_function('test_hash', '{"warehouse":"int","product":"int"}')$$,
    '_generate_hash_function should succeed with valid inputs'
);

SELECT has_function(
    'accum', '_hash_test_hash',
    '_hash_test_hash function should exist after generation'
);

-- ============================================================
-- TEST: Generated hash function returns bigint
-- ============================================================
SELECT is(
    pg_typeof(accum._hash_test_hash(1, 2))::text,
    'bigint',
    'Hash function should return bigint'
);

-- ============================================================
-- TEST: Determinism — same inputs produce same hash
-- ============================================================
SELECT is(
    accum._hash_test_hash(1, 100),
    accum._hash_test_hash(1, 100),
    'Same inputs should produce identical hash'
);

-- ============================================================
-- TEST: Different inputs produce different hashes
-- ============================================================
SELECT isnt(
    accum._hash_test_hash(1, 100),
    accum._hash_test_hash(1, 200),
    'Different inputs should produce different hashes'
);

SELECT isnt(
    accum._hash_test_hash(1, 100),
    accum._hash_test_hash(2, 100),
    'Different first dimension should produce different hash'
);

-- ============================================================
-- TEST: NULL handling — NULL is a distinct value
-- ============================================================
SELECT isnt(
    accum._hash_test_hash(1, NULL),
    accum._hash_test_hash(1, 0),
    'NULL should hash differently from 0'
);

SELECT is(
    accum._hash_test_hash(NULL, NULL),
    accum._hash_test_hash(NULL, NULL),
    'NULL + NULL should be deterministic'
);

-- ============================================================
-- TEST: Single-dimension hash function
-- ============================================================
SELECT lives_ok(
    $$SELECT accum._generate_hash_function('test_single', '{"account":"text"}')$$,
    'Single-dimension hash function should be created'
);

SELECT is(
    accum._hash_test_single('abc'),
    accum._hash_test_single('abc'),
    'Single-dim text hash should be deterministic'
);

SELECT isnt(
    accum._hash_test_single('abc'),
    accum._hash_test_single('xyz'),
    'Single-dim different text should produce different hash'
);

-- ============================================================
-- TEST: _generate_hash_function validates inputs
-- ============================================================
SELECT throws_ok(
    $$SELECT accum._generate_hash_function('', '{"a":"int"}')$$,
    NULL, NULL,
    '_generate_hash_function rejects empty name'
);

SELECT throws_ok(
    $$SELECT accum._generate_hash_function('ok', '{}')$$,
    NULL, NULL,
    '_generate_hash_function rejects empty dimensions'
);

-- ============================================================
-- TEST: _drop_hash_function removes the function
-- ============================================================
SELECT lives_ok(
    $$SELECT accum._drop_hash_function('test_hash')$$,
    '_drop_hash_function should succeed'
);

SELECT hasnt_function(
    'accum', '_hash_test_hash',
    '_hash_test_hash should not exist after drop'
);

-- Also clean up test_single
SELECT accum._drop_hash_function('test_single');

SELECT * FROM finish();
ROLLBACK;
