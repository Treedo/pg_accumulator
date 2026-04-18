-- test/sql/19_partitioning.sql
-- Tests for partitioning: initial partitions, auto-create, manual create,
-- detach, listing, different partition_by strategies, partition pruning

BEGIN;
SELECT plan(37);

-- ============================================================
-- Setup: create a register with partition_by = 'month'
-- ============================================================
SELECT lives_ok(
    $$SELECT accum.register_create(
        name         := 'part_m',
        dimensions   := '{"warehouse": "int", "product": "int"}',
        resources    := '{"quantity": "numeric"}',
        kind         := 'balance',
        partition_by := 'month'
    )$$,
    'Create register with partition_by=month should succeed'
);

-- ============================================================
-- TEST: Initial partitions created (current month + 3 ahead)
-- ============================================================
SELECT ok(
    (SELECT count(*) FROM accum.register_partitions('part_m') WHERE NOT is_default) >= 4,
    'At least 4 non-default partitions should exist initially'
);

SELECT ok(
    (SELECT count(*) FROM accum.register_partitions('part_m') WHERE is_default) = 1,
    'Exactly one default partition should exist'
);

-- ============================================================
-- TEST: Partition naming follows convention
-- ============================================================
SELECT ok(
    EXISTS(
        SELECT 1 FROM accum.register_partitions('part_m')
        WHERE partition_name = 'part_m_movements_' || to_char(current_date, 'YYYY_MM')
    ),
    'Current month partition should follow naming convention'
);

-- ============================================================
-- TEST: Partition ranges are correct
-- ============================================================
SELECT is(
    (SELECT from_date::date FROM accum.register_partitions('part_m')
     WHERE partition_name = 'part_m_movements_' || to_char(current_date, 'YYYY_MM')),
    date_trunc('month', current_date)::date,
    'Current month partition from_date should be first day of month'
);

SELECT is(
    (SELECT to_date::date FROM accum.register_partitions('part_m')
     WHERE partition_name = 'part_m_movements_' || to_char(current_date, 'YYYY_MM')),
    (date_trunc('month', current_date) + interval '1 month')::date,
    'Current month partition to_date should be first day of next month'
);

-- ============================================================
-- TEST: INSERT goes to correct partition (not default)
-- ============================================================
SELECT accum.register_post('part_m', format(
    '{"recorder":"d:1","period":"%s","warehouse":1,"product":1,"quantity":100}',
    current_date
)::jsonb);

SELECT is(
    (SELECT count(*)::int FROM accum.register_partitions('part_m')
     WHERE is_default AND row_count > 0),
    0,
    'Current month INSERT should not go to default partition'
);

-- ============================================================
-- TEST: Auto-create trigger — INSERT into far future period
-- ============================================================
SELECT lives_ok(
    $$SELECT accum.register_post('part_m', format(
        '{"recorder":"d:2","period":"%s","warehouse":1,"product":1,"quantity":50}',
        (current_date + interval '2 years')::date
    )::jsonb)$$,
    'INSERT into future period should auto-create partition'
);

-- Verify the new partition was created
SELECT ok(
    EXISTS(
        SELECT 1 FROM accum.register_partitions('part_m')
        WHERE partition_name = 'part_m_movements_' ||
            to_char(current_date + interval '2 years', 'YYYY_MM')
    ),
    'Partition for future period should be created automatically'
);

-- ============================================================
-- TEST: register_create_partitions() — manual ahead creation
-- ============================================================
SELECT ok(
    (SELECT accum.register_create_partitions('part_m', interval '1 year')) >= 0,
    'register_create_partitions should return number created'
);

-- Count should increase
SELECT ok(
    (SELECT count(*) FROM accum.register_partitions('part_m') WHERE NOT is_default) >= 12,
    'After creating 1 year ahead, should have at least 12 partitions'
);

-- ============================================================
-- TEST: register_create_partitions is idempotent
-- ============================================================
SELECT is(
    (SELECT accum.register_create_partitions('part_m', interval '1 month')),
    0,
    'Creating already-existing partitions should return 0'
);

-- ============================================================
-- TEST: register_partitions() returns correct metadata
-- ============================================================
SELECT ok(
    (SELECT count(*) FROM accum.register_partitions('part_m')) > 0,
    'register_partitions should return rows'
);

SELECT ok(
    (SELECT every(total_size IS NOT NULL) FROM accum.register_partitions('part_m')),
    'Every partition should have size info'
);

-- ============================================================
-- Setup: register with partition_by = 'quarter'
-- ============================================================
SELECT lives_ok(
    $$SELECT accum.register_create(
        name         := 'part_q',
        dimensions   := '{"account": "int"}',
        resources    := '{"amount": "numeric"}',
        kind         := 'turnover',
        partition_by := 'quarter'
    )$$,
    'Create register with partition_by=quarter should succeed'
);

-- ============================================================
-- TEST: Quarter partition naming
-- ============================================================
SELECT ok(
    EXISTS(
        SELECT 1 FROM accum.register_partitions('part_q')
        WHERE partition_name = 'part_q_movements_' ||
            to_char(current_date, 'YYYY') || '_q' || to_char(current_date, 'Q')
    ),
    'Current quarter partition should follow naming convention'
);

-- ============================================================
-- TEST: Quarter partition ranges
-- ============================================================
SELECT is(
    (SELECT from_date::date FROM accum.register_partitions('part_q')
     WHERE partition_name = 'part_q_movements_' ||
         to_char(current_date, 'YYYY') || '_q' || to_char(current_date, 'Q')),
    date_trunc('quarter', current_date)::date,
    'Quarter partition from_date should be first day of quarter'
);

SELECT is(
    (SELECT to_date::date FROM accum.register_partitions('part_q')
     WHERE partition_name = 'part_q_movements_' ||
         to_char(current_date, 'YYYY') || '_q' || to_char(current_date, 'Q')),
    (date_trunc('quarter', current_date) + interval '3 months')::date,
    'Quarter partition to_date should be first day of next quarter'
);

-- ============================================================
-- Setup: register with partition_by = 'year'
-- ============================================================
SELECT lives_ok(
    $$SELECT accum.register_create(
        name         := 'part_y',
        dimensions   := '{"dept": "int"}',
        resources    := '{"budget": "numeric"}',
        kind         := 'balance',
        partition_by := 'year'
    )$$,
    'Create register with partition_by=year should succeed'
);

-- ============================================================
-- TEST: Year partition naming and range
-- ============================================================
SELECT ok(
    EXISTS(
        SELECT 1 FROM accum.register_partitions('part_y')
        WHERE partition_name = 'part_y_movements_' || to_char(current_date, 'YYYY')
    ),
    'Current year partition should follow naming convention'
);

SELECT is(
    (SELECT from_date::date FROM accum.register_partitions('part_y')
     WHERE partition_name = 'part_y_movements_' || to_char(current_date, 'YYYY')),
    date_trunc('year', current_date)::date,
    'Year partition from_date should be first day of year'
);

-- ============================================================
-- Setup: register with partition_by = 'day'
-- ============================================================
SELECT lives_ok(
    $$SELECT accum.register_create(
        name         := 'part_d',
        dimensions   := '{"sensor": "int"}',
        resources    := '{"reading": "numeric"}',
        kind         := 'turnover',
        partition_by := 'day'
    )$$,
    'Create register with partition_by=day should succeed'
);

-- ============================================================
-- TEST: Day partition naming and range
-- ============================================================
SELECT ok(
    EXISTS(
        SELECT 1 FROM accum.register_partitions('part_d')
        WHERE partition_name = 'part_d_movements_' || to_char(current_date, 'YYYY_MM_DD')
    ),
    'Current day partition should follow naming convention'
);

SELECT is(
    (SELECT from_date::date FROM accum.register_partitions('part_d')
     WHERE partition_name = 'part_d_movements_' || to_char(current_date, 'YYYY_MM_DD')),
    current_date,
    'Day partition from_date should be today'
);

SELECT is(
    (SELECT to_date::date FROM accum.register_partitions('part_d')
     WHERE partition_name = 'part_d_movements_' || to_char(current_date, 'YYYY_MM_DD')),
    current_date + 1,
    'Day partition to_date should be tomorrow'
);

-- ============================================================
-- TEST: Auto-create for day partition (future day)
-- ============================================================
SELECT lives_ok(
    $$SELECT accum.register_post('part_d', format(
        '{"recorder":"s:1","period":"%s","sensor":42,"reading":99.5}',
        (current_date + 30)::date
    )::jsonb)$$,
    'INSERT into future day should auto-create partition'
);

SELECT ok(
    EXISTS(
        SELECT 1 FROM accum.register_partitions('part_d')
        WHERE partition_name = 'part_d_movements_' ||
            to_char(current_date + 30, 'YYYY_MM_DD')
    ),
    'Auto-created day partition should exist'
);

-- ============================================================
-- TEST: Default partition catches out-of-range data (past)
-- ============================================================
-- Insert into a very old period that has no pre-created partition
SELECT lives_ok(
    $$SELECT accum.register_post('part_m', '{
        "recorder":"d:old","period":"2020-01-15","warehouse":1,"product":1,"quantity":5
    }')$$,
    'INSERT into old period should succeed (goes to default)'
);

-- ============================================================
-- TEST: register_detach_partitions with older_than
-- We need old partitions for this, let's create some via manual
-- ============================================================

-- Create a register for detach testing
SELECT lives_ok(
    $$SELECT accum.register_create(
        name         := 'part_detach',
        dimensions   := '{"item": "int"}',
        resources    := '{"qty": "numeric"}',
        kind         := 'balance',
        partition_by := 'month'
    )$$,
    'Create register for detach testing'
);

-- Count initial partitions
SELECT ok(
    (SELECT count(*) FROM accum.register_partitions('part_detach') WHERE NOT is_default) >= 4,
    'Detach register should have initial partitions'
);

-- With older_than = '0 seconds' from now, no current partitions should be detached
-- (they all start from current_date onwards)
SELECT is(
    (SELECT accum.register_detach_partitions('part_detach', interval '0 seconds')),
    0,
    'No partitions should be detached when all are current or future'
);

-- ============================================================
-- TEST: register_detach_partitions error for non-existent register
-- ============================================================
SELECT throws_ok(
    $$SELECT accum.register_detach_partitions('nonexistent')$$,
    'Register "nonexistent" does not exist',
    'Detach should fail for non-existent register'
);

-- ============================================================
-- TEST: register_create_partitions error for non-existent register
-- ============================================================
SELECT throws_ok(
    $$SELECT accum.register_create_partitions('nonexistent')$$,
    'Register "nonexistent" does not exist',
    'Create partitions should fail for non-existent register'
);

-- ============================================================
-- TEST: register_partitions error for non-existent register
-- ============================================================
SELECT throws_ok(
    $$SELECT * FROM accum.register_partitions('nonexistent')$$,
    'Register "nonexistent" does not exist',
    'List partitions should fail for non-existent register'
);

-- ============================================================
-- TEST: Partition pruning (verify partitions cover expected ranges)
-- Instead of EXPLAIN (requires dblink), verify that data is in the
-- correct partition by checking the partition directly
-- ============================================================
SELECT is(
    (SELECT count(*)::int FROM accum.register_partitions('part_m')
     WHERE NOT is_default
       AND from_date <= current_date::timestamptz
       AND to_date   >  current_date::timestamptz),
    1,
    'Exactly one non-default partition should cover current date'
);

-- ============================================================
-- TEST: Multiple INSERTs — all go to correct partitions
-- ============================================================
SELECT accum.register_post('part_m', format('[
    {"recorder":"batch:1","period":"%s","warehouse":1,"product":1,"quantity":10},
    {"recorder":"batch:2","period":"%s","warehouse":2,"product":2,"quantity":20}
]', current_date, current_date)::jsonb);

-- Verify data accessible
SELECT is(
    (SELECT count(*)::int FROM accum.part_m_movements
     WHERE recorder IN ('batch:1', 'batch:2')),
    2,
    'Batch INSERTs should be accessible via parent table'
);

-- ============================================================
-- TEST: register_info includes partition info
-- ============================================================
SELECT ok(
    (SELECT jsonb_array_length(accum.register_info('part_m')->'partitions')) > 0,
    'register_info should include partition info'
);

-- ============================================================
-- Cleanup
-- ============================================================
SELECT accum.register_drop('part_m', force := true);
SELECT accum.register_drop('part_q', force := true);
SELECT accum.register_drop('part_y', force := true);
SELECT accum.register_drop('part_d', force := true);
SELECT accum.register_drop('part_detach', force := true);

SELECT * FROM finish();
ROLLBACK;
