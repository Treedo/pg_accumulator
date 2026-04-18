-- sql/09_partitioning.sql
-- Partition management functions
-- Provides: partition period calculation, initial creation, ahead creation,
--           detach, listing, and auto-create trigger

-- ============================================================
-- HELPER: Compute partition suffix from a date and partition_by strategy
-- Returns text like '2026_04', '2026_q2', '2026_04_18', '2026'
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@._partition_suffix(
    p_date         date,
    p_partition_by text
) RETURNS text
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    CASE p_partition_by
        WHEN 'day' THEN
            RETURN to_char(p_date, 'YYYY_MM_DD');
        WHEN 'month' THEN
            RETURN to_char(p_date, 'YYYY_MM');
        WHEN 'quarter' THEN
            RETURN to_char(p_date, 'YYYY') || '_q' || to_char(p_date, 'Q');
        WHEN 'year' THEN
            RETURN to_char(p_date, 'YYYY');
        ELSE
            RAISE EXCEPTION 'Invalid partition_by: %', p_partition_by;
    END CASE;
END;
$$;

-- ============================================================
-- HELPER: Compute partition range start (inclusive) for a date
-- Returns the first day of the period containing p_date
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@._partition_range_start(
    p_date         date,
    p_partition_by text
) RETURNS date
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    CASE p_partition_by
        WHEN 'day' THEN
            RETURN p_date;
        WHEN 'month' THEN
            RETURN date_trunc('month', p_date)::date;
        WHEN 'quarter' THEN
            RETURN date_trunc('quarter', p_date)::date;
        WHEN 'year' THEN
            RETURN date_trunc('year', p_date)::date;
        ELSE
            RAISE EXCEPTION 'Invalid partition_by: %', p_partition_by;
    END CASE;
END;
$$;

-- ============================================================
-- HELPER: Compute partition range end (exclusive) — next period start
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@._partition_range_end(
    p_range_start  date,
    p_partition_by text
) RETURNS date
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    CASE p_partition_by
        WHEN 'day' THEN
            RETURN p_range_start + interval '1 day';
        WHEN 'month' THEN
            RETURN (p_range_start + interval '1 month')::date;
        WHEN 'quarter' THEN
            RETURN (p_range_start + interval '3 months')::date;
        WHEN 'year' THEN
            RETURN (p_range_start + interval '1 year')::date;
        ELSE
            RAISE EXCEPTION 'Invalid partition_by: %', p_partition_by;
    END CASE;
END;
$$;

-- ============================================================
-- HELPER: Check if a partition already exists for a given range
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@._partition_exists(
    p_parent_table text,
    p_suffix       text
) RETURNS boolean
LANGUAGE plpgsql STABLE AS $$
BEGIN
    RETURN EXISTS (
        SELECT 1
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = '@extschema@'
          AND c.relname = p_parent_table || '_' || p_suffix
    );
END;
$$;

-- ============================================================
-- CREATE A SINGLE PARTITION for a given period
-- Uses advisory lock to prevent concurrent duplicate creation
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@._create_partition(
    p_name         text,
    p_partition_by text,
    p_range_start  date
) RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE
    v_parent     text := p_name || '_movements';
    v_suffix     text;
    v_part_name  text;
    v_range_end  date;
    v_lock_key   bigint;
BEGIN
    v_suffix    := @extschema@._partition_suffix(p_range_start, p_partition_by);
    v_part_name := v_parent || '_' || v_suffix;
    v_range_end := @extschema@._partition_range_end(p_range_start, p_partition_by);

    -- Advisory lock based on hash of partition name to prevent concurrent creation
    v_lock_key := hashtext(v_part_name);
    PERFORM pg_advisory_xact_lock(v_lock_key);

    -- Re-check after acquiring lock
    IF @extschema@._partition_exists(v_parent, v_suffix) THEN
        RETURN false;
    END IF;

    EXECUTE format(
        'CREATE TABLE @extschema@.%I PARTITION OF @extschema@.%I
         FOR VALUES FROM (%L) TO (%L)',
        v_part_name,
        v_parent,
        p_range_start::timestamptz,
        v_range_end::timestamptz
    );

    RETURN true;
END;
$$;

-- ============================================================
-- CREATE INITIAL PARTITIONS when a register is created
-- Creates partition for current period + partitions_ahead periods
-- Called automatically by register_create()
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@._create_initial_partitions(
    p_name         text,
    p_partition_by text,
    p_ahead        int DEFAULT 3
) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    v_current    date;
    v_count      int := 0;
    v_created    boolean;
    i            int;
BEGIN
    v_current := @extschema@._partition_range_start(current_date, p_partition_by);

    FOR i IN 0..p_ahead
    LOOP
        v_created := @extschema@._create_partition(p_name, p_partition_by, v_current);
        IF v_created THEN
            v_count := v_count + 1;
        END IF;

        -- Advance to next period
        CASE p_partition_by
            WHEN 'day' THEN
                v_current := v_current + interval '1 day';
            WHEN 'month' THEN
                v_current := (v_current + interval '1 month')::date;
            WHEN 'quarter' THEN
                v_current := (v_current + interval '3 months')::date;
            WHEN 'year' THEN
                v_current := (v_current + interval '1 year')::date;
        END CASE;
    END LOOP;

    RETURN v_count;
END;
$$;

-- ============================================================
-- REGISTER_CREATE_PARTITIONS: Manually create partitions ahead
-- Public API function
--
-- Parameters:
--   p_name  — register name
--   p_ahead — interval specifying how far ahead to create partitions
--
-- Returns: number of partitions created
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@.register_create_partitions(
    p_name  text,
    p_ahead interval DEFAULT interval '6 months'
) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    reg          record;
    v_current    date;
    v_end        date;
    v_count      int := 0;
    v_created    boolean;
BEGIN
    SELECT * INTO reg FROM @extschema@._registers r WHERE r.name = p_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_name;
    END IF;

    v_current := @extschema@._partition_range_start(current_date, reg.partition_by);
    v_end     := (current_date + p_ahead)::date;

    WHILE v_current <= v_end
    LOOP
        v_created := @extschema@._create_partition(p_name, reg.partition_by, v_current);
        IF v_created THEN
            v_count := v_count + 1;
        END IF;

        CASE reg.partition_by
            WHEN 'day' THEN
                v_current := v_current + interval '1 day';
            WHEN 'month' THEN
                v_current := (v_current + interval '1 month')::date;
            WHEN 'quarter' THEN
                v_current := (v_current + interval '3 months')::date;
            WHEN 'year' THEN
                v_current := (v_current + interval '1 year')::date;
        END CASE;
    END LOOP;

    RETURN v_count;
END;
$$;

-- ============================================================
-- REGISTER_DETACH_PARTITIONS: Detach old partitions (for archiving)
-- Does NOT drop the detached tables — they can be moved or dropped separately.
--
-- Parameters:
--   p_name       — register name
--   p_older_than — detach partitions whose range_end <= (now() - p_older_than)
--
-- Returns: number of partitions detached
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@.register_detach_partitions(
    p_name       text,
    p_older_than interval DEFAULT interval '2 years'
) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    reg         record;
    v_cutoff    timestamptz;
    v_count     int := 0;
    part_rec    record;
    v_range_end text;
BEGIN
    SELECT * INTO reg FROM @extschema@._registers r WHERE r.name = p_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_name;
    END IF;

    v_cutoff := now() - p_older_than;

    -- Iterate over child partitions (exclude default)
    FOR part_rec IN
        SELECT child.relname AS part_name,
               pg_get_expr(c.relpartbound, c.oid) AS bound_expr
        FROM pg_inherits i
        JOIN pg_class parent ON i.inhparent = parent.oid
        JOIN pg_class child  ON i.inhrelid  = child.oid
        JOIN pg_catalog.pg_class c ON c.oid = child.oid
        JOIN pg_namespace ns ON parent.relnamespace = ns.oid
        WHERE parent.relname = p_name || '_movements'
          AND ns.nspname = '@extschema@'
          AND child.relname != p_name || '_movements_default'
        ORDER BY child.relname
    LOOP
        -- Extract upper bound from expression like:
        -- "FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')"
        v_range_end := substring(part_rec.bound_expr FROM 'TO \(''([^'']+)''\)');

        IF v_range_end IS NOT NULL AND v_range_end::timestamptz <= v_cutoff THEN
            EXECUTE format(
                'ALTER TABLE @extschema@.%I DETACH PARTITION @extschema@.%I',
                p_name || '_movements',
                part_rec.part_name
            );
            v_count := v_count + 1;
        END IF;
    END LOOP;

    RETURN v_count;
END;
$$;

-- ============================================================
-- REGISTER_PARTITIONS: List partitions with metadata
-- Returns partitions info (name, range, row count, size) as a table
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@.register_partitions(p_name text)
RETURNS TABLE(
    partition_name text,
    from_date      timestamptz,
    to_date        timestamptz,
    row_count      bigint,
    total_size     text,
    is_default     boolean
)
LANGUAGE plpgsql STABLE AS $$
DECLARE
    reg      record;
    part_rec record;
    v_from   text;
    v_to     text;
    v_cnt    bigint;
    v_sz     text;
BEGIN
    SELECT * INTO reg FROM @extschema@._registers r WHERE r.name = p_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_name;
    END IF;

    FOR part_rec IN
        SELECT child.relname AS part_name,
               pg_get_expr(c.relpartbound, c.oid) AS bound_expr,
               child.oid AS child_oid
        FROM pg_inherits i
        JOIN pg_class parent ON i.inhparent = parent.oid
        JOIN pg_class child  ON i.inhrelid  = child.oid
        JOIN pg_catalog.pg_class c ON c.oid = child.oid
        JOIN pg_namespace ns ON parent.relnamespace = ns.oid
        WHERE parent.relname = p_name || '_movements'
          AND ns.nspname = '@extschema@'
        ORDER BY child.relname
    LOOP
        -- Row count (live tuples estimate for performance)
        SELECT COALESCE(s.n_live_tup, 0) INTO v_cnt
        FROM pg_stat_user_tables s
        WHERE s.relid = part_rec.child_oid;
        IF v_cnt IS NULL THEN v_cnt := 0; END IF;

        -- Size
        v_sz := pg_size_pretty(pg_total_relation_size(part_rec.child_oid));

        partition_name := part_rec.part_name;
        total_size     := v_sz;
        row_count      := v_cnt;

        IF part_rec.part_name = p_name || '_movements_default' THEN
            is_default := true;
            from_date  := NULL;
            to_date    := NULL;
        ELSE
            is_default := false;
            v_from := substring(part_rec.bound_expr FROM 'FROM \(''([^'']+)''\)');
            v_to   := substring(part_rec.bound_expr FROM 'TO \(''([^'']+)''\)');
            from_date := v_from::timestamptz;
            to_date   := v_to::timestamptz;
        END IF;

        RETURN NEXT;
    END LOOP;
END;
$$;

-- ============================================================
-- ENSURE PARTITION EXISTS: Called before INSERT to auto-create
-- partitions for any period not yet covered.
-- Must be called OUTSIDE the INSERT statement (e.g. from register_post)
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@._ensure_partition(
    p_name   text,
    p_period timestamptz
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_reg          record;
    v_range_start  date;
    v_suffix       text;
    v_parent       text;
BEGIN
    SELECT * INTO v_reg
    FROM @extschema@._registers r
    WHERE r.name = p_name;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    v_range_start := @extschema@._partition_range_start(p_period::date, v_reg.partition_by);
    v_suffix      := @extschema@._partition_suffix(v_range_start, v_reg.partition_by);
    v_parent      := p_name || '_movements';

    IF NOT @extschema@._partition_exists(v_parent, v_suffix) THEN
        PERFORM @extschema@._create_partition(p_name, v_reg.partition_by, v_range_start);
    END IF;
END;
$$;
