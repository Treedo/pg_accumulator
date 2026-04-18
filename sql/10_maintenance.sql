-- sql/10_maintenance.sql
-- Maintenance and diagnostic functions
-- Provides: register_verify, register_rebuild_totals, register_rebuild_cache, register_stats

-- ============================================================
-- REGISTER_VERIFY: Verify data consistency of a register
-- Compares balance_cache with actual SUM of movements,
-- and checks totals_month / totals_year against movements.
--
-- Returns SETOF (check_type, dim_hash, expected, actual, status)
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@.register_verify(p_name text)
RETURNS TABLE(
    check_type text,
    dim_hash   bigint,
    period     date,
    expected   jsonb,
    actual     jsonb,
    status     text
)
LANGUAGE plpgsql STABLE AS $$
DECLARE
    reg          record;
    res_key      text;
    res_cols     text := '';
    res_sum_cols text := '';
    res_case     text := '';
    first_res    boolean := true;
    dim_cols     text := '';
    first_dim    boolean := true;
    dim_key      text;
BEGIN
    -- 1. Get register metadata
    SELECT * INTO reg FROM @extschema@._registers r WHERE r.name = p_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_name;
    END IF;

    -- 2. Build column expressions
    FOR dim_key IN SELECT key FROM jsonb_each_text(reg.dimensions) ORDER BY key
    LOOP
        IF NOT first_dim THEN dim_cols := dim_cols || ', '; END IF;
        dim_cols := dim_cols || format('%I', dim_key);
        first_dim := false;
    END LOOP;

    FOR res_key IN SELECT key FROM jsonb_each_text(reg.resources) ORDER BY key
    LOOP
        IF NOT first_res THEN
            res_cols     := res_cols     || ', ';
            res_sum_cols := res_sum_cols || ', ';
        END IF;
        res_cols     := res_cols     || format('%I', res_key);
        res_sum_cols := res_sum_cols || format('SUM(%I) AS %I', res_key, res_key);
        first_res := false;
    END LOOP;

    -- 3. Verify balance_cache (only for balance kind)
    IF reg.kind = 'balance' THEN
        RETURN QUERY EXECUTE format(
            'WITH actual AS (
                SELECT dim_hash, %s
                FROM @extschema@.%I
                GROUP BY dim_hash
            ),
            cached AS (
                SELECT dim_hash, %s
                FROM @extschema@.%I
            )
            SELECT
                ''balance_cache''::text AS check_type,
                COALESCE(a.dim_hash, c.dim_hash) AS dim_hash,
                NULL::date AS period,
                (SELECT jsonb_object_agg(k, v) FROM (
                    SELECT * FROM jsonb_each(to_jsonb(c) - ''dim_hash'' - ''last_movement_at'' - ''last_movement_id'' - ''version'' %s)
                ) x(k,v)) AS expected,
                (SELECT jsonb_object_agg(k, v) FROM (
                    SELECT * FROM jsonb_each(to_jsonb(a) - ''dim_hash'')
                ) x(k,v)) AS actual,
                CASE
                    WHEN c.dim_hash IS NULL THEN ''MISSING_IN_CACHE''
                    WHEN a.dim_hash IS NULL THEN ''ORPHAN_IN_CACHE''
                    WHEN to_jsonb(c) - ''dim_hash'' - ''last_movement_at'' - ''last_movement_id'' - ''version'' %s
                         = to_jsonb(a) - ''dim_hash'' THEN ''OK''
                    ELSE ''MISMATCH''
                END AS status
            FROM actual a
            FULL OUTER JOIN cached c USING (dim_hash)',
            res_sum_cols,
            p_name || '_movements',
            res_cols,
            p_name || '_balance_cache',
            -- Remove dimension columns from comparison (they are structural, not resource)
            (SELECT string_agg(format(' - %L', key), '') FROM jsonb_each_text(reg.dimensions)),
            (SELECT string_agg(format(' - %L', key), '') FROM jsonb_each_text(reg.dimensions))
        );
    END IF;

    -- 4. Verify totals_month
    RETURN QUERY EXECUTE format(
        'WITH actual AS (
            SELECT dim_hash, date_trunc(''month'', period)::date AS period, %s
            FROM @extschema@.%I
            GROUP BY dim_hash, date_trunc(''month'', period)::date
        ),
        stored AS (
            SELECT dim_hash, period, %s
            FROM @extschema@.%I
        )
        SELECT
            ''totals_month''::text AS check_type,
            COALESCE(a.dim_hash, s.dim_hash) AS dim_hash,
            COALESCE(a.period, s.period) AS period,
            (SELECT jsonb_object_agg(k, v) FROM (
                SELECT * FROM jsonb_each(to_jsonb(s) - ''dim_hash'' - ''period'' %s)
            ) x(k,v)) AS expected,
            (SELECT jsonb_object_agg(k, v) FROM (
                SELECT * FROM jsonb_each(to_jsonb(a) - ''dim_hash'' - ''period'')
            ) x(k,v)) AS actual,
            CASE
                WHEN s.dim_hash IS NULL THEN ''MISSING_IN_TOTALS''
                WHEN a.dim_hash IS NULL THEN ''ORPHAN_IN_TOTALS''
                WHEN to_jsonb(s) - ''dim_hash'' - ''period'' %s
                     = to_jsonb(a) - ''dim_hash'' - ''period'' THEN ''OK''
                ELSE ''MISMATCH''
            END AS status
        FROM actual a
        FULL OUTER JOIN stored s USING (dim_hash, period)',
        res_sum_cols,
        p_name || '_movements',
        res_cols,
        p_name || '_totals_month',
        (SELECT string_agg(format(' - %L', key), '') FROM jsonb_each_text(reg.dimensions)),
        (SELECT string_agg(format(' - %L', key), '') FROM jsonb_each_text(reg.dimensions))
    );

    -- 5. Verify totals_year against totals_month
    RETURN QUERY EXECUTE format(
        'WITH actual AS (
            SELECT dim_hash, date_trunc(''year'', period)::date AS period, %s
            FROM @extschema@.%I
            GROUP BY dim_hash, date_trunc(''year'', period)::date
        ),
        stored AS (
            SELECT dim_hash, period, %s
            FROM @extschema@.%I
        )
        SELECT
            ''totals_year''::text AS check_type,
            COALESCE(a.dim_hash, s.dim_hash) AS dim_hash,
            COALESCE(a.period, s.period) AS period,
            (SELECT jsonb_object_agg(k, v) FROM (
                SELECT * FROM jsonb_each(to_jsonb(s) - ''dim_hash'' - ''period'' %s)
            ) x(k,v)) AS expected,
            (SELECT jsonb_object_agg(k, v) FROM (
                SELECT * FROM jsonb_each(to_jsonb(a) - ''dim_hash'' - ''period'')
            ) x(k,v)) AS actual,
            CASE
                WHEN s.dim_hash IS NULL THEN ''MISSING_IN_TOTALS''
                WHEN a.dim_hash IS NULL THEN ''ORPHAN_IN_TOTALS''
                WHEN to_jsonb(s) - ''dim_hash'' - ''period'' %s
                     = to_jsonb(a) - ''dim_hash'' - ''period'' THEN ''OK''
                ELSE ''MISMATCH''
            END AS status
        FROM actual a
        FULL OUTER JOIN stored s USING (dim_hash, period)',
        res_sum_cols,
        p_name || '_totals_month',
        res_cols,
        p_name || '_totals_year',
        (SELECT string_agg(format(' - %L', key), '') FROM jsonb_each_text(reg.dimensions)),
        (SELECT string_agg(format(' - %L', key), '') FROM jsonb_each_text(reg.dimensions))
    );
END;
$$;

COMMENT ON FUNCTION @extschema@.register_verify(text) IS
    'Verify data consistency: compare balance_cache, totals_month, totals_year against actual movements';


-- ============================================================
-- REGISTER_REBUILD_TOTALS: Full rebuild of totals from movements
-- Truncates and re-aggregates totals_month and totals_year.
--
-- Returns: number of rebuilt rows (month + year)
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@.register_rebuild_totals(p_name text)
RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    reg          record;
    dim_cols     text := '';
    res_cols     text := '';
    res_sum_cols text := '';
    first_dim    boolean := true;
    first_res    boolean := true;
    dim_key      text;
    res_key      text;
    month_count  int;
    year_count   int;
BEGIN
    -- 1. Get register metadata
    SELECT * INTO reg FROM @extschema@._registers r WHERE r.name = p_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_name;
    END IF;

    -- 2. Build column expressions
    FOR dim_key IN SELECT key FROM jsonb_each_text(reg.dimensions) ORDER BY key
    LOOP
        IF NOT first_dim THEN dim_cols := dim_cols || ', '; END IF;
        dim_cols := dim_cols || format('%I', dim_key);
        first_dim := false;
    END LOOP;

    FOR res_key IN SELECT key FROM jsonb_each_text(reg.resources) ORDER BY key
    LOOP
        IF NOT first_res THEN
            res_cols     := res_cols     || ', ';
            res_sum_cols := res_sum_cols || ', ';
        END IF;
        res_cols     := res_cols     || format('%I', res_key);
        res_sum_cols := res_sum_cols || format('SUM(%I) AS %I', res_key, res_key);
        first_res := false;
    END LOOP;

    -- 3. Truncate and rebuild totals_month
    EXECUTE format('TRUNCATE @extschema@.%I', p_name || '_totals_month');

    EXECUTE format(
        'INSERT INTO @extschema@.%I (dim_hash, period, %s, %s)
         SELECT dim_hash, date_trunc(''month'', period)::date, %s, %s
         FROM @extschema@.%I
         GROUP BY dim_hash, date_trunc(''month'', period)::date, %s',
        p_name || '_totals_month',
        dim_cols, res_cols,
        dim_cols, res_sum_cols,
        p_name || '_movements',
        dim_cols
    );
    GET DIAGNOSTICS month_count = ROW_COUNT;

    -- 4. Truncate and rebuild totals_year from totals_month
    EXECUTE format('TRUNCATE @extschema@.%I', p_name || '_totals_year');

    EXECUTE format(
        'INSERT INTO @extschema@.%I (dim_hash, period, %s, %s)
         SELECT dim_hash, date_trunc(''year'', period)::date, %s, %s
         FROM @extschema@.%I
         GROUP BY dim_hash, date_trunc(''year'', period)::date, %s',
        p_name || '_totals_year',
        dim_cols, res_cols,
        dim_cols, res_sum_cols,
        p_name || '_totals_month',
        dim_cols
    );
    GET DIAGNOSTICS year_count = ROW_COUNT;

    RETURN month_count + year_count;
END;
$$;

COMMENT ON FUNCTION @extschema@.register_rebuild_totals(text) IS
    'Rebuild totals_month and totals_year from movements data';


-- ============================================================
-- REGISTER_REBUILD_CACHE: Rebuild balance_cache from movements
-- Full (dim_hash IS NULL) or partial (specific dim_hash).
--
-- Returns: number of rebuilt cache rows
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@.register_rebuild_cache(
    p_name     text,
    p_dim_hash bigint DEFAULT NULL
)
RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    reg          record;
    dim_cols     text := '';
    res_cols     text := '';
    res_sum_cols text := '';
    first_dim    boolean := true;
    first_res    boolean := true;
    dim_key      text;
    res_key      text;
    rebuilt      int;
BEGIN
    -- 1. Get register metadata
    SELECT * INTO reg FROM @extschema@._registers r WHERE r.name = p_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_name;
    END IF;

    IF reg.kind != 'balance' THEN
        RAISE EXCEPTION 'rebuild_cache is only available for balance-type registers, "%" is %',
            p_name, reg.kind;
    END IF;

    -- 2. Build column expressions
    FOR dim_key IN SELECT key FROM jsonb_each_text(reg.dimensions) ORDER BY key
    LOOP
        IF NOT first_dim THEN dim_cols := dim_cols || ', '; END IF;
        dim_cols := dim_cols || format('%I', dim_key);
        first_dim := false;
    END LOOP;

    FOR res_key IN SELECT key FROM jsonb_each_text(reg.resources) ORDER BY key
    LOOP
        IF NOT first_res THEN
            res_cols     := res_cols     || ', ';
            res_sum_cols := res_sum_cols || ', ';
        END IF;
        res_cols     := res_cols     || format('%I', res_key);
        res_sum_cols := res_sum_cols || format('SUM(%I) AS %I', res_key, res_key);
        first_res := false;
    END LOOP;

    IF p_dim_hash IS NULL THEN
        -- Full rebuild
        EXECUTE format('TRUNCATE @extschema@.%I', p_name || '_balance_cache');

        -- Also flush delta buffer if high_write
        IF reg.high_write THEN
            EXECUTE format('TRUNCATE @extschema@.%I', p_name || '_balance_cache_delta');
        END IF;

        EXECUTE format(
            'INSERT INTO @extschema@.%I (dim_hash, %s, %s, last_movement_at, last_movement_id)
             SELECT dim_hash, %s, %s,
                    MAX(recorded_at),
                    (array_agg(id ORDER BY recorded_at DESC))[1]
             FROM @extschema@.%I
             GROUP BY dim_hash, %s',
            p_name || '_balance_cache',
            dim_cols, res_cols,
            dim_cols, res_sum_cols,
            p_name || '_movements',
            dim_cols
        );
    ELSE
        -- Partial rebuild: specific dim_hash
        EXECUTE format('DELETE FROM @extschema@.%I WHERE dim_hash = $1',
            p_name || '_balance_cache')
            USING p_dim_hash;

        -- Flush deltas for this dim_hash if high_write
        IF reg.high_write THEN
            EXECUTE format('DELETE FROM @extschema@.%I WHERE dim_hash = $1',
                p_name || '_balance_cache_delta')
                USING p_dim_hash;
        END IF;

        EXECUTE format(
            'INSERT INTO @extschema@.%I (dim_hash, %s, %s, last_movement_at, last_movement_id)
             SELECT dim_hash, %s, %s,
                    MAX(recorded_at),
                    (array_agg(id ORDER BY recorded_at DESC))[1]
             FROM @extschema@.%I
             WHERE dim_hash = $1
             GROUP BY dim_hash, %s',
            p_name || '_balance_cache',
            dim_cols, res_cols,
            dim_cols, res_sum_cols,
            p_name || '_movements',
            dim_cols
        ) USING p_dim_hash;
    END IF;

    GET DIAGNOSTICS rebuilt = ROW_COUNT;
    RETURN rebuilt;
END;
$$;

COMMENT ON FUNCTION @extschema@.register_rebuild_cache(text, bigint) IS
    'Rebuild balance_cache from movements. Full rebuild if dim_hash IS NULL, partial otherwise';


-- ============================================================
-- REGISTER_STATS: Collect register statistics
-- Returns a JSON object with counts and table sizes.
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@.register_stats(p_name text)
RETURNS jsonb
LANGUAGE plpgsql STABLE AS $$
DECLARE
    reg               record;
    result            jsonb;
    v_movements       bigint := 0;
    v_partitions      int    := 0;
    v_cache_rows      bigint := 0;
    v_month_rows      bigint := 0;
    v_year_rows       bigint := 0;
    v_delta_pending   bigint := 0;
    v_last_delta      timestamptz;
    v_table_sizes     jsonb;
    v_movements_size  text;
    v_month_size      text;
    v_year_size       text;
    v_cache_size      text;
BEGIN
    -- 1. Get register metadata
    SELECT * INTO reg FROM @extschema@._registers r WHERE r.name = p_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_name;
    END IF;

    -- 2. Count movements
    BEGIN
        EXECUTE format('SELECT count(*) FROM @extschema@.%I',
            p_name || '_movements') INTO v_movements;
    EXCEPTION WHEN undefined_table THEN
        v_movements := 0;
    END;

    -- 3. Count partitions
    SELECT count(*)::int INTO v_partitions
    FROM pg_inherits i
    JOIN pg_class parent ON i.inhparent = parent.oid
    JOIN pg_class child ON i.inhrelid = child.oid
    JOIN pg_namespace ns ON parent.relnamespace = ns.oid
    WHERE parent.relname = p_name || '_movements'
      AND ns.nspname = (SELECT nspname FROM pg_namespace WHERE oid = (
          SELECT relnamespace FROM pg_class WHERE relname = p_name || '_movements'
          AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '@extschema@')
      ));

    -- Fallback: try with accum schema directly for test environment
    IF v_partitions = 0 THEN
        SELECT count(*)::int INTO v_partitions
        FROM pg_inherits i
        JOIN pg_class parent ON i.inhparent = parent.oid
        JOIN pg_namespace ns ON parent.relnamespace = ns.oid
        WHERE parent.relname = p_name || '_movements'
          AND ns.nspname = 'accum';
    END IF;

    -- 4. Count totals rows
    BEGIN
        EXECUTE format('SELECT count(*) FROM @extschema@.%I',
            p_name || '_totals_month') INTO v_month_rows;
    EXCEPTION WHEN undefined_table THEN
        v_month_rows := 0;
    END;

    BEGIN
        EXECUTE format('SELECT count(*) FROM @extschema@.%I',
            p_name || '_totals_year') INTO v_year_rows;
    EXCEPTION WHEN undefined_table THEN
        v_year_rows := 0;
    END;

    -- 5. Count balance_cache rows (if applicable)
    IF reg.kind = 'balance' THEN
        BEGIN
            EXECUTE format('SELECT count(*) FROM @extschema@.%I',
                p_name || '_balance_cache') INTO v_cache_rows;
        EXCEPTION WHEN undefined_table THEN
            v_cache_rows := 0;
        END;
    END IF;

    -- 6. Delta buffer info (if high_write)
    IF reg.high_write THEN
        BEGIN
            EXECUTE format('SELECT count(*) FROM @extschema@.%I',
                p_name || '_balance_cache_delta') INTO v_delta_pending;
            EXECUTE format('SELECT MAX(created_at) FROM @extschema@.%I',
                p_name || '_balance_cache_delta') INTO v_last_delta;
        EXCEPTION WHEN undefined_table THEN
            v_delta_pending := 0;
            v_last_delta := NULL;
        END;
    END IF;

    -- 7. Table sizes
    BEGIN
        SELECT pg_size_pretty(pg_total_relation_size(
            format('@extschema@.%I', p_name || '_movements')::regclass))
            INTO v_movements_size;
    EXCEPTION WHEN OTHERS THEN
        v_movements_size := '0 bytes';
    END;

    BEGIN
        SELECT pg_size_pretty(pg_total_relation_size(
            format('@extschema@.%I', p_name || '_totals_month')::regclass))
            INTO v_month_size;
    EXCEPTION WHEN OTHERS THEN
        v_month_size := '0 bytes';
    END;

    BEGIN
        SELECT pg_size_pretty(pg_total_relation_size(
            format('@extschema@.%I', p_name || '_totals_year')::regclass))
            INTO v_year_size;
    EXCEPTION WHEN OTHERS THEN
        v_year_size := '0 bytes';
    END;

    v_table_sizes := jsonb_build_object(
        'movements', v_movements_size,
        'totals_month', v_month_size,
        'totals_year', v_year_size
    );

    IF reg.kind = 'balance' THEN
        BEGIN
            SELECT pg_size_pretty(pg_total_relation_size(
                format('@extschema@.%I', p_name || '_balance_cache')::regclass))
                INTO v_cache_size;
        EXCEPTION WHEN OTHERS THEN
            v_cache_size := '0 bytes';
        END;
        v_table_sizes := v_table_sizes || jsonb_build_object('balance_cache', v_cache_size);
    END IF;

    -- 8. Build result
    result := jsonb_build_object(
        'movements_count',      v_movements,
        'partitions_count',     v_partitions,
        'cache_rows',           v_cache_rows,
        'totals_month_rows',    v_month_rows,
        'totals_year_rows',     v_year_rows,
        'delta_buffer_pending', v_delta_pending,
        'last_delta_merge',     v_last_delta,
        'table_sizes',          v_table_sizes
    );

    RETURN result;
END;
$$;

COMMENT ON FUNCTION @extschema@.register_stats(text) IS
    'Collect register statistics: row counts, partition count, table sizes';
