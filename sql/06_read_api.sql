-- sql/06_read_api.sql
-- Read API: balance, turnover, movements
-- Internal generic functions + per-register wrapper generator

-- ============================================================
-- INTERNAL: Balance query (current or historical)
-- Returns a single jsonb object with resource values.
-- Only for kind='balance' registers.
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@._balance_internal(
    p_register text,
    p_dims     jsonb DEFAULT NULL,
    p_at_date  timestamptz DEFAULT NULL
) RETURNS jsonb
LANGUAGE plpgsql STABLE AS $$
DECLARE
    reg         record;
    dim_key     text;
    dim_type    text;
    res_key     text;
    dim_where   text := '';
    res_agg     text := '';
    res_cols    text := '';
    first_res   boolean := true;
    result      jsonb;
    all_dims    boolean := false;
    dim_count   int;
    provided    int;
    hash_args   text := '';
    first_dim   boolean := true;
    v_dim_hash  bigint;
BEGIN
    -- 1. Get register metadata
    SELECT * INTO reg FROM @extschema@._registers r WHERE r.name = p_register;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_register;
    END IF;

    IF reg.kind != 'balance' THEN
        RAISE EXCEPTION 'balance() is only available for balance-type registers, "%" is %',
            p_register, reg.kind;
    END IF;

    -- 2. Build resource expressions
    FOR res_key IN SELECT key FROM jsonb_each_text(reg.resources) ORDER BY key
    LOOP
        IF NOT first_res THEN
            res_agg  := res_agg  || ', ';
            res_cols := res_cols || ', ';
        END IF;
        res_agg  := res_agg  || format('''%s'', COALESCE(SUM(%I), 0)', res_key, res_key);
        res_cols := res_cols || format('%I', res_key);
        first_res := false;
    END LOOP;

    -- 3. Build dimension filter + check if all dims provided
    dim_count := (SELECT count(*)::int FROM jsonb_object_keys(reg.dimensions));
    provided  := 0;
    IF p_dims IS NOT NULL AND p_dims != '{}'::jsonb THEN
        provided := (SELECT count(*)::int FROM jsonb_object_keys(p_dims));
    END IF;
    all_dims := (provided = dim_count AND provided > 0);

    -- Build hash call args (for exact match) and WHERE clause
    FOR dim_key, dim_type IN SELECT key, value FROM jsonb_each_text(reg.dimensions) ORDER BY key
    LOOP
        IF NOT first_dim THEN hash_args := hash_args || ', '; END IF;

        IF p_dims IS NOT NULL AND p_dims ? dim_key THEN
            hash_args := hash_args || format('%L::%s', p_dims->>dim_key, dim_type);
            dim_where := dim_where || format(' AND %I = %L::%s', dim_key, p_dims->>dim_key, dim_type);
        ELSE
            hash_args := hash_args || format('NULL::%s', dim_type);
        END IF;
        first_dim := false;
    END LOOP;

    -- Compute dim_hash for exact match
    IF all_dims THEN
        EXECUTE format('SELECT @extschema@.%I(%s)', '_hash_' || p_register, hash_args)
            INTO v_dim_hash;
    END IF;

    -- 4. Current balance (at_date IS NULL)
    IF p_at_date IS NULL THEN
        IF NOT reg.high_write THEN
            -- Standard mode: read from balance_cache
            IF all_dims THEN
                EXECUTE format(
                    'SELECT jsonb_build_object(%s)
                     FROM @extschema@.%I WHERE dim_hash = $1',
                    res_agg, p_register || '_balance_cache'
                ) INTO result USING v_dim_hash;
            ELSE
                EXECUTE format(
                    'SELECT jsonb_build_object(%s)
                     FROM @extschema@.%I WHERE TRUE %s',
                    res_agg, p_register || '_balance_cache', dim_where
                ) INTO result;
            END IF;
        ELSE
            -- High-write mode: cache + delta buffer
            IF all_dims THEN
                EXECUTE format(
                    'SELECT jsonb_build_object(%s) FROM (
                         SELECT %s FROM @extschema@.%I WHERE dim_hash = $1
                         UNION ALL
                         SELECT %s FROM @extschema@.%I WHERE dim_hash = $1
                     ) _combined',
                    res_agg,
                    res_cols, p_register || '_balance_cache',
                    res_cols, p_register || '_balance_cache_delta'
                ) INTO result USING v_dim_hash;
            ELSE
                EXECUTE format(
                    'SELECT jsonb_build_object(%s) FROM (
                         SELECT %s FROM @extschema@.%I WHERE TRUE %s
                         UNION ALL
                         SELECT d.%s FROM @extschema@.%I d
                         WHERE d.dim_hash IN (
                             SELECT c.dim_hash FROM @extschema@.%I c WHERE TRUE %s
                         )
                     ) _combined',
                    res_agg,
                    res_cols, p_register || '_balance_cache', dim_where,
                    res_cols, p_register || '_balance_cache_delta',
                    p_register || '_balance_cache', dim_where
                ) INTO result;
            END IF;
        END IF;
    ELSE
        -- 5. Historical balance: hierarchical computation
        --    totals_year (full years) + totals_month (full months) + movements (partial month)
        IF all_dims THEN
            EXECUTE format(
                'SELECT jsonb_build_object(%s) FROM (
                     SELECT %s FROM @extschema@.%I
                     WHERE dim_hash = $1
                       AND period < date_trunc(''year'', $2)::date
                     UNION ALL
                     SELECT %s FROM @extschema@.%I
                     WHERE dim_hash = $1
                       AND period >= date_trunc(''year'', $2)::date
                       AND period < date_trunc(''month'', $2)::date
                     UNION ALL
                     SELECT %s FROM @extschema@.%I
                     WHERE dim_hash = $1
                       AND period >= date_trunc(''month'', $2)
                       AND period <= $2
                 ) _hierarchy',
                res_agg,
                res_cols, p_register || '_totals_year',
                res_cols, p_register || '_totals_month',
                res_cols, p_register || '_movements'
            ) INTO result USING v_dim_hash, p_at_date;
        ELSE
            EXECUTE format(
                'SELECT jsonb_build_object(%s) FROM (
                     SELECT %s FROM @extschema@.%I
                     WHERE TRUE %s
                       AND period < date_trunc(''year'', $1)::date
                     UNION ALL
                     SELECT %s FROM @extschema@.%I
                     WHERE TRUE %s
                       AND period >= date_trunc(''year'', $1)::date
                       AND period < date_trunc(''month'', $1)::date
                     UNION ALL
                     SELECT %s FROM @extschema@.%I
                     WHERE TRUE %s
                       AND period >= date_trunc(''month'', $1)
                       AND period <= $1
                 ) _hierarchy',
                res_agg,
                res_cols, p_register || '_totals_year', dim_where,
                res_cols, p_register || '_totals_month', dim_where,
                res_cols, p_register || '_movements', dim_where
            ) INTO result USING p_at_date;
        END IF;
    END IF;

    -- Return zeros if nothing found
    IF result IS NULL THEN
        SELECT jsonb_object_agg(key, 0) INTO result
        FROM jsonb_each_text(reg.resources);
    END IF;

    RETURN result;
END;
$$;


-- ============================================================
-- INTERNAL: Turnover query (optimized via totals hierarchy)
-- Returns SETOF jsonb. Each row is an object with resource values
-- and (optionally) grouped dimension values.
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@._turnover_internal(
    p_register text,
    p_from     timestamptz DEFAULT NULL,
    p_to       timestamptz DEFAULT NULL,
    p_dims     jsonb DEFAULT NULL,
    p_group_by jsonb DEFAULT NULL
) RETURNS SETOF jsonb
LANGUAGE plpgsql STABLE AS $$
DECLARE
    reg           record;
    dim_key       text;
    dim_type      text;
    res_key       text;
    dim_where     text := '';
    res_agg       text := '';
    res_cols      text := '';
    group_cols    text := '';
    group_select  text := '';
    group_by_sql  text := '';
    first_res     boolean := true;
    first_grp     boolean := true;
    grp_key       text;
    v_from_month  date;
    v_to_month    date;
    v_from_year   date;
    v_to_year     date;
    query         text;
BEGIN
    -- 1. Get register metadata
    SELECT * INTO reg FROM @extschema@._registers r WHERE r.name = p_register;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_register;
    END IF;

    -- 2. Build resource expressions
    FOR res_key IN SELECT key FROM jsonb_each_text(reg.resources) ORDER BY key
    LOOP
        IF NOT first_res THEN
            res_agg  := res_agg  || ', ';
            res_cols := res_cols || ', ';
        END IF;
        res_agg  := res_agg  || format('''%s'', COALESCE(SUM(%I), 0)', res_key, res_key);
        res_cols := res_cols || format('%I', res_key);
        first_res := false;
    END LOOP;

    -- 3. Build dimension filter
    IF p_dims IS NOT NULL AND p_dims != '{}'::jsonb THEN
        FOR dim_key, dim_type IN SELECT key, value FROM jsonb_each_text(reg.dimensions) ORDER BY key
        LOOP
            IF p_dims ? dim_key THEN
                dim_where := dim_where || format(' AND %I = %L::%s',
                    dim_key, p_dims->>dim_key, dim_type);
            END IF;
        END LOOP;
    END IF;

    -- 4. Build GROUP BY for group_by parameter
    IF p_group_by IS NOT NULL AND p_group_by != '[]'::jsonb THEN
        FOR grp_key IN SELECT value FROM jsonb_array_elements_text(p_group_by)
        LOOP
            IF NOT reg.dimensions ? grp_key THEN
                RAISE EXCEPTION 'group_by dimension "%" does not exist in register "%"',
                    grp_key, p_register;
            END IF;
            IF NOT first_grp THEN
                group_cols   := group_cols   || ', ';
                group_select := group_select || ', ';
            END IF;
            group_cols   := group_cols   || format('%I', grp_key);
            group_select := group_select || format('''%s'', %I', grp_key, grp_key);
            first_grp := false;
        END LOOP;
        group_by_sql := ' GROUP BY ' || group_cols;
    END IF;

    -- 5. Determine date boundaries for optimization
    v_from_month := date_trunc('month', p_from)::date;
    v_to_month   := date_trunc('month', p_to)::date;

    -- First full month start (month after p_from's month, unless p_from is 1st of month)
    DECLARE
        v_first_full_month date;
        v_last_full_month  date;
    BEGIN
        IF p_from = date_trunc('month', p_from) THEN
            v_first_full_month := v_from_month;
        ELSE
            v_first_full_month := (v_from_month + interval '1 month')::date;
        END IF;

        -- Last day check: if p_to is last day of its month or later, include that month
        IF p_to >= (date_trunc('month', p_to) + interval '1 month' - interval '1 day')::timestamptz THEN
            v_last_full_month := v_to_month;
        ELSE
            v_last_full_month := (v_to_month - interval '1 month')::date;
        END IF;

        -- Build optimized query:
        -- 1) Partial month at start (movements)
        -- 2) Full months in range (totals_month)
        -- 3) Partial month at end (movements)
        query := format(
            'SELECT jsonb_build_object(%s %s) FROM (', res_agg,
            CASE WHEN group_select != '' THEN ', ' || group_select ELSE '' END);

        -- Part 1: movements before first full month
        IF v_first_full_month > v_from_month OR p_from != date_trunc('month', p_from) THEN
            query := query || format(
                'SELECT %s %s FROM @extschema@.%I
                 WHERE period >= $1 AND period < $3::timestamptz
                 %s',
                CASE WHEN group_cols != '' THEN group_cols || ', ' ELSE '' END,
                res_cols, p_register || '_movements', dim_where);
        ELSE
            query := query || format(
                'SELECT %s %s FROM @extschema@.%I WHERE FALSE',
                CASE WHEN group_cols != '' THEN group_cols || ', ' ELSE '' END,
                res_cols, p_register || '_movements');
        END IF;

        query := query || ' UNION ALL ';

        -- Part 2: full months from totals_month
        query := query || format(
            'SELECT %s %s FROM @extschema@.%I
             WHERE period >= $3 AND period <= $4
             %s',
            CASE WHEN group_cols != '' THEN group_cols || ', ' ELSE '' END,
            res_cols, p_register || '_totals_month', dim_where);

        query := query || ' UNION ALL ';

        -- Part 3: movements after last full month
        IF v_last_full_month < v_to_month OR
           p_to < (date_trunc('month', p_to) + interval '1 month' - interval '1 day')::timestamptz THEN
            query := query || format(
                'SELECT %s %s FROM @extschema@.%I
                 WHERE period > ($4 + interval ''1 month'' - interval ''1 day'')::timestamptz
                   AND period <= $2
                 %s',
                CASE WHEN group_cols != '' THEN group_cols || ', ' ELSE '' END,
                res_cols, p_register || '_movements', dim_where);
        ELSE
            query := query || format(
                'SELECT %s %s FROM @extschema@.%I WHERE FALSE',
                CASE WHEN group_cols != '' THEN group_cols || ', ' ELSE '' END,
                res_cols, p_register || '_movements');
        END IF;

        query := query || ') _turnover' || group_by_sql;

        RETURN QUERY EXECUTE query
            USING p_from, p_to, v_first_full_month, v_last_full_month;
    END;
END;
$$;


-- ============================================================
-- INTERNAL: Movements query (filtered SELECT)
-- Returns SETOF jsonb, each row is a full movement record.
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@._movements_internal(
    p_register  text,
    p_recorder  text DEFAULT NULL,
    p_from      timestamptz DEFAULT NULL,
    p_to        timestamptz DEFAULT NULL,
    p_dims      jsonb DEFAULT NULL
) RETURNS SETOF jsonb
LANGUAGE plpgsql STABLE AS $$
DECLARE
    reg         record;
    dim_key     text;
    dim_type    text;
    dim_where   text := '';
    query       text;
BEGIN
    -- 1. Get register metadata
    SELECT * INTO reg FROM @extschema@._registers r WHERE r.name = p_register;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_register;
    END IF;

    -- 2. Build dimension filter
    IF p_dims IS NOT NULL AND p_dims != '{}'::jsonb THEN
        FOR dim_key, dim_type IN SELECT key, value FROM jsonb_each_text(reg.dimensions) ORDER BY key
        LOOP
            IF p_dims ? dim_key THEN
                dim_where := dim_where || format(' AND %I = %L::%s',
                    dim_key, p_dims->>dim_key, dim_type);
            END IF;
        END LOOP;
    END IF;

    -- 3. Build and execute query
    query := format(
        'SELECT to_jsonb(m) FROM @extschema@.%I m
         WHERE TRUE
           AND ($1 IS NULL OR recorder = $1)
           AND ($2 IS NULL OR period >= $2)
           AND ($3 IS NULL OR period <= $3)
           %s
         ORDER BY period, recorded_at',
        p_register || '_movements', dim_where
    );

    RETURN QUERY EXECUTE query USING p_recorder, p_from, p_to;
END;
$$;


-- ============================================================
-- GENERATE PER-REGISTER READ FUNCTIONS
-- Creates <name>_balance(), <name>_turnover(), <name>_movements()
-- as thin wrappers around the internal generic functions.
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@._generate_read_functions(
    p_name       text,
    p_kind       text,
    p_dimensions jsonb,
    p_resources  jsonb,
    p_high_write boolean
) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    -- Generate _balance() only for balance-kind registers
    IF p_kind = 'balance' THEN
        EXECUTE format(
            'CREATE OR REPLACE FUNCTION @extschema@.%I(
                dimensions jsonb DEFAULT NULL,
                at_date    timestamptz DEFAULT NULL
            ) RETURNS jsonb
            LANGUAGE sql STABLE AS $fn$
                SELECT @extschema@._balance_internal(%L, dimensions, at_date);
            $fn$',
            p_name || '_balance',
            p_name
        );
    END IF;

    -- Generate _turnover()
    EXECUTE format(
        'CREATE OR REPLACE FUNCTION @extschema@.%I(
            from_date  timestamptz DEFAULT NULL,
            to_date    timestamptz DEFAULT NULL,
            dimensions jsonb DEFAULT NULL,
            group_by   jsonb DEFAULT NULL
        ) RETURNS SETOF jsonb
        LANGUAGE sql STABLE AS $fn$
            SELECT @extschema@._turnover_internal(%L, from_date, to_date, dimensions, group_by);
        $fn$',
        p_name || '_turnover',
        p_name
    );

    -- Generate _movements()
    EXECUTE format(
        'CREATE OR REPLACE FUNCTION @extschema@.%I(
            p_recorder text DEFAULT NULL,
            from_date  timestamptz DEFAULT NULL,
            to_date    timestamptz DEFAULT NULL,
            dimensions jsonb DEFAULT NULL
        ) RETURNS SETOF jsonb
        LANGUAGE sql STABLE AS $fn$
            SELECT @extschema@._movements_internal(%L, p_recorder, from_date, to_date, dimensions);
        $fn$',
        p_name || '_movements',
        p_name
    );
END;
$$;


-- ============================================================
-- DROP PER-REGISTER READ FUNCTIONS
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@._drop_read_functions(p_name text)
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE format('DROP FUNCTION IF EXISTS @extschema@.%I(jsonb, timestamptz) CASCADE',
        p_name || '_balance');
    EXECUTE format('DROP FUNCTION IF EXISTS @extschema@.%I(timestamptz, timestamptz, jsonb, jsonb) CASCADE',
        p_name || '_turnover');
    EXECUTE format('DROP FUNCTION IF EXISTS @extschema@.%I(text, timestamptz, timestamptz, jsonb) CASCADE',
        p_name || '_movements');
END;
$$;
