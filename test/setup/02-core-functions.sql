-- test/setup/02-core-functions.sql
-- Core emulated functions for testing (pure SQL/PL/pgSQL prototypes)
-- These replicate the C API documented in README

-- ============================================================
-- REGISTER_CREATE: Creates a new accumulation register
-- ============================================================
CREATE OR REPLACE FUNCTION accum.register_create(
    name          text,
    dimensions    jsonb,
    resources     jsonb,
    kind          text DEFAULT 'balance',
    totals_period text DEFAULT 'day',
    partition_by  text DEFAULT 'month',
    high_write    boolean DEFAULT false,
    recorder_type text DEFAULT 'text'
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    dim_key   text;
    dim_type  text;
    res_key   text;
    res_type  text;
    col_defs  text := '';
    res_defs  text := '';
    dim_cols  text := '';
    res_cols  text := '';
    idx_name  text;
BEGIN
    -- Validate name
    IF name !~ '^[a-z][a-z0-9_]*$' THEN
        RAISE EXCEPTION 'Invalid register name: %. Must match ^[a-z][a-z0-9_]*$', name;
    END IF;

    -- Validate kind
    IF kind NOT IN ('balance', 'turnover') THEN
        RAISE EXCEPTION 'Invalid kind: %. Must be balance or turnover', kind;
    END IF;

    -- Validate totals_period
    IF totals_period NOT IN ('day', 'month', 'year') THEN
        RAISE EXCEPTION 'Invalid totals_period: %', totals_period;
    END IF;

    -- Validate partition_by
    IF partition_by NOT IN ('day', 'month', 'quarter', 'year') THEN
        RAISE EXCEPTION 'Invalid partition_by: %', partition_by;
    END IF;

    -- Check duplicate
    IF EXISTS (SELECT 1 FROM accum._registers WHERE _registers.name = register_create.name) THEN
        RAISE EXCEPTION 'Register "%" already exists', name;
    END IF;

    -- Validate dimensions not empty
    IF dimensions IS NULL OR dimensions = '{}'::jsonb THEN
        RAISE EXCEPTION 'At least one dimension is required';
    END IF;

    -- Validate resources not empty
    IF resources IS NULL OR resources = '{}'::jsonb THEN
        RAISE EXCEPTION 'At least one resource is required';
    END IF;

    -- Save to registry
    INSERT INTO accum._registers (name, kind, dimensions, resources, totals_period, partition_by, high_write, recorder_type)
    VALUES (register_create.name, kind, dimensions, resources, totals_period, partition_by, high_write, recorder_type);

    -- Build dimension columns
    FOR dim_key, dim_type IN SELECT * FROM jsonb_each_text(dimensions)
    LOOP
        col_defs := col_defs || format(', %I %s', dim_key, dim_type);
        dim_cols := dim_cols || format(', %I', dim_key);
    END LOOP;

    -- Build resource columns
    FOR res_key, res_type IN SELECT * FROM jsonb_each_text(resources)
    LOOP
        res_defs := res_defs || format(', %I %s NOT NULL DEFAULT 0', res_key, res_type);
        res_cols := res_cols || format(', %I', res_key);
    END LOOP;

    -- ============================================================
    -- CREATE MOVEMENTS TABLE (partitioned)
    -- ============================================================
    EXECUTE format(
        'CREATE TABLE accum.%I (
            id             uuid          DEFAULT gen_random_uuid() PRIMARY KEY,
            recorded_at    timestamptz   DEFAULT now() NOT NULL,
            recorder       %s            NOT NULL,
            period         timestamptz   NOT NULL,
            movement_type  text          DEFAULT ''regular'' NOT NULL,
            dim_hash       bigint        NOT NULL
            %s
            %s
        ) PARTITION BY RANGE (period)',
        name || '_movements',
        recorder_type,
        col_defs,
        res_defs
    );

    -- Default partition
    EXECUTE format(
        'CREATE TABLE accum.%I PARTITION OF accum.%I DEFAULT',
        name || '_movements_default',
        name || '_movements'
    );

    -- Indexes on movements
    EXECUTE format('CREATE INDEX ON accum.%I (dim_hash, period)', name || '_movements');
    EXECUTE format('CREATE INDEX ON accum.%I (recorder)', name || '_movements');
    EXECUTE format('CREATE INDEX ON accum.%I (period)', name || '_movements');

    -- ============================================================
    -- CREATE TOTALS TABLES
    -- ============================================================

    -- totals_month
    EXECUTE format(
        'CREATE TABLE accum.%I (
            dim_hash       bigint        NOT NULL,
            period         date          NOT NULL
            %s
            %s,
            PRIMARY KEY (dim_hash, period)
        )',
        name || '_totals_month',
        col_defs,
        res_defs
    );

    -- totals_year
    EXECUTE format(
        'CREATE TABLE accum.%I (
            dim_hash       bigint        NOT NULL,
            period         date          NOT NULL
            %s
            %s,
            PRIMARY KEY (dim_hash, period)
        )',
        name || '_totals_year',
        col_defs,
        res_defs
    );

    -- ============================================================
    -- CREATE BALANCE CACHE (only for 'balance' kind)
    -- ============================================================
    IF kind = 'balance' THEN
        EXECUTE format(
            'CREATE TABLE accum.%I (
                dim_hash         bigint          NOT NULL PRIMARY KEY
                %s
                %s,
                last_movement_at timestamptz     NOT NULL DEFAULT now(),
                last_movement_id uuid,
                version          bigint          NOT NULL DEFAULT 0
            )',
            name || '_balance_cache',
            col_defs,
            res_defs
        );

        -- Indexes on balance_cache for each dimension
        FOR dim_key IN SELECT * FROM jsonb_object_keys(dimensions)
        LOOP
            EXECUTE format('CREATE INDEX ON accum.%I (%I)',
                name || '_balance_cache', dim_key);
        END LOOP;
    END IF;

    -- ============================================================
    -- CREATE DELTA BUFFER (only if high_write)
    -- ============================================================
    IF high_write THEN
        EXECUTE format(
            'CREATE UNLOGGED TABLE accum.%I (
                id         bigserial     PRIMARY KEY,
                dim_hash   bigint        NOT NULL
                %s,
                created_at timestamptz   DEFAULT now()
            )',
            name || '_balance_cache_delta',
            res_defs
        );

        EXECUTE format('CREATE INDEX ON accum.%I (dim_hash)', name || '_balance_cache_delta');
        EXECUTE format('CREATE INDEX ON accum.%I (created_at)', name || '_balance_cache_delta');
    END IF;

    -- ============================================================
    -- CREATE HASH FUNCTION
    -- ============================================================
    DECLARE
        hash_args text := '';
        hash_body text := '';
        arg_idx   int  := 0;
    BEGIN
        FOR dim_key, dim_type IN SELECT * FROM jsonb_each_text(dimensions)
        LOOP
            IF arg_idx > 0 THEN
                hash_args := hash_args || ', ';
            END IF;
            hash_args := hash_args || format('p_%s %s', dim_key, dim_type);
            hash_body := hash_body || format(' || coalesce(%I::text, ''__NULL__'')', 'p_' || dim_key);
            arg_idx := arg_idx + 1;
        END LOOP;

        EXECUTE format(
            'CREATE OR REPLACE FUNCTION accum.%I(%s) RETURNS bigint
             LANGUAGE sql IMMUTABLE AS $fn$
                SELECT hashtextextended(''''%s$fn$,
            ''_hash_' || name || '''',
            hash_args
        ) || hash_body || format(', 0)
            $fn$');
    END;

    -- ============================================================
    -- CREATE TRIGGER FUNCTION & TRIGGERS
    -- ============================================================
    DECLARE
        trg_body        text;
        totals_upsert_m text := '';
        totals_upsert_y text := '';
        cache_upsert    text := '';
        dim_new_cols    text := '';
        res_new_cols    text := '';
        res_update_m    text := '';
        res_update_c    text := '';
        hash_call_args  text := '';
        dim_insert_cols text := '';
        res_insert_cols text := '';
        first_dim       boolean := true;
        first_res       boolean := true;
    BEGIN
        -- Build column references
        FOR dim_key IN SELECT * FROM jsonb_object_keys(dimensions)
        LOOP
            IF NOT first_dim THEN
                hash_call_args := hash_call_args || ', ';
                dim_insert_cols := dim_insert_cols || ', ';
                dim_new_cols := dim_new_cols || ', ';
            END IF;
            hash_call_args := hash_call_args || format('NEW.%I', dim_key);
            dim_insert_cols := dim_insert_cols || format('%I', dim_key);
            dim_new_cols := dim_new_cols || format('NEW.%I', dim_key);
            first_dim := false;
        END LOOP;

        FOR res_key IN SELECT * FROM jsonb_object_keys(resources)
        LOOP
            IF NOT first_res THEN
                res_insert_cols := res_insert_cols || ', ';
                res_new_cols := res_new_cols || ', ';
                res_update_m := res_update_m || ', ';
                res_update_c := res_update_c || ', ';
            END IF;
            res_insert_cols := res_insert_cols || format('%I', res_key);
            res_new_cols := res_new_cols || format('NEW.%I', res_key);
            res_update_m := res_update_m || format('%I = accum.%I.%I + EXCLUDED.%I',
                res_key, name || '_totals_month', res_key, res_key);
            res_update_c := res_update_c || format('%I = accum.%I.%I + EXCLUDED.%I',
                res_key, name || '_balance_cache', res_key, res_key);
            first_res := false;
        END LOOP;

        -- BEFORE INSERT trigger: compute dim_hash
        EXECUTE format(
            'CREATE OR REPLACE FUNCTION accum.%I() RETURNS trigger
             LANGUAGE plpgsql AS $trg$
             BEGIN
                 NEW.dim_hash := accum.%I(%s);
                 IF NEW.period < now() - interval ''1 day'' THEN
                     NEW.movement_type := ''adjustment'';
                 END IF;
                 RETURN NEW;
             END;
             $trg$',
            '_trg_' || name || '_before_insert',
            '_hash_' || name,
            hash_call_args
        );

        EXECUTE format(
            'CREATE TRIGGER trg_%s_before_insert
             BEFORE INSERT ON accum.%I
             FOR EACH ROW EXECUTE FUNCTION accum.%I()',
            name,
            name || '_movements',
            '_trg_' || name || '_before_insert'
        );

        -- AFTER INSERT trigger: update totals + cache
        -- Build totals_month UPSERT
        totals_upsert_m := format(
            'INSERT INTO accum.%I (dim_hash, period, %s, %s)
             VALUES (NEW.dim_hash, date_trunc(''month'', NEW.period)::date, %s, %s)
             ON CONFLICT (dim_hash, period) DO UPDATE SET %s',
            name || '_totals_month',
            dim_insert_cols, res_insert_cols,
            dim_new_cols, res_new_cols,
            res_update_m
        );

        -- Build totals_year UPSERT (reuse same pattern with different table)
        DECLARE
            res_update_y text := '';
            first_y boolean := true;
        BEGIN
            FOR res_key IN SELECT * FROM jsonb_object_keys(resources)
            LOOP
                IF NOT first_y THEN
                    res_update_y := res_update_y || ', ';
                END IF;
                res_update_y := res_update_y || format('%I = accum.%I.%I + EXCLUDED.%I',
                    res_key, name || '_totals_year', res_key, res_key);
                first_y := false;
            END LOOP;

            totals_upsert_y := format(
                'INSERT INTO accum.%I (dim_hash, period, %s, %s)
                 VALUES (NEW.dim_hash, date_trunc(''year'', NEW.period)::date, %s, %s)
                 ON CONFLICT (dim_hash, period) DO UPDATE SET %s',
                name || '_totals_year',
                dim_insert_cols, res_insert_cols,
                dim_new_cols, res_new_cols,
                res_update_y
            );
        END;

        -- Build cache UPSERT (only for balance)
        IF kind = 'balance' THEN
            IF NOT high_write THEN
                cache_upsert := format(
                    'INSERT INTO accum.%I (dim_hash, %s, %s, last_movement_at, last_movement_id, version)
                     VALUES (NEW.dim_hash, %s, %s, now(), NEW.id, 1)
                     ON CONFLICT (dim_hash) DO UPDATE SET %s,
                         last_movement_at = EXCLUDED.last_movement_at,
                         last_movement_id = EXCLUDED.last_movement_id,
                         version = accum.%I.version + 1',
                    name || '_balance_cache',
                    dim_insert_cols, res_insert_cols,
                    dim_new_cols, res_new_cols,
                    res_update_c,
                    name || '_balance_cache'
                );
            ELSE
                -- High-write: INSERT into delta buffer instead
                cache_upsert := format(
                    'INSERT INTO accum.%I (dim_hash, %s)
                     VALUES (NEW.dim_hash, %s)',
                    name || '_balance_cache_delta',
                    res_insert_cols,
                    res_new_cols
                );
            END IF;
        END IF;

        -- Create AFTER INSERT trigger function
        trg_body := totals_upsert_m || '; ' || totals_upsert_y || ';';
        IF cache_upsert != '' THEN
            trg_body := trg_body || ' ' || cache_upsert || ';';
        END IF;

        EXECUTE format(
            'CREATE OR REPLACE FUNCTION accum.%I() RETURNS trigger
             LANGUAGE plpgsql AS $trg$
             BEGIN
                 %s
                 RETURN NEW;
             END;
             $trg$',
            '_trg_' || name || '_after_insert',
            trg_body
        );

        EXECUTE format(
            'CREATE TRIGGER trg_%s_after_insert
             AFTER INSERT ON accum.%I
             FOR EACH ROW EXECUTE FUNCTION accum.%I()',
            name,
            name || '_movements',
            '_trg_' || name || '_after_insert'
        );

        -- AFTER DELETE trigger: reverse the operations
        DECLARE
            del_totals_m text := '';
            del_totals_y text := '';
            del_cache    text := '';
            res_sub_m    text := '';
            res_sub_y    text := '';
            res_sub_c    text := '';
            first_d      boolean := true;
        BEGIN
            FOR res_key IN SELECT * FROM jsonb_object_keys(resources)
            LOOP
                IF NOT first_d THEN
                    res_sub_m := res_sub_m || ', ';
                    res_sub_y := res_sub_y || ', ';
                    res_sub_c := res_sub_c || ', ';
                END IF;
                res_sub_m := res_sub_m || format('%I = accum.%I.%I - OLD.%I',
                    res_key, name || '_totals_month', res_key, res_key);
                res_sub_y := res_sub_y || format('%I = accum.%I.%I - OLD.%I',
                    res_key, name || '_totals_year', res_key, res_key);
                res_sub_c := res_sub_c || format('%I = accum.%I.%I - OLD.%I',
                    res_key, name || '_balance_cache', res_key, res_key);
                first_d := false;
            END LOOP;

            del_totals_m := format(
                'UPDATE accum.%I SET %s WHERE dim_hash = OLD.dim_hash AND period = date_trunc(''month'', OLD.period)::date',
                name || '_totals_month', res_sub_m);

            del_totals_y := format(
                'UPDATE accum.%I SET %s WHERE dim_hash = OLD.dim_hash AND period = date_trunc(''year'', OLD.period)::date',
                name || '_totals_year', res_sub_y);

            IF kind = 'balance' THEN
                del_cache := format(
                    'UPDATE accum.%I SET %s, version = accum.%I.version + 1 WHERE dim_hash = OLD.dim_hash',
                    name || '_balance_cache', res_sub_c, name || '_balance_cache');
            END IF;

            EXECUTE format(
                'CREATE OR REPLACE FUNCTION accum.%I() RETURNS trigger
                 LANGUAGE plpgsql AS $trg$
                 BEGIN
                     %s;
                     %s;
                     %s
                     RETURN OLD;
                 END;
                 $trg$',
                '_trg_' || name || '_after_delete',
                del_totals_m,
                del_totals_y,
                CASE WHEN del_cache != '' THEN del_cache || ';' ELSE '' END
            );

            EXECUTE format(
                'CREATE TRIGGER trg_%s_after_delete
                 AFTER DELETE ON accum.%I
                 FOR EACH ROW EXECUTE FUNCTION accum.%I()',
                name,
                name || '_movements',
                '_trg_' || name || '_after_delete'
            );
        END;
    END;
END;
$$;


-- ============================================================
-- REGISTER_DROP: Removes a register and all its infrastructure
-- ============================================================
CREATE OR REPLACE FUNCTION accum.register_drop(
    name  text,
    force boolean DEFAULT false
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    reg record;
BEGIN
    SELECT * INTO reg FROM accum._registers r WHERE r.name = register_drop.name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', name;
    END IF;

    -- Check for data if not forced
    IF NOT force THEN
        DECLARE
            cnt bigint;
        BEGIN
            EXECUTE format('SELECT count(*) FROM accum.%I', name || '_movements') INTO cnt;
            IF cnt > 0 THEN
                RAISE EXCEPTION 'Register "%" contains % movements. Use force := true to drop.',
                    name, cnt;
            END IF;
        END;
    END IF;

    -- Drop tables
    EXECUTE format('DROP TABLE IF EXISTS accum.%I CASCADE', name || '_movements');
    EXECUTE format('DROP TABLE IF EXISTS accum.%I CASCADE', name || '_totals_month');
    EXECUTE format('DROP TABLE IF EXISTS accum.%I CASCADE', name || '_totals_year');
    EXECUTE format('DROP TABLE IF EXISTS accum.%I CASCADE', name || '_balance_cache');
    EXECUTE format('DROP TABLE IF EXISTS accum.%I CASCADE', name || '_balance_cache_delta');

    -- Drop functions
    EXECUTE format('DROP FUNCTION IF EXISTS accum.%I CASCADE', '_hash_' || name);
    EXECUTE format('DROP FUNCTION IF EXISTS accum.%I CASCADE', '_trg_' || name || '_before_insert');
    EXECUTE format('DROP FUNCTION IF EXISTS accum.%I CASCADE', '_trg_' || name || '_after_insert');
    EXECUTE format('DROP FUNCTION IF EXISTS accum.%I CASCADE', '_trg_' || name || '_after_delete');

    -- Remove from registry
    DELETE FROM accum._registers WHERE _registers.name = register_drop.name;
END;
$$;


-- ============================================================
-- REGISTER_LIST: List all registers
-- ============================================================
CREATE OR REPLACE FUNCTION accum.register_list()
RETURNS TABLE(name text, kind text, dimensions int, resources int, created_at timestamptz)
LANGUAGE sql STABLE AS $$
    SELECT
        r.name,
        r.kind,
        (SELECT count(*)::int FROM jsonb_object_keys(r.dimensions)),
        (SELECT count(*)::int FROM jsonb_object_keys(r.resources)),
        r.created_at
    FROM accum._registers r
    ORDER BY r.created_at;
$$;


-- ============================================================
-- REGISTER_INFO: Detailed register info
-- ============================================================
CREATE OR REPLACE FUNCTION accum.register_info(p_name text)
RETURNS jsonb
LANGUAGE plpgsql STABLE AS $$
DECLARE
    reg record;
    result jsonb;
BEGIN
    SELECT * INTO reg FROM accum._registers r WHERE r.name = p_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_name;
    END IF;

    result := jsonb_build_object(
        'name',          reg.name,
        'kind',          reg.kind,
        'dimensions',    reg.dimensions,
        'resources',     reg.resources,
        'totals_period', reg.totals_period,
        'partition_by',  reg.partition_by,
        'high_write',    reg.high_write,
        'recorder_type', reg.recorder_type,
        'created_at',    reg.created_at
    );

    RETURN result;
END;
$$;


-- ============================================================
-- REGISTER_POST: Post movements to a register
-- ============================================================
CREATE OR REPLACE FUNCTION accum.register_post(
    p_register text,
    p_data     jsonb
) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    reg          record;
    movements    jsonb;
    mov          jsonb;
    dim_key      text;
    dim_type     text;
    res_key      text;
    res_type     text;
    col_names    text := '';
    col_values   text := '';
    sql_stmt     text;
    total_count  int := 0;
    first        boolean;
BEGIN
    -- Get register metadata
    SELECT * INTO reg FROM accum._registers r WHERE r.name = p_register;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_register;
    END IF;

    -- Normalize to array
    IF jsonb_typeof(p_data) = 'object' THEN
        movements := jsonb_build_array(p_data);
    ELSIF jsonb_typeof(p_data) = 'array' THEN
        movements := p_data;
    ELSE
        RAISE EXCEPTION 'Data must be a JSON object or array';
    END IF;

    -- Process each movement
    FOR mov IN SELECT * FROM jsonb_array_elements(movements)
    LOOP
        -- Validate recorder
        IF mov->>'recorder' IS NULL THEN
            RAISE EXCEPTION 'recorder is required';
        END IF;

        -- Validate period
        IF mov->>'period' IS NULL THEN
            RAISE EXCEPTION 'period is required';
        END IF;

        -- Build INSERT
        first := true;
        col_names := 'recorder, period';
        col_values := format('%L, %L::timestamptz', mov->>'recorder', mov->>'period');

        -- Add dimensions
        FOR dim_key, dim_type IN SELECT * FROM jsonb_each_text(reg.dimensions)
        LOOP
            IF mov->>dim_key IS NULL THEN
                RAISE EXCEPTION 'dimension "%" is required', dim_key;
            END IF;
            col_names := col_names || ', ' || quote_ident(dim_key);
            col_values := col_values || ', ' || format('%L::%s', mov->>dim_key, dim_type);
        END LOOP;

        -- Add resources
        FOR res_key, res_type IN SELECT * FROM jsonb_each_text(reg.resources)
        LOOP
            col_names := col_names || ', ' || quote_ident(res_key);
            col_values := col_values || ', ' || format('coalesce(%L, ''0'')::%s', mov->>res_key, res_type);
        END LOOP;

        -- Execute INSERT
        sql_stmt := format(
            'INSERT INTO accum.%I (%s) VALUES (%s)',
            p_register || '_movements',
            col_names,
            col_values
        );
        EXECUTE sql_stmt;
        total_count := total_count + 1;
    END LOOP;

    RETURN total_count;
END;
$$;


-- ============================================================
-- REGISTER_UNPOST: Cancel all movements by recorder
-- ============================================================
CREATE OR REPLACE FUNCTION accum.register_unpost(
    p_register text,
    p_recorder text
) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    reg   record;
    cnt   int;
BEGIN
    SELECT * INTO reg FROM accum._registers r WHERE r.name = p_register;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_register;
    END IF;

    EXECUTE format(
        'WITH deleted AS (
            DELETE FROM accum.%I WHERE recorder = %L RETURNING 1
        ) SELECT count(*) FROM deleted',
        p_register || '_movements',
        p_recorder
    ) INTO cnt;

    RETURN cnt;
END;
$$;


-- ============================================================
-- REGISTER_REPOST: Atomic re-post (unpost + post)
-- ============================================================
CREATE OR REPLACE FUNCTION accum.register_repost(
    p_register text,
    p_recorder text,
    p_data     jsonb
) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    cnt int;
BEGIN
    -- Unpost old movements
    PERFORM accum.register_unpost(p_register, p_recorder);

    -- Add recorder to each movement in data
    IF jsonb_typeof(p_data) = 'object' THEN
        p_data := jsonb_set(p_data, '{recorder}', to_jsonb(p_recorder));
    ELSIF jsonb_typeof(p_data) = 'array' THEN
        SELECT jsonb_agg(jsonb_set(elem, '{recorder}', to_jsonb(p_recorder)))
        INTO p_data
        FROM jsonb_array_elements(p_data) AS elem;
    END IF;

    -- Post new movements
    cnt := accum.register_post(p_register, p_data);
    RETURN cnt;
END;
$$;
