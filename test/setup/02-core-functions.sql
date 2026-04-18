-- test/setup/02-core-functions.sql
-- Core emulated functions for testing (pure SQL/PL/pgSQL prototypes)
-- These replicate the C extension API for testing without compiling C code.
-- The structure mirrors the real extension SQL files (03_ddl, 04_triggers, 07_registry_api).

-- ============================================================
-- DDL HELPERS (mirrors sql/03_ddl.sql with accum instead of @extschema@)
-- ============================================================

CREATE OR REPLACE FUNCTION accum._build_dim_columns(p_dimensions jsonb)
RETURNS text
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    dim_key  text;
    dim_type text;
    result   text := '';
BEGIN
    FOR dim_key, dim_type IN SELECT key, value FROM jsonb_each_text(p_dimensions) ORDER BY key
    LOOP
        result := result || format(', %I %s NOT NULL', dim_key, dim_type);
    END LOOP;
    RETURN result;
END;
$$;

CREATE OR REPLACE FUNCTION accum._build_res_columns(p_resources jsonb)
RETURNS text
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    res_key  text;
    res_type text;
    result   text := '';
BEGIN
    FOR res_key, res_type IN SELECT key, value FROM jsonb_each_text(p_resources) ORDER BY key
    LOOP
        result := result || format(', %I %s NOT NULL DEFAULT 0', res_key, res_type);
    END LOOP;
    RETURN result;
END;
$$;

CREATE OR REPLACE FUNCTION accum._ddl_create_movements_table(
    p_name          text,
    p_recorder_type text,
    p_dimensions    jsonb,
    p_resources     jsonb
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    col_defs text;
    res_defs text;
BEGIN
    col_defs := accum._build_dim_columns(p_dimensions);
    res_defs := accum._build_res_columns(p_resources);

    EXECUTE format(
        'CREATE TABLE accum.%I (
            id             uuid          DEFAULT gen_random_uuid(),
            recorded_at    timestamptz   DEFAULT now() NOT NULL,
            recorder       %s            NOT NULL,
            period         timestamptz   NOT NULL,
            movement_type  text          DEFAULT ''regular'' NOT NULL,
            dim_hash       bigint        NOT NULL
            %s
            %s,
            PRIMARY KEY (id, period)
        ) PARTITION BY RANGE (period)',
        p_name || '_movements',
        p_recorder_type,
        col_defs,
        res_defs
    );

    EXECUTE format(
        'CREATE TABLE accum.%I PARTITION OF accum.%I DEFAULT',
        p_name || '_movements_default',
        p_name || '_movements'
    );
END;
$$;

CREATE OR REPLACE FUNCTION accum._ddl_create_totals_tables(
    p_name       text,
    p_dimensions jsonb,
    p_resources  jsonb
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    col_defs text;
    res_defs text;
BEGIN
    col_defs := accum._build_dim_columns(p_dimensions);
    res_defs := accum._build_res_columns(p_resources);

    EXECUTE format(
        'CREATE TABLE accum.%I (
            dim_hash       bigint        NOT NULL,
            period         date          NOT NULL
            %s
            %s,
            PRIMARY KEY (dim_hash, period)
        )',
        p_name || '_totals_month',
        col_defs,
        res_defs
    );

    EXECUTE format(
        'CREATE TABLE accum.%I (
            dim_hash       bigint        NOT NULL,
            period         date          NOT NULL
            %s
            %s,
            PRIMARY KEY (dim_hash, period)
        )',
        p_name || '_totals_year',
        col_defs,
        res_defs
    );
END;
$$;

CREATE OR REPLACE FUNCTION accum._ddl_create_balance_cache(
    p_name       text,
    p_dimensions jsonb,
    p_resources  jsonb
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    col_defs text;
    res_defs text;
BEGIN
    col_defs := accum._build_dim_columns(p_dimensions);
    res_defs := accum._build_res_columns(p_resources);

    EXECUTE format(
        'CREATE TABLE accum.%I (
            dim_hash         bigint          NOT NULL PRIMARY KEY
            %s
            %s,
            last_movement_at timestamptz     NOT NULL DEFAULT now(),
            last_movement_id uuid,
            version          bigint          NOT NULL DEFAULT 0
        ) WITH (fillfactor = 70)',
        p_name || '_balance_cache',
        col_defs,
        res_defs
    );
END;
$$;

CREATE OR REPLACE FUNCTION accum._ddl_create_delta_buffer(
    p_name      text,
    p_resources jsonb
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    res_defs text;
BEGIN
    res_defs := accum._build_res_columns(p_resources);

    EXECUTE format(
        'CREATE UNLOGGED TABLE accum.%I (
            id         bigserial     PRIMARY KEY,
            dim_hash   bigint        NOT NULL
            %s,
            created_at timestamptz   DEFAULT now()
        )',
        p_name || '_balance_cache_delta',
        res_defs
    );
END;
$$;

CREATE OR REPLACE FUNCTION accum._ddl_create_indexes(
    p_name       text,
    p_kind       text,
    p_dimensions jsonb,
    p_high_write boolean
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    dim_key text;
BEGIN
    EXECUTE format('CREATE INDEX ON accum.%I (dim_hash, period)', p_name || '_movements');
    EXECUTE format('CREATE INDEX ON accum.%I (recorder)', p_name || '_movements');
    EXECUTE format('CREATE INDEX ON accum.%I (period)', p_name || '_movements');
    EXECUTE format('CREATE INDEX ON accum.%I (movement_type) WHERE movement_type != ''regular''', p_name || '_movements');

    IF p_kind = 'balance' THEN
        FOR dim_key IN SELECT key FROM jsonb_each_text(p_dimensions) ORDER BY key
        LOOP
            EXECUTE format('CREATE INDEX ON accum.%I (%I)',
                p_name || '_balance_cache', dim_key);
        END LOOP;
    END IF;

    -- Dimension indexes on totals tables for dimension-based queries
    FOR dim_key IN SELECT key FROM jsonb_each_text(p_dimensions) ORDER BY key
    LOOP
        EXECUTE format('CREATE INDEX ON accum.%I (%I)', p_name || '_totals_month', dim_key);
        EXECUTE format('CREATE INDEX ON accum.%I (%I)', p_name || '_totals_year', dim_key);
    END LOOP;

    IF p_high_write THEN
        EXECUTE format('CREATE INDEX ON accum.%I (dim_hash)', p_name || '_balance_cache_delta');
        EXECUTE format('CREATE INDEX ON accum.%I (created_at)', p_name || '_balance_cache_delta');
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION accum._ddl_drop_infrastructure(p_name text)
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE format('DROP TABLE IF EXISTS accum.%I CASCADE', p_name || '_movements');
    EXECUTE format('DROP TABLE IF EXISTS accum.%I CASCADE', p_name || '_totals_month');
    EXECUTE format('DROP TABLE IF EXISTS accum.%I CASCADE', p_name || '_totals_year');
    EXECUTE format('DROP TABLE IF EXISTS accum.%I CASCADE', p_name || '_balance_cache');
    EXECUTE format('DROP TABLE IF EXISTS accum.%I CASCADE', p_name || '_balance_cache_delta');

    EXECUTE format('DROP FUNCTION IF EXISTS accum.%I CASCADE', '_trg_' || p_name || '_before_insert');
    EXECUTE format('DROP FUNCTION IF EXISTS accum.%I CASCADE', '_trg_' || p_name || '_after_insert');
    EXECUTE format('DROP FUNCTION IF EXISTS accum.%I CASCADE', '_trg_' || p_name || '_after_delete');

    PERFORM accum._drop_read_functions(p_name);
    PERFORM accum._drop_hash_function(p_name);
END;
$$;

-- ============================================================
-- TRIGGER GENERATION (mirrors sql/04_triggers.sql)
-- ============================================================

CREATE OR REPLACE FUNCTION accum._generate_triggers(
    p_name       text,
    p_kind       text,
    p_dimensions jsonb,
    p_resources  jsonb,
    p_high_write boolean
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    dim_key          text;
    res_key          text;
    hash_call_args   text := '';
    dim_cols         text := '';
    res_cols         text := '';
    res_sum_cols     text := '';
    res_update_m     text := '';
    res_update_y     text := '';
    res_update_c     text := '';
    res_sub_m        text := '';
    res_sub_y        text := '';
    res_sub_c        text := '';
    totals_upsert_m  text;
    totals_upsert_y  text;
    cache_upsert     text := '';
    del_totals_m     text;
    del_totals_y     text;
    del_cache        text := '';
    trg_body_insert  text;
    first_dim        boolean := true;
    first_res        boolean := true;
BEGIN
    -- Build dimension column lists (ORDER BY key for determinism)
    FOR dim_key IN SELECT key FROM jsonb_each_text(p_dimensions) ORDER BY key
    LOOP
        IF NOT first_dim THEN
            hash_call_args := hash_call_args || ', ';
            dim_cols       := dim_cols       || ', ';
        END IF;
        hash_call_args := hash_call_args || format('NEW.%I', dim_key);
        dim_cols       := dim_cols       || format('%I', dim_key);
        first_dim := false;
    END LOOP;

    -- Build resource column lists (ORDER BY key for determinism)
    FOR res_key IN SELECT key FROM jsonb_each_text(p_resources) ORDER BY key
    LOOP
        IF NOT first_res THEN
            res_cols     := res_cols     || ', ';
            res_sum_cols := res_sum_cols || ', ';
            res_update_m := res_update_m || ', ';
            res_update_y := res_update_y || ', ';
            res_update_c := res_update_c || ', ';
            res_sub_m    := res_sub_m    || ', ';
            res_sub_y    := res_sub_y    || ', ';
            res_sub_c    := res_sub_c    || ', ';
        END IF;
        res_cols     := res_cols     || format('%I', res_key);
        res_sum_cols := res_sum_cols || format('SUM(%I) AS %I', res_key, res_key);
        res_update_m := res_update_m || format('%I = accum.%I.%I + EXCLUDED.%I',
            res_key, p_name || '_totals_month', res_key, res_key);
        res_update_y := res_update_y || format('%I = accum.%I.%I + EXCLUDED.%I',
            res_key, p_name || '_totals_year', res_key, res_key);
        res_update_c := res_update_c || format('%I = accum.%I.%I + EXCLUDED.%I',
            res_key, p_name || '_balance_cache', res_key, res_key);
        res_sub_m    := res_sub_m || format('%I = t.%I - agg.%I', res_key, res_key, res_key);
        res_sub_y    := res_sub_y || format('%I = t.%I - agg.%I', res_key, res_key, res_key);
        res_sub_c    := res_sub_c || format('%I = c.%I - agg.%I', res_key, res_key, res_key);
        first_res := false;
    END LOOP;

    -- ============================================================
    -- BEFORE INSERT trigger (FOR EACH ROW — modifies NEW)
    -- ============================================================
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
        '_trg_' || p_name || '_before_insert',
        '_hash_' || p_name,
        hash_call_args
    );

    EXECUTE format(
        'CREATE TRIGGER trg_%s_before_insert
         BEFORE INSERT ON accum.%I
         FOR EACH ROW EXECUTE FUNCTION accum.%I()',
        p_name,
        p_name || '_movements',
        '_trg_' || p_name || '_before_insert'
    );

    -- ============================================================
    -- AFTER INSERT trigger (FOR EACH STATEMENT — batch aggregation)
    -- ============================================================
    totals_upsert_m := format(
        'INSERT INTO accum.%I (dim_hash, period, %s, %s)
         SELECT dim_hash, date_trunc(''month'', period)::date, %s, %s
         FROM new_rows
         GROUP BY dim_hash, date_trunc(''month'', period)::date, %s
         ON CONFLICT (dim_hash, period) DO UPDATE SET %s',
        p_name || '_totals_month',
        dim_cols, res_cols,
        dim_cols, res_sum_cols,
        dim_cols,
        res_update_m
    );

    totals_upsert_y := format(
        'INSERT INTO accum.%I (dim_hash, period, %s, %s)
         SELECT dim_hash, date_trunc(''year'', period)::date, %s, %s
         FROM new_rows
         GROUP BY dim_hash, date_trunc(''year'', period)::date, %s
         ON CONFLICT (dim_hash, period) DO UPDATE SET %s',
        p_name || '_totals_year',
        dim_cols, res_cols,
        dim_cols, res_sum_cols,
        dim_cols,
        res_update_y
    );

    IF p_kind = 'balance' THEN
        IF NOT p_high_write THEN
            cache_upsert := format(
                'INSERT INTO accum.%I (dim_hash, %s, %s, last_movement_at, last_movement_id, version)
                 SELECT dim_hash, %s, %s, now(), (array_agg(id))[1], 1
                 FROM new_rows
                 GROUP BY dim_hash, %s
                 ON CONFLICT (dim_hash) DO UPDATE SET %s,
                     last_movement_at = EXCLUDED.last_movement_at,
                     last_movement_id = EXCLUDED.last_movement_id,
                     version = accum.%I.version + 1',
                p_name || '_balance_cache',
                dim_cols, res_cols,
                dim_cols, res_sum_cols,
                dim_cols,
                res_update_c,
                p_name || '_balance_cache'
            );
        ELSE
            -- High-write: seed balance_cache rows (zeroed resources) then append to delta buffer
            cache_upsert := format(
                'INSERT INTO accum.%I (dim_hash, %s, last_movement_at, last_movement_id, version)
                 SELECT DISTINCT ON (dim_hash) dim_hash, %s, now(), id, 0
                 FROM new_rows
                 ON CONFLICT (dim_hash) DO NOTHING;
                 INSERT INTO accum.%I (dim_hash, %s)
                 SELECT dim_hash, %s
                 FROM new_rows',
                p_name || '_balance_cache',
                dim_cols,
                dim_cols,
                p_name || '_balance_cache_delta',
                res_cols,
                res_cols
            );
        END IF;
    END IF;

    trg_body_insert := totals_upsert_m || '; ' || totals_upsert_y || ';';
    IF cache_upsert != '' THEN
        trg_body_insert := trg_body_insert || ' ' || cache_upsert || ';';
    END IF;

    EXECUTE format(
        'CREATE OR REPLACE FUNCTION accum.%I() RETURNS trigger
         LANGUAGE plpgsql AS $trg$
         BEGIN
             %s
             RETURN NULL;
         END;
         $trg$',
        '_trg_' || p_name || '_after_insert',
        trg_body_insert
    );

    EXECUTE format(
        'CREATE TRIGGER trg_%s_after_insert
         AFTER INSERT ON accum.%I
         REFERENCING NEW TABLE AS new_rows
         FOR EACH STATEMENT EXECUTE FUNCTION accum.%I()',
        p_name,
        p_name || '_movements',
        '_trg_' || p_name || '_after_insert'
    );

    -- ============================================================
    -- AFTER DELETE trigger (FOR EACH STATEMENT — batch aggregation)
    -- ============================================================
    del_totals_m := format(
        'UPDATE accum.%I t SET %s
         FROM (SELECT dim_hash, date_trunc(''month'', period)::date AS period, %s
               FROM old_rows GROUP BY dim_hash, date_trunc(''month'', period)::date) agg
         WHERE t.dim_hash = agg.dim_hash AND t.period = agg.period',
        p_name || '_totals_month', res_sub_m, res_sum_cols);

    del_totals_y := format(
        'UPDATE accum.%I t SET %s
         FROM (SELECT dim_hash, date_trunc(''year'', period)::date AS period, %s
               FROM old_rows GROUP BY dim_hash, date_trunc(''year'', period)::date) agg
         WHERE t.dim_hash = agg.dim_hash AND t.period = agg.period',
        p_name || '_totals_year', res_sub_y, res_sum_cols);

    IF p_kind = 'balance' THEN
        del_cache := format(
            'UPDATE accum.%I c SET %s, version = c.version + 1
             FROM (SELECT dim_hash, %s FROM old_rows GROUP BY dim_hash) agg
             WHERE c.dim_hash = agg.dim_hash',
            p_name || '_balance_cache', res_sub_c, res_sum_cols);
    END IF;

    EXECUTE format(
        'CREATE OR REPLACE FUNCTION accum.%I() RETURNS trigger
         LANGUAGE plpgsql AS $trg$
         BEGIN
             %s;
             %s;
             %s
             RETURN NULL;
         END;
         $trg$',
        '_trg_' || p_name || '_after_delete',
        del_totals_m,
        del_totals_y,
        CASE WHEN del_cache != '' THEN del_cache || ';' ELSE '' END
    );

    EXECUTE format(
        'CREATE TRIGGER trg_%s_after_delete
         AFTER DELETE ON accum.%I
         REFERENCING OLD TABLE AS old_rows
         FOR EACH STATEMENT EXECUTE FUNCTION accum.%I()',
        p_name,
        p_name || '_movements',
        '_trg_' || p_name || '_after_delete'
    );
END;
$$;

-- ============================================================
-- REGISTRY API (mirrors sql/07_registry_api.sql)
-- ============================================================

CREATE OR REPLACE FUNCTION accum.register_create(
    name          text,
    dimensions    jsonb,
    resources     jsonb,
    kind          text    DEFAULT 'balance',
    totals_period text    DEFAULT 'day',
    partition_by  text    DEFAULT 'month',
    high_write    boolean DEFAULT false,
    recorder_type text    DEFAULT 'text'
) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    -- Validate
    PERFORM accum._validate_name(name);
    PERFORM accum._validate_dimensions(dimensions);
    PERFORM accum._validate_resources(resources);

    IF kind NOT IN ('balance', 'turnover') THEN
        RAISE EXCEPTION 'Invalid kind: %. Must be balance or turnover', kind;
    END IF;
    IF totals_period NOT IN ('day', 'month', 'year') THEN
        RAISE EXCEPTION 'Invalid totals_period: %', totals_period;
    END IF;
    IF partition_by NOT IN ('day', 'month', 'quarter', 'year') THEN
        RAISE EXCEPTION 'Invalid partition_by: %', partition_by;
    END IF;

    IF accum._register_exists(name) THEN
        RAISE EXCEPTION 'Register "%" already exists', name;
    END IF;

    -- Save metadata
    PERFORM accum._register_put(name, kind, dimensions, resources,
        totals_period, partition_by, high_write, recorder_type);

    -- DDL: tables, indexes, hash, triggers — with cleanup on failure
    BEGIN
        PERFORM accum._ddl_create_movements_table(name, recorder_type, dimensions, resources);
        PERFORM accum._ddl_create_totals_tables(name, dimensions, resources);

        IF kind = 'balance' THEN
            PERFORM accum._ddl_create_balance_cache(name, dimensions, resources);
        END IF;

        IF high_write THEN
            PERFORM accum._ddl_create_delta_buffer(name, resources);
        END IF;

        PERFORM accum._ddl_create_indexes(name, kind, dimensions, high_write);
        PERFORM accum._generate_hash_function(name, dimensions);
        PERFORM accum._generate_triggers(name, kind, dimensions, resources, high_write);
        PERFORM accum._generate_read_functions(name, kind, dimensions, resources, high_write);

        -- Create initial partitions
        PERFORM accum._create_initial_partitions(name, partition_by, 3);
    EXCEPTION WHEN OTHERS THEN
        -- Cleanup partially created objects on failure
        PERFORM accum._ddl_drop_infrastructure(name);
        PERFORM accum._register_delete(name);
        RAISE;
    END;
END;
$$;

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

    PERFORM accum._ddl_drop_infrastructure(name);
    PERFORM accum._register_delete(name);
END;
$$;

DROP FUNCTION IF EXISTS accum.register_list();
CREATE OR REPLACE FUNCTION accum.register_list()
RETURNS TABLE(name text, kind text, dimensions int, resources int, movements_count bigint, created_at timestamptz)
LANGUAGE plpgsql STABLE AS $$
DECLARE
    reg record;
    cnt bigint;
BEGIN
    FOR reg IN SELECT * FROM accum._registers r ORDER BY r.created_at
    LOOP
        BEGIN
            EXECUTE format('SELECT count(*) FROM accum.%I',
                reg.name || '_movements') INTO cnt;
        EXCEPTION WHEN undefined_table THEN
            cnt := 0;
        END;

        name       := reg.name;
        kind       := reg.kind;
        dimensions := (SELECT count(*)::int FROM jsonb_object_keys(reg.dimensions));
        resources  := (SELECT count(*)::int FROM jsonb_object_keys(reg.resources));
        movements_count := cnt;
        created_at := reg.created_at;
        RETURN NEXT;
    END LOOP;
END;
$$;


-- ============================================================
-- REGISTER_INFO: Detailed register info
-- ============================================================
CREATE OR REPLACE FUNCTION accum.register_info(p_name text)
RETURNS jsonb
LANGUAGE plpgsql STABLE AS $$
DECLARE
    reg           record;
    result        jsonb;
    movements_cnt bigint := 0;
    partitions    jsonb  := '[]'::jsonb;
    tables_info   jsonb;
BEGIN
    SELECT * INTO reg FROM accum._registers r WHERE r.name = p_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_name;
    END IF;

    -- Movement count
    BEGIN
        EXECUTE format('SELECT count(*) FROM accum.%I',
            p_name || '_movements') INTO movements_cnt;
    EXCEPTION WHEN undefined_table THEN
        movements_cnt := 0;
    END;

    -- Partition info
    BEGIN
        SELECT jsonb_agg(jsonb_build_object(
            'name', child.relname,
            'range', pg_get_expr(c.relpartbound, c.oid)
        ) ORDER BY child.relname)
        INTO partitions
        FROM pg_inherits i
        JOIN pg_class parent ON i.inhparent = parent.oid
        JOIN pg_class child ON i.inhrelid = child.oid
        JOIN pg_catalog.pg_class c ON c.oid = child.oid
        JOIN pg_namespace ns ON parent.relnamespace = ns.oid
        WHERE parent.relname = p_name || '_movements'
          AND ns.nspname = 'accum';
    EXCEPTION WHEN OTHERS THEN
        partitions := '[]'::jsonb;
    END;

    IF partitions IS NULL THEN
        partitions := '[]'::jsonb;
    END IF;

    tables_info := jsonb_build_object(
        'movements', p_name || '_movements',
        'totals_month', p_name || '_totals_month',
        'totals_year', p_name || '_totals_year'
    );

    IF reg.kind = 'balance' THEN
        tables_info := tables_info || jsonb_build_object(
            'balance_cache', p_name || '_balance_cache');
    END IF;

    IF reg.high_write THEN
        tables_info := tables_info || jsonb_build_object(
            'balance_cache_delta', p_name || '_balance_cache_delta');
    END IF;

    result := jsonb_build_object(
        'name',            reg.name,
        'kind',            reg.kind,
        'dimensions',      reg.dimensions,
        'resources',       reg.resources,
        'totals_period',   reg.totals_period,
        'partition_by',    reg.partition_by,
        'high_write',      reg.high_write,
        'recorder_type',   reg.recorder_type,
        'created_at',      reg.created_at,
        'movements_count', movements_cnt,
        'tables',          tables_info,
        'partitions',      partitions
    );

    RETURN result;
END;
$$;


-- ============================================================
-- REGISTER_POST: Post movements to a register (batch INSERT)
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
    col_list     text := 'recorder, period';
    tuple        text;
    values_str   text := '';
    total_count  int := 0;
BEGIN
    SELECT * INTO reg FROM accum._registers r WHERE r.name = p_register;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_register;
    END IF;

    IF jsonb_typeof(p_data) = 'object' THEN
        movements := jsonb_build_array(p_data);
    ELSIF jsonb_typeof(p_data) = 'array' THEN
        movements := p_data;
    ELSE
        RAISE EXCEPTION 'Data must be a JSON object or array';
    END IF;

    -- Build column list once from register metadata
    FOR dim_key IN SELECT key FROM jsonb_each_text(reg.dimensions) ORDER BY key
    LOOP
        col_list := col_list || ', ' || quote_ident(dim_key);
    END LOOP;

    FOR res_key IN SELECT key FROM jsonb_each_text(reg.resources) ORDER BY key
    LOOP
        col_list := col_list || ', ' || quote_ident(res_key);
    END LOOP;

    -- Build VALUES tuples for batch INSERT
    FOR mov IN SELECT * FROM jsonb_array_elements(movements)
    LOOP
        IF mov->>'recorder' IS NULL THEN
            RAISE EXCEPTION 'recorder is required';
        END IF;
        IF mov->>'period' IS NULL THEN
            RAISE EXCEPTION 'period is required';
        END IF;

        tuple := format('%L, %L::timestamptz', mov->>'recorder', mov->>'period');

        FOR dim_key, dim_type IN SELECT key, value FROM jsonb_each_text(reg.dimensions) ORDER BY key
        LOOP
            IF mov->>dim_key IS NULL THEN
                RAISE EXCEPTION 'dimension "%" is required', dim_key;
            END IF;
            tuple := tuple || ', ' || format('%L::%s', mov->>dim_key, dim_type);
        END LOOP;

        FOR res_key, res_type IN SELECT key, value FROM jsonb_each_text(reg.resources) ORDER BY key
        LOOP
            tuple := tuple || ', ' || format('coalesce(%L, ''0'')::%s', mov->>res_key, res_type);
        END LOOP;

        IF values_str != '' THEN
            values_str := values_str || ', ';
        END IF;
        values_str := values_str || '(' || tuple || ')';
        total_count := total_count + 1;
    END LOOP;

    -- Ensure partitions exist for all periods (before INSERT to avoid DDL-during-DML)
    FOR mov IN SELECT * FROM jsonb_array_elements(movements)
    LOOP
        PERFORM accum._ensure_partition(p_register, (mov->>'period')::timestamptz);
    END LOOP;

    -- Single batch INSERT — triggers fire once per statement
    IF total_count > 0 THEN
        EXECUTE format('INSERT INTO accum.%I (%s) VALUES %s',
            p_register || '_movements', col_list, values_str);
    END IF;

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
    PERFORM accum.register_unpost(p_register, p_recorder);

    IF jsonb_typeof(p_data) = 'object' THEN
        p_data := jsonb_set(p_data, '{recorder}', to_jsonb(p_recorder));
    ELSIF jsonb_typeof(p_data) = 'array' THEN
        SELECT jsonb_agg(jsonb_set(elem, '{recorder}', to_jsonb(p_recorder)))
        INTO p_data
        FROM jsonb_array_elements(p_data) AS elem;
    END IF;

    cnt := accum.register_post(p_register, p_data);
    RETURN cnt;
END;
$$;


-- ============================================================
-- READ API: Internal generic functions (mirrors sql/06_read_api.sql)
-- ============================================================

CREATE OR REPLACE FUNCTION accum._balance_internal(
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
    SELECT * INTO reg FROM accum._registers r WHERE r.name = p_register;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_register;
    END IF;

    IF reg.kind != 'balance' THEN
        RAISE EXCEPTION 'balance() is only available for balance-type registers, "%" is %',
            p_register, reg.kind;
    END IF;

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

    dim_count := (SELECT count(*)::int FROM jsonb_object_keys(reg.dimensions));
    provided  := 0;
    IF p_dims IS NOT NULL AND p_dims != '{}'::jsonb THEN
        provided := (SELECT count(*)::int FROM jsonb_object_keys(p_dims));
    END IF;
    all_dims := (provided = dim_count AND provided > 0);

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

    IF all_dims THEN
        EXECUTE format('SELECT accum.%I(%s)', '_hash_' || p_register, hash_args)
            INTO v_dim_hash;
    END IF;

    IF p_at_date IS NULL THEN
        IF NOT reg.high_write THEN
            IF all_dims THEN
                EXECUTE format(
                    'SELECT jsonb_build_object(%s)
                     FROM accum.%I WHERE dim_hash = $1',
                    res_agg, p_register || '_balance_cache'
                ) INTO result USING v_dim_hash;
            ELSE
                EXECUTE format(
                    'SELECT jsonb_build_object(%s)
                     FROM accum.%I WHERE TRUE %s',
                    res_agg, p_register || '_balance_cache', dim_where
                ) INTO result;
            END IF;
        ELSE
            IF all_dims THEN
                EXECUTE format(
                    'SELECT jsonb_build_object(%s) FROM (
                         SELECT %s FROM accum.%I WHERE dim_hash = $1
                         UNION ALL
                         SELECT %s FROM accum.%I WHERE dim_hash = $1
                     ) _combined',
                    res_agg,
                    res_cols, p_register || '_balance_cache',
                    res_cols, p_register || '_balance_cache_delta'
                ) INTO result USING v_dim_hash;
            ELSE
                EXECUTE format(
                    'SELECT jsonb_build_object(%s) FROM (
                         SELECT %s FROM accum.%I WHERE TRUE %s
                         UNION ALL
                         SELECT d.%s FROM accum.%I d
                         WHERE d.dim_hash IN (
                             SELECT c.dim_hash FROM accum.%I c WHERE TRUE %s
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
        IF all_dims THEN
            EXECUTE format(
                'SELECT jsonb_build_object(%s) FROM (
                     SELECT %s FROM accum.%I
                     WHERE dim_hash = $1
                       AND period < date_trunc(''year'', $2)::date
                     UNION ALL
                     SELECT %s FROM accum.%I
                     WHERE dim_hash = $1
                       AND period >= date_trunc(''year'', $2)::date
                       AND period < date_trunc(''month'', $2)::date
                     UNION ALL
                     SELECT %s FROM accum.%I
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
                     SELECT %s FROM accum.%I
                     WHERE TRUE %s
                       AND period < date_trunc(''year'', $1)::date
                     UNION ALL
                     SELECT %s FROM accum.%I
                     WHERE TRUE %s
                       AND period >= date_trunc(''year'', $1)::date
                       AND period < date_trunc(''month'', $1)::date
                     UNION ALL
                     SELECT %s FROM accum.%I
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

    IF result IS NULL THEN
        SELECT jsonb_object_agg(key, 0) INTO result
        FROM jsonb_each_text(reg.resources);
    END IF;

    RETURN result;
END;
$$;


CREATE OR REPLACE FUNCTION accum._turnover_internal(
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
    query         text;
BEGIN
    SELECT * INTO reg FROM accum._registers r WHERE r.name = p_register;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_register;
    END IF;

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

    IF p_dims IS NOT NULL AND p_dims != '{}'::jsonb THEN
        FOR dim_key, dim_type IN SELECT key, value FROM jsonb_each_text(reg.dimensions) ORDER BY key
        LOOP
            IF p_dims ? dim_key THEN
                dim_where := dim_where || format(' AND %I = %L::%s',
                    dim_key, p_dims->>dim_key, dim_type);
            END IF;
        END LOOP;
    END IF;

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

    v_from_month := date_trunc('month', p_from)::date;
    v_to_month   := date_trunc('month', p_to)::date;

    DECLARE
        v_first_full_month date;
        v_last_full_month  date;
    BEGIN
        IF p_from = date_trunc('month', p_from) THEN
            v_first_full_month := v_from_month;
        ELSE
            v_first_full_month := (v_from_month + interval '1 month')::date;
        END IF;

        IF p_to >= (date_trunc('month', p_to) + interval '1 month' - interval '1 day')::timestamptz THEN
            v_last_full_month := v_to_month;
        ELSE
            v_last_full_month := (v_to_month - interval '1 month')::date;
        END IF;

        query := format(
            'SELECT jsonb_build_object(%s %s) FROM (', res_agg,
            CASE WHEN group_select != '' THEN ', ' || group_select ELSE '' END);

        IF v_first_full_month > v_from_month OR p_from != date_trunc('month', p_from) THEN
            query := query || format(
                'SELECT %s %s FROM accum.%I
                 WHERE period >= $1 AND period < $3::timestamptz
                 %s',
                CASE WHEN group_cols != '' THEN group_cols || ', ' ELSE '' END,
                res_cols, p_register || '_movements', dim_where);
        ELSE
            query := query || format(
                'SELECT %s %s FROM accum.%I WHERE FALSE',
                CASE WHEN group_cols != '' THEN group_cols || ', ' ELSE '' END,
                res_cols, p_register || '_movements');
        END IF;

        query := query || ' UNION ALL ';

        query := query || format(
            'SELECT %s %s FROM accum.%I
             WHERE period >= $3 AND period <= $4
             %s',
            CASE WHEN group_cols != '' THEN group_cols || ', ' ELSE '' END,
            res_cols, p_register || '_totals_month', dim_where);

        query := query || ' UNION ALL ';

        IF v_last_full_month < v_to_month OR
           p_to < (date_trunc('month', p_to) + interval '1 month' - interval '1 day')::timestamptz THEN
            query := query || format(
                'SELECT %s %s FROM accum.%I
                 WHERE period > ($4 + interval ''1 month'' - interval ''1 day'')::timestamptz
                   AND period <= $2
                 %s',
                CASE WHEN group_cols != '' THEN group_cols || ', ' ELSE '' END,
                res_cols, p_register || '_movements', dim_where);
        ELSE
            query := query || format(
                'SELECT %s %s FROM accum.%I WHERE FALSE',
                CASE WHEN group_cols != '' THEN group_cols || ', ' ELSE '' END,
                res_cols, p_register || '_movements');
        END IF;

        query := query || ') _turnover' || group_by_sql;

        RETURN QUERY EXECUTE query
            USING p_from, p_to, v_first_full_month, v_last_full_month;
    END;
END;
$$;


CREATE OR REPLACE FUNCTION accum._movements_internal(
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
    SELECT * INTO reg FROM accum._registers r WHERE r.name = p_register;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_register;
    END IF;

    IF p_dims IS NOT NULL AND p_dims != '{}'::jsonb THEN
        FOR dim_key, dim_type IN SELECT key, value FROM jsonb_each_text(reg.dimensions) ORDER BY key
        LOOP
            IF p_dims ? dim_key THEN
                dim_where := dim_where || format(' AND %I = %L::%s',
                    dim_key, p_dims->>dim_key, dim_type);
            END IF;
        END LOOP;
    END IF;

    query := format(
        'SELECT to_jsonb(m) FROM accum.%I m
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


CREATE OR REPLACE FUNCTION accum._generate_read_functions(
    p_name       text,
    p_kind       text,
    p_dimensions jsonb,
    p_resources  jsonb,
    p_high_write boolean
) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    IF p_kind = 'balance' THEN
        EXECUTE format(
            'CREATE OR REPLACE FUNCTION accum.%I(
                dimensions jsonb DEFAULT NULL,
                at_date    timestamptz DEFAULT NULL
            ) RETURNS jsonb
            LANGUAGE sql STABLE AS $fn$
                SELECT accum._balance_internal(%L, dimensions, at_date);
            $fn$',
            p_name || '_balance',
            p_name
        );
    END IF;

    EXECUTE format(
        'CREATE OR REPLACE FUNCTION accum.%I(
            from_date  timestamptz DEFAULT NULL,
            to_date    timestamptz DEFAULT NULL,
            dimensions jsonb DEFAULT NULL,
            group_by   jsonb DEFAULT NULL
        ) RETURNS SETOF jsonb
        LANGUAGE sql STABLE AS $fn$
            SELECT accum._turnover_internal(%L, from_date, to_date, dimensions, group_by);
        $fn$',
        p_name || '_turnover',
        p_name
    );

    EXECUTE format(
        'CREATE OR REPLACE FUNCTION accum.%I(
            p_recorder text DEFAULT NULL,
            from_date  timestamptz DEFAULT NULL,
            to_date    timestamptz DEFAULT NULL,
            dimensions jsonb DEFAULT NULL
        ) RETURNS SETOF jsonb
        LANGUAGE sql STABLE AS $fn$
            SELECT accum._movements_internal(%L, p_recorder, from_date, to_date, dimensions);
        $fn$',
        p_name || '_movements',
        p_name
    );
END;
$$;


CREATE OR REPLACE FUNCTION accum._drop_read_functions(p_name text)
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE format('DROP FUNCTION IF EXISTS accum.%I(jsonb, timestamptz) CASCADE',
        p_name || '_balance');
    EXECUTE format('DROP FUNCTION IF EXISTS accum.%I(timestamptz, timestamptz, jsonb, jsonb) CASCADE',
        p_name || '_turnover');
    EXECUTE format('DROP FUNCTION IF EXISTS accum.%I(text, timestamptz, timestamptz, jsonb) CASCADE',
        p_name || '_movements');
END;
$$;


-- ============================================================
-- REBUILD TOTALS FROM MOVEMENTS (helper for alter)
-- ============================================================
CREATE OR REPLACE FUNCTION accum._rebuild_totals_from_movements(
    p_name       text,
    p_kind       text,
    p_dimensions jsonb,
    p_resources  jsonb
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    dim_cols     text := '';
    res_cols     text := '';
    res_sum_cols text := '';
    res_key      text;
    dim_key      text;
    first_dim    boolean := true;
    first_res    boolean := true;
BEGIN
    FOR dim_key IN SELECT key FROM jsonb_each_text(p_dimensions) ORDER BY key
    LOOP
        IF NOT first_dim THEN dim_cols := dim_cols || ', '; END IF;
        dim_cols := dim_cols || format('%I', dim_key);
        first_dim := false;
    END LOOP;

    FOR res_key IN SELECT key FROM jsonb_each_text(p_resources) ORDER BY key
    LOOP
        IF NOT first_res THEN
            res_cols     := res_cols || ', ';
            res_sum_cols := res_sum_cols || ', ';
        END IF;
        res_cols     := res_cols || format('%I', res_key);
        res_sum_cols := res_sum_cols || format('SUM(%I) AS %I', res_key, res_key);
        first_res := false;
    END LOOP;

    -- Rebuild totals_month
    EXECUTE format(
        'INSERT INTO accum.%I (dim_hash, period, %s, %s)
         SELECT dim_hash, date_trunc(''month'', period)::date, %s, %s
         FROM accum.%I
         GROUP BY dim_hash, date_trunc(''month'', period)::date, %s',
        p_name || '_totals_month',
        dim_cols, res_cols,
        dim_cols, res_sum_cols,
        p_name || '_movements',
        dim_cols
    );

    -- Rebuild totals_year
    EXECUTE format(
        'INSERT INTO accum.%I (dim_hash, period, %s, %s)
         SELECT dim_hash, date_trunc(''year'', period)::date, %s, %s
         FROM accum.%I
         GROUP BY dim_hash, date_trunc(''year'', period)::date, %s',
        p_name || '_totals_year',
        dim_cols, res_cols,
        dim_cols, res_sum_cols,
        p_name || '_movements',
        dim_cols
    );

    -- Rebuild balance_cache
    IF p_kind = 'balance' THEN
        EXECUTE format(
            'INSERT INTO accum.%I (dim_hash, %s, %s)
             SELECT dim_hash, %s, %s
             FROM accum.%I
             GROUP BY dim_hash, %s',
            p_name || '_balance_cache',
            dim_cols, res_cols,
            dim_cols, res_sum_cols,
            p_name || '_movements',
            dim_cols
        );
    END IF;
END;
$$;


-- ============================================================
-- REGISTER_ALTER: Modify an existing register
-- ============================================================
CREATE OR REPLACE FUNCTION accum.register_alter(
    p_name            text,
    add_dimensions    jsonb    DEFAULT NULL,
    add_resources     jsonb    DEFAULT NULL,
    high_write        boolean  DEFAULT NULL
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    reg             record;
    dim_key         text;
    dim_type        text;
    res_key         text;
    res_type        text;
    new_dimensions  jsonb;
    new_resources   jsonb;
    dims_added      boolean := false;
BEGIN
    -- 1. Get current register metadata
    SELECT * INTO reg FROM accum._registers r WHERE r.name = p_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_name;
    END IF;

    new_dimensions := reg.dimensions;
    new_resources  := reg.resources;

    -- 2. Add new dimensions (requires recalculation)
    IF add_dimensions IS NOT NULL AND add_dimensions != '{}'::jsonb THEN
        PERFORM accum._validate_dimensions(add_dimensions);

        FOR dim_key, dim_type IN SELECT * FROM jsonb_each_text(add_dimensions)
        LOOP
            IF reg.dimensions ? dim_key THEN
                RAISE EXCEPTION 'Dimension "%" already exists in register "%"', dim_key, p_name;
            END IF;

            EXECUTE format('ALTER TABLE accum.%I ADD COLUMN %I %s',
                p_name || '_movements', dim_key, dim_type);
            EXECUTE format('ALTER TABLE accum.%I ADD COLUMN %I %s',
                p_name || '_totals_month', dim_key, dim_type);
            EXECUTE format('ALTER TABLE accum.%I ADD COLUMN %I %s',
                p_name || '_totals_year', dim_key, dim_type);
            IF reg.kind = 'balance' THEN
                EXECUTE format('ALTER TABLE accum.%I ADD COLUMN %I %s',
                    p_name || '_balance_cache', dim_key, dim_type);
            END IF;
        END LOOP;

        new_dimensions := reg.dimensions || add_dimensions;
        dims_added := true;
    END IF;

    -- 3. Add new resources (no recalculation needed)
    IF add_resources IS NOT NULL AND add_resources != '{}'::jsonb THEN
        PERFORM accum._validate_resources(add_resources);

        FOR res_key, res_type IN SELECT * FROM jsonb_each_text(add_resources)
        LOOP
            IF reg.resources ? res_key THEN
                RAISE EXCEPTION 'Resource "%" already exists in register "%"', res_key, p_name;
            END IF;

            EXECUTE format('ALTER TABLE accum.%I ADD COLUMN %I %s NOT NULL DEFAULT 0',
                p_name || '_movements', res_key, res_type);
            EXECUTE format('ALTER TABLE accum.%I ADD COLUMN %I %s NOT NULL DEFAULT 0',
                p_name || '_totals_month', res_key, res_type);
            EXECUTE format('ALTER TABLE accum.%I ADD COLUMN %I %s NOT NULL DEFAULT 0',
                p_name || '_totals_year', res_key, res_type);
            IF reg.kind = 'balance' THEN
                EXECUTE format('ALTER TABLE accum.%I ADD COLUMN %I %s NOT NULL DEFAULT 0',
                    p_name || '_balance_cache', res_key, res_type);
            END IF;
        END LOOP;

        new_resources := reg.resources || add_resources;
    END IF;

    -- 4. Toggle high_write mode
    IF high_write IS NOT NULL AND high_write IS DISTINCT FROM reg.high_write THEN
        IF high_write AND NOT reg.high_write THEN
            PERFORM accum._ddl_create_delta_buffer(p_name, new_resources);
            EXECUTE format('CREATE INDEX ON accum.%I (dim_hash)',
                p_name || '_balance_cache_delta');
            EXECUTE format('CREATE INDEX ON accum.%I (created_at)',
                p_name || '_balance_cache_delta');
        ELSIF NOT high_write AND reg.high_write THEN
            EXECUTE format('DROP TABLE IF EXISTS accum.%I CASCADE',
                p_name || '_balance_cache_delta');
        END IF;
    END IF;

    -- 5. Regenerate hash function with new dimensions
    IF dims_added THEN
        -- Drop old hash function (may have different arg count)
        PERFORM accum._drop_hash_function(p_name);
        PERFORM accum._generate_hash_function(p_name, new_dimensions);

        -- Recalculate dim_hash for existing movements
        DECLARE
            hash_args text := '';
            first_d   boolean := true;
            d_key     text;
        BEGIN
            FOR d_key IN SELECT key FROM jsonb_each_text(new_dimensions) ORDER BY key
            LOOP
                IF NOT first_d THEN hash_args := hash_args || ', '; END IF;
                hash_args := hash_args || format('%I', d_key);
                first_d := false;
            END LOOP;

            EXECUTE format('UPDATE accum.%I SET dim_hash = accum.%I(%s)',
                p_name || '_movements',
                '_hash_' || p_name,
                hash_args
            );
        END;

        -- Rebuild totals and cache from scratch
        EXECUTE format('TRUNCATE accum.%I', p_name || '_totals_month');
        EXECUTE format('TRUNCATE accum.%I', p_name || '_totals_year');
        IF reg.kind = 'balance' THEN
            EXECUTE format('TRUNCATE accum.%I', p_name || '_balance_cache');
        END IF;

        PERFORM accum._rebuild_totals_from_movements(p_name, reg.kind, new_dimensions, new_resources);
    END IF;

    -- 6. Regenerate triggers
    IF dims_added OR (add_resources IS NOT NULL AND add_resources != '{}'::jsonb) OR
       (high_write IS NOT NULL AND high_write IS DISTINCT FROM reg.high_write) THEN
        EXECUTE format('DROP TRIGGER IF EXISTS trg_%s_before_insert ON accum.%I',
            p_name, p_name || '_movements');
        EXECUTE format('DROP TRIGGER IF EXISTS trg_%s_after_insert ON accum.%I',
            p_name, p_name || '_movements');
        EXECUTE format('DROP TRIGGER IF EXISTS trg_%s_after_delete ON accum.%I',
            p_name, p_name || '_movements');
        EXECUTE format('DROP FUNCTION IF EXISTS accum.%I CASCADE',
            '_trg_' || p_name || '_before_insert');
        EXECUTE format('DROP FUNCTION IF EXISTS accum.%I CASCADE',
            '_trg_' || p_name || '_after_insert');
        EXECUTE format('DROP FUNCTION IF EXISTS accum.%I CASCADE',
            '_trg_' || p_name || '_after_delete');

        PERFORM accum._generate_triggers(
            p_name, reg.kind, new_dimensions, new_resources,
            coalesce(high_write, reg.high_write)
        );

        PERFORM accum._drop_read_functions(p_name);
        PERFORM accum._generate_read_functions(
            p_name, reg.kind, new_dimensions, new_resources,
            coalesce(high_write, reg.high_write)
        );
    END IF;

    -- 7. Update metadata
    PERFORM accum._register_put(
        p_name, reg.kind, new_dimensions, new_resources,
        reg.totals_period, reg.partition_by,
        coalesce(high_write, reg.high_write), reg.recorder_type
    );
END;
$$;


-- ============================================================
-- DELTA BUFFER FUNCTIONS (mirrors sql/08_delta_buffer.sql)
-- ============================================================

CREATE OR REPLACE FUNCTION accum._delta_merge_register(
    p_name        text,
    p_max_age     interval DEFAULT interval '2 seconds',
    p_batch_size  int DEFAULT 10000
) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    reg          record;
    res_key      text;
    res_cols     text := '';
    res_sum_cols text := '';
    res_update   text := '';
    first_res    boolean := true;
    consumed     int;
BEGIN
    SELECT * INTO reg FROM accum._registers r WHERE r.name = p_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_name;
    END IF;

    IF NOT reg.high_write THEN
        RAISE EXCEPTION 'Register "%" is not in high_write mode', p_name;
    END IF;

    IF reg.kind != 'balance' THEN
        RAISE EXCEPTION 'Delta merge only applies to balance registers, "%" is %', p_name, reg.kind;
    END IF;

    FOR res_key IN SELECT key FROM jsonb_each_text(reg.resources) ORDER BY key
    LOOP
        IF NOT first_res THEN
            res_cols     := res_cols     || ', ';
            res_sum_cols := res_sum_cols || ', ';
            res_update   := res_update   || ', ';
        END IF;
        res_cols     := res_cols     || format('%I', res_key);
        res_sum_cols := res_sum_cols || format('SUM(%I) AS %I', res_key, res_key);
        res_update   := res_update   || format('%I = c.%I + a.%I', res_key, res_key, res_key);
        first_res := false;
    END LOOP;

    EXECUTE format(
        'WITH consumed AS (
            DELETE FROM accum.%I
            WHERE id IN (
                SELECT id FROM accum.%I
                WHERE created_at < now() - $1
                ORDER BY id
                LIMIT $2
            )
            RETURNING dim_hash, %s
        ),
        agg AS (
            SELECT dim_hash, %s
            FROM consumed
            GROUP BY dim_hash
        )
        UPDATE accum.%I c
        SET %s,
            version = c.version + 1
        FROM agg a
        WHERE c.dim_hash = a.dim_hash',
        p_name || '_balance_cache_delta',
        p_name || '_balance_cache_delta',
        res_cols,
        res_sum_cols,
        p_name || '_balance_cache',
        res_update
    ) USING p_max_age, p_batch_size;

    GET DIAGNOSTICS consumed = ROW_COUNT;
    RETURN consumed;
END;
$$;


CREATE OR REPLACE FUNCTION accum._delta_merge(
    p_max_age     interval DEFAULT interval '2 seconds',
    p_batch_size  int DEFAULT 10000
) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    reg      record;
    total    int := 0;
    merged   int;
BEGIN
    FOR reg IN
        SELECT r.name FROM accum._registers r
        WHERE r.high_write = true AND r.kind = 'balance'
        ORDER BY r.name
    LOOP
        merged := accum._delta_merge_register(reg.name, p_max_age, p_batch_size);
        total := total + merged;
    END LOOP;

    RETURN total;
END;
$$;


CREATE OR REPLACE FUNCTION accum._delta_flush_register(p_name text)
RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    reg          record;
    res_key      text;
    res_cols     text := '';
    res_sum_cols text := '';
    res_update   text := '';
    first_res    boolean := true;
    consumed     int;
BEGIN
    SELECT * INTO reg FROM accum._registers r WHERE r.name = p_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_name;
    END IF;

    IF NOT reg.high_write THEN
        RETURN 0;
    END IF;

    IF reg.kind != 'balance' THEN
        RETURN 0;
    END IF;

    FOR res_key IN SELECT key FROM jsonb_each_text(reg.resources) ORDER BY key
    LOOP
        IF NOT first_res THEN
            res_cols     := res_cols     || ', ';
            res_sum_cols := res_sum_cols || ', ';
            res_update   := res_update   || ', ';
        END IF;
        res_cols     := res_cols     || format('%I', res_key);
        res_sum_cols := res_sum_cols || format('SUM(%I) AS %I', res_key, res_key);
        res_update   := res_update   || format('%I = c.%I + a.%I', res_key, res_key, res_key);
        first_res := false;
    END LOOP;

    EXECUTE format(
        'WITH consumed AS (
            DELETE FROM accum.%I
            RETURNING dim_hash, %s
        ),
        agg AS (
            SELECT dim_hash, %s
            FROM consumed
            GROUP BY dim_hash
        )
        UPDATE accum.%I c
        SET %s,
            version = c.version + 1
        FROM agg a
        WHERE c.dim_hash = a.dim_hash',
        p_name || '_balance_cache_delta',
        res_cols,
        res_sum_cols,
        p_name || '_balance_cache',
        res_update
    );

    GET DIAGNOSTICS consumed = ROW_COUNT;
    RETURN consumed;
END;
$$;


CREATE OR REPLACE FUNCTION accum._delta_count(p_name text)
RETURNS bigint
LANGUAGE plpgsql STABLE AS $$
DECLARE
    reg record;
    cnt bigint;
BEGIN
    SELECT * INTO reg FROM accum._registers r WHERE r.name = p_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_name;
    END IF;

    IF NOT reg.high_write THEN
        RETURN 0;
    END IF;

    EXECUTE format('SELECT count(*) FROM accum.%I',
        p_name || '_balance_cache_delta') INTO cnt;

    RETURN cnt;
END;
$$;


-- ============================================================
-- PARTITIONING (mirrors sql/09_partitioning.sql)
-- ============================================================

CREATE OR REPLACE FUNCTION accum._partition_suffix(
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

CREATE OR REPLACE FUNCTION accum._partition_range_start(
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

CREATE OR REPLACE FUNCTION accum._partition_range_end(
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

CREATE OR REPLACE FUNCTION accum._partition_exists(
    p_parent_table text,
    p_suffix       text
) RETURNS boolean
LANGUAGE plpgsql STABLE AS $$
BEGIN
    RETURN EXISTS (
        SELECT 1
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'accum'
          AND c.relname = p_parent_table || '_' || p_suffix
    );
END;
$$;

CREATE OR REPLACE FUNCTION accum._create_partition(
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
    v_suffix    := accum._partition_suffix(p_range_start, p_partition_by);
    v_part_name := v_parent || '_' || v_suffix;
    v_range_end := accum._partition_range_end(p_range_start, p_partition_by);

    -- Advisory lock to prevent concurrent creation
    v_lock_key := hashtext(v_part_name);
    PERFORM pg_advisory_xact_lock(v_lock_key);

    -- Re-check after lock
    IF accum._partition_exists(v_parent, v_suffix) THEN
        RETURN false;
    END IF;

    EXECUTE format(
        'CREATE TABLE accum.%I PARTITION OF accum.%I
         FOR VALUES FROM (%L) TO (%L)',
        v_part_name,
        v_parent,
        p_range_start::timestamptz,
        v_range_end::timestamptz
    );

    RETURN true;
END;
$$;

CREATE OR REPLACE FUNCTION accum._create_initial_partitions(
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
    v_current := accum._partition_range_start(current_date, p_partition_by);

    FOR i IN 0..p_ahead
    LOOP
        v_created := accum._create_partition(p_name, p_partition_by, v_current);
        IF v_created THEN
            v_count := v_count + 1;
        END IF;

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

CREATE OR REPLACE FUNCTION accum.register_create_partitions(
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
    SELECT * INTO reg FROM accum._registers r WHERE r.name = p_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_name;
    END IF;

    v_current := accum._partition_range_start(current_date, reg.partition_by);
    v_end     := (current_date + p_ahead)::date;

    WHILE v_current <= v_end
    LOOP
        v_created := accum._create_partition(p_name, reg.partition_by, v_current);
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

CREATE OR REPLACE FUNCTION accum.register_detach_partitions(
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
    SELECT * INTO reg FROM accum._registers r WHERE r.name = p_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_name;
    END IF;

    v_cutoff := now() - p_older_than;

    FOR part_rec IN
        SELECT child.relname AS part_name,
               pg_get_expr(c.relpartbound, c.oid) AS bound_expr
        FROM pg_inherits i
        JOIN pg_class parent ON i.inhparent = parent.oid
        JOIN pg_class child  ON i.inhrelid  = child.oid
        JOIN pg_catalog.pg_class c ON c.oid = child.oid
        JOIN pg_namespace ns ON parent.relnamespace = ns.oid
        WHERE parent.relname = p_name || '_movements'
          AND ns.nspname = 'accum'
          AND child.relname != p_name || '_movements_default'
        ORDER BY child.relname
    LOOP
        v_range_end := substring(part_rec.bound_expr FROM 'TO \(''([^'']+)''\)');

        IF v_range_end IS NOT NULL AND v_range_end::timestamptz <= v_cutoff THEN
            EXECUTE format(
                'ALTER TABLE accum.%I DETACH PARTITION accum.%I',
                p_name || '_movements',
                part_rec.part_name
            );
            v_count := v_count + 1;
        END IF;
    END LOOP;

    RETURN v_count;
END;
$$;

CREATE OR REPLACE FUNCTION accum.register_partitions(p_name text)
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
    SELECT * INTO reg FROM accum._registers r WHERE r.name = p_name;
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
          AND ns.nspname = 'accum'
        ORDER BY child.relname
    LOOP
        SELECT COALESCE(s.n_live_tup, 0) INTO v_cnt
        FROM pg_stat_user_tables s
        WHERE s.relid = part_rec.child_oid;
        IF v_cnt IS NULL THEN v_cnt := 0; END IF;

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

CREATE OR REPLACE FUNCTION accum._ensure_partition(
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
    FROM accum._registers r
    WHERE r.name = p_name;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    v_range_start := accum._partition_range_start(p_period::date, v_reg.partition_by);
    v_suffix      := accum._partition_suffix(v_range_start, v_reg.partition_by);
    v_parent      := p_name || '_movements';

    IF NOT accum._partition_exists(v_parent, v_suffix) THEN
        PERFORM accum._create_partition(p_name, v_reg.partition_by, v_range_start);
    END IF;
END;
$$;


-- ============================================================
-- MAINTENANCE FUNCTIONS (mirrors sql/10_maintenance.sql)
-- ============================================================

CREATE OR REPLACE FUNCTION accum.register_verify(p_name text)
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
    first_res    boolean := true;
    dim_cols     text := '';
    first_dim    boolean := true;
    dim_key      text;
BEGIN
    SELECT * INTO reg FROM accum._registers r WHERE r.name = p_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_name;
    END IF;

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

    -- Verify balance_cache (only for balance kind)
    IF reg.kind = 'balance' THEN
        RETURN QUERY EXECUTE format(
            'WITH actual AS (
                SELECT dim_hash, %s
                FROM accum.%I
                GROUP BY dim_hash
            ),
            cached AS (
                SELECT dim_hash, %s
                FROM accum.%I
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
            (SELECT string_agg(format(' - %L', key), '') FROM jsonb_each_text(reg.dimensions)),
            (SELECT string_agg(format(' - %L', key), '') FROM jsonb_each_text(reg.dimensions))
        );
    END IF;

    -- Verify totals_month
    RETURN QUERY EXECUTE format(
        'WITH actual AS (
            SELECT dim_hash, date_trunc(''month'', period)::date AS period, %s
            FROM accum.%I
            GROUP BY dim_hash, date_trunc(''month'', period)::date
        ),
        stored AS (
            SELECT dim_hash, period, %s
            FROM accum.%I
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

    -- Verify totals_year against totals_month
    RETURN QUERY EXECUTE format(
        'WITH actual AS (
            SELECT dim_hash, date_trunc(''year'', period)::date AS period, %s
            FROM accum.%I
            GROUP BY dim_hash, date_trunc(''year'', period)::date
        ),
        stored AS (
            SELECT dim_hash, period, %s
            FROM accum.%I
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


CREATE OR REPLACE FUNCTION accum.register_rebuild_totals(p_name text)
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
    SELECT * INTO reg FROM accum._registers r WHERE r.name = p_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_name;
    END IF;

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

    EXECUTE format('TRUNCATE accum.%I', p_name || '_totals_month');

    EXECUTE format(
        'INSERT INTO accum.%I (dim_hash, period, %s, %s)
         SELECT dim_hash, date_trunc(''month'', period)::date, %s, %s
         FROM accum.%I
         GROUP BY dim_hash, date_trunc(''month'', period)::date, %s',
        p_name || '_totals_month',
        dim_cols, res_cols,
        dim_cols, res_sum_cols,
        p_name || '_movements',
        dim_cols
    );
    GET DIAGNOSTICS month_count = ROW_COUNT;

    EXECUTE format('TRUNCATE accum.%I', p_name || '_totals_year');

    EXECUTE format(
        'INSERT INTO accum.%I (dim_hash, period, %s, %s)
         SELECT dim_hash, date_trunc(''year'', period)::date, %s, %s
         FROM accum.%I
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


CREATE OR REPLACE FUNCTION accum.register_rebuild_cache(
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
    SELECT * INTO reg FROM accum._registers r WHERE r.name = p_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_name;
    END IF;

    IF reg.kind != 'balance' THEN
        RAISE EXCEPTION 'rebuild_cache is only available for balance-type registers, "%" is %',
            p_name, reg.kind;
    END IF;

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
        EXECUTE format('TRUNCATE accum.%I', p_name || '_balance_cache');

        IF reg.high_write THEN
            EXECUTE format('TRUNCATE accum.%I', p_name || '_balance_cache_delta');
        END IF;

        EXECUTE format(
            'INSERT INTO accum.%I (dim_hash, %s, %s, last_movement_at, last_movement_id)
             SELECT dim_hash, %s, %s,
                    MAX(recorded_at),
                    (array_agg(id ORDER BY recorded_at DESC))[1]
             FROM accum.%I
             GROUP BY dim_hash, %s',
            p_name || '_balance_cache',
            dim_cols, res_cols,
            dim_cols, res_sum_cols,
            p_name || '_movements',
            dim_cols
        );
    ELSE
        -- Partial rebuild: specific dim_hash
        EXECUTE format('DELETE FROM accum.%I WHERE dim_hash = $1',
            p_name || '_balance_cache')
            USING p_dim_hash;

        IF reg.high_write THEN
            EXECUTE format('DELETE FROM accum.%I WHERE dim_hash = $1',
                p_name || '_balance_cache_delta')
                USING p_dim_hash;
        END IF;

        EXECUTE format(
            'INSERT INTO accum.%I (dim_hash, %s, %s, last_movement_at, last_movement_id)
             SELECT dim_hash, %s, %s,
                    MAX(recorded_at),
                    (array_agg(id ORDER BY recorded_at DESC))[1]
             FROM accum.%I
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


CREATE OR REPLACE FUNCTION accum.register_stats(p_name text)
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
    SELECT * INTO reg FROM accum._registers r WHERE r.name = p_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_name;
    END IF;

    BEGIN
        EXECUTE format('SELECT count(*) FROM accum.%I',
            p_name || '_movements') INTO v_movements;
    EXCEPTION WHEN undefined_table THEN
        v_movements := 0;
    END;

    SELECT count(*)::int INTO v_partitions
    FROM pg_inherits i
    JOIN pg_class parent ON i.inhparent = parent.oid
    JOIN pg_namespace ns ON parent.relnamespace = ns.oid
    WHERE parent.relname = p_name || '_movements'
      AND ns.nspname = 'accum';

    BEGIN
        EXECUTE format('SELECT count(*) FROM accum.%I',
            p_name || '_totals_month') INTO v_month_rows;
    EXCEPTION WHEN undefined_table THEN
        v_month_rows := 0;
    END;

    BEGIN
        EXECUTE format('SELECT count(*) FROM accum.%I',
            p_name || '_totals_year') INTO v_year_rows;
    EXCEPTION WHEN undefined_table THEN
        v_year_rows := 0;
    END;

    IF reg.kind = 'balance' THEN
        BEGIN
            EXECUTE format('SELECT count(*) FROM accum.%I',
                p_name || '_balance_cache') INTO v_cache_rows;
        EXCEPTION WHEN undefined_table THEN
            v_cache_rows := 0;
        END;
    END IF;

    IF reg.high_write THEN
        BEGIN
            EXECUTE format('SELECT count(*) FROM accum.%I',
                p_name || '_balance_cache_delta') INTO v_delta_pending;
            EXECUTE format('SELECT MAX(created_at) FROM accum.%I',
                p_name || '_balance_cache_delta') INTO v_last_delta;
        EXCEPTION WHEN undefined_table THEN
            v_delta_pending := 0;
            v_last_delta := NULL;
        END;
    END IF;

    BEGIN
        SELECT pg_size_pretty(pg_total_relation_size(
            format('accum.%I', p_name || '_movements')::regclass))
            INTO v_movements_size;
    EXCEPTION WHEN OTHERS THEN
        v_movements_size := '0 bytes';
    END;

    BEGIN
        SELECT pg_size_pretty(pg_total_relation_size(
            format('accum.%I', p_name || '_totals_month')::regclass))
            INTO v_month_size;
    EXCEPTION WHEN OTHERS THEN
        v_month_size := '0 bytes';
    END;

    BEGIN
        SELECT pg_size_pretty(pg_total_relation_size(
            format('accum.%I', p_name || '_totals_year')::regclass))
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
                format('accum.%I', p_name || '_balance_cache')::regclass))
                INTO v_cache_size;
        EXCEPTION WHEN OTHERS THEN
            v_cache_size := '0 bytes';
        END;
        v_table_sizes := v_table_sizes || jsonb_build_object('balance_cache', v_cache_size);
    END IF;

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


-- ============================================================
-- CONFIG / MAINTENANCE HELPERS (mirrors sql/11_config.sql)
-- ============================================================

CREATE OR REPLACE VIEW accum._config AS
SELECT
    current_setting('pg_accumulator.background_workers',  true) AS background_workers,
    current_setting('pg_accumulator.maintenance_interval', true) AS maintenance_interval,
    current_setting('pg_accumulator.delta_merge_interval', true) AS delta_merge_interval,
    current_setting('pg_accumulator.delta_merge_delay',   true) AS delta_merge_delay,
    current_setting('pg_accumulator.delta_merge_batch_size', true) AS delta_merge_batch_size,
    current_setting('pg_accumulator.partitions_ahead',    true) AS partitions_ahead,
    current_setting('pg_accumulator.schema',              true) AS schema;

CREATE OR REPLACE FUNCTION accum._maintenance_status()
RETURNS TABLE(
    pid        int,
    worker_name text,
    state      text,
    query      text,
    started_at timestamptz
)
LANGUAGE sql STABLE AS $$
    SELECT
        pid,
        backend_type AS worker_name,
        state,
        query,
        backend_start AS started_at
    FROM pg_stat_activity
    WHERE backend_type LIKE 'pg_accumulator%'
    ORDER BY pid;
$$;

CREATE OR REPLACE FUNCTION accum._force_delta_merge(
    p_max_age     interval DEFAULT interval '0 seconds',
    p_batch_size  int DEFAULT 1000000
) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    total int;
BEGIN
    total := accum._delta_merge(p_max_age, p_batch_size);
    RETURN total;
END;
$$;
$$;