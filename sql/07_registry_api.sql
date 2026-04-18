-- sql/07_registry_api.sql
-- Public registry management API
-- Orchestrates DDL, triggers, and hash generation

-- ============================================================
-- REGISTER_CREATE: Creates a new accumulation register
-- Orchestrates: validation → metadata → DDL → hash → triggers
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@.register_create(
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
    -- 1. Validate inputs
    PERFORM @extschema@._validate_name(name);
    PERFORM @extschema@._validate_dimensions(dimensions);
    PERFORM @extschema@._validate_resources(resources);

    IF kind NOT IN ('balance', 'turnover') THEN
        RAISE EXCEPTION 'Invalid kind: %. Must be balance or turnover', kind;
    END IF;

    IF totals_period NOT IN ('day', 'month', 'year') THEN
        RAISE EXCEPTION 'Invalid totals_period: %', totals_period;
    END IF;

    IF partition_by NOT IN ('day', 'month', 'quarter', 'year') THEN
        RAISE EXCEPTION 'Invalid partition_by: %', partition_by;
    END IF;

    -- Check for duplicates
    IF @extschema@._register_exists(name) THEN
        RAISE EXCEPTION 'Register "%" already exists', name;
    END IF;

    -- 2. Save metadata to registry
    PERFORM @extschema@._register_put(
        name, kind, dimensions, resources,
        totals_period, partition_by, high_write, recorder_type
    );

    -- 3. Generate storage infrastructure (DDL)
    PERFORM @extschema@._ddl_create_movements_table(name, recorder_type, dimensions, resources);
    PERFORM @extschema@._ddl_create_totals_tables(name, dimensions, resources);

    IF kind = 'balance' THEN
        PERFORM @extschema@._ddl_create_balance_cache(name, dimensions, resources);
    END IF;

    IF high_write THEN
        PERFORM @extschema@._ddl_create_delta_buffer(name, resources);
    END IF;

    -- 4. Create indexes
    PERFORM @extschema@._ddl_create_indexes(name, kind, dimensions, high_write);

    -- 5. Generate hash function
    PERFORM @extschema@._generate_hash_function(name, dimensions);

    -- 6. Generate triggers (movement→totals→cache chain)
    PERFORM @extschema@._generate_triggers(name, kind, dimensions, resources, high_write);

    -- 7. Generate read functions (balance/turnover/movements)
    PERFORM @extschema@._generate_read_functions(name, kind, dimensions, resources, high_write);

    -- 8. Create initial partitions
    PERFORM @extschema@._create_initial_partitions(name, partition_by, 3);
END;
$$;

-- ============================================================
-- REGISTER_ALTER: Modify an existing register's structure
-- Supports: adding dimensions, adding resources, toggling high_write
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@.register_alter(
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
    SELECT * INTO reg FROM @extschema@._registers r WHERE r.name = p_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_name;
    END IF;

    new_dimensions := reg.dimensions;
    new_resources  := reg.resources;

    -- 2. Add new dimensions (requires recalculation)
    IF add_dimensions IS NOT NULL AND add_dimensions != '{}'::jsonb THEN
        PERFORM @extschema@._validate_dimensions(add_dimensions);

        -- Check for conflicts with existing dimensions
        FOR dim_key, dim_type IN SELECT * FROM jsonb_each_text(add_dimensions)
        LOOP
            IF reg.dimensions ? dim_key THEN
                RAISE EXCEPTION 'Dimension "%" already exists in register "%"', dim_key, p_name;
            END IF;

            -- Add column to movements table
            EXECUTE format('ALTER TABLE @extschema@.%I ADD COLUMN %I %s',
                p_name || '_movements', dim_key, dim_type);
            -- Add column to totals tables
            EXECUTE format('ALTER TABLE @extschema@.%I ADD COLUMN %I %s',
                p_name || '_totals_month', dim_key, dim_type);
            EXECUTE format('ALTER TABLE @extschema@.%I ADD COLUMN %I %s',
                p_name || '_totals_year', dim_key, dim_type);
            -- Add column to balance_cache if it exists
            IF reg.kind = 'balance' THEN
                EXECUTE format('ALTER TABLE @extschema@.%I ADD COLUMN %I %s',
                    p_name || '_balance_cache', dim_key, dim_type);
            END IF;
        END LOOP;

        new_dimensions := reg.dimensions || add_dimensions;
        dims_added := true;
    END IF;

    -- 3. Add new resources (no recalculation needed, DEFAULT 0)
    IF add_resources IS NOT NULL AND add_resources != '{}'::jsonb THEN
        PERFORM @extschema@._validate_resources(add_resources);

        FOR res_key, res_type IN SELECT * FROM jsonb_each_text(add_resources)
        LOOP
            IF reg.resources ? res_key THEN
                RAISE EXCEPTION 'Resource "%" already exists in register "%"', res_key, p_name;
            END IF;

            EXECUTE format('ALTER TABLE @extschema@.%I ADD COLUMN %I %s NOT NULL DEFAULT 0',
                p_name || '_movements', res_key, res_type);
            EXECUTE format('ALTER TABLE @extschema@.%I ADD COLUMN %I %s NOT NULL DEFAULT 0',
                p_name || '_totals_month', res_key, res_type);
            EXECUTE format('ALTER TABLE @extschema@.%I ADD COLUMN %I %s NOT NULL DEFAULT 0',
                p_name || '_totals_year', res_key, res_type);
            IF reg.kind = 'balance' THEN
                EXECUTE format('ALTER TABLE @extschema@.%I ADD COLUMN %I %s NOT NULL DEFAULT 0',
                    p_name || '_balance_cache', res_key, res_type);
            END IF;
        END LOOP;

        new_resources := reg.resources || add_resources;
    END IF;

    -- 4. Toggle high_write mode
    IF high_write IS NOT NULL AND high_write IS DISTINCT FROM reg.high_write THEN
        IF high_write AND NOT reg.high_write THEN
            -- Enable high_write: create delta buffer
            PERFORM @extschema@._ddl_create_delta_buffer(p_name, new_resources);
            EXECUTE format('CREATE INDEX ON @extschema@.%I (dim_hash)',
                p_name || '_balance_cache_delta');
            EXECUTE format('CREATE INDEX ON @extschema@.%I (created_at)',
                p_name || '_balance_cache_delta');
        ELSIF NOT high_write AND reg.high_write THEN
            -- Disable high_write: drop delta buffer
            EXECUTE format('DROP TABLE IF EXISTS @extschema@.%I CASCADE',
                p_name || '_balance_cache_delta');
        END IF;
    END IF;

    -- 5. Regenerate hash function with new dimensions
    IF dims_added THEN
        -- Drop old hash function (may have different arg count)
        PERFORM @extschema@._drop_hash_function(p_name);
        PERFORM @extschema@._generate_hash_function(p_name, new_dimensions);

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

            EXECUTE format('UPDATE @extschema@.%I SET dim_hash = @extschema@.%I(%s)',
                p_name || '_movements',
                '_hash_' || p_name,
                hash_args
            );
        END;

        -- Rebuild totals and cache from scratch
        EXECUTE format('TRUNCATE @extschema@.%I', p_name || '_totals_month');
        EXECUTE format('TRUNCATE @extschema@.%I', p_name || '_totals_year');
        IF reg.kind = 'balance' THEN
            EXECUTE format('TRUNCATE @extschema@.%I', p_name || '_balance_cache');
        END IF;

        PERFORM @extschema@._rebuild_totals_from_movements(p_name, reg.kind, new_dimensions, new_resources);
    END IF;

    -- 6. Regenerate triggers (with updated dimensions/resources/high_write)
    IF dims_added OR (add_resources IS NOT NULL AND add_resources != '{}'::jsonb) OR
       (high_write IS NOT NULL AND high_write IS DISTINCT FROM reg.high_write) THEN
        -- Drop old triggers
        EXECUTE format('DROP TRIGGER IF EXISTS trg_%s_before_insert ON @extschema@.%I',
            p_name, p_name || '_movements');
        EXECUTE format('DROP TRIGGER IF EXISTS trg_%s_after_insert ON @extschema@.%I',
            p_name, p_name || '_movements');
        EXECUTE format('DROP TRIGGER IF EXISTS trg_%s_after_delete ON @extschema@.%I',
            p_name, p_name || '_movements');
        EXECUTE format('DROP FUNCTION IF EXISTS @extschema@.%I CASCADE',
            '_trg_' || p_name || '_before_insert');
        EXECUTE format('DROP FUNCTION IF EXISTS @extschema@.%I CASCADE',
            '_trg_' || p_name || '_after_insert');
        EXECUTE format('DROP FUNCTION IF EXISTS @extschema@.%I CASCADE',
            '_trg_' || p_name || '_after_delete');

        PERFORM @extschema@._generate_triggers(
            p_name, reg.kind, new_dimensions, new_resources,
            coalesce(high_write, reg.high_write)
        );

        -- Regenerate read functions with updated structure
        PERFORM @extschema@._drop_read_functions(p_name);
        PERFORM @extschema@._generate_read_functions(
            p_name, reg.kind, new_dimensions, new_resources,
            coalesce(high_write, reg.high_write)
        );
    END IF;

    -- 7. Update metadata
    PERFORM @extschema@._register_put(
        p_name, reg.kind, new_dimensions, new_resources,
        reg.totals_period, reg.partition_by,
        coalesce(high_write, reg.high_write), reg.recorder_type
    );
END;
$$;

-- ============================================================
-- HELPER: Rebuild totals and cache from movements (used by alter)
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@._rebuild_totals_from_movements(
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

    -- Rebuild totals_year
    EXECUTE format(
        'INSERT INTO @extschema@.%I (dim_hash, period, %s, %s)
         SELECT dim_hash, date_trunc(''year'', period)::date, %s, %s
         FROM @extschema@.%I
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
            'INSERT INTO @extschema@.%I (dim_hash, %s, %s)
             SELECT dim_hash, %s, %s
             FROM @extschema@.%I
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
-- REGISTER_DROP: Removes a register and all its infrastructure
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@.register_drop(
    name  text,
    force boolean DEFAULT false
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    reg record;
BEGIN
    -- Check existence
    SELECT * INTO reg FROM @extschema@._registers r WHERE r.name = register_drop.name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', name;
    END IF;

    -- Check for data if not forced
    IF NOT force THEN
        DECLARE
            cnt bigint;
        BEGIN
            EXECUTE format('SELECT count(*) FROM @extschema@.%I',
                name || '_movements') INTO cnt;
            IF cnt > 0 THEN
                RAISE EXCEPTION 'Register "%" contains % movements. Use force := true to drop.',
                    name, cnt;
            END IF;
        END;
    END IF;

    -- Drop all infrastructure (tables, triggers, functions)
    PERFORM @extschema@._ddl_drop_infrastructure(name);

    -- Remove from registry
    PERFORM @extschema@._register_delete(name);
END;
$$;

-- ============================================================
-- REGISTER_LIST: List all registers with summary info
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@.register_list()
RETURNS TABLE(name text, kind text, dimensions int, resources int, movements_count bigint, created_at timestamptz)
LANGUAGE plpgsql STABLE AS $$
DECLARE
    reg record;
    cnt bigint;
BEGIN
    FOR reg IN SELECT * FROM @extschema@._registers r ORDER BY r.created_at
    LOOP
        BEGIN
            EXECUTE format('SELECT count(*) FROM @extschema@.%I',
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
-- REGISTER_INFO: Detailed register information as JSONB
-- Includes structure, statistics, and partition info
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@.register_info(p_name text)
RETURNS jsonb
LANGUAGE plpgsql STABLE AS $$
DECLARE
    reg           record;
    result        jsonb;
    movements_cnt bigint := 0;
    partitions    jsonb  := '[]'::jsonb;
    tables_info   jsonb;
BEGIN
    SELECT * INTO reg FROM @extschema@._registers r WHERE r.name = p_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_name;
    END IF;

    -- Movement count
    BEGIN
        EXECUTE format('SELECT count(*) FROM @extschema@.%I',
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
          AND ns.nspname = '@extschema@';
    EXCEPTION WHEN OTHERS THEN
        partitions := '[]'::jsonb;
    END;

    IF partitions IS NULL THEN
        partitions := '[]'::jsonb;
    END IF;

    -- Tables info
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
