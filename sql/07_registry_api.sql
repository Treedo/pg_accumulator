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
RETURNS TABLE(name text, kind text, dimensions int, resources int, created_at timestamptz)
LANGUAGE sql STABLE AS $$
    SELECT
        r.name,
        r.kind,
        (SELECT count(*)::int FROM jsonb_object_keys(r.dimensions)),
        (SELECT count(*)::int FROM jsonb_object_keys(r.resources)),
        r.created_at
    FROM @extschema@._registers r
    ORDER BY r.created_at;
$$;

-- ============================================================
-- REGISTER_INFO: Detailed register information as JSONB
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@.register_info(p_name text)
RETURNS jsonb
LANGUAGE plpgsql STABLE AS $$
DECLARE
    reg    record;
    result jsonb;
BEGIN
    SELECT * INTO reg FROM @extschema@._registers r WHERE r.name = p_name;
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
