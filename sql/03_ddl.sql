-- sql/03_ddl.sql
-- DDL generation for register infrastructure
-- Creates tables, indexes when a new register is created

-- ============================================================
-- HELPER: Build dimension column SQL fragment
-- Returns comma-prefixed text: ", warehouse int, product int"
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@._build_dim_columns(p_dimensions jsonb)
RETURNS text
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    dim_key  text;
    dim_type text;
    result   text := '';
BEGIN
    FOR dim_key, dim_type IN SELECT * FROM jsonb_each_text(p_dimensions)
    LOOP
        result := result || format(', %I %s', dim_key, dim_type);
    END LOOP;
    RETURN result;
END;
$$;

-- ============================================================
-- HELPER: Build resource column SQL fragment
-- Returns comma-prefixed text: ", quantity numeric NOT NULL DEFAULT 0"
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@._build_res_columns(p_resources jsonb)
RETURNS text
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    res_key  text;
    res_type text;
    result   text := '';
BEGIN
    FOR res_key, res_type IN SELECT * FROM jsonb_each_text(p_resources)
    LOOP
        result := result || format(', %I %s NOT NULL DEFAULT 0', res_key, res_type);
    END LOOP;
    RETURN result;
END;
$$;

-- ============================================================
-- CREATE MOVEMENTS TABLE (partitioned by period)
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@._ddl_create_movements_table(
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
    col_defs := @extschema@._build_dim_columns(p_dimensions);
    res_defs := @extschema@._build_res_columns(p_resources);

    EXECUTE format(
        'CREATE TABLE @extschema@.%I (
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

    -- Default partition (catches periods without a specific partition)
    EXECUTE format(
        'CREATE TABLE @extschema@.%I PARTITION OF @extschema@.%I DEFAULT',
        p_name || '_movements_default',
        p_name || '_movements'
    );
END;
$$;

-- ============================================================
-- CREATE TOTALS TABLES (daily + monthly + yearly aggregations)
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@._ddl_create_totals_tables(
    p_name       text,
    p_dimensions jsonb,
    p_resources  jsonb
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    col_defs text;
    res_defs text;
BEGIN
    col_defs := @extschema@._build_dim_columns(p_dimensions);
    res_defs := @extschema@._build_res_columns(p_resources);

    -- totals_day: daily turnover aggregation
    EXECUTE format(
        'CREATE TABLE @extschema@.%I (
            dim_hash       bigint        NOT NULL,
            period         date          NOT NULL
            %s
            %s,
            PRIMARY KEY (dim_hash, period)
        )',
        p_name || '_totals_day',
        col_defs,
        res_defs
    );

    -- totals_month: monthly turnover aggregation
    EXECUTE format(
        'CREATE TABLE @extschema@.%I (
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

    -- totals_year: yearly turnover aggregation
    EXECUTE format(
        'CREATE TABLE @extschema@.%I (
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

-- ============================================================
-- CREATE BALANCE CACHE (only for kind='balance')
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@._ddl_create_balance_cache(
    p_name       text,
    p_dimensions jsonb,
    p_resources  jsonb
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    col_defs text;
    res_defs text;
BEGIN
    col_defs := @extschema@._build_dim_columns(p_dimensions);
    res_defs := @extschema@._build_res_columns(p_resources);

    EXECUTE format(
        'CREATE TABLE @extschema@.%I (
            dim_hash         bigint          NOT NULL PRIMARY KEY
            %s
            %s,
            last_movement_at timestamptz     NOT NULL DEFAULT now(),
            last_movement_id uuid,
            version          bigint          NOT NULL DEFAULT 0
        )',
        p_name || '_balance_cache',
        col_defs,
        res_defs
    );
END;
$$;

-- ============================================================
-- CREATE DELTA BUFFER (only if high_write=true)
-- Unlogged table for fast append-only delta writes
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@._ddl_create_delta_buffer(
    p_name      text,
    p_resources jsonb
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    res_defs text;
BEGIN
    res_defs := @extschema@._build_res_columns(p_resources);

    EXECUTE format(
        'CREATE UNLOGGED TABLE @extschema@.%I (
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

-- ============================================================
-- CREATE ALL INDEXES for register infrastructure
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@._ddl_create_indexes(
    p_name       text,
    p_kind       text,
    p_dimensions jsonb,
    p_high_write boolean
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    dim_key text;
BEGIN
    -- Movements indexes
    EXECUTE format('CREATE INDEX ON @extschema@.%I (dim_hash, period)',
        p_name || '_movements');
    EXECUTE format('CREATE INDEX ON @extschema@.%I (recorder)',
        p_name || '_movements');
    EXECUTE format('CREATE INDEX ON @extschema@.%I (period)',
        p_name || '_movements');

    -- Balance cache indexes: per-dimension for filtered queries
    IF p_kind = 'balance' THEN
        FOR dim_key IN SELECT * FROM jsonb_object_keys(p_dimensions)
        LOOP
            EXECUTE format('CREATE INDEX ON @extschema@.%I (%I)',
                p_name || '_balance_cache', dim_key);
        END LOOP;
    END IF;

    -- Delta buffer indexes
    IF p_high_write THEN
        EXECUTE format('CREATE INDEX ON @extschema@.%I (dim_hash)',
            p_name || '_balance_cache_delta');
        EXECUTE format('CREATE INDEX ON @extschema@.%I (created_at)',
            p_name || '_balance_cache_delta');
    END IF;
END;
$$;

-- ============================================================
-- DROP ALL REGISTER INFRASTRUCTURE
-- Drops tables (CASCADE removes triggers), trigger functions,
-- and hash function for the given register name.
-- Uses IF EXISTS for idempotent cleanup.
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@._ddl_drop_infrastructure(p_name text)
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    -- Drop tables (CASCADE removes attached triggers)
    EXECUTE format('DROP TABLE IF EXISTS @extschema@.%I CASCADE',
        p_name || '_movements');
    EXECUTE format('DROP TABLE IF EXISTS @extschema@.%I CASCADE',
        p_name || '_totals_day');
    EXECUTE format('DROP TABLE IF EXISTS @extschema@.%I CASCADE',
        p_name || '_totals_month');
    EXECUTE format('DROP TABLE IF EXISTS @extschema@.%I CASCADE',
        p_name || '_totals_year');
    EXECUTE format('DROP TABLE IF EXISTS @extschema@.%I CASCADE',
        p_name || '_balance_cache');
    EXECUTE format('DROP TABLE IF EXISTS @extschema@.%I CASCADE',
        p_name || '_balance_cache_delta');

    -- Drop trigger functions (not removed by DROP TABLE CASCADE)
    EXECUTE format('DROP FUNCTION IF EXISTS @extschema@.%I CASCADE',
        '_trg_' || p_name || '_before_insert');
    EXECUTE format('DROP FUNCTION IF EXISTS @extschema@.%I CASCADE',
        '_trg_' || p_name || '_after_insert');
    EXECUTE format('DROP FUNCTION IF EXISTS @extschema@.%I CASCADE',
        '_trg_' || p_name || '_after_delete');
    EXECUTE format('DROP FUNCTION IF EXISTS @extschema@.%I CASCADE',
        '_trg_' || p_name || '_block_update');
    EXECUTE format('DROP FUNCTION IF EXISTS @extschema@.%I CASCADE',
        '_trg_' || p_name || '_protect_derived');

    -- Drop read functions (balance/turnover/movements)
    PERFORM @extschema@._drop_read_functions(p_name);

    -- Drop hash function
    PERFORM @extschema@._drop_hash_function(p_name);
END;
$$;
