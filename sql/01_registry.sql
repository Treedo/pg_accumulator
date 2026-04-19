-- sql/01_registry.sql
-- Internal metadata registry for accumulation registers

-- ============================================================
-- REGISTRY TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS @extschema@._registers (
    name           text PRIMARY KEY,
    kind           text NOT NULL DEFAULT 'balance'
                       CHECK (kind IN ('balance', 'turnover')),
    dimensions     jsonb NOT NULL,
    resources      jsonb NOT NULL,
    totals_period  text NOT NULL DEFAULT 'day'
                       CHECK (totals_period IN ('day', 'month', 'year')),
    partition_by   text NOT NULL DEFAULT 'month'
                       CHECK (partition_by IN ('day', 'month', 'quarter', 'year')),
    high_write     boolean NOT NULL DEFAULT false,
    recorder_type  text NOT NULL DEFAULT 'text',
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE @extschema@._registers IS
    'Internal registry of all accumulation registers managed by pg_accumulator';

-- ============================================================
-- VALIDATION FUNCTIONS
-- ============================================================

-- Validate register name: lowercase Latin, digits, underscore; must start with a letter
CREATE OR REPLACE FUNCTION @extschema@._validate_name(p_name text)
RETURNS void
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF p_name IS NULL OR p_name = '' THEN
        RAISE EXCEPTION 'Register name cannot be empty';
    END IF;
    IF length(p_name) > 48 THEN
        RAISE EXCEPTION 'Register name too long: %. Maximum 48 characters', p_name;
    END IF;
    IF p_name !~ '^[a-z][a-z0-9_]*$' THEN
        RAISE EXCEPTION 'Invalid register name: %. Must start with a lowercase letter and contain only lowercase letters, digits, and underscores', p_name;
    END IF;
END;
$$;

-- Validate dimensions: non-empty JSON object with valid column names
CREATE OR REPLACE FUNCTION @extschema@._validate_dimensions(p_dimensions jsonb)
RETURNS void
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    dim_key  text;
    dim_type text;
BEGIN
    IF p_dimensions IS NULL OR p_dimensions = '{}'::jsonb THEN
        RAISE EXCEPTION 'At least one dimension is required';
    END IF;
    IF jsonb_typeof(p_dimensions) != 'object' THEN
        RAISE EXCEPTION 'Dimensions must be a JSON object';
    END IF;
    FOR dim_key, dim_type IN SELECT * FROM jsonb_each_text(p_dimensions)
    LOOP
        IF dim_key !~ '^[a-z][a-z0-9_]*$' THEN
            RAISE EXCEPTION 'Invalid dimension name: %', dim_key;
        END IF;
        IF dim_type IS NULL OR dim_type = '' THEN
            RAISE EXCEPTION 'Dimension "%" must have a type', dim_key;
        END IF;
    END LOOP;
END;
$$;

-- Validate resources: non-empty JSON object with valid column names
CREATE OR REPLACE FUNCTION @extschema@._validate_resources(p_resources jsonb)
RETURNS void
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    res_key  text;
    res_type text;
BEGIN
    IF p_resources IS NULL OR p_resources = '{}'::jsonb THEN
        RAISE EXCEPTION 'At least one resource is required';
    END IF;
    IF jsonb_typeof(p_resources) != 'object' THEN
        RAISE EXCEPTION 'Resources must be a JSON object';
    END IF;
    FOR res_key, res_type IN SELECT * FROM jsonb_each_text(p_resources)
    LOOP
        IF res_key !~ '^[a-z][a-z0-9_]*$' THEN
            RAISE EXCEPTION 'Invalid resource name: %', res_key;
        END IF;
        IF res_type IS NULL OR res_type = '' THEN
            RAISE EXCEPTION 'Resource "%" must have a type', res_key;
        END IF;
    END LOOP;
END;
$$;

-- ============================================================
-- REGISTRY CRUD FUNCTIONS
-- ============================================================

-- Check if a register exists
CREATE OR REPLACE FUNCTION @extschema@._register_exists(p_name text)
RETURNS boolean
LANGUAGE sql STABLE AS $$
    SELECT EXISTS (SELECT 1 FROM @extschema@._registers WHERE name = p_name);
$$;

-- Get register metadata (returns NULL row if not found)
CREATE OR REPLACE FUNCTION @extschema@._register_get(p_name text)
RETURNS @extschema@._registers
LANGUAGE sql STABLE AS $$
    SELECT * FROM @extschema@._registers WHERE name = p_name;
$$;

-- Insert or update register metadata
CREATE OR REPLACE FUNCTION @extschema@._register_put(
    p_name          text,
    p_kind          text    DEFAULT 'balance',
    p_dimensions    jsonb   DEFAULT NULL,
    p_resources     jsonb   DEFAULT NULL,
    p_totals_period text    DEFAULT 'day',
    p_partition_by  text    DEFAULT 'month',
    p_high_write    boolean DEFAULT false,
    p_recorder_type text    DEFAULT 'text'
) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO @extschema@._registers
        (name, kind, dimensions, resources, totals_period, partition_by, high_write, recorder_type)
    VALUES
        (p_name, p_kind, p_dimensions, p_resources, p_totals_period, p_partition_by, p_high_write, p_recorder_type)
    ON CONFLICT (name) DO UPDATE SET
        kind          = EXCLUDED.kind,
        dimensions    = EXCLUDED.dimensions,
        resources     = EXCLUDED.resources,
        totals_period = EXCLUDED.totals_period,
        partition_by  = EXCLUDED.partition_by,
        high_write    = EXCLUDED.high_write,
        recorder_type = EXCLUDED.recorder_type,
        updated_at    = now();
END;
$$;

-- Delete register metadata; returns TRUE if a row was deleted
CREATE OR REPLACE FUNCTION @extschema@._register_delete(p_name text)
RETURNS boolean
LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM @extschema@._registers WHERE name = p_name;
    RETURN FOUND;
END;
$$;

-- List all registered registers (ordered by name)
CREATE OR REPLACE FUNCTION @extschema@._register_list()
RETURNS SETOF @extschema@._registers
LANGUAGE sql STABLE AS $$
    SELECT * FROM @extschema@._registers ORDER BY name;
$$;
