-- test/setup/01-registry-table.sql
-- Internal metadata registry for accumulation registers

-- ============================================================
-- REGISTRY TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS accum._registers (
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

-- ============================================================
-- VALIDATION FUNCTIONS
-- ============================================================

CREATE OR REPLACE FUNCTION accum._validate_name(p_name text)
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

CREATE OR REPLACE FUNCTION accum._validate_dimensions(p_dimensions jsonb)
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

CREATE OR REPLACE FUNCTION accum._validate_resources(p_resources jsonb)
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

CREATE OR REPLACE FUNCTION accum._register_exists(p_name text)
RETURNS boolean
LANGUAGE sql STABLE AS $$
    SELECT EXISTS (SELECT 1 FROM accum._registers WHERE name = p_name);
$$;

CREATE OR REPLACE FUNCTION accum._register_get(p_name text)
RETURNS accum._registers
LANGUAGE sql STABLE AS $$
    SELECT * FROM accum._registers WHERE name = p_name;
$$;

CREATE OR REPLACE FUNCTION accum._register_put(
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
    INSERT INTO accum._registers
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

CREATE OR REPLACE FUNCTION accum._register_delete(p_name text)
RETURNS boolean
LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM accum._registers WHERE name = p_name;
    RETURN FOUND;
END;
$$;

CREATE OR REPLACE FUNCTION accum._register_list()
RETURNS SETOF accum._registers
LANGUAGE sql STABLE AS $$
    SELECT * FROM accum._registers ORDER BY name;
$$;

-- ============================================================
-- HASH FUNCTION GENERATION
-- ============================================================

CREATE OR REPLACE FUNCTION accum._generate_hash_function(
    p_name       text,
    p_dimensions jsonb
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    dim_key   text;
    dim_type  text;
    hash_args text := '';
    hash_body text := '';
    arg_idx   int  := 0;
BEGIN
    PERFORM accum._validate_name(p_name);
    PERFORM accum._validate_dimensions(p_dimensions);

    FOR dim_key, dim_type IN SELECT * FROM jsonb_each_text(p_dimensions)
    LOOP
        IF arg_idx > 0 THEN
            hash_args := hash_args || ', ';
            hash_body := hash_body || ' || ''|'' || ';
        END IF;
        hash_args := hash_args || format('%I %s', 'p_' || dim_key, dim_type);
        hash_body := hash_body || format('coalesce(%I::text, ''__NULL__'')', 'p_' || dim_key);
        arg_idx := arg_idx + 1;
    END LOOP;

    EXECUTE format(
        'CREATE OR REPLACE FUNCTION accum.%I(%s) RETURNS bigint
         LANGUAGE sql IMMUTABLE AS $fn$
             SELECT hashtextextended(%s, 0)
         $fn$',
        '_hash_' || p_name,
        hash_args,
        hash_body
    );
END;
$$;

CREATE OR REPLACE FUNCTION accum._drop_hash_function(p_name text)
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE format('DROP FUNCTION IF EXISTS accum.%I CASCADE', '_hash_' || p_name);
END;
$$;
