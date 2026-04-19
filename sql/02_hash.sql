-- sql/02_hash.sql
-- Hash function generation for dimension hashing
-- Creates per-register _hash_<name>() functions

-- ============================================================
-- GENERATE HASH FUNCTION for a register
-- ============================================================
-- Given a register name and its dimensions (jsonb), generates:
--   accum._hash_<name>(p_dim1 type1, p_dim2 type2, ...) RETURNS bigint
-- Uses hashtextextended() as the default hash backend (PL/pgSQL prototype).
-- Will be replaced by C xxhash64/murmur3 in production builds.

CREATE OR REPLACE FUNCTION @extschema@._generate_hash_function(
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
    -- Validate inputs
    PERFORM @extschema@._validate_name(p_name);
    PERFORM @extschema@._validate_dimensions(p_dimensions);

    -- Build function signature and body
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

    -- Create the hash function
    EXECUTE format(
        'CREATE OR REPLACE FUNCTION @extschema@.%I(%s) RETURNS bigint
         LANGUAGE sql IMMUTABLE AS $fn$
             SELECT hashtextextended(%s, 0)
         $fn$',
        '_hash_' || p_name,
        hash_args,
        hash_body
    );
END;
$$;

-- ============================================================
-- DROP HASH FUNCTION for a register
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@._drop_hash_function(p_name text)
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE format('DROP FUNCTION IF EXISTS @extschema@.%I CASCADE', '_hash_' || p_name);
END;
$$;
