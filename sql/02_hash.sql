-- sql/02_hash.sql
-- Hash function generation for dimension hashing
-- Creates per-register _hash_<name>() functions

-- Legacy helper functions for tests and backward compatibility.
-- These are defined in the accum schema when the extension is installed.
CREATE OR REPLACE FUNCTION @extschema@._md5_to_bigint(val text)
RETURNS bigint
LANGUAGE sql IMMUTABLE AS $$
  SELECT ('x' || substr(md5(coalesce(val, '')), 1, 16))::bit(64)::bigint;
$$;

CREATE OR REPLACE FUNCTION @extschema@._hash_legacy_sales(product integer, region text)
RETURNS bigint
LANGUAGE sql IMMUTABLE AS $$
  SELECT @extschema@._md5_to_bigint(coalesce($1::text, '') || '|' || coalesce($2, ''));
$$;

CREATE OR REPLACE FUNCTION @extschema@._hash_legacy_multi_dim(a text, b integer, c text, d integer)
RETURNS bigint
LANGUAGE sql IMMUTABLE AS $$
  SELECT @extschema@._md5_to_bigint(
    coalesce($1, '') || '|' || coalesce($2::text, '') || '|' || coalesce($3, '') || '|' || coalesce($4::text, '')
  );
$$;

CREATE OR REPLACE FUNCTION @extschema@._hash_legacy_alter_test(a text, b integer, c integer)
RETURNS bigint
LANGUAGE sql IMMUTABLE AS $$
  SELECT @extschema@._md5_to_bigint(
    coalesce($1, '') || '|' || coalesce($2::text, '') || '|' || coalesce($3::text, '')
  );
$$;

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
    FOR dim_key, dim_type IN SELECT * FROM jsonb_each_text(p_dimensions) ORDER BY key
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
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT p.oid::regprocedure::text AS fn,
               e.extname
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        LEFT JOIN pg_depend d ON d.objid = p.oid
                           AND d.classid = 'pg_proc'::regclass
                           AND d.deptype = 'e'
        LEFT JOIN pg_extension e ON d.refobjid = e.oid
        WHERE n.nspname = 'accum'
          AND p.proname = '_hash_' || p_name
    LOOP
        BEGIN
            IF r.extname IS NOT NULL THEN
                RAISE NOTICE 'Skipping drop of extension-owned hash function % (extension %)', r.fn, r.extname;
            ELSE
                EXECUTE format('DROP FUNCTION %s CASCADE', r.fn);
            END IF;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'Skipping drop of hash function %: %', r.fn, SQLERRM;
        END;
    END LOOP;
END;
$$;
