-- sql/05_write_api.sql
-- Public write API: register_post, register_unpost, register_repost
-- Provides validated batch INSERT, DELETE by recorder, and atomic re-post

-- ============================================================
-- REGISTER_POST: Post movements to a register (batch INSERT)
-- Accepts a single JSON object or an array of objects.
-- Returns the number of inserted movements.
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@.register_post(
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
    -- 1. Lookup register metadata
    SELECT * INTO reg FROM @extschema@._registers r WHERE r.name = p_register;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_register;
    END IF;

    -- 2. Normalize input: single object → array of one
    IF jsonb_typeof(p_data) = 'object' THEN
        movements := jsonb_build_array(p_data);
    ELSIF jsonb_typeof(p_data) = 'array' THEN
        movements := p_data;
    ELSE
        RAISE EXCEPTION 'Data must be a JSON object or array';
    END IF;

    -- 3. Build column list from register metadata (deterministic order)
    FOR dim_key IN SELECT key FROM jsonb_each_text(reg.dimensions) ORDER BY key
    LOOP
        col_list := col_list || ', ' || quote_ident(dim_key);
    END LOOP;

    FOR res_key IN SELECT key FROM jsonb_each_text(reg.resources) ORDER BY key
    LOOP
        col_list := col_list || ', ' || quote_ident(res_key);
    END LOOP;

    -- 4. Build VALUES tuples with validation and type casting
    FOR mov IN SELECT * FROM jsonb_array_elements(movements)
    LOOP
        -- Validate required fields
        IF mov->>'recorder' IS NULL THEN
            RAISE EXCEPTION 'recorder is required';
        END IF;
        IF mov->>'period' IS NULL THEN
            RAISE EXCEPTION 'period is required';
        END IF;

        tuple := format('%L, %L::timestamptz', mov->>'recorder', mov->>'period');

        -- Dimensions: all required
        FOR dim_key, dim_type IN SELECT key, value FROM jsonb_each_text(reg.dimensions) ORDER BY key
        LOOP
            IF mov->>dim_key IS NULL THEN
                RAISE EXCEPTION 'dimension "%" is required', dim_key;
            END IF;
            tuple := tuple || ', ' || format('%L::%s', mov->>dim_key, dim_type);
        END LOOP;

        -- Resources: default to 0 if absent
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

    -- 5. Ensure partitions exist for all periods (before INSERT to avoid DDL-during-DML)
    FOR mov IN SELECT * FROM jsonb_array_elements(movements)
    LOOP
        PERFORM @extschema@._ensure_partition(p_register, (mov->>'period')::timestamptz);
    END LOOP;

    -- 6. Single batch INSERT (triggers fire once per statement)
    IF total_count > 0 THEN
        EXECUTE format('INSERT INTO @extschema@.%I (%s) VALUES %s',
            p_register || '_movements', col_list, values_str);
    END IF;

    RETURN total_count;
END;
$$;


-- ============================================================
-- REGISTER_UNPOST: Cancel all movements by recorder
-- Deletes all movements matching the given recorder value.
-- After-delete triggers automatically roll back totals and cache.
-- Returns the number of deleted movements.
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@.register_unpost(
    p_register text,
    p_recorder text
) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    reg   record;
    cnt   int;
BEGIN
    -- Verify register exists
    SELECT * INTO reg FROM @extschema@._registers r WHERE r.name = p_register;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_register;
    END IF;

    -- Delete movements and count via CTE
    EXECUTE format(
        'WITH deleted AS (
            DELETE FROM @extschema@.%I WHERE recorder = %L RETURNING 1
        ) SELECT count(*) FROM deleted',
        p_register || '_movements',
        p_recorder
    ) INTO cnt;

    RETURN cnt;
END;
$$;


-- ============================================================
-- REGISTER_REPOST: Atomic re-post (unpost old + post new)
-- Automatically injects the recorder into all new movement objects.
-- Returns the number of new movements posted.
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@.register_repost(
    p_register text,
    p_recorder text,
    p_data     jsonb
) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    cnt int;
BEGIN
    -- 1. Remove old movements (triggers roll back totals/cache)
    PERFORM @extschema@.register_unpost(p_register, p_recorder);

    -- 2. Inject recorder into new data
    IF jsonb_typeof(p_data) = 'object' THEN
        p_data := jsonb_set(p_data, '{recorder}', to_jsonb(p_recorder));
    ELSIF jsonb_typeof(p_data) = 'array' THEN
        SELECT jsonb_agg(jsonb_set(elem, '{recorder}', to_jsonb(p_recorder)))
        INTO p_data
        FROM jsonb_array_elements(p_data) AS elem;
    END IF;

    -- 3. Post new movements (triggers apply totals/cache)
    cnt := @extschema@.register_post(p_register, p_data);
    RETURN cnt;
END;
$$;
