-- sql/08_delta_buffer.sql
-- Delta buffer functions for high_write mode
-- Provides: delta merge, delta count, delta flush

-- ============================================================
-- DELTA_MERGE_REGISTER: Merge accumulated deltas into balance_cache
-- for a specific register. Atomically deletes consumed deltas and
-- applies aggregated values to balance_cache via CTE.
--
-- Parameters:
--   p_name       — register name
--   p_max_age    — minimum age of delta before merge (default 2s)
--   p_batch_size — max number of delta rows to consume (default 10000)
--
-- Returns: number of delta rows consumed
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@._delta_merge_register(
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
    -- 1. Get register metadata
    SELECT * INTO reg FROM @extschema@._registers r WHERE r.name = p_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_name;
    END IF;

    IF NOT reg.high_write THEN
        RAISE EXCEPTION 'Register "%" is not in high_write mode', p_name;
    END IF;

    IF reg.kind != 'balance' THEN
        RAISE EXCEPTION 'Delta merge only applies to balance registers, "%" is %', p_name, reg.kind;
    END IF;

    -- 2. Build resource column expressions
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

    -- 3. Atomic merge: DELETE old deltas → aggregate → UPDATE cache
    EXECUTE format(
        'WITH consumed AS (
            DELETE FROM @extschema@.%I
            WHERE id IN (
                SELECT id FROM @extschema@.%I
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
        UPDATE @extschema@.%I c
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


-- ============================================================
-- DELTA_MERGE: Merge deltas for ALL high_write registers
-- Iterates over all registers with high_write=true and calls
-- _delta_merge_register() for each.
--
-- Returns: total number of delta rows consumed across all registers
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@._delta_merge(
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
        SELECT r.name FROM @extschema@._registers r
        WHERE r.high_write = true AND r.kind = 'balance'
        ORDER BY r.name
    LOOP
        merged := @extschema@._delta_merge_register(reg.name, p_max_age, p_batch_size);
        total := total + merged;
    END LOOP;

    RETURN total;
END;
$$;


-- ============================================================
-- DELTA_FLUSH_REGISTER: Flush ALL pending deltas for a register
-- (regardless of age). Used before disabling high_write mode
-- or during maintenance.
--
-- Returns: number of delta rows consumed
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@._delta_flush_register(p_name text)
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
    SELECT * INTO reg FROM @extschema@._registers r WHERE r.name = p_name;
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

    -- Flush ALL deltas (no age filter, no batch limit)
    EXECUTE format(
        'WITH consumed AS (
            DELETE FROM @extschema@.%I
            RETURNING dim_hash, %s
        ),
        agg AS (
            SELECT dim_hash, %s
            FROM consumed
            GROUP BY dim_hash
        )
        UPDATE @extschema@.%I c
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


-- ============================================================
-- DELTA_COUNT: Count pending delta rows for a register
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@._delta_count(p_name text)
RETURNS bigint
LANGUAGE plpgsql STABLE AS $$
DECLARE
    reg record;
    cnt bigint;
BEGIN
    SELECT * INTO reg FROM @extschema@._registers r WHERE r.name = p_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_name;
    END IF;

    IF NOT reg.high_write THEN
        RETURN 0;
    END IF;

    EXECUTE format('SELECT count(*) FROM @extschema@.%I',
        p_name || '_balance_cache_delta') INTO cnt;

    RETURN cnt;
END;
$$;
