-- sql/04_triggers.sql
-- Trigger generation for the movement→totals→cache chain
-- Creates BEFORE/AFTER INSERT and AFTER DELETE triggers per register
--
-- Trigger chain per register:
--   1. BEFORE INSERT (FOR EACH ROW):
--      - Compute dim_hash via _hash_<name>()
--      - Set movement_type = 'adjustment' for retroactive entries
--   2. AFTER INSERT (FOR EACH STATEMENT):
--      - Batch UPSERT into totals_month, totals_year
--      - UPSERT balance_cache (standard) or INSERT delta (high_write)
--   3. AFTER DELETE (FOR EACH STATEMENT):
--      - Batch subtract from totals_month, totals_year
--      - Subtract from balance_cache

-- ============================================================
-- GENERATE ALL TRIGGERS for a register
-- ============================================================
CREATE OR REPLACE FUNCTION @extschema@._generate_triggers(
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
    -- Column reference fragments
    hash_call_args   text := '';
    dim_cols         text := '';
    res_cols         text := '';
    res_sum_cols     text := '';
    -- UPSERT SET clauses for INSERT triggers
    res_update_m     text := '';
    res_update_y     text := '';
    res_update_c     text := '';
    -- UPDATE SET clauses for DELETE triggers
    res_sub_m        text := '';
    res_sub_y        text := '';
    res_sub_c        text := '';
    -- Assembled SQL statements
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
    -- --------------------------------------------------------
    -- Build dimension column references (ORDER BY key for determinism)
    -- --------------------------------------------------------
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

    -- --------------------------------------------------------
    -- Build resource column references + update clauses (ORDER BY key)
    -- --------------------------------------------------------
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
        -- UPSERT: add incoming value to existing
        res_update_m := res_update_m || format('%I = @extschema@.%I.%I + EXCLUDED.%I',
            res_key, p_name || '_totals_month', res_key, res_key);
        res_update_y := res_update_y || format('%I = @extschema@.%I.%I + EXCLUDED.%I',
            res_key, p_name || '_totals_year', res_key, res_key);
        res_update_c := res_update_c || format('%I = @extschema@.%I.%I + EXCLUDED.%I',
            res_key, p_name || '_balance_cache', res_key, res_key);
        -- DELETE: subtract aggregated value
        res_sub_m := res_sub_m || format('%I = t.%I - agg.%I', res_key, res_key, res_key);
        res_sub_y := res_sub_y || format('%I = t.%I - agg.%I', res_key, res_key, res_key);
        res_sub_c := res_sub_c || format('%I = c.%I - agg.%I', res_key, res_key, res_key);
        first_res := false;
    END LOOP;

    -- ============================================================
    -- 1. BEFORE INSERT trigger (FOR EACH ROW): compute dim_hash
    -- ============================================================
    EXECUTE format(
        'CREATE OR REPLACE FUNCTION @extschema@.%I() RETURNS trigger
         LANGUAGE plpgsql AS $trg$
         BEGIN
             NEW.dim_hash := @extschema@.%I(%s);
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
         BEFORE INSERT ON @extschema@.%I
         FOR EACH ROW EXECUTE FUNCTION @extschema@.%I()',
        p_name,
        p_name || '_movements',
        '_trg_' || p_name || '_before_insert'
    );

    -- ============================================================
    -- 2. AFTER INSERT trigger (FOR EACH STATEMENT): batch aggregation
    --    Uses REFERENCING NEW TABLE AS new_rows for efficient batch processing
    -- ============================================================

    -- totals_month UPSERT with aggregation
    totals_upsert_m := format(
        'INSERT INTO @extschema@.%I (dim_hash, period, %s, %s)
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

    -- totals_year UPSERT with aggregation
    totals_upsert_y := format(
        'INSERT INTO @extschema@.%I (dim_hash, period, %s, %s)
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

    -- balance_cache UPSERT (only for balance kind)
    IF p_kind = 'balance' THEN
        IF NOT p_high_write THEN
            cache_upsert := format(
                'INSERT INTO @extschema@.%I (dim_hash, %s, %s, last_movement_at, last_movement_id, version)
                 SELECT dim_hash, %s, %s, now(), (array_agg(id))[1], 1
                 FROM new_rows
                 GROUP BY dim_hash, %s
                 ON CONFLICT (dim_hash) DO UPDATE SET %s,
                     last_movement_at = EXCLUDED.last_movement_at,
                     last_movement_id = EXCLUDED.last_movement_id,
                     version = @extschema@.%I.version + 1',
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
                'INSERT INTO @extschema@.%I (dim_hash, %s, last_movement_at, last_movement_id, version)
                 SELECT DISTINCT ON (dim_hash) dim_hash, %s, now(), id, 0
                 FROM new_rows
                 ON CONFLICT (dim_hash) DO NOTHING;
                 INSERT INTO @extschema@.%I (dim_hash, %s)
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

    -- Assemble AFTER INSERT trigger body
    trg_body_insert := totals_upsert_m || '; ' || totals_upsert_y || ';';
    IF cache_upsert != '' THEN
        trg_body_insert := trg_body_insert || ' ' || cache_upsert || ';';
    END IF;

    EXECUTE format(
        'CREATE OR REPLACE FUNCTION @extschema@.%I() RETURNS trigger
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
         AFTER INSERT ON @extschema@.%I
         REFERENCING NEW TABLE AS new_rows
         FOR EACH STATEMENT EXECUTE FUNCTION @extschema@.%I()',
        p_name,
        p_name || '_movements',
        '_trg_' || p_name || '_after_insert'
    );

    -- ============================================================
    -- 3. AFTER DELETE trigger (FOR EACH STATEMENT): batch subtraction
    --    Uses REFERENCING OLD TABLE AS old_rows
    -- ============================================================

    -- Subtract aggregated resources from totals_month
    del_totals_m := format(
        'UPDATE @extschema@.%I t SET %s
         FROM (SELECT dim_hash, date_trunc(''month'', period)::date AS period, %s
               FROM old_rows GROUP BY dim_hash, date_trunc(''month'', period)::date) agg
         WHERE t.dim_hash = agg.dim_hash AND t.period = agg.period',
        p_name || '_totals_month', res_sub_m, res_sum_cols);

    -- Subtract aggregated resources from totals_year
    del_totals_y := format(
        'UPDATE @extschema@.%I t SET %s
         FROM (SELECT dim_hash, date_trunc(''year'', period)::date AS period, %s
               FROM old_rows GROUP BY dim_hash, date_trunc(''year'', period)::date) agg
         WHERE t.dim_hash = agg.dim_hash AND t.period = agg.period',
        p_name || '_totals_year', res_sub_y, res_sum_cols);

    -- Subtract aggregated resources from balance_cache
    IF p_kind = 'balance' THEN
        del_cache := format(
            'UPDATE @extschema@.%I c SET %s, version = c.version + 1
             FROM (SELECT dim_hash, %s FROM old_rows GROUP BY dim_hash) agg
             WHERE c.dim_hash = agg.dim_hash',
            p_name || '_balance_cache', res_sub_c, res_sum_cols);
    END IF;

    EXECUTE format(
        'CREATE OR REPLACE FUNCTION @extschema@.%I() RETURNS trigger
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
         AFTER DELETE ON @extschema@.%I
         REFERENCING OLD TABLE AS old_rows
         FOR EACH STATEMENT EXECUTE FUNCTION @extschema@.%I()',
        p_name,
        p_name || '_movements',
        '_trg_' || p_name || '_after_delete'
    );
END;
$$;
