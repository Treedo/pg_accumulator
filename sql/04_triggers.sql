-- sql/04_triggers.sql
-- Trigger generation for the movement→totals→cache chain
-- Creates BEFORE/AFTER INSERT and AFTER DELETE triggers per register

-- ============================================================
-- GENERATE ALL TRIGGERS for a register
-- Creates:
--   1. BEFORE INSERT: compute dim_hash, set movement_type
--   2. AFTER INSERT:  UPSERT totals_month/year + balance_cache
--   3. AFTER DELETE:  subtract from totals + cache
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
    dim_insert_cols  text := '';
    dim_new_cols     text := '';
    res_insert_cols  text := '';
    res_new_cols     text := '';
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
    trg_body         text;
    first_dim        boolean := true;
    first_res        boolean := true;
BEGIN
    -- --------------------------------------------------------
    -- Build dimension column references
    -- --------------------------------------------------------
    FOR dim_key IN SELECT * FROM jsonb_object_keys(p_dimensions)
    LOOP
        IF NOT first_dim THEN
            hash_call_args  := hash_call_args  || ', ';
            dim_insert_cols := dim_insert_cols || ', ';
            dim_new_cols    := dim_new_cols    || ', ';
        END IF;
        hash_call_args  := hash_call_args  || format('NEW.%I', dim_key);
        dim_insert_cols := dim_insert_cols || format('%I', dim_key);
        dim_new_cols    := dim_new_cols    || format('NEW.%I', dim_key);
        first_dim := false;
    END LOOP;

    -- --------------------------------------------------------
    -- Build resource column references + update clauses
    -- --------------------------------------------------------
    FOR res_key IN SELECT * FROM jsonb_object_keys(p_resources)
    LOOP
        IF NOT first_res THEN
            res_insert_cols := res_insert_cols || ', ';
            res_new_cols    := res_new_cols    || ', ';
            res_update_m    := res_update_m   || ', ';
            res_update_y    := res_update_y   || ', ';
            res_update_c    := res_update_c   || ', ';
            res_sub_m       := res_sub_m      || ', ';
            res_sub_y       := res_sub_y      || ', ';
            res_sub_c       := res_sub_c      || ', ';
        END IF;
        res_insert_cols := res_insert_cols || format('%I', res_key);
        res_new_cols    := res_new_cols    || format('NEW.%I', res_key);
        -- UPSERT: add incoming value to existing
        res_update_m := res_update_m || format('%I = @extschema@.%I.%I + EXCLUDED.%I',
            res_key, p_name || '_totals_month', res_key, res_key);
        res_update_y := res_update_y || format('%I = @extschema@.%I.%I + EXCLUDED.%I',
            res_key, p_name || '_totals_year', res_key, res_key);
        res_update_c := res_update_c || format('%I = @extschema@.%I.%I + EXCLUDED.%I',
            res_key, p_name || '_balance_cache', res_key, res_key);
        -- DELETE: subtract removed value
        res_sub_m := res_sub_m || format('%I = @extschema@.%I.%I - OLD.%I',
            res_key, p_name || '_totals_month', res_key, res_key);
        res_sub_y := res_sub_y || format('%I = @extschema@.%I.%I - OLD.%I',
            res_key, p_name || '_totals_year', res_key, res_key);
        res_sub_c := res_sub_c || format('%I = @extschema@.%I.%I - OLD.%I',
            res_key, p_name || '_balance_cache', res_key, res_key);
        first_res := false;
    END LOOP;

    -- ============================================================
    -- 1. BEFORE INSERT trigger: compute dim_hash
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
    -- 2. AFTER INSERT trigger: update totals + cache
    -- ============================================================

    -- totals_month UPSERT
    totals_upsert_m := format(
        'INSERT INTO @extschema@.%I (dim_hash, period, %s, %s)
         VALUES (NEW.dim_hash, date_trunc(''month'', NEW.period)::date, %s, %s)
         ON CONFLICT (dim_hash, period) DO UPDATE SET %s',
        p_name || '_totals_month',
        dim_insert_cols, res_insert_cols,
        dim_new_cols, res_new_cols,
        res_update_m
    );

    -- totals_year UPSERT
    totals_upsert_y := format(
        'INSERT INTO @extschema@.%I (dim_hash, period, %s, %s)
         VALUES (NEW.dim_hash, date_trunc(''year'', NEW.period)::date, %s, %s)
         ON CONFLICT (dim_hash, period) DO UPDATE SET %s',
        p_name || '_totals_year',
        dim_insert_cols, res_insert_cols,
        dim_new_cols, res_new_cols,
        res_update_y
    );

    -- balance_cache UPSERT (only for balance kind)
    IF p_kind = 'balance' THEN
        IF NOT p_high_write THEN
            cache_upsert := format(
                'INSERT INTO @extschema@.%I (dim_hash, %s, %s, last_movement_at, last_movement_id, version)
                 VALUES (NEW.dim_hash, %s, %s, now(), NEW.id, 1)
                 ON CONFLICT (dim_hash) DO UPDATE SET %s,
                     last_movement_at = EXCLUDED.last_movement_at,
                     last_movement_id = EXCLUDED.last_movement_id,
                     version = @extschema@.%I.version + 1',
                p_name || '_balance_cache',
                dim_insert_cols, res_insert_cols,
                dim_new_cols, res_new_cols,
                res_update_c,
                p_name || '_balance_cache'
            );
        ELSE
            -- High-write: append to delta buffer instead of UPSERT cache
            cache_upsert := format(
                'INSERT INTO @extschema@.%I (dim_hash, %s)
                 VALUES (NEW.dim_hash, %s)',
                p_name || '_balance_cache_delta',
                res_insert_cols,
                res_new_cols
            );
        END IF;
    END IF;

    -- Assemble AFTER INSERT trigger body
    trg_body := totals_upsert_m || '; ' || totals_upsert_y || ';';
    IF cache_upsert != '' THEN
        trg_body := trg_body || ' ' || cache_upsert || ';';
    END IF;

    EXECUTE format(
        'CREATE OR REPLACE FUNCTION @extschema@.%I() RETURNS trigger
         LANGUAGE plpgsql AS $trg$
         BEGIN
             %s
             RETURN NEW;
         END;
         $trg$',
        '_trg_' || p_name || '_after_insert',
        trg_body
    );

    EXECUTE format(
        'CREATE TRIGGER trg_%s_after_insert
         AFTER INSERT ON @extschema@.%I
         FOR EACH ROW EXECUTE FUNCTION @extschema@.%I()',
        p_name,
        p_name || '_movements',
        '_trg_' || p_name || '_after_insert'
    );

    -- ============================================================
    -- 3. AFTER DELETE trigger: reverse operations
    -- ============================================================
    del_totals_m := format(
        'UPDATE @extschema@.%I SET %s WHERE dim_hash = OLD.dim_hash AND period = date_trunc(''month'', OLD.period)::date',
        p_name || '_totals_month', res_sub_m);

    del_totals_y := format(
        'UPDATE @extschema@.%I SET %s WHERE dim_hash = OLD.dim_hash AND period = date_trunc(''year'', OLD.period)::date',
        p_name || '_totals_year', res_sub_y);

    IF p_kind = 'balance' THEN
        del_cache := format(
            'UPDATE @extschema@.%I SET %s, version = @extschema@.%I.version + 1 WHERE dim_hash = OLD.dim_hash',
            p_name || '_balance_cache', res_sub_c, p_name || '_balance_cache');
    END IF;

    EXECUTE format(
        'CREATE OR REPLACE FUNCTION @extschema@.%I() RETURNS trigger
         LANGUAGE plpgsql AS $trg$
         BEGIN
             %s;
             %s;
             %s
             RETURN OLD;
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
         FOR EACH ROW EXECUTE FUNCTION @extschema@.%I()',
        p_name,
        p_name || '_movements',
        '_trg_' || p_name || '_after_delete'
    );
END;
$$;
