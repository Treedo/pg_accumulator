-- test/setup/02-core-functions.sql
-- Core emulated functions for testing (pure SQL/PL/pgSQL prototypes)
-- These replicate the C extension API for testing without compiling C code.
-- The structure mirrors the real extension SQL files (03_ddl, 04_triggers, 07_registry_api).

-- ============================================================
-- DDL HELPERS (mirrors sql/03_ddl.sql with accum instead of @extschema@)
-- ============================================================

CREATE OR REPLACE FUNCTION accum._build_dim_columns(p_dimensions jsonb)
RETURNS text
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    dim_key  text;
    dim_type text;
    result   text := '';
BEGIN
    FOR dim_key, dim_type IN SELECT key, value FROM jsonb_each_text(p_dimensions) ORDER BY key
    LOOP
        result := result || format(', %I %s NOT NULL', dim_key, dim_type);
    END LOOP;
    RETURN result;
END;
$$;

CREATE OR REPLACE FUNCTION accum._build_res_columns(p_resources jsonb)
RETURNS text
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    res_key  text;
    res_type text;
    result   text := '';
BEGIN
    FOR res_key, res_type IN SELECT key, value FROM jsonb_each_text(p_resources) ORDER BY key
    LOOP
        result := result || format(', %I %s NOT NULL DEFAULT 0', res_key, res_type);
    END LOOP;
    RETURN result;
END;
$$;

-- ============================================================
-- HELPER: Build ledger resource column SQL fragment with _dr and _cr
-- Returns comma-prefixed text: ", amount_dr numeric NOT NULL DEFAULT 0, amount_cr numeric NOT NULL DEFAULT 0"
-- ============================================================
CREATE OR REPLACE FUNCTION accum._build_ledger_res_columns(p_resources jsonb)
RETURNS text
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    res_key  text;
    res_type text;
    result   text := '';
BEGIN
    FOR res_key, res_type IN SELECT key, value FROM jsonb_each_text(p_resources) ORDER BY key
    LOOP
        result := result || format(', %I_dr %s NOT NULL DEFAULT 0', res_key, res_type);
        result := result || format(', %I_cr %s NOT NULL DEFAULT 0', res_key, res_type);
    END LOOP;
    RETURN result;
END;
$$;

CREATE OR REPLACE FUNCTION accum._ddl_create_movements_table(
    p_name          text,
    p_recorder_type text,
    p_dimensions    jsonb,
    p_resources     jsonb
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    col_defs text;
    res_defs text;
    v_kind   text;
BEGIN
    SELECT kind INTO v_kind FROM accum._registers WHERE name = p_name;
    col_defs := accum._build_dim_columns(p_dimensions);

    IF v_kind = 'ledger' THEN
        res_defs := accum._build_res_columns(p_resources);
        EXECUTE format(
            'CREATE TABLE accum.%I (
                id             uuid          DEFAULT gen_random_uuid(),
                recorded_at    timestamptz   DEFAULT now() NOT NULL,
                recorder       %s            NOT NULL,
                period         timestamptz   NOT NULL,
                movement_type  text          DEFAULT ''regular'' NOT NULL,
                account_dr     text          NOT NULL,
                subconto_dr    jsonb         NOT NULL DEFAULT ''{}''::jsonb,
                account_cr     text          NOT NULL,
                subconto_cr    jsonb         NOT NULL DEFAULT ''{}''::jsonb,
                dim_hash_dr    bigint        NOT NULL,
                dim_hash_cr    bigint        NOT NULL
                %s
                %s,
                PRIMARY KEY (id, period)
            ) PARTITION BY RANGE (period)',
            p_name || '_movements',
            p_recorder_type,
            col_defs,
            res_defs
        );
    ELSE
        res_defs := accum._build_res_columns(p_resources);
        EXECUTE format(
            'CREATE TABLE accum.%I (
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
    END IF;

    EXECUTE format(
        'CREATE TABLE accum.%I PARTITION OF accum.%I DEFAULT',
        p_name || '_movements_default',
        p_name || '_movements'
    );
END;
$$;

CREATE OR REPLACE FUNCTION accum._ddl_create_totals_tables(
    p_name       text,
    p_dimensions jsonb,
    p_resources  jsonb
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    col_defs text;
    res_defs text;
    v_kind   text;
BEGIN
    SELECT kind INTO v_kind FROM accum._registers WHERE name = p_name;
    col_defs := accum._build_dim_columns(p_dimensions);

    IF v_kind = 'ledger' THEN
        res_defs := accum._build_ledger_res_columns(p_resources);
        -- totals_day: daily turnover aggregation
        EXECUTE format(
            'CREATE TABLE accum.%I (
                account        text          NOT NULL,
                dim_hash       bigint        NOT NULL,
                subconto       jsonb         NOT NULL DEFAULT ''{}''::jsonb,
                period         date          NOT NULL
                %s
                %s,
                PRIMARY KEY (account, dim_hash, period)
            )',
            p_name || '_totals_day',
            col_defs,
            res_defs
        );

        -- totals_month: monthly turnover aggregation
        EXECUTE format(
            'CREATE TABLE accum.%I (
                account        text          NOT NULL,
                dim_hash       bigint        NOT NULL,
                subconto       jsonb         NOT NULL DEFAULT ''{}''::jsonb,
                period         date          NOT NULL
                %s
                %s,
                PRIMARY KEY (account, dim_hash, period)
            )',
            p_name || '_totals_month',
            col_defs,
            res_defs
        );

        -- totals_year: yearly turnover aggregation
        EXECUTE format(
            'CREATE TABLE accum.%I (
                account        text          NOT NULL,
                dim_hash       bigint        NOT NULL,
                subconto       jsonb         NOT NULL DEFAULT ''{}''::jsonb,
                period         date          NOT NULL
                %s
                %s,
                PRIMARY KEY (account, dim_hash, period)
            )',
            p_name || '_totals_year',
            col_defs,
            res_defs
        );
    ELSE
        res_defs := accum._build_res_columns(p_resources);

        EXECUTE format(
            'CREATE TABLE accum.%I (
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

        EXECUTE format(
            'CREATE TABLE accum.%I (
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

        EXECUTE format(
            'CREATE TABLE accum.%I (
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
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION accum._ddl_create_balance_cache(
    p_name       text,
    p_dimensions jsonb,
    p_resources  jsonb
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    col_defs text;
    res_defs text;
    v_kind   text;
BEGIN
    SELECT kind INTO v_kind FROM accum._registers WHERE name = p_name;
    col_defs := accum._build_dim_columns(p_dimensions);

    IF v_kind = 'ledger' THEN
        res_defs := accum._build_ledger_res_columns(p_resources);
        EXECUTE format(
            'CREATE TABLE accum.%I (
                account          text            NOT NULL,
                dim_hash         bigint          NOT NULL,
                subconto         jsonb           NOT NULL DEFAULT ''{}''::jsonb
                %s
                %s,
                last_movement_at timestamptz     NOT NULL DEFAULT now(),
                last_movement_id uuid,
                version          bigint          NOT NULL DEFAULT 0,
                PRIMARY KEY (account, dim_hash)
            ) WITH (fillfactor = 70)',
            p_name || '_balance_cache',
            col_defs,
            res_defs
        );
    ELSE
        res_defs := accum._build_res_columns(p_resources);
        EXECUTE format(
            'CREATE TABLE accum.%I (
                dim_hash         bigint          NOT NULL PRIMARY KEY
                %s
                %s,
                last_movement_at timestamptz     NOT NULL DEFAULT now(),
                last_movement_id uuid,
                version          bigint          NOT NULL DEFAULT 0
            ) WITH (fillfactor = 70)',
            p_name || '_balance_cache',
            col_defs,
            res_defs
        );
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION accum._ddl_create_delta_buffer(
    p_name      text,
    p_resources jsonb
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    res_defs text;
BEGIN
    res_defs := accum._build_res_columns(p_resources);

    EXECUTE format(
        'CREATE UNLOGGED TABLE accum.%I (
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

CREATE OR REPLACE FUNCTION accum._ddl_create_indexes(
    p_name       text,
    p_kind       text,
    p_dimensions jsonb,
    p_high_write boolean
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    dim_key text;
BEGIN
    IF p_kind = 'ledger' THEN
        EXECUTE format('CREATE INDEX ON accum.%I (dim_hash_dr, period)', p_name || '_movements');
        EXECUTE format('CREATE INDEX ON accum.%I (dim_hash_cr, period)', p_name || '_movements');
        EXECUTE format('CREATE INDEX ON accum.%I (account_dr)', p_name || '_movements');
        EXECUTE format('CREATE INDEX ON accum.%I (account_cr)', p_name || '_movements');
        EXECUTE format('CREATE INDEX ON accum.%I (recorder)', p_name || '_movements');
        EXECUTE format('CREATE INDEX ON accum.%I (period)', p_name || '_movements');
    ELSE
        EXECUTE format('CREATE INDEX ON accum.%I (dim_hash, period)', p_name || '_movements');
        EXECUTE format('CREATE INDEX ON accum.%I (recorder)', p_name || '_movements');
        EXECUTE format('CREATE INDEX ON accum.%I (period)', p_name || '_movements');
        EXECUTE format('CREATE INDEX ON accum.%I (movement_type) WHERE movement_type != ''regular''', p_name || '_movements');
    END IF;

    -- Dimension indexes
    IF p_kind = 'balance' OR p_kind = 'ledger' THEN
        FOR dim_key IN SELECT key FROM jsonb_each_text(p_dimensions) ORDER BY key
        LOOP
            EXECUTE format('CREATE INDEX ON accum.%I (%I)',
                p_name || '_balance_cache', dim_key);
        END LOOP;
    END IF;

    -- Dimension indexes on totals tables for dimension-based queries
    FOR dim_key IN SELECT key FROM jsonb_each_text(p_dimensions) ORDER BY key
    LOOP
        EXECUTE format('CREATE INDEX ON accum.%I (%I)', p_name || '_totals_day', dim_key);
        EXECUTE format('CREATE INDEX ON accum.%I (%I)', p_name || '_totals_month', dim_key);
        EXECUTE format('CREATE INDEX ON accum.%I (%I)', p_name || '_totals_year', dim_key);
    END LOOP;

    IF p_high_write THEN
        EXECUTE format('CREATE INDEX ON accum.%I (dim_hash)', p_name || '_balance_cache_delta');
        EXECUTE format('CREATE INDEX ON accum.%I (created_at)', p_name || '_balance_cache_delta');
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION accum._ddl_drop_infrastructure(p_name text)
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE format('DROP TABLE IF EXISTS accum.%I CASCADE', p_name || '_movements');
    EXECUTE format('DROP TABLE IF EXISTS accum.%I CASCADE', p_name || '_totals_day');
    EXECUTE format('DROP TABLE IF EXISTS accum.%I CASCADE', p_name || '_totals_month');
    EXECUTE format('DROP TABLE IF EXISTS accum.%I CASCADE', p_name || '_totals_year');
    EXECUTE format('DROP TABLE IF EXISTS accum.%I CASCADE', p_name || '_balance_cache');
    EXECUTE format('DROP TABLE IF EXISTS accum.%I CASCADE', p_name || '_balance_cache_delta');

    EXECUTE format('DROP FUNCTION IF EXISTS accum.%I CASCADE', '_trg_' || p_name || '_before_insert');
    EXECUTE format('DROP FUNCTION IF EXISTS accum.%I CASCADE', '_trg_' || p_name || '_after_insert');
    EXECUTE format('DROP FUNCTION IF EXISTS accum.%I CASCADE', '_trg_' || p_name || '_after_delete');
    EXECUTE format('DROP FUNCTION IF EXISTS accum.%I CASCADE', '_trg_' || p_name || '_block_update');
    EXECUTE format('DROP FUNCTION IF EXISTS accum.%I CASCADE', '_trg_' || p_name || '_protect_derived');

    PERFORM accum._drop_hash_function(p_name);
END;
$$;

-- ============================================================
-- TRIGGER GENERATION (mirrors sql/04_triggers.sql)
-- ============================================================

CREATE OR REPLACE FUNCTION accum._generate_triggers(
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
    res_update_d     text := '';
    res_update_m     text := '';
    res_update_y     text := '';
    res_update_c     text := '';
    -- UPDATE SET clauses for DELETE triggers
    res_sub_d        text := '';
    res_sub_m        text := '';
    res_sub_y        text := '';
    res_sub_c        text := '';

    -- Ledger-specific variables
    ledger_res_cols      text := '';
    ledger_res_sum_cols  text := '';
    ledger_res_dr_select text := '';
    ledger_res_cr_select text := '';
    res_ledger_update_d  text := '';
    res_ledger_update_m  text := '';
    res_ledger_update_y  text := '';
    res_ledger_update_c  text := '';
    res_ledger_sub_d     text := '';
    res_ledger_sub_m     text := '';
    res_ledger_sub_y     text := '';
    res_ledger_sub_c     text := '';

    -- Assembled SQL statements
    totals_upsert_d  text;
    totals_upsert_m  text;
    totals_upsert_y  text;
    cache_upsert     text := '';
    del_totals_d     text;
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
            res_update_d := res_update_d || ', ';
            res_update_m := res_update_m || ', ';
            res_update_y := res_update_y || ', ';
            res_update_c := res_update_c || ', ';
            res_sub_d    := res_sub_d    || ', ';
            res_sub_m    := res_sub_m    || ', ';
            res_sub_y    := res_sub_y    || ', ';
            res_sub_c    := res_sub_c    || ', ';

            ledger_res_cols      := ledger_res_cols || ', ';
            ledger_res_sum_cols  := ledger_res_sum_cols || ', ';
            ledger_res_dr_select := ledger_res_dr_select || ', ';
            ledger_res_cr_select := ledger_res_cr_select || ', ';
            res_ledger_update_d  := res_ledger_update_d || ', ';
            res_ledger_update_m  := res_ledger_update_m || ', ';
            res_ledger_update_y  := res_ledger_update_y || ', ';
            res_ledger_update_c  := res_ledger_update_c || ', ';
            res_ledger_sub_d     := res_ledger_sub_d || ', ';
            res_ledger_sub_m     := res_ledger_sub_m || ', ';
            res_ledger_sub_y     := res_ledger_sub_y || ', ';
            res_ledger_sub_c     := res_ledger_sub_c || ', ';
        END IF;

        res_cols     := res_cols     || format('%I', res_key);
        res_sum_cols := res_sum_cols || format('SUM(%I) AS %I', res_key, res_key);
        -- UPSERT: add incoming value to existing
        res_update_d := res_update_d || format('%I = accum.%I.%I + EXCLUDED.%I',
            res_key, p_name || '_totals_day', res_key, res_key);
        res_update_m := res_update_m || format('%I = accum.%I.%I + EXCLUDED.%I',
            res_key, p_name || '_totals_month', res_key, res_key);
        res_update_y := res_update_y || format('%I = accum.%I.%I + EXCLUDED.%I',
            res_key, p_name || '_totals_year', res_key, res_key);
        res_update_c := res_update_c || format('%I = accum.%I.%I + EXCLUDED.%I',
            res_key, p_name || '_balance_cache', res_key, res_key);
        -- DELETE: subtract aggregated value
        res_sub_d := res_sub_d || format('%I = t.%I - agg.%I', res_key, res_key, res_key);
        res_sub_m := res_sub_m || format('%I = t.%I - agg.%I', res_key, res_key, res_key);
        res_sub_y := res_sub_y || format('%I = t.%I - agg.%I', res_key, res_key, res_key);
        res_sub_c := res_sub_c || format('%I = c.%I - agg.%I', res_key, res_key, res_key);

        -- Ledger specific formulas
        ledger_res_cols := ledger_res_cols || format('%I_dr, %I_cr', res_key, res_key);
        ledger_res_sum_cols := ledger_res_sum_cols || format('SUM(%I_dr) AS %I_dr, SUM(%I_cr) AS %I_cr', res_key, res_key, res_key, res_key);
        
        ledger_res_dr_select := ledger_res_dr_select || format('%I AS %I_dr, 0::numeric AS %I_cr', res_key, res_key, res_key);
        ledger_res_cr_select := ledger_res_cr_select || format('0::numeric AS %I_dr, %I AS %I_cr', res_key, res_key, res_key);

        res_ledger_update_d := res_ledger_update_d || format(
            '%I_dr = accum.%I.%I_dr + EXCLUDED.%I_dr, %I_cr = accum.%I.%I_cr + EXCLUDED.%I_cr',
            res_key, p_name || '_totals_day', res_key, res_key, res_key, p_name || '_totals_day', res_key, res_key
        );
        res_ledger_update_m := res_ledger_update_m || format(
            '%I_dr = accum.%I.%I_dr + EXCLUDED.%I_dr, %I_cr = accum.%I.%I_cr + EXCLUDED.%I_cr',
            res_key, p_name || '_totals_month', res_key, res_key, res_key, p_name || '_totals_month', res_key, res_key
        );
        res_ledger_update_y := res_ledger_update_y || format(
            '%I_dr = accum.%I.%I_dr + EXCLUDED.%I_dr, %I_cr = accum.%I.%I_cr + EXCLUDED.%I_cr',
            res_key, p_name || '_totals_year', res_key, res_key, res_key, p_name || '_totals_year', res_key, res_key
        );
        res_ledger_update_c := res_ledger_update_c || format(
            '%I_dr = accum.%I.%I_dr + EXCLUDED.%I_dr, %I_cr = accum.%I.%I_cr + EXCLUDED.%I_cr',
            res_key, p_name || '_balance_cache', res_key, res_key, res_key, p_name || '_balance_cache', res_key, res_key
        );

        res_ledger_sub_d := res_ledger_sub_d || format(
            '%I_dr = t.%I_dr - agg.%I_dr, %I_cr = t.%I_cr - agg.%I_cr',
            res_key, res_key, res_key, res_key, res_key, res_key
        );
        res_ledger_sub_m := res_ledger_sub_m || format(
            '%I_dr = t.%I_dr - agg.%I_dr, %I_cr = t.%I_cr - agg.%I_cr',
            res_key, res_key, res_key, res_key, res_key, res_key
        );
        res_ledger_sub_y := res_ledger_sub_y || format(
            '%I_dr = t.%I_dr - agg.%I_dr, %I_cr = t.%I_cr - agg.%I_cr',
            res_key, res_key, res_key, res_key, res_key, res_key
        );
        res_ledger_sub_c := res_ledger_sub_c || format(
            '%I_dr = c.%I_dr - agg.%I_dr, %I_cr = c.%I_cr - agg.%I_cr',
            res_key, res_key, res_key, res_key, res_key, res_key
        );

        first_res := false;
    END LOOP;

    -- ============================================================
    -- 1. BEFORE INSERT trigger (FOR EACH ROW): compute dim_hash
    -- ============================================================
    IF p_kind = 'ledger' THEN
        DECLARE
            hash_call_args_dr text;
            hash_call_args_cr text;
        BEGIN
            hash_call_args_dr := 'NEW.subconto_dr' || CASE WHEN hash_call_args != '' THEN ', ' || hash_call_args ELSE '' END;
            hash_call_args_cr := 'NEW.subconto_cr' || CASE WHEN hash_call_args != '' THEN ', ' || hash_call_args ELSE '' END;
            EXECUTE format(
                'CREATE OR REPLACE FUNCTION accum.%I() RETURNS trigger
                 LANGUAGE plpgsql AS $trg$
                 BEGIN
                     NEW.dim_hash_dr := accum.%I(%s);
                     NEW.dim_hash_cr := accum.%I(%s);
                     IF NEW.period < now() - interval ''1 day'' THEN
                         NEW.movement_type := ''adjustment'';
                     END IF;
                     RETURN NEW;
                 END;
                 $trg$',
                '_trg_' || p_name || '_before_insert',
                '_hash_' || p_name,
                hash_call_args_dr,
                '_hash_' || p_name,
                hash_call_args_cr
            );
        END;
    ELSE
        EXECUTE format(
            'CREATE OR REPLACE FUNCTION accum.%I() RETURNS trigger
             LANGUAGE plpgsql AS $trg$
             BEGIN
                 NEW.dim_hash := accum.%I(%s);
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
    END IF;

    EXECUTE format(
        'CREATE TRIGGER trg_%s_before_insert
         BEFORE INSERT ON accum.%I
         FOR EACH ROW EXECUTE FUNCTION accum.%I()',
        p_name,
        p_name || '_movements',
        '_trg_' || p_name || '_before_insert'
    );

    -- ============================================================
    -- 2. AFTER INSERT trigger (FOR EACH STATEMENT): batch aggregation
    --    Uses REFERENCING NEW TABLE AS new_rows for efficient batch processing
    -- ============================================================

    IF p_kind = 'ledger' THEN
        -- totals_day UPSERT with aggregation
        totals_upsert_d := format(
            'WITH splits AS (
                 SELECT account_dr AS account, dim_hash_dr AS dim_hash, subconto_dr AS subconto, period::date AS period, %s, %s FROM new_rows
                 UNION ALL
                 SELECT account_cr AS account, dim_hash_cr AS dim_hash, subconto_cr AS subconto, period::date AS period, %s, %s FROM new_rows
             )
             INSERT INTO accum.%I (account, dim_hash, subconto, period, %s, %s)
             SELECT account, dim_hash, subconto, period, %s, %s
             FROM splits
             GROUP BY account, dim_hash, subconto, period, %s
             ON CONFLICT (account, dim_hash, period) DO UPDATE SET %s',
            dim_cols, ledger_res_dr_select,
            dim_cols, ledger_res_cr_select,
            p_name || '_totals_day',
            dim_cols, ledger_res_cols,
            dim_cols, ledger_res_sum_cols,
            dim_cols,
            res_ledger_update_d
        );

        -- totals_month UPSERT with aggregation
        totals_upsert_m := format(
            'WITH splits AS (
                 SELECT account_dr AS account, dim_hash_dr AS dim_hash, subconto_dr AS subconto, date_trunc(''month'', period)::date AS period, %s, %s FROM new_rows
                 UNION ALL
                 SELECT account_cr AS account, dim_hash_cr AS dim_hash, subconto_cr AS subconto, date_trunc(''month'', period)::date AS period, %s, %s FROM new_rows
             )
             INSERT INTO accum.%I (account, dim_hash, subconto, period, %s, %s)
             SELECT account, dim_hash, subconto, period, %s, %s
             FROM splits
             GROUP BY account, dim_hash, subconto, period, %s
             ON CONFLICT (account, dim_hash, period) DO UPDATE SET %s',
            dim_cols, ledger_res_dr_select,
            dim_cols, ledger_res_cr_select,
            p_name || '_totals_month',
            dim_cols, ledger_res_cols,
            dim_cols, ledger_res_sum_cols,
            dim_cols,
            res_ledger_update_m
        );

        -- totals_year UPSERT with aggregation
        totals_upsert_y := format(
            'WITH splits AS (
                 SELECT account_dr AS account, dim_hash_dr AS dim_hash, subconto_dr AS subconto, date_trunc(''year'', period)::date AS period, %s, %s FROM new_rows
                 UNION ALL
                 SELECT account_cr AS account, dim_hash_cr AS dim_hash, subconto_cr AS subconto, date_trunc(''year'', period)::date AS period, %s, %s FROM new_rows
             )
             INSERT INTO accum.%I (account, dim_hash, subconto, period, %s, %s)
             SELECT account, dim_hash, subconto, period, %s, %s
             FROM splits
             GROUP BY account, dim_hash, subconto, period, %s
             ON CONFLICT (account, dim_hash, period) DO UPDATE SET %s',
            dim_cols, ledger_res_dr_select,
            dim_cols, ledger_res_cr_select,
            p_name || '_totals_year',
            dim_cols, ledger_res_cols,
            dim_cols, ledger_res_sum_cols,
            dim_cols,
            res_ledger_update_y
        );

        -- balance_cache UPSERT
        cache_upsert := format(
            'WITH splits AS (
                 SELECT account_dr AS account, dim_hash_dr AS dim_hash, subconto_dr AS subconto, %s, %s FROM new_rows
                 UNION ALL
                 SELECT account_cr AS account, dim_hash_cr AS dim_hash, subconto_cr AS subconto, %s, %s FROM new_rows
             )
             INSERT INTO accum.%I (account, dim_hash, subconto, %s, %s, last_movement_at, last_movement_id, version)
             SELECT account, dim_hash, subconto, %s, %s, now(), NULL, 1
             FROM splits
             GROUP BY account, dim_hash, subconto, %s
             ON CONFLICT (account, dim_hash) DO UPDATE SET %s,
                 last_movement_at = EXCLUDED.last_movement_at,
                 version = accum.%I.version + 1',
            dim_cols, ledger_res_dr_select,
            dim_cols, ledger_res_cr_select,
            p_name || '_balance_cache',
            dim_cols, ledger_res_cols,
            dim_cols, ledger_res_sum_cols,
            dim_cols,
            res_ledger_update_c,
            p_name || '_balance_cache'
        );
    ELSE
        -- totals_day UPSERT with aggregation (standard)
        totals_upsert_d := format(
            'INSERT INTO accum.%I (dim_hash, period, %s, %s)
             SELECT dim_hash, period::date, %s, %s
             FROM new_rows
             GROUP BY dim_hash, period::date, %s
             ON CONFLICT (dim_hash, period) DO UPDATE SET %s',
            p_name || '_totals_day',
            dim_cols, res_cols,
            dim_cols, res_sum_cols,
            dim_cols,
            res_update_d
        );

        -- totals_month UPSERT with aggregation (standard)
        totals_upsert_m := format(
            'INSERT INTO accum.%I (dim_hash, period, %s, %s)
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

        -- totals_year UPSERT with aggregation (standard)
        totals_upsert_y := format(
            'INSERT INTO accum.%I (dim_hash, period, %s, %s)
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
                    'INSERT INTO accum.%I (dim_hash, %s, %s, last_movement_at, last_movement_id, version)
                     SELECT dim_hash, %s, %s, now(), (array_agg(id))[1], 1
                     FROM new_rows
                     GROUP BY dim_hash, %s
                     ON CONFLICT (dim_hash) DO UPDATE SET %s,
                         last_movement_at = EXCLUDED.last_movement_at,
                         last_movement_id = EXCLUDED.last_movement_id,
                         version = accum.%I.version + 1',
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
                    'INSERT INTO accum.%I (dim_hash, %s, last_movement_at, last_movement_id, version)
                     SELECT DISTINCT ON (dim_hash) dim_hash, %s, now(), id, 0
                     FROM new_rows
                     ON CONFLICT (dim_hash) DO NOTHING;
                     INSERT INTO accum.%I (dim_hash, %s)
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
    END IF;

    -- Assemble AFTER INSERT trigger body
    trg_body_insert := totals_upsert_d || '; ' || totals_upsert_m || '; ' || totals_upsert_y || ';';
    IF cache_upsert != '' THEN
        trg_body_insert := trg_body_insert || ' ' || cache_upsert || ';';
    END IF;

    EXECUTE format(
        'CREATE OR REPLACE FUNCTION accum.%I() RETURNS trigger
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
         AFTER INSERT ON accum.%I
         REFERENCING NEW TABLE AS new_rows
         FOR EACH STATEMENT EXECUTE FUNCTION accum.%I()',
        p_name,
        p_name || '_movements',
        '_trg_' || p_name || '_after_insert'
    );

    -- ============================================================
    -- 3. AFTER DELETE trigger (FOR EACH STATEMENT): batch subtraction
    --    Uses REFERENCING OLD TABLE AS old_rows
    -- ============================================================

    IF p_kind = 'ledger' THEN
        -- Subtract aggregated resources from totals_day
        del_totals_d := format(
            'WITH splits_old AS (
                 SELECT account_dr AS account, dim_hash_dr AS dim_hash, period::date AS period, %s, %s FROM old_rows
                 UNION ALL
                 SELECT account_cr AS account, dim_hash_cr AS dim_hash, period::date AS period, %s, %s FROM old_rows
             ),
             agg AS (
                 SELECT account, dim_hash, period, %s
                 FROM splits_old
                 GROUP BY account, dim_hash, period
             )
             UPDATE accum.%I t SET %s
             FROM agg
             WHERE t.account = agg.account AND t.dim_hash = agg.dim_hash AND t.period = agg.period',
            dim_cols, ledger_res_dr_select,
            dim_cols, ledger_res_cr_select,
            ledger_res_sum_cols,
            p_name || '_totals_day',
            res_ledger_sub_d
        );

        -- Subtract aggregated resources from totals_month
        del_totals_m := format(
            'WITH splits_old AS (
                 SELECT account_dr AS account, dim_hash_dr AS dim_hash, date_trunc(''month'', period)::date AS period, %s, %s FROM old_rows
                 UNION ALL
                 SELECT account_cr AS account, dim_hash_cr AS dim_hash, date_trunc(''month'', period)::date AS period, %s, %s FROM old_rows
             ),
             agg AS (
                 SELECT account, dim_hash, period, %s
                 FROM splits_old
                 GROUP BY account, dim_hash, period
             )
             UPDATE accum.%I t SET %s
             FROM agg
             WHERE t.account = agg.account AND t.dim_hash = agg.dim_hash AND t.period = agg.period',
            dim_cols, ledger_res_dr_select,
            dim_cols, ledger_res_cr_select,
            ledger_res_sum_cols,
            p_name || '_totals_month',
            res_ledger_sub_m
        );

        -- Subtract aggregated resources from totals_year
        del_totals_y := format(
            'WITH splits_old AS (
                 SELECT account_dr AS account, dim_hash_dr AS dim_hash, date_trunc(''year'', period)::date AS period, %s, %s FROM old_rows
                 UNION ALL
                 SELECT account_cr AS account, dim_hash_cr AS dim_hash, date_trunc(''year'', period)::date AS period, %s, %s FROM old_rows
             ),
             agg AS (
                 SELECT account, dim_hash, period, %s
                 FROM splits_old
                 GROUP BY account, dim_hash, period
             )
             UPDATE accum.%I t SET %s
             FROM agg
             WHERE t.account = agg.account AND t.dim_hash = agg.dim_hash AND t.period = agg.period',
            dim_cols, ledger_res_dr_select,
            dim_cols, ledger_res_cr_select,
            ledger_res_sum_cols,
            p_name || '_totals_year',
            res_ledger_sub_y
        );

        -- Subtract aggregated resources from balance_cache
        del_cache := format(
            'WITH splits_old AS (
                 SELECT account_dr AS account, dim_hash_dr AS dim_hash, %s, %s FROM old_rows
                 UNION ALL
                 SELECT account_cr AS account, dim_hash_cr AS dim_hash, %s, %s FROM old_rows
             ),
             agg AS (
                 SELECT account, dim_hash, %s
                 FROM splits_old
                 GROUP BY account, dim_hash
             )
             UPDATE accum.%I c SET %s, version = c.version + 1
             FROM agg
             WHERE c.account = agg.account AND c.dim_hash = agg.dim_hash',
            dim_cols, ledger_res_dr_select,
            dim_cols, ledger_res_cr_select,
            ledger_res_sum_cols,
            p_name || '_balance_cache',
            res_ledger_sub_c
        );
    ELSE
        -- Subtract aggregated resources from totals_day (standard)
        del_totals_d := format(
            'UPDATE accum.%I t SET %s
             FROM (SELECT dim_hash, period::date AS period, %s
                   FROM old_rows GROUP BY dim_hash, period::date) agg
             WHERE t.dim_hash = agg.dim_hash AND t.period = agg.period',
            p_name || '_totals_day', res_sub_d, res_sum_cols);

        -- Subtract aggregated resources from totals_month (standard)
        del_totals_m := format(
            'UPDATE accum.%I t SET %s
             FROM (SELECT dim_hash, date_trunc(''month'', period)::date AS period, %s
                   FROM old_rows GROUP BY dim_hash, date_trunc(''month'', period)::date) agg
             WHERE t.dim_hash = agg.dim_hash AND t.period = agg.period',
            p_name || '_totals_month', res_sub_m, res_sum_cols);

        -- Subtract aggregated resources from totals_year (standard)
        del_totals_y := format(
            'UPDATE accum.%I t SET %s
             FROM (SELECT dim_hash, date_trunc(''year'', period)::date AS period, %s
                   FROM old_rows GROUP BY dim_hash, date_trunc(''year'', period)::date) agg
             WHERE t.dim_hash = agg.dim_hash AND t.period = agg.period',
            p_name || '_totals_year', res_sub_y, res_sum_cols);

        -- Subtract aggregated resources from balance_cache (standard)
        IF p_kind = 'balance' THEN
            del_cache := format(
                'UPDATE accum.%I c SET %s, version = c.version + 1
                 FROM (SELECT dim_hash, %s FROM old_rows GROUP BY dim_hash) agg
                 WHERE c.dim_hash = agg.dim_hash',
                p_name || '_balance_cache', res_sub_c, res_sum_cols);
        END IF;
    END IF;

    EXECUTE format(
        'CREATE OR REPLACE FUNCTION accum.%I() RETURNS trigger
         LANGUAGE plpgsql AS $trg$
         BEGIN
             %s;
             %s;
             %s;
             %s
             RETURN NULL;
         END;
         $trg$',
        '_trg_' || p_name || '_after_delete',
        del_totals_d,
        del_totals_m,
        del_totals_y,
        CASE WHEN del_cache != '' THEN del_cache || ';' ELSE '' END
    );

    EXECUTE format(
        'CREATE TRIGGER trg_%s_after_delete
         AFTER DELETE ON accum.%I
         REFERENCING OLD TABLE AS old_rows
         FOR EACH STATEMENT EXECUTE FUNCTION accum.%I()',
        p_name,
        p_name || '_movements',
        '_trg_' || p_name || '_after_delete'
    );

    -- ============================================================
    -- 4. Protection triggers: prevent direct UPDATE on movements
    --    and direct modification of derived tables outside trigger context
    -- ============================================================

    -- Block UPDATE on movements (must use register_repost)
    EXECUTE format(
        'CREATE OR REPLACE FUNCTION accum.%I() RETURNS trigger
         LANGUAGE plpgsql AS $trg$
         BEGIN
             IF current_setting(''pg_accumulator.allow_internal'', true) = ''on'' THEN
                 RETURN NEW;
             END IF;
             RAISE EXCEPTION ''Direct UPDATE on movements is not allowed. Use register_repost() instead.'';
         END;
         $trg$',
        '_trg_' || p_name || '_block_update'
    );

    EXECUTE format(
        'CREATE TRIGGER trg_%s_block_update
         BEFORE UPDATE ON accum.%I
         FOR EACH ROW EXECUTE FUNCTION accum.%I()',
        p_name,
        p_name || '_movements',
        '_trg_' || p_name || '_block_update'
    );

    -- Block direct modification of derived tables (totals + cache)
    -- pg_trigger_depth() > 0 means we are inside a trigger chain (legitimate)
    -- pg_accumulator.allow_internal = 'on' means maintenance function is running
    EXECUTE format(
        'CREATE OR REPLACE FUNCTION accum.%I() RETURNS trigger
         LANGUAGE plpgsql AS $trg$
         BEGIN
             IF pg_trigger_depth() > 1 THEN
                 RETURN COALESCE(NEW, OLD);
             END IF;
             IF current_setting(''pg_accumulator.allow_internal'', true) = ''on'' THEN
                 RETURN COALESCE(NEW, OLD);
             END IF;
             RAISE EXCEPTION ''Direct modification of derived table is not allowed. Use register_rebuild_totals() / register_rebuild_cache() for corrections.'';
         END;
         $trg$',
        '_trg_' || p_name || '_protect_derived'
    );

    -- Protect totals_day
    EXECUTE format(
        'CREATE TRIGGER trg_%s_protect_totals_day
         BEFORE INSERT OR UPDATE OR DELETE ON accum.%I
         FOR EACH ROW EXECUTE FUNCTION accum.%I()',
        p_name,
        p_name || '_totals_day',
        '_trg_' || p_name || '_protect_derived'
    );

    -- Protect totals_month
    EXECUTE format(
        'CREATE TRIGGER trg_%s_protect_totals_month
         BEFORE INSERT OR UPDATE OR DELETE ON accum.%I
         FOR EACH ROW EXECUTE FUNCTION accum.%I()',
        p_name,
        p_name || '_totals_month',
        '_trg_' || p_name || '_protect_derived'
    );

    -- Protect totals_year
    EXECUTE format(
        'CREATE TRIGGER trg_%s_protect_totals_year
         BEFORE INSERT OR UPDATE OR DELETE ON accum.%I
         FOR EACH ROW EXECUTE FUNCTION accum.%I()',
        p_name,
        p_name || '_totals_year',
        '_trg_' || p_name || '_protect_derived'
    );

    -- Protect balance_cache (if applicable)
    IF p_kind = 'balance' OR p_kind = 'ledger' THEN
        EXECUTE format(
            'CREATE TRIGGER trg_%s_protect_balance_cache
             BEFORE INSERT OR UPDATE OR DELETE ON accum.%I
             FOR EACH ROW EXECUTE FUNCTION accum.%I()',
            p_name,
            p_name || '_balance_cache',
            '_trg_' || p_name || '_protect_derived'
        );
    END IF;
END;
$$;

-- ============================================================
-- REGISTRY API (mirrors sql/07_registry_api.sql)
-- ============================================================

CREATE OR REPLACE FUNCTION accum.register_create(
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
    -- Validate
    PERFORM accum._validate_name(name);
    PERFORM accum._validate_dimensions(dimensions);
    PERFORM accum._validate_resources(resources);

    IF kind NOT IN ('balance', 'turnover', 'ledger') THEN
        RAISE EXCEPTION 'Invalid kind: %. Must be balance, turnover or ledger', kind;
    END IF;
    IF totals_period NOT IN ('day', 'month', 'year') THEN
        RAISE EXCEPTION 'Invalid totals_period: %', totals_period;
    END IF;
    IF partition_by NOT IN ('day', 'month', 'quarter', 'year') THEN
        RAISE EXCEPTION 'Invalid partition_by: %', partition_by;
    END IF;

    IF accum._register_exists(name) THEN
        RAISE EXCEPTION 'Register "%" already exists', name;
    END IF;

    -- Save metadata
    PERFORM accum._register_put(name, kind, dimensions, resources,
        totals_period, partition_by, high_write, recorder_type);

    -- DDL: tables, indexes, hash, triggers — with cleanup on failure
    BEGIN
        PERFORM accum._ddl_create_movements_table(name, recorder_type, dimensions, resources);
        PERFORM accum._ddl_create_totals_tables(name, dimensions, resources);

        IF kind = 'balance' OR kind = 'ledger' THEN
            PERFORM accum._ddl_create_balance_cache(name, dimensions, resources);
        END IF;

        IF high_write THEN
            PERFORM accum._ddl_create_delta_buffer(name, resources);
        END IF;

        PERFORM accum._ddl_create_indexes(name, kind, dimensions, high_write);
        PERFORM accum._generate_hash_function(name, dimensions);
        PERFORM accum._generate_triggers(name, kind, dimensions, resources, high_write);

        -- Create initial partitions for the movements table
        PERFORM accum._create_initial_partitions(name, partition_by, 3);

        -- Generate per-register read functions (balance, turnover, movements)
        -- Only available when extension is loaded (not in pure emulation mode)
        BEGIN
            PERFORM accum._generate_read_functions(name, kind, dimensions, resources, high_write);
        EXCEPTION WHEN undefined_function THEN
            NULL; -- Skip if read API not loaded
        END;
    EXCEPTION WHEN OTHERS THEN
        -- Cleanup partially created objects on failure
        PERFORM accum._ddl_drop_infrastructure(name);
        PERFORM accum._register_delete(name);
        RAISE;
    END;
END;
$$;

CREATE OR REPLACE FUNCTION accum.register_drop(
    name  text,
    force boolean DEFAULT false
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    reg record;
BEGIN
    SELECT * INTO reg FROM accum._registers r WHERE r.name = register_drop.name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', name;
    END IF;

    IF NOT force THEN
        DECLARE
            cnt bigint;
        BEGIN
            EXECUTE format('SELECT count(*) FROM accum.%I', name || '_movements') INTO cnt;
            IF cnt > 0 THEN
                RAISE EXCEPTION 'Register "%" contains % movements. Use force := true to drop.',
                    name, cnt;
            END IF;
        END;
    END IF;

    PERFORM accum._ddl_drop_infrastructure(name);
    PERFORM accum._register_delete(name);
END;
$$;

CREATE OR REPLACE FUNCTION accum.register_list()
RETURNS TABLE(name text, kind text, dimensions int, resources int, created_at timestamptz)
LANGUAGE sql STABLE AS $$
    SELECT
        r.name,
        r.kind,
        (SELECT count(*)::int FROM jsonb_object_keys(r.dimensions)),
        (SELECT count(*)::int FROM jsonb_object_keys(r.resources)),
        r.created_at
    FROM accum._registers r
    ORDER BY r.created_at;
$$;


-- ============================================================
-- REGISTER_INFO: Detailed register info
-- ============================================================
CREATE OR REPLACE FUNCTION accum.register_info(p_name text)
RETURNS jsonb
LANGUAGE plpgsql STABLE AS $$
DECLARE
    reg           record;
    result        jsonb;
    movements_cnt bigint := 0;
    partitions    jsonb  := '[]'::jsonb;
    tables_info   jsonb;
BEGIN
    SELECT * INTO reg FROM accum._registers r WHERE r.name = p_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_name;
    END IF;

    -- Movement count
    BEGIN
        EXECUTE format('SELECT count(*) FROM accum.%I',
            p_name || '_movements') INTO movements_cnt;
    EXCEPTION WHEN undefined_table THEN
        movements_cnt := 0;
    END;

    -- Partition info
    BEGIN
        SELECT jsonb_agg(jsonb_build_object(
            'name', child.relname,
            'range', pg_get_expr(c.relpartbound, c.oid)
        ) ORDER BY child.relname)
        INTO partitions
        FROM pg_inherits i
        JOIN pg_class parent ON i.inhparent = parent.oid
        JOIN pg_class child  ON i.inhrelid  = child.oid
        JOIN pg_catalog.pg_class c ON c.oid = child.oid
        JOIN pg_namespace ns ON parent.relnamespace = ns.oid
        WHERE parent.relname = p_name || '_movements'
          AND ns.nspname = 'accum';
    EXCEPTION WHEN OTHERS THEN
        partitions := '[]'::jsonb;
    END;

    IF partitions IS NULL THEN
        partitions := '[]'::jsonb;
    END IF;

    -- Tables info
    tables_info := jsonb_build_object(
        'movements', p_name || '_movements',
        'totals_day', p_name || '_totals_day',
        'totals_month', p_name || '_totals_month',
        'totals_year', p_name || '_totals_year'
    );

    IF reg.kind = 'balance' THEN
        tables_info := tables_info || jsonb_build_object(
            'balance_cache', p_name || '_balance_cache');
    END IF;

    IF reg.high_write THEN
        tables_info := tables_info || jsonb_build_object(
            'balance_cache_delta', p_name || '_balance_cache_delta');
    END IF;

    result := jsonb_build_object(
        'name',            reg.name,
        'kind',            reg.kind,
        'dimensions',      reg.dimensions,
        'resources',       reg.resources,
        'totals_period',   reg.totals_period,
        'partition_by',    reg.partition_by,
        'high_write',      reg.high_write,
        'recorder_type',   reg.recorder_type,
        'created_at',      reg.created_at,
        'movements_count', movements_cnt,
        'tables',          tables_info,
        'partitions',      partitions
    );

    RETURN result;
END;
$$;


-- ============================================================
-- REGISTER_POST: Post movements to a register (batch INSERT)
-- ============================================================
CREATE OR REPLACE FUNCTION accum.register_post(
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
    SELECT * INTO reg FROM accum._registers r WHERE r.name = p_register;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_register;
    END IF;

    IF jsonb_typeof(p_data) = 'object' THEN
        movements := jsonb_build_array(p_data);
    ELSIF jsonb_typeof(p_data) = 'array' THEN
        movements := p_data;
    ELSE
        RAISE EXCEPTION 'Data must be a JSON object or array';
    END IF;

    -- Build column list once from register metadata
    FOR dim_key IN SELECT key FROM jsonb_each_text(reg.dimensions) ORDER BY key
    LOOP
        col_list := col_list || ', ' || quote_ident(dim_key);
    END LOOP;

    FOR res_key IN SELECT key FROM jsonb_each_text(reg.resources) ORDER BY key
    LOOP
        col_list := col_list || ', ' || quote_ident(res_key);
    END LOOP;

    -- Build VALUES tuples for batch INSERT
    FOR mov IN SELECT * FROM jsonb_array_elements(movements)
    LOOP
        IF mov->>'recorder' IS NULL THEN
            RAISE EXCEPTION 'recorder is required';
        END IF;
        IF mov->>'period' IS NULL THEN
            RAISE EXCEPTION 'period is required';
        END IF;

        -- Ensure partition exists for this movement's period
        PERFORM accum._ensure_partition(p_register, (mov->>'period')::timestamptz);

        tuple := format('%L, %L::timestamptz', mov->>'recorder', mov->>'period');

        FOR dim_key, dim_type IN SELECT key, value FROM jsonb_each_text(reg.dimensions) ORDER BY key
        LOOP
            IF mov->>dim_key IS NULL THEN
                RAISE EXCEPTION 'dimension "%" is required', dim_key;
            END IF;
            tuple := tuple || ', ' || format('%L::%s', mov->>dim_key, dim_type);
        END LOOP;

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

    -- Single batch INSERT — triggers fire once per statement
    IF total_count > 0 THEN
        EXECUTE format('INSERT INTO accum.%I (%s) VALUES %s',
            p_register || '_movements', col_list, values_str);
    END IF;

    RETURN total_count;
END;
$$;


-- ============================================================
-- REGISTER_UNPOST: Cancel all movements by recorder
-- ============================================================
CREATE OR REPLACE FUNCTION accum.register_unpost(
    p_register text,
    p_recorder text
) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    reg   record;
    cnt   int;
BEGIN
    SELECT * INTO reg FROM accum._registers r WHERE r.name = p_register;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Register "%" does not exist', p_register;
    END IF;

    EXECUTE format(
        'WITH deleted AS (
            DELETE FROM accum.%I WHERE recorder = %L RETURNING 1
        ) SELECT count(*) FROM deleted',
        p_register || '_movements',
        p_recorder
    ) INTO cnt;

    RETURN cnt;
END;
$$;


-- ============================================================
-- REGISTER_REPOST: Atomic re-post (unpost + post)
-- ============================================================
CREATE OR REPLACE FUNCTION accum.register_repost(
    p_register text,
    p_recorder text,
    p_data     jsonb
) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    cnt int;
BEGIN
    PERFORM accum.register_unpost(p_register, p_recorder);

    IF jsonb_typeof(p_data) = 'object' THEN
        p_data := jsonb_set(p_data, '{recorder}', to_jsonb(p_recorder));
    ELSIF jsonb_typeof(p_data) = 'array' THEN
        SELECT jsonb_agg(jsonb_set(elem, '{recorder}', to_jsonb(p_recorder)))
        INTO p_data
        FROM jsonb_array_elements(p_data) AS elem;
    END IF;

    cnt := accum.register_post(p_register, p_data);
    RETURN cnt;
END;
$$;


