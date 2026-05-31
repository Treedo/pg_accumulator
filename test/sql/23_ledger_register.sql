-- test/sql/23_ledger_register.sql
-- Comprehensive test suite for Double-Entry Ledger Registers (kind := 'ledger')

BEGIN;
SELECT plan(22);

-- ============================================================
-- 1. EMULATE LEDGER REGISTER CREATION & INFRASTRUCTURE
-- ============================================================

-- Function to dynamically create a ledger register infrastructure
CREATE OR REPLACE FUNCTION accum.test_create_ledger_register(
    p_name text,
    p_global_dimensions jsonb, -- e.g., {"currency": "text"}
    p_resources jsonb         -- e.g., {"amount": "numeric"}
) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    v_dim_cols text := '';
    v_res_cols text := '';
    v_dim_key text;
    v_dim_type text;
    v_res_key text;
    v_res_type text;
BEGIN
    -- Build global dimension column definitions
    FOR v_dim_key, v_dim_type IN SELECT * FROM jsonb_each_text(p_global_dimensions) ORDER BY key
    LOOP
        v_dim_cols := v_dim_cols || format(', %I %s NOT NULL', v_dim_key, v_dim_type);
    END LOOP;

    -- Build resource column definitions
    FOR v_res_key, v_res_type IN SELECT * FROM jsonb_each_text(p_resources) ORDER BY key
    LOOP
        v_res_cols := v_res_cols || format(', %I %s NOT NULL DEFAULT 0', v_res_key, v_res_type);
    END LOOP;

    -- A. Create Movements Table for Double Entry
    EXECUTE format(
        'CREATE TABLE accum.%I (
            id             uuid          DEFAULT gen_random_uuid(),
            recorded_at    timestamptz   DEFAULT now() NOT NULL,
            recorder       text          NOT NULL,
            period         timestamptz   NOT NULL,
            account_dr     text          NOT NULL,
            subconto_dr    jsonb         NOT NULL DEFAULT ''{}''::jsonb,
            account_cr     text          NOT NULL,
            subconto_cr    jsonb         NOT NULL DEFAULT ''{}''::jsonb,
            dim_hash_dr    bigint        NOT NULL,
            dim_hash_cr    bigint        NOT NULL
            %s
            %s,
            PRIMARY KEY (id, period)
        )',
        p_name || '_movements',
        v_dim_cols,
        v_res_cols
    );

    -- B. Create Balance Cache Table for Flat Account Balances
    EXECUTE format(
        'CREATE TABLE accum.%I (
            account        text          NOT NULL,
            dim_hash       bigint        NOT NULL,
            subconto       jsonb         NOT NULL DEFAULT ''{}''::jsonb,
            amount_dr      numeric(18,2) NOT NULL DEFAULT 0,
            amount_cr      numeric(18,2) NOT NULL DEFAULT 0,
            last_period    timestamptz   NOT NULL,
            PRIMARY KEY (account, dim_hash)
        )',
        p_name || '_balance_cache'
    );
END;
$$;

-- Function to generate dynamic 64-bit dim_hash for dynamic Subconto
CREATE OR REPLACE FUNCTION accum.test_hash_ledger_dim(
    p_subconto jsonb,
    p_currency text
) RETURNS bigint LANGUAGE sql IMMUTABLE AS $$
    -- Simple robust hashing mimicking Murmur/xxHash for dynamic JSON key/value pairs
    SELECT accum._md5_to_bigint(coalesce(p_currency, '') || '|' || coalesce(p_subconto::text, ''));
$$;

-- Trigger function for BEFORE INSERT to populate hashes
CREATE OR REPLACE FUNCTION accum.test_trg_ledger_before_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.dim_hash_dr := accum.test_hash_ledger_dim(NEW.subconto_dr, NEW.currency);
    NEW.dim_hash_cr := accum.test_hash_ledger_dim(NEW.subconto_cr, NEW.currency);
    RETURN NEW;
END;
$$;

-- Trigger function for AFTER INSERT to propagate to balance_cache (double entry split)
CREATE OR REPLACE FUNCTION accum.test_trg_ledger_after_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    -- 1. Update DEBIT side in cache
    INSERT INTO accum.test_ledger_balance_cache (account, dim_hash, subconto, amount_dr, amount_cr, last_period)
    VALUES (NEW.account_dr, NEW.dim_hash_dr, NEW.subconto_dr, NEW.amount, 0, NEW.period)
    ON CONFLICT (account, dim_hash) DO UPDATE SET 
        amount_dr = accum.test_ledger_balance_cache.amount_dr + EXCLUDED.amount_dr,
        last_period = LEATEST_TIMESTAMPTZ(accum.test_ledger_balance_cache.last_period, EXCLUDED.last_period);

    -- 2. Update CREDIT side in cache
    INSERT INTO accum.test_ledger_balance_cache (account, dim_hash, subconto, amount_dr, amount_cr, last_period)
    VALUES (NEW.account_cr, NEW.dim_hash_cr, NEW.subconto_cr, 0, NEW.amount, NEW.period)
    ON CONFLICT (account, dim_hash) DO UPDATE SET 
        amount_cr = accum.test_ledger_balance_cache.amount_cr + EXCLUDED.amount_cr,
        last_period = LEATEST_TIMESTAMPTZ(accum.test_ledger_balance_cache.last_period, EXCLUDED.last_period);

    RETURN NULL;
END;
$$;

-- Trigger function for AFTER DELETE to subtract from balance_cache on unpost/deletion
CREATE OR REPLACE FUNCTION accum.test_trg_ledger_after_delete()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    -- 1. Subtract from DEBIT side
    UPDATE accum.test_ledger_balance_cache
    SET amount_dr = amount_dr - OLD.amount
    WHERE account = OLD.account_dr AND dim_hash = OLD.dim_hash_dr;

    -- 2. Subtract from CREDIT side
    UPDATE accum.test_ledger_balance_cache
    SET amount_cr = amount_cr - OLD.amount
    WHERE account = OLD.account_cr AND dim_hash = OLD.dim_hash_cr;

    RETURN NULL;
END;
$$;

-- Helper to find latest timestamp (handling nulls)
CREATE OR REPLACE FUNCTION LEATEST_TIMESTAMPTZ(a timestamptz, b timestamptz)
RETURNS timestamptz LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN a IS NULL THEN b WHEN b IS NULL THEN a WHEN a > b THEN a ELSE b END;
$$;

-- ============================================================
-- 2. READ API: BALANCE GET WITH SIGN LOGIC
-- ============================================================
CREATE OR REPLACE FUNCTION accum.test_ledger_get_balance(
    p_account text,
    p_subconto jsonb DEFAULT NULL,
    p_currency text DEFAULT 'UAH'
) RETURNS TABLE (
    account text,
    subconto jsonb,
    turnover_dr numeric,
    amount_cr numeric,
    balance numeric
) LANGUAGE plpgsql AS $$
DECLARE
    v_dim_hash bigint;
    v_acc_type text; -- 'A' (Active), 'P' (Passive), 'AP' (Active-Passive)
BEGIN
    IF p_subconto IS NOT NULL THEN
        v_dim_hash := accum.test_hash_ledger_dim(p_subconto, p_currency);
    END IF;

    -- Determine account type based on standard accounting rules:
    -- 1xx, 2xx, 9xx are Active. 4xx, 5xx, 7xx are Passive. Others are Active-Passive (AP).
    v_acc_type := CASE 
        WHEN p_account ~ '^(1|2|9)' THEN 'A'
        WHEN p_account ~ '^(4|5|7)' THEN 'P'
        ELSE 'AP'
    END;

    RETURN QUERY
    SELECT 
        c.account,
        c.subconto,
        c.amount_dr,
        c.amount_cr,
        CASE 
            WHEN v_acc_type = 'A' THEN c.amount_dr - c.amount_cr
            WHEN v_acc_type = 'P' THEN c.amount_cr - c.amount_dr
            ELSE c.amount_dr - c.amount_cr -- Default net for AP info
        END AS balance
    FROM accum.test_ledger_balance_cache c
    WHERE c.account = p_account
      AND (v_dim_hash IS NULL OR c.dim_hash = v_dim_hash);
END;
$$;

-- ============================================================
-- 3. FINANCIAL SOUNDNESS / DYNAMIC AUDIT FUNCTION
-- ============================================================
CREATE OR REPLACE FUNCTION accum.test_ledger_verify_soundness()
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
    v_total_dr numeric;
    v_total_cr numeric;
BEGIN
    SELECT COALESCE(sum(amount_dr), 0), COALESCE(sum(amount_cr), 0)
    INTO v_total_dr, v_total_cr
    FROM accum.test_ledger_balance_cache;

    RETURN v_total_dr = v_total_cr;
END;
$$;


-- ============================================================
-- 4. START RUNNING TAP ASSERTS
-- ============================================================

-- A. Establish Ledger Register Infrastructure
SELECT lives_ok(
    $$SELECT accum.test_create_ledger_register('test_ledger', '{"currency": "text"}', '{"amount": "numeric(18,2)"}')$$,
    'Should create ledger register movements and cache tables successfully'
);

-- B. Attach the triggers to mimic compilation DDL logic
CREATE TRIGGER trg_test_ledger_before
    BEFORE INSERT ON accum.test_ledger_movements
    FOR EACH ROW EXECUTE FUNCTION accum.test_trg_ledger_before_insert();

CREATE TRIGGER trg_test_ledger_after_ins
    AFTER INSERT ON accum.test_ledger_movements
    FOR EACH ROW EXECUTE FUNCTION accum.test_trg_ledger_after_insert();

CREATE TRIGGER trg_test_ledger_after_del
    AFTER DELETE ON accum.test_ledger_movements
    FOR EACH ROW EXECUTE FUNCTION accum.test_trg_ledger_after_delete();

-- C. Check empty state assertions
SELECT is(
    (SELECT count(*)::int FROM accum.test_ledger_movements),
    0,
    'Movements table should start empty'
);

SELECT is(
    (SELECT count(*)::int FROM accum.test_ledger_balance_cache),
    0,
    'Balance cache table should start empty'
);

SELECT is(
    accum.test_ledger_verify_soundness(),
    true,
    'Empty ledger register is naturally sound/balanced (0 = 0)'
);

-- ============================================================
-- OPERATION 1: Post initial deposit (Debit: Cash 1010, Credit: Share Capital 4010)
-- ============================================================
-- This operation funds the company with 100,000 UAH.
INSERT INTO accum.test_ledger_movements (recorder, period, account_dr, subconto_dr, account_cr, subconto_cr, currency, amount)
VALUES (
    'capital_deposit_1', 
    '2026-05-30 09:00:00+00', 
    '1010', '{"bank_account_id": 1}'::jsonb, 
    '4010', '{"shareholder_id": 42}'::jsonb, 
    'UAH', 
    100000.00
);

-- Verifications of single movement splitting
SELECT is(
    (SELECT count(*)::int FROM accum.test_ledger_movements),
    1,
    'One movement row recorded in the ledger movements table'
);

SELECT is(
    (SELECT count(*)::int FROM accum.test_ledger_balance_cache),
    2,
    'Two separate balance cache entries created (one for 1010-Debit, one for 4010-Credit)'
);

SELECT is(
    accum.test_ledger_verify_soundness(),
    true,
    'Double-entry balance remains sound after capital funding (UAH 100,000)'
);

-- Assert balance of Cash Account (1010, Active)
SELECT is(
    (SELECT balance FROM accum.test_ledger_get_balance('1010', '{"bank_account_id": 1}'::jsonb, 'UAH')),
    100000.00::numeric,
    'Active cash account 1010 shows positive balance of 100,000.00 (Dr - Cr)'
);

-- Assert balance of Share Capital Account (4010, Passive)
SELECT is(
    (SELECT balance FROM accum.test_ledger_get_balance('4010', '{"shareholder_id": 42}'::jsonb, 'UAH')),
    100000.00::numeric,
    'Passive equity account 4010 shows positive balance of 100,000.00 (Cr - Dr)'
);

-- ============================================================
-- OPERATION 2: Pay cash to purchase Inventory (Debit: Goods 2810, Credit: Cash 1010)
-- ============================================================
-- Purchase goods worth 35,000 UAH.
INSERT INTO accum.test_ledger_movements (recorder, period, account_dr, subconto_dr, account_cr, subconto_cr, currency, amount)
VALUES (
    'inventory_purchase_1', 
    '2026-05-30 10:00:00+00', 
    '2810', '{"warehouse_id": 5, "product_id": 99}'::jsonb, 
    '1010', '{"bank_account_id": 1}'::jsonb, 
    'UAH', 
    35000.00
);

SELECT is(
    accum.test_ledger_verify_soundness(),
    true,
    'Verification equation matches perfectly after asset structure recomposition'
);

-- Cash account (1010) should have decreased to 65,000
SELECT is(
    (SELECT balance FROM accum.test_ledger_get_balance('1010', '{"bank_account_id": 1}'::jsonb, 'UAH')),
    65000.00::numeric,
    'Cash balance reflects purchase reduction (100k - 35k = 65k)'
);

-- Goods inventory account (2810, Active) should have 35,000
SELECT is(
    (SELECT balance FROM accum.test_ledger_get_balance('2810', '{"warehouse_id": 5, "product_id": 99}'::jsonb, 'UAH')),
    35000.00::numeric,
    'Inventory account holds 35,000 worth of active assets'
);

-- ============================================================
-- OPERATION 3: Business expense from Share Capital (Debit: Expense 9200, Credit: Cash 1010)
-- ============================================================
-- Pay operational expenses: 5,000 UAH.
INSERT INTO accum.test_ledger_movements (recorder, period, account_dr, subconto_dr, account_cr, subconto_cr, currency, amount)
VALUES (
    'expense_payment_1', 
    '2026-05-30 11:30:00+00', 
    '9200', '{"expense_type": "rent"}'::jsonb, 
    '1010', '{"bank_account_id": 1}'::jsonb, 
    'UAH', 
    5000.00
);

SELECT is(
    (SELECT balance FROM accum.test_ledger_get_balance('1010', '{"bank_account_id": 1}'::jsonb, 'UAH')),
    60000.00::numeric,
    'Cash balance down to 60k UAH after operational rent expense'
);

SELECT is(
    (SELECT balance FROM accum.test_ledger_get_balance('9200', '{"expense_type": "rent"}'::jsonb, 'UAH')),
    5000.00::numeric,
    'Expense account 9200 (Active) shows rent expense balance of 5,000.00'
);

-- ============================================================
-- AUDITING: Verify cumulative numbers and aggregate statements
-- ============================================================
SELECT is(
    (SELECT sum(amount_dr) FROM accum.test_ledger_balance_cache),
    140000.00::numeric,
    'Debit total turnover is 100k + 35k + 5k = 140,000'
);

SELECT is(
    (SELECT sum(amount_cr) FROM accum.test_ledger_balance_cache),
    140000.00::numeric,
    'Credit total turnover is 100k + 35k + 5k = 140,000'
);

-- ============================================================
-- UNPOSTING (CANCELLATION) OF MOVEMENTS
-- ============================================================
-- Cancel the inventory purchase (1)
DELETE FROM accum.test_ledger_movements WHERE recorder = 'inventory_purchase_1';

SELECT is(
    accum.test_ledger_verify_soundness(),
    true,
    'Double-entry soundness persists perfectly after transaction cancellation/unposting'
);

-- After canceling inventory purchase, cash should return to 95,000 (100k capital - 5k rent)
SELECT is(
    (SELECT balance FROM accum.test_ledger_get_balance('1010', '{"bank_account_id": 1}'::jsonb, 'UAH')),
    95000.00::numeric,
    'Cash balance automatically restored to 95,000.00'
);

-- Goods inventory should be back to 0
SELECT is(
    (SELECT balance FROM accum.test_ledger_get_balance('2810', '{"warehouse_id": 5, "product_id": 99}'::jsonb, 'UAH')),
    0.00::numeric,
    'Goods inventory assets reduced to 0.00'
);

-- ============================================================
-- RECORD CORRECTION (REPOSTING MECHANISM)
-- ============================================================
-- Re-post operational expenses as corrected: 4,500.00 instead of 5,000.00.
-- We emulate repost by deleting and inserting inside a transaction, just like register_repost() does.

DELETE FROM accum.test_ledger_movements WHERE recorder = 'expense_payment_1';
INSERT INTO accum.test_ledger_movements (recorder, period, account_dr, subconto_dr, account_cr, subconto_cr, currency, amount)
VALUES (
    'expense_payment_1', 
    '2026-05-30 11:30:00+00', 
    '9200', '{"expense_type": "rent"}'::jsonb, 
    '1010', '{"bank_account_id": 1}'::jsonb, 
    'UAH', 
    4500.00
);

SELECT is(
    accum.test_ledger_verify_soundness(),
    true,
    'Debit matches Credit after operational expenses correction'
);

SELECT is(
    (SELECT balance FROM accum.test_ledger_get_balance('1010', '{"bank_account_id": 1}'::jsonb, 'UAH')),
    95500.00::numeric,
    'Cash balance corrects itself to 95,500.00'
);

SELECT is(
    (SELECT balance FROM accum.test_ledger_get_balance('9200', '{"expense_type": "rent"}'::jsonb, 'UAH')),
    4500.00::numeric,
    'Rent expense balance corrects itself to 4,500.00'
);


-- Cleanup tables and functions
DROP TABLE accum.test_ledger_movements CASCADE;
DROP TABLE accum.test_ledger_balance_cache CASCADE;
DROP FUNCTION accum.test_create_ledger_register;
DROP FUNCTION accum.test_hash_ledger_dim;
DROP FUNCTION accum.test_trg_ledger_before_insert;
DROP FUNCTION accum.test_trg_ledger_after_insert;
DROP FUNCTION accum.test_trg_ledger_after_delete;
DROP FUNCTION accum.test_ledger_get_balance;
DROP FUNCTION accum.test_ledger_verify_soundness;

SELECT * FROM finish();
ROLLBACK;
