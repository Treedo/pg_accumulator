-- test/sql/15_end_to_end_finance.sql
-- End-to-end test: financial transactions scenario from README

BEGIN;
SELECT plan(6);

-- ============================================================
-- Create financial register
-- ============================================================
SELECT accum.register_create(
    name       := 'account_balance',
    dimensions := '{"account": "int", "currency": "text"}',
    resources  := '{"debit": "numeric(18,2)", "credit": "numeric(18,2)", "net": "numeric(18,2)"}',
    kind       := 'balance'
);

-- ============================================================
-- Deposit to account 1001
-- ============================================================
SELECT accum.register_post('account_balance', '{
    "recorder":"deposit:1","period":"2026-04-01",
    "account":1001,"currency":"UAH",
    "debit":10000,"credit":0,"net":10000
}');

SELECT is(
    (SELECT net FROM accum.account_balance_balance_cache WHERE account=1001 AND currency='UAH'),
    10000.00::numeric,
    'Account 1001 should have 10000 UAH after deposit'
);

-- ============================================================
-- Transfer 5000 from 1001 to 2001
-- ============================================================
SELECT accum.register_post('account_balance', '[
    {"recorder":"transfer:T1","period":"2026-04-18","account":1001,"currency":"UAH","debit":0,"credit":5000,"net":-5000},
    {"recorder":"transfer:T1","period":"2026-04-18","account":2001,"currency":"UAH","debit":5000,"credit":0,"net":5000}
]');

SELECT is(
    (SELECT net FROM accum.account_balance_balance_cache WHERE account=1001 AND currency='UAH'),
    5000.00::numeric,
    'Account 1001 should have 5000 after transfer'
);

SELECT is(
    (SELECT net FROM accum.account_balance_balance_cache WHERE account=2001 AND currency='UAH'),
    5000.00::numeric,
    'Account 2001 should have 5000 after transfer'
);

-- ============================================================
-- Cancel transfer
-- ============================================================
SELECT accum.register_unpost('account_balance', 'transfer:T1');

SELECT is(
    (SELECT net FROM accum.account_balance_balance_cache WHERE account=1001 AND currency='UAH'),
    10000.00::numeric,
    'Account 1001 restored to 10000 after cancel'
);

SELECT is(
    (SELECT net FROM accum.account_balance_balance_cache WHERE account=2001 AND currency='UAH'),
    0.00::numeric,
    'Account 2001 should be 0 after cancel'
);

-- ============================================================
-- Verify no money created/destroyed (sum = deposit)
-- ============================================================
SELECT is(
    (SELECT sum(net) FROM accum.account_balance_balance_cache WHERE currency='UAH'),
    10000.00::numeric,
    'Total UAH across all accounts should always equal deposit amount'
);

-- Cleanup
SELECT accum.register_drop('account_balance', force := true);

SELECT * FROM finish();
ROLLBACK;
