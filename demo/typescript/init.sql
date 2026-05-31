-- Demo init: create extension, register 'finance', seed transactions
-- Runs automatically on first docker compose up

CREATE EXTENSION IF NOT EXISTS pg_accumulator;

-- Balance register: tracks financial account balances by category
SELECT accum.register_create(
    name          := 'finance',
    dimensions    := '{"account": "int", "category": "int"}',
    resources     := '{"amount": "numeric(18,2)"}',
    kind          := 'balance',
    totals_period := 'month'
);

-- Accounts:   1=Готівка, 2=Банківський рахунок, 3=Кредитна картка
-- Categories: 1=Зарплата, 2=Їжа, 3=Транспорт, 4=Комунальні, 5=Розваги, 6=Інше

-- April salary
SELECT accum.register_post('finance', '{
    "recorder": "income:001", "period": "2026-04-01",
    "account": 2, "category": 1, "amount": 55000
}');
SELECT accum.register_post('finance', '{
    "recorder": "income:002", "period": "2026-04-01",
    "account": 1, "category": 1, "amount": 15000
}');

-- Food expenses
SELECT accum.register_post('finance', '{
    "recorder": "expense:001", "period": "2026-04-03",
    "account": 1, "category": 2, "amount": -2400
}');
SELECT accum.register_post('finance', '{
    "recorder": "expense:002", "period": "2026-04-10",
    "account": 2, "category": 2, "amount": -3100
}');
SELECT accum.register_post('finance', '{
    "recorder": "expense:003", "period": "2026-04-17",
    "account": 1, "category": 2, "amount": -1800
}');

-- Transport
SELECT accum.register_post('finance', '{
    "recorder": "expense:004", "period": "2026-04-05",
    "account": 2, "category": 3, "amount": -850
}');

-- Utilities
SELECT accum.register_post('finance', '{
    "recorder": "expense:005", "period": "2026-04-08",
    "account": 2, "category": 4, "amount": -3400
}');

-- Entertainment (credit card)
SELECT accum.register_post('finance', '{
    "recorder": "expense:006", "period": "2026-04-14",
    "account": 3, "category": 5, "amount": -4200
}');
SELECT accum.register_post('finance', '{
    "recorder": "expense:007", "period": "2026-04-18",
    "account": 3, "category": 5, "amount": -1500
}');

-- Compatibility View for Demo application
CREATE OR REPLACE VIEW accum.registers AS
SELECT name, kind, dimensions, resources, totals_period, partition_by, high_write, created_at, updated_at
FROM accum._registers;

-- Create a ledger register for general ledger bookkeeping
SELECT accum.register_create(
    name          := 'general_ledger',
    dimensions    := '{"currency": "text"}',
    resources     := '{"amount": "numeric(18,2)"}',
    kind          := 'ledger',
    totals_period := 'day'
);

-- Seed Ledger postings:
-- 1. Initial capital input (Active debit cash 10, Passive credit capital 40)
SELECT accum.register_post('general_ledger', '{
    "recorder": "capital:1",
    "period":   "2026-04-01",
    "currency": "USD",
    "account_dr": "10",
    "subconto_dr": {"bank": "Головний банк"},
    "account_cr": "40",
    "subconto_cr": {"owner": "Засновник"},
    "amount": 100000.00
}');

-- 2. Buy raw materials / goods (Active debit inventory 28, Active credit cash 10)
SELECT accum.register_post('general_ledger', '{
    "recorder": "purchase:1",
    "period":   "2026-04-02",
    "currency": "USD",
    "account_dr": "28",
    "subconto_dr": {"item_id": 1, "supplier": "Оптовий постачальник"},
    "account_cr": "10",
    "subconto_cr": {"bank": "Головний банк"},
    "amount": 30000.00
}');

-- 3. Office Rent payment (Active debit rent expense 90, Active credit cash 10)
SELECT accum.register_post('general_ledger', '{
    "recorder": "rent:1",
    "period":   "2026-04-05",
    "currency": "USD",
    "account_dr": "90",
    "subconto_dr": {"purpose": "Оренда офісу за Квітень"},
    "account_cr": "10",
    "subconto_cr": {"bank": "Головний банк"},
    "amount": 2000.00
}');

-- 4. Get short term bank loan (Active debit cash 10, Passive credit loan 50)
SELECT accum.register_post('general_ledger', '{
    "recorder": "loan:1",
    "period":   "2026-04-06",
    "currency": "USD",
    "account_dr": "10",
    "subconto_dr": {"bank": "Головний банк"},
    "account_cr": "50",
    "subconto_cr": {"lender": "Альфа Банк"},
    "amount": 50000.00
}');
