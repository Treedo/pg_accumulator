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
