-- Demo init: create extension, register and seed data
-- Runs automatically on first docker compose up

CREATE EXTENSION IF NOT EXISTS pg_accumulator;

-- Create a balance register for warehouse inventory
SELECT accum.register_create(
    name          := 'inventory',
    dimensions    := '{"warehouse": "int", "product": "int"}',
    resources     := '{"quantity": "numeric(18,4)", "amount": "numeric(18,2)"}',
    kind          := 'balance',
    totals_period := 'day'
);

-- Seed: receipts to warehouse 1
SELECT accum.register_post('inventory', '{
    "recorder": "receipt:1",
    "period":   "2026-04-01",
    "warehouse": 1, "product": 1,
    "quantity": 100, "amount": 50000
}');

SELECT accum.register_post('inventory', '{
    "recorder": "receipt:2",
    "period":   "2026-04-01",
    "warehouse": 1, "product": 2,
    "quantity": 200, "amount": 30000
}');

SELECT accum.register_post('inventory', '{
    "recorder": "receipt:3",
    "period":   "2026-04-05",
    "warehouse": 2, "product": 1,
    "quantity": 50, "amount": 25000
}');

-- Seed: shipments (negative quantities)
SELECT accum.register_post('inventory', '{
    "recorder": "shipment:1",
    "period":   "2026-04-10",
    "warehouse": 1, "product": 1,
    "quantity": -30, "amount": -15000
}');

SELECT accum.register_post('inventory', '{
    "recorder": "shipment:2",
    "period":   "2026-04-12",
    "warehouse": 1, "product": 2,
    "quantity": -50, "amount": -7500
}');

SELECT accum.register_post('inventory', '{
    "recorder": "receipt:4",
    "period":   "2026-04-15",
    "warehouse": 2, "product": 2,
    "quantity": 80, "amount": 12000
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
    "subconto_dr": {"bank": "Main Bank"},
    "account_cr": "40",
    "subconto_cr": {"owner": "Founder"},
    "amount": 100000.00
}');

-- 2. Buy raw materials / goods (Active debit inventory 28, Active credit cash 10)
SELECT accum.register_post('general_ledger', '{
    "recorder": "purchase:1",
    "period":   "2026-04-02",
    "currency": "USD",
    "account_dr": "28",
    "subconto_dr": {"item_id": 1, "supplier": "Wholesale Corp"},
    "account_cr": "10",
    "subconto_cr": {"bank": "Main Bank"},
    "amount": 30000.00
}');

-- 3. Office Rent payment (Active debit rent expense 90, Active credit cash 10)
SELECT accum.register_post('general_ledger', '{
    "recorder": "rent:1",
    "period":   "2026-04-05",
    "currency": "USD",
    "account_dr": "90",
    "subconto_dr": {"purpose": "Office Rent April"},
    "account_cr": "10",
    "subconto_cr": {"bank": "Main Bank"},
    "amount": 2000.00
}');

-- 4. Get short term bank loan (Active debit cash 10, Passive credit loan 50)
SELECT accum.register_post('general_ledger', '{
    "recorder": "loan:1",
    "period":   "2026-04-06",
    "currency": "USD",
    "account_dr": "10",
    "subconto_dr": {"bank": "Main Bank"},
    "account_cr": "50",
    "subconto_cr": {"lender": "First Federal"},
    "amount": 50000.00
}');
