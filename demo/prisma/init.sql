-- Demo init: create extension + register + seed products & warehouses
-- Runs automatically on first docker compose up

CREATE EXTENSION IF NOT EXISTS pg_accumulator;

-- Create the inventory register
SELECT accum.register_create(
    name          := 'inventory',
    dimensions    := '{"warehouse_id": "int", "product_id": "int"}',
    resources     := '{"quantity": "numeric", "cost": "numeric(14,2)"}',
    kind          := 'balance',
    totals_period := 'month'
);

-- Application tables (managed by Prisma, but seeded here for convenience)
CREATE TABLE IF NOT EXISTS "Product" (
    id    SERIAL PRIMARY KEY,
    name  VARCHAR(200) NOT NULL,
    sku   VARCHAR(50) UNIQUE NOT NULL,
    price DECIMAL(12,2) DEFAULT 0
);

CREATE TABLE IF NOT EXISTS "Warehouse" (
    id   SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    city VARCHAR(100) NOT NULL
);

-- Seed warehouses
INSERT INTO "Warehouse" (id, name, city) VALUES
    (1, 'Central Warehouse', 'Kyiv'),
    (2, 'West Warehouse', 'Lviv'),
    (3, 'South Warehouse', 'Odesa')
ON CONFLICT (id) DO NOTHING;

-- Seed products
INSERT INTO "Product" (id, name, sku, price) VALUES
    (1, 'Laptop Lenovo T14', 'NB-T14', 32000.00),
    (2, 'Monitor Dell 27"', 'MON-D27', 12500.00),
    (3, 'Mechanical Keyboard', 'KB-MECH', 3200.00),
    (4, 'Wireless Mouse', 'MS-WIFI', 1450.00),
    (5, 'USB Hub 7-port', 'USB-HUB7', 890.00)
ON CONFLICT (id) DO NOTHING;

-- Seed initial inventory movements
SELECT accum.register_post('inventory', '{
    "recorder": "init:001", "period": "2026-04-01",
    "warehouse_id": 1, "product_id": 1, "quantity": 20, "cost": 640000
}');
SELECT accum.register_post('inventory', '{
    "recorder": "init:002", "period": "2026-04-01",
    "warehouse_id": 1, "product_id": 2, "quantity": 50, "cost": 625000
}');
SELECT accum.register_post('inventory', '{
    "recorder": "init:003", "period": "2026-04-01",
    "warehouse_id": 1, "product_id": 3, "quantity": 100, "cost": 320000
}');
SELECT accum.register_post('inventory', '{
    "recorder": "init:004", "period": "2026-04-01",
    "warehouse_id": 2, "product_id": 1, "quantity": 10, "cost": 320000
}');
SELECT accum.register_post('inventory', '{
    "recorder": "init:005", "period": "2026-04-01",
    "warehouse_id": 2, "product_id": 4, "quantity": 200, "cost": 290000
}');
SELECT accum.register_post('inventory', '{
    "recorder": "init:006", "period": "2026-04-01",
    "warehouse_id": 3, "product_id": 5, "quantity": 500, "cost": 445000
}');

-- Simulate some shipments
SELECT accum.register_post('inventory', '{
    "recorder": "ship:001", "period": "2026-04-10",
    "warehouse_id": 1, "product_id": 1, "quantity": -3, "cost": -96000
}');
SELECT accum.register_post('inventory', '{
    "recorder": "ship:002", "period": "2026-04-12",
    "warehouse_id": 1, "product_id": 2, "quantity": -10, "cost": -125000
}');
SELECT accum.register_post('inventory', '{
    "recorder": "ship:003", "period": "2026-04-15",
    "warehouse_id": 2, "product_id": 4, "quantity": -50, "cost": -72500
}');

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
