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
    (1, 'Центральний склад', 'Київ'),
    (2, 'Склад Захід', 'Львів'),
    (3, 'Склад Південь', 'Одеса')
ON CONFLICT (id) DO NOTHING;

-- Seed products
INSERT INTO "Product" (id, name, sku, price) VALUES
    (1, 'Ноутбук Lenovo T14', 'NB-T14', 32000.00),
    (2, 'Монітор Dell 27"', 'MON-D27', 12500.00),
    (3, 'Клавіатура механічна', 'KB-MECH', 3200.00),
    (4, 'Миша бездротова', 'MS-WIFI', 1450.00),
    (5, 'USB-хаб 7 портів', 'USB-HUB7', 890.00)
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
