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
