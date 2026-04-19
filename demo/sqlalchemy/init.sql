-- Demo init: create extension, ORM tables, register and seed data
-- Runs automatically on first docker compose up

CREATE EXTENSION IF NOT EXISTS pg_accumulator;

-- =====================================================================
-- ORM-managed tables (standard SQLAlchemy models)
-- =====================================================================

CREATE TABLE IF NOT EXISTS warehouses (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    address VARCHAR(255),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    sku VARCHAR(50) NOT NULL UNIQUE,
    name VARCHAR(200) NOT NULL,
    unit_price NUMERIC(18,2) DEFAULT 0,
    category VARCHAR(100),
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS clients (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    email VARCHAR(200),
    phone VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    client_id INTEGER REFERENCES clients(id),
    warehouse_id INTEGER REFERENCES warehouses(id),
    status VARCHAR(20) DEFAULT 'draft',
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS order_lines (
    id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(id) ON DELETE CASCADE,
    product_id INTEGER REFERENCES products(id),
    quantity NUMERIC(18,4) NOT NULL,
    unit_price NUMERIC(18,2) NOT NULL,
    amount NUMERIC(18,2) NOT NULL
);

-- =====================================================================
-- Seed ORM data
-- =====================================================================

INSERT INTO warehouses (id, name, address) VALUES
    (1, 'Main Warehouse',   '100 Industrial Blvd, Austin TX'),
    (2, 'East Distribution', '42 Logistics Ave, Atlanta GA'),
    (3, 'West Fulfillment',  '88 Harbor Dr, Portland OR');

INSERT INTO products (id, sku, name, unit_price, category) VALUES
    (101, 'WDG-A1',  'Widget Alpha',     250.00, 'Widgets'),
    (102, 'WDG-B2',  'Widget Beta',      150.00, 'Widgets'),
    (103, 'GDG-X1',  'Gadget X',         600.00, 'Gadgets'),
    (104, 'GDG-Y2',  'Gadget Y',         450.00, 'Gadgets'),
    (105, 'SPR-001', 'Spare Part #001',   35.00, 'Parts');

INSERT INTO clients (id, name, email, phone) VALUES
    (1, 'Acme Corp',       'orders@acme.example.com',    '+1-555-0101'),
    (2, 'Globex Inc',      'procurement@globex.example', '+1-555-0202'),
    (3, 'Initech LLC',     'buying@initech.example',     '+1-555-0303');

-- =====================================================================
-- pg_accumulator register
-- =====================================================================

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
    "warehouse": 1, "product": 101,
    "quantity": 500, "amount": 125000
}');

SELECT accum.register_post('inventory', '{
    "recorder": "receipt:2",
    "period":   "2026-04-01",
    "warehouse": 1, "product": 102,
    "quantity": 300, "amount": 45000
}');

SELECT accum.register_post('inventory', '{
    "recorder": "receipt:3",
    "period":   "2026-04-05",
    "warehouse": 2, "product": 101,
    "quantity": 200, "amount": 50000
}');

SELECT accum.register_post('inventory', '{
    "recorder": "receipt:4",
    "period":   "2026-04-10",
    "warehouse": 2, "product": 103,
    "quantity": 150, "amount": 90000
}');

-- Seed: shipments (negative quantities)
SELECT accum.register_post('inventory', '{
    "recorder": "shipment:1",
    "period":   "2026-04-12",
    "warehouse": 1, "product": 101,
    "quantity": -80, "amount": -20000
}');

SELECT accum.register_post('inventory', '{
    "recorder": "shipment:2",
    "period":   "2026-04-15",
    "warehouse": 1, "product": 102,
    "quantity": -50, "amount": -7500
}');

SELECT accum.register_post('inventory', '{
    "recorder": "shipment:3",
    "period":   "2026-04-18",
    "warehouse": 2, "product": 101,
    "quantity": -30, "amount": -7500
}');

-- =====================================================================
-- Seed demo orders (linked to ORM entities + accumulator movements)
-- =====================================================================

-- Order 1: Acme Corp buys from Main Warehouse
INSERT INTO orders (id, client_id, warehouse_id, status) VALUES (1, 1, 1, 'posted');
INSERT INTO order_lines (order_id, product_id, quantity, unit_price, amount) VALUES
    (1, 101, 80, 250.00, 20000.00);

-- Order 2: Globex buys from Main Warehouse
INSERT INTO orders (id, client_id, warehouse_id, status) VALUES (2, 2, 1, 'posted');
INSERT INTO order_lines (order_id, product_id, quantity, unit_price, amount) VALUES
    (2, 102, 50, 150.00, 7500.00);

-- Order 3: Initech buys from East Distribution
INSERT INTO orders (id, client_id, warehouse_id, status) VALUES (3, 3, 2, 'posted');
INSERT INTO order_lines (order_id, product_id, quantity, unit_price, amount) VALUES
    (3, 101, 30, 250.00, 7500.00);

-- Reset sequences
SELECT setval('warehouses_id_seq', 10);
SELECT setval('products_id_seq', 200);
SELECT setval('clients_id_seq', 10);
SELECT setval('orders_id_seq', 10);
SELECT setval('order_lines_id_seq', 10);
