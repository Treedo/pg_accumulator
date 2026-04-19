# pg_accumulator

**Accumulation Registers for PostgreSQL** — an extension that turns PostgreSQL into a full-featured resource accounting platform with instant access to balances and turnovers.

---

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Quick Start](#quick-start)
- [Installation](#installation)
- [Concepts](#concepts)
  - [What is an Accumulation Register](#what-is-an-accumulation-register)
  - [Dimensions and Resources](#dimensions-and-resources)
  - [Register Types](#register-types)
- [API Reference](#api-reference)
  - [Registry Management](#registry-management)
    - [`register_create()`](#register_create)
    - [`register_alter()`](#register_alter)
    - [`register_drop()`](#register_drop)
    - [`register_list()`](#register_list)
    - [`register_info()`](#register_info)
  - [Writing Data](#writing-data)
    - [`register_post()`](#register_post)
    - [`register_unpost()`](#register_unpost)
    - [`register_repost()`](#register_repost)
    - [Direct INSERT](#direct-insert)
  - [Reading Data](#reading-data)
    - [`<register>_balance()`](#register_balance)
    - [`<register>_turnover()`](#register_turnover)
    - [`<register>_movements()`](#register_movements)
    - [`<register>_balance_cache`](#register_balance_cache)
- [Architecture](#architecture)
  - [Overview Diagram](#overview-diagram)
  - [Movements Layer](#movements-layer)
  - [Totals Layer](#totals-layer)
  - [Balance Cache](#balance-cache)
  - [Append-Only Writes and Retroactive Corrections](#append-only-writes-and-retroactive-corrections)
  - [Trigger Chain](#trigger-chain)
- [Concurrency and Performance](#concurrency-and-performance)
  - [Row-Level Locking](#row-level-locking)
  - [Delta Buffer (High-Write Mode)](#delta-buffer-high-write-mode)
  - [Benchmarks](#benchmarks)
- [Retroactive Corrections](#retroactive-corrections)
- [Design Guidelines](#design-guidelines)
- [Usage Examples](#usage-examples)
  - [Warehouse Inventory](#warehouse-inventory)
  - [Financial Accounting](#financial-accounting)
  - [Subscription Usage Tracking](#subscription-usage-tracking)
- [Diagnostics and Maintenance](#diagnostics-and-maintenance)
- [Configuration](#configuration)
- [Compatibility](#compatibility)
- [FAQ](#faq)
- [License](#license)

---

## Overview

`pg_accumulator` is a PostgreSQL extension that provides a declarative mechanism for **accumulation registers** — a high-level abstraction for tracking resource balances and turnovers across arbitrary dimensions.

**The Problem.** Developers and AI agents building accounting systems must repeatedly hand-craft movement tables, totals tables, triggers to keep aggregates current, recalculation logic when historical data changes, and indexes for fast reads.

**The Solution.** A single call to `register_create()` automatically creates the entire infrastructure: tables, indexes, partitions, triggers, read functions, and a background maintenance process.

```sql
SELECT register_create(
    name       := 'inventory',
    dimensions := '{"warehouse": "int", "product": "int", "lot": "text"}',
    resources  := '{"quantity": "numeric", "amount": "numeric"}',
    kind       := 'balance',
    totals_period := 'day'
);
```

After that:

```sql
-- Record a movement
SELECT register_post('inventory', '{
    "recorder":  "purchase_order:7001",
    "period":    "2026-04-18",
    "warehouse": 1,
    "product":   42,
    "lot":       "LOT-A",
    "quantity":  100,
    "amount":    5000
}');

-- Get the current balance — O(1), ~0.1ms
SELECT * FROM accum.inventory_balance(
    dimensions := '{"warehouse": 1, "product": 42}'
);
```

---

## Key Features

| Feature | Description |
|---|---|
| **Declarative creation** | One function call instead of dozens of DDL statements |
| **Append-only movements** | Linear write log: INSERT only, never UPDATE |
| **Hierarchical totals** | Year → Month turnovers for fast historical queries |
| **Balance Cache** | Current balance always available in O(1) |
| **Synchronous updates** | Totals and cache are current immediately after COMMIT |
| **Retroactive corrections** | O(1) — no cascade recalculation |
| **Recorder pattern** | Atomic post/cancel of an entire business document |
| **High-write mode** | Delta buffer to reduce hot-row contention |
| **Auto-partitioning** | Movements table partitioned by period automatically |
| **Full audit trail** | Original movements and corrections stored separately |

---

## Quick Start

### 1. Install

```bash
git clone https://github.com/example/pg_accumulator.git
cd pg_accumulator
make && sudo make install

psql -c "CREATE EXTENSION pg_accumulator;"
```

### 2. Create a register

```sql
SELECT register_create(
    name       := 'inventory',
    dimensions := '{"warehouse": "int", "product": "int"}',
    resources  := '{"quantity": "numeric(18,4)", "amount": "numeric(18,2)"}',
    kind       := 'balance'
);
```

### 3. Post movements

```sql
SELECT register_post('inventory', '{
    "recorder":  "receipt:1",
    "period":    "2026-04-18",
    "warehouse": 1,
    "product":   42,
    "quantity":  100,
    "amount":    5000.00
}');
```

### 4. Read the balance

```sql
SELECT * FROM accum.inventory_balance(
    dimensions := '{"warehouse": 1, "product": 42}'
);
-- Result: {"quantity": 100.0000, "amount": 5000.00}
```

### 5. Run via Docker

```bash
# Start the development environment
docker compose -f docker/docker-compose.yml up --build

# Run the test suite
make test-docker

# Run the benchmark suite
make bench-docker
```

---

## Installation

### Requirements

- PostgreSQL 15 or later (17 recommended)
- Superuser privileges to install the extension
- Build tools: `gcc`, `make`, `postgresql-server-dev-<version>`

### From Source

```bash
git clone https://github.com/example/pg_accumulator.git
cd pg_accumulator
make PG_CONFIG=/path/to/pg_config
sudo make install PG_CONFIG=/path/to/pg_config
```

### Docker (development)

```bash
cd docker
docker compose up --build
```

The container starts PostgreSQL 17 with the extension pre-installed and the background worker enabled.

### Enable in Database

```sql
-- Creates the 'accum' schema and internal metadata tables
CREATE EXTENSION pg_accumulator;

-- Or install into a custom schema
CREATE EXTENSION pg_accumulator SCHEMA my_schema;
```

The `shared_preload_libraries` parameter must include `pg_accumulator` for the background worker to start:

```ini
# postgresql.conf
shared_preload_libraries = 'pg_accumulator'
```

### Upgrade

```bash
sudo make install   # install new version files
psql -c "ALTER EXTENSION pg_accumulator UPDATE;"
```

---

## Concepts

### What is an Accumulation Register

An accumulation register is a structured mechanism for recording **movements** (receipts and expenditures) of resources, and for rapid retrieval of **summaries** (balances and turnovers) across arbitrary dimensions.

```
Real world:                             pg_accumulator:

  "Warehouse 1 currently holds           register: inventory
   42 units of product X"                dimensions: warehouse, product
                                         resources:  quantity, amount
  "In March, warehouse 1 shipped         kind: balance
   15 units"
```

The key insight is that you define *what* you want to track (dimensions and resources) and the extension handles *how* to store, index, aggregate, and query it.

### Dimensions and Resources

**Dimensions** are the analytical axes along which balances are separated:

```
warehouse ──┐
product   ──┼── Combination of dimensions = a unique "cell"
lot       ──┘   (one point in the accounting space)
```

Each unique combination of dimension values gets its own balance row. A warehouse + product + lot combination is the finest granularity of accounting; a warehouse alone is an aggregate.

**Resources** are the numeric quantities that accumulate:

```
quantity ──┐
amount   ──┴── Values added or subtracted with each movement
```

Resources can be positive or negative. A negative value represents a decrease.

### Register Types

| Type | Purpose | Supports balance | Supports turnover |
|---|---|---|---|
| `balance` | Resource balances (inventory, accounts) | ✅ | ✅ |
| `turnover` | Turnover only (sales, statistics, counters) | ❌ | ✅ |

```sql
-- Balance register: "how much is in stock right now"
SELECT register_create(name := 'stock', kind := 'balance', ...);

-- Turnover register: "how much was sold this month"
SELECT register_create(name := 'sales', kind := 'turnover', ...);
```

A `balance` register maintains a cumulative `balance_cache` and supports the `_balance()` function. A `turnover` register only maintains period totals.

---

## API Reference

All extension functions reside in the schema chosen at `CREATE EXTENSION` time (default: `accum`). Examples below use schema-qualified names; if `accum` is in your `search_path`, the schema prefix can be omitted.

---

### Registry Management

#### `register_create()`

Creates a new accumulation register with its complete storage infrastructure.

```sql
SELECT accum.register_create(
    name          := 'inventory',       -- Unique register name
    dimensions    := '{               
        "warehouse": "int",
        "product":   "int",
        "lot":       "text"
    }',
    resources     := '{
        "quantity": "numeric(18,4)",
        "amount":   "numeric(18,2)"
    }',
    kind          := 'balance',         -- 'balance' or 'turnover'
    totals_period := 'day',             -- 'day', 'month', or 'year'
    partition_by  := 'month',           -- movements partitioning granularity
    high_write    := false,             -- enable delta buffer
    recorder_type := 'text'             -- PostgreSQL type for the recorder field
);
```

**Parameters:**

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `name` | `text` | ✅ | — | Unique register name. Allowed: letters, digits, `_`. |
| `dimensions` | `jsonb` | ✅ | — | Dimensions as `{"name": "pg_type", ...}` |
| `resources` | `jsonb` | ✅ | — | Resources as `{"name": "pg_type", ...}` |
| `kind` | `text` | | `'balance'` | `'balance'` or `'turnover'` |
| `totals_period` | `text` | | `'day'` | Totals granularity: `'day'`, `'month'`, `'year'` |
| `partition_by` | `text` | | `'month'` | Movements partitioning: `'day'`, `'month'`, `'quarter'`, `'year'` |
| `high_write` | `boolean` | | `false` | Enable the delta buffer for high-contention writes |
| `recorder_type` | `text` | | `'text'` | PostgreSQL type for the `recorder` column |

**Supported dimension types:** `int`, `bigint`, `smallint`, `text`, `varchar(n)`, `uuid`, `date`, `boolean`

**Supported resource types:** `numeric(p,s)`, `integer`, `bigint`, `double precision`, `real`

**What gets created automatically:**

```
accum.inventory_movements          -- Partitioned movements table
accum.inventory_totals_day         -- Daily turnover totals
accum.inventory_totals_month       -- Monthly turnover totals
accum.inventory_totals_year        -- Annual turnover totals
accum.inventory_balance_cache      -- Cached current balance (balance kind only)
accum.inventory_balance(...)       -- Balance query function (balance kind only)
accum.inventory_turnover(...)      -- Turnover query function
accum.inventory_movements(...)     -- Filtered movements function
accum._trg_inventory_*             -- Internal trigger functions
accum._hash_inventory(...)         -- Dimension hash function
```

---

#### `register_alter()`

Modifies the structure of an existing register. Supports adding dimensions, adding resources, and toggling high-write mode.

```sql
-- Add a new dimension
SELECT accum.register_alter(
    p_name         := 'inventory',
    add_dimensions := '{"color": "text"}'
);

-- Add a new resource
SELECT accum.register_alter(
    p_name        := 'inventory',
    add_resources := '{"weight": "numeric(12,3)"}'
);

-- Enable high-write mode
SELECT accum.register_alter(
    p_name     := 'inventory',
    high_write := true
);
```

> **Note:** Adding dimensions triggers a full rebuild of totals and cache from movements. For large registers this may take time; it is performed without blocking readers.

> **Note:** Adding resources adds a new column with `DEFAULT 0`. Existing rows are unaffected; new resources start at zero for all historical periods.

---

#### `register_drop()`

Removes a register and its entire infrastructure (tables, indexes, triggers, functions).

```sql
-- Fails if the register contains movements
SELECT accum.register_drop('inventory');

-- Drop regardless of data
SELECT accum.register_drop('inventory', force := true);
```

---

#### `register_list()`

Returns a summary of all registers in the database.

```sql
SELECT * FROM accum.register_list();
```

| name | kind | dimensions | resources | movements_count | created_at |
|---|---|---|---|---|---|
| inventory | balance | 3 | 2 | 1,234,567 | 2026-01-15 |
| sales | turnover | 2 | 1 | 456,789 | 2026-02-01 |

---

#### `register_info()`

Returns detailed metadata about a single register as a JSONB object.

```sql
SELECT accum.register_info('inventory');
```

```json
{
  "name": "inventory",
  "kind": "balance",
  "dimensions": {"warehouse": "integer", "product": "integer", "lot": "text"},
  "resources": {"quantity": "numeric(18,4)", "amount": "numeric(18,2)"},
  "totals_period": "day",
  "partition_by": "month",
  "high_write": false,
  "recorder_type": "text",
  "movements_count": 1234567,
  "created_at": "2026-01-15T10:30:00Z"
}
```

---

### Writing Data

#### `register_post()`

Records one or more movements tied to a business document (recorder).

```sql
-- Single movement
SELECT accum.register_post('inventory', '{
    "recorder":  "purchase_order:7001",
    "period":    "2026-04-18T14:30:00",
    "warehouse": 1,
    "product":   42,
    "lot":       "LOT-A",
    "quantity":  100,
    "amount":    5000.00
}');

-- Batch of movements (single call, single transaction)
SELECT accum.register_post('inventory', '[
    {
        "recorder":  "sales_order:8001",
        "period":    "2026-04-18",
        "warehouse": 1, "product": 42, "lot": "LOT-A",
        "quantity":  -10, "amount": -500.00
    },
    {
        "recorder":  "sales_order:8001",
        "period":    "2026-04-18",
        "warehouse": 2, "product": 42, "lot": "LOT-A",
        "quantity":  10, "amount": 500.00
    }
]');
```

**JSON fields for each movement:**

| Field | Type | Required | Description |
|---|---|---|---|
| `recorder` | matches `recorder_type` | ✅ | Identifier of the business document |
| `period` | timestamp / date / ISO-8601 | ✅ | Accounting date of the movement |
| `<dimension>` | as declared | ✅ | Value for each dimension |
| `<resource>` | numeric | ✅ | Value for each resource (positive = receipt, negative = expenditure) |

**Returns:** the number of movements inserted.

**What happens internally:**

```
register_post('inventory', data)
        │
        ▼
  1. Validate JSON (all dimensions and resources present)
  2. Compute dim_hash for each movement
  3. INSERT INTO accum.inventory_movements
        │
        │  AFTER INSERT trigger (same transaction)
        │
  4. UPSERT totals_month  +=  movement delta
  5. UPSERT totals_year   +=  movement delta
  6. UPSERT balance_cache +=  movement delta
        │
        ▼
  COMMIT → fully visible to all other clients
```

---

#### `register_unpost()`

Cancels all movements belonging to a recorder, reversing their effect on totals and cache.

```sql
SELECT accum.register_unpost('inventory', 'sales_order:8001');
-- Returns: number of movements deleted
```

**What happens:**

```
1. DELETE FROM movements WHERE recorder = 'sales_order:8001'
        │
        │  AFTER DELETE trigger (same transaction)
        │
2. totals_month  -= deleted resources
3. totals_year   -= deleted resources
4. balance_cache -= deleted resources
        │
        ▼
All balances are correct as if the document never existed
```

---

#### `register_repost()`

Atomically replaces a recorder's movements with new data. Equivalent to `unpost` + `post` but executed in a single transaction with optimised delta computation.

```sql
SELECT accum.register_repost('inventory', 'sales_order:8001', '[
    {
        "period":    "2026-04-18",
        "warehouse": 1, "product": 42, "lot": "LOT-A",
        "quantity":  -12, "amount": -600.00
    }
]');
```

This is the correct tool for correcting an already-posted document: the old movements are deleted, the new movements are inserted, and totals/cache are updated with the net delta — all within one transaction.

---

#### Direct INSERT

For bulk imports or integrations, you may write directly into the movements table. Triggers fire normally and update all derived tables.

```sql
INSERT INTO accum.inventory_movements
    (recorder, period, warehouse, product, lot, quantity, amount)
VALUES
    ('import:batch_42', '2026-04-18', 1, 42, 'LOT-A', 100, 5000.00),
    ('import:batch_42', '2026-04-18', 1, 43, 'LOT-B', 200, 8000.00),
    ('import:batch_42', '2026-04-18', 2, 42, 'LOT-A',  50, 2500.00);
-- Triggers update totals and cache automatically
```

> `register_post()` performs additional JSON validation. With direct INSERT you are responsible for data integrity.

---

### Reading Data

#### `<register>_balance()`

Returns the resource balances for the given dimension values. Available only for `balance`-kind registers.

```sql
-- Current balance (from balance_cache — O(1))
SELECT * FROM accum.inventory_balance(
    dimensions := '{"warehouse": 1, "product": 42}'
);

-- Historical balance at a specific point in time (hierarchical query)
SELECT * FROM accum.inventory_balance(
    dimensions := '{"warehouse": 1, "product": 42}',
    at_date    := '2026-03-15'
);

-- Partial dimensions — aggregates across missing dimensions
SELECT * FROM accum.inventory_balance(
    dimensions := '{"warehouse": 1}'
);
-- Returns total balance across all products and lots in warehouse 1

-- No filter — aggregate across the entire register
SELECT * FROM accum.inventory_balance();
```

**Returns:** a JSONB object `{"quantity": ..., "amount": ...}`.

**How historical balance is computed:**

```
Query: balance as of March 15, 2026

Step 1: Sum totals_year for all years before 2026
Step 2: Add totals_month for completed months of 2026 (Jan + Feb)
Step 3: Add individual movements from Mar 1 through Mar 15

Result = Step1 + Step2 + Step3

Maximum rows scanned:
  ~20 (years) + ~11 (months) + ~31 (daily movements) ≈ 62 rows
  instead of millions of movements across all history
```

---

#### `<register>_turnover()`

Returns turnovers (net change in resources) for a time range, optionally grouped by a dimension.

```sql
-- Total turnover for a month
SELECT * FROM accum.inventory_turnover(
    from_date  := '2026-04-01',
    to_date    := '2026-04-30',
    dimensions := '{"warehouse": 1}'
);

-- Turnover broken out by product
SELECT * FROM accum.inventory_turnover(
    from_date  := '2026-04-01',
    to_date    := '2026-04-30',
    dimensions := '{"warehouse": 1}',
    group_by   := '["product"]'
);
```

**Result with `group_by`:** returns SETOF JSONB, one object per group:

```json
{"product": 42, "quantity": -10.0000, "amount": -500.00}
{"product": 43, "quantity": 200.0000, "amount": 8000.00}
```

The query uses the totals hierarchy to avoid scanning raw movements whenever possible.

---

#### `<register>_movements()`

Returns raw movements with optional filters.

```sql
-- All movements for a recorder
SELECT * FROM accum.inventory_movements(
    p_recorder := 'sales_order:8001'
);

-- Movements in a time range for specific dimensions
SELECT * FROM accum.inventory_movements(
    from_date  := '2026-04-01',
    to_date    := '2026-04-18',
    dimensions := '{"warehouse": 1, "product": 42}'
);
```

Returns SETOF JSONB, one object per movement row.

---

#### `<register>_balance_cache`

The balance cache table is a regular PostgreSQL table and can be queried directly with full SQL flexibility.

```sql
-- All non-zero balances in warehouse 1
SELECT product, lot, quantity, amount
FROM accum.inventory_balance_cache
WHERE warehouse = 1
  AND quantity != 0
ORDER BY product;

-- Items with a negative balance (data quality alert)
SELECT warehouse, product, quantity
FROM accum.inventory_balance_cache
WHERE quantity < 0;

-- JOIN with application tables
SELECT
    p.name  AS product_name,
    c.quantity,
    c.amount
FROM accum.inventory_balance_cache c
JOIN products p ON p.id = c.product
WHERE c.warehouse = 1
ORDER BY c.amount DESC
LIMIT 10;
```

**Table structure:**

| Column | Type | Description |
|---|---|---|
| `dim_hash` | `bigint` | Primary key — hash of the dimension combination |
| `<dimension>` | as declared | Dimension values (denormalised) |
| `<resource>` | as declared | Current balance of the resource |
| `last_movement_at` | `timestamptz` | Timestamp of the last movement |
| `last_movement_id` | `uuid` | ID of the last movement |
| `version` | `bigint` | Update counter (useful for optimistic locking) |

---

## Architecture

### Overview Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                          PUBLIC API                             │
│   register_create / alter / drop / post / unpost / repost      │
│   <register>_balance() / _turnover() / _movements()            │
└──────────────────────────────┬──────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────┐
│                      pg_accumulator core                        │
│                                                                 │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────────────┐ │
│  │  DDL Gen    │  │ Trigger Eng. │  │    Query Builder       │ │
│  │             │  │              │  │                        │ │
│  │  Tables     │  │  Updates     │  │  Optimal queries       │ │
│  │  Indexes    │  │  totals &    │  │  using totals          │ │
│  │  Partitions │  │  cache in    │  │  hierarchy for         │ │
│  │  Functions  │  │  same TX     │  │  balance/turnover      │ │
│  └─────────────┘  └──────────────┘  └────────────────────────┘ │
│  ┌─────────────┐  ┌──────────────┐                             │
│  │  Hash Func  │  │  BG Worker   │                             │
│  │             │  │              │                             │
│  │  dim_hash   │  │ Delta merge  │                             │
│  │  per register│  │ Partition    │                             │
│  └─────────────┘  │ maintenance  │                             │
│                   └──────────────┘                             │
└──────────────────────────────┬──────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────┐
│                        STORAGE LAYER                            │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  movements  (PARTITION BY RANGE period)                   │  │
│  │  Append-only, source of truth                             │  │
│  └──────────────────────────────────────────────────────────┘  │
│  ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐│
│  │  totals_day       │ │  totals_month    │ │  totals_year     ││
│  │  Daily turnovers  │ │  Monthly totals  │ │  Annual totals   ││
│  └──────────────────┘ └──────────────────┘ └──────────────────┘│
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  balance_cache                                            │  │
│  │  Cumulative current balance, O(1) point lookup           │  │
│  └──────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  balance_cache_delta  (high_write mode only)             │  │
│  │  Append-only delta buffer, merged by background worker   │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

---

### Movements Layer

Movements are the **source of truth** and an **append-only log**.

```sql
-- Auto-created movements table:
CREATE TABLE accum.inventory_movements (
    id            uuid          DEFAULT gen_random_uuid(),
    recorded_at   timestamptz   DEFAULT now() NOT NULL,
    recorder      text          NOT NULL,
    period        timestamptz   NOT NULL,
    movement_type text          DEFAULT 'regular' NOT NULL,
    dim_hash      bigint        NOT NULL,

    -- Dimensions (denormalised)
    warehouse     integer       NOT NULL,
    product       integer       NOT NULL,
    lot           text,

    -- Resources
    quantity      numeric(18,4) NOT NULL DEFAULT 0,
    amount        numeric(18,2) NOT NULL DEFAULT 0
) PARTITION BY RANGE (period);

-- Auto-created indexes:
CREATE INDEX ON accum.inventory_movements (dim_hash, period);
CREATE INDEX ON accum.inventory_movements (recorder);
CREATE INDEX ON accum.inventory_movements (period);
```

**Key fields:**

| Field | Purpose |
|---|---|
| `id` | Unique movement identifier (UUID) |
| `recorded_at` | Physical write time (`now()` at INSERT) |
| `recorder` | Reference to the source business document |
| `period` | Accounting date (may be in the past — correction scenario) |
| `movement_type` | `'regular'`, `'adjustment'`, `'reversal'` |
| `dim_hash` | 64-bit hash of dimension values for fast lookup |

**Partitioning:**

```
accum.inventory_movements
├── accum.inventory_movements_2026_01   (2026-01-01 .. 2026-02-01)
├── accum.inventory_movements_2026_02   (2026-02-01 .. 2026-03-01)
├── accum.inventory_movements_2026_03   (2026-03-01 .. 2026-04-01)
├── accum.inventory_movements_2026_04   (2026-04-01 .. 2026-05-01)  ← current
└── accum.inventory_movements_default   (fallback)

Future partitions are created automatically by the background worker
(controlled by pg_accumulator.partitions_ahead).
```

---

### Totals Layer

Totals store **turnovers** (not cumulative balances) per period at three granularities: day, month, and year. This is the key architectural decision that makes retroactive corrections O(1).

The totals hierarchy depends on the `totals_period` setting (default: `'day'`):
- `totals_period = 'day'` → `totals_day` + `totals_month` + `totals_year`
- `totals_period = 'month'` → `totals_month` + `totals_year`
- `totals_period = 'year'` → `totals_year`

```sql
-- Monthly totals table (auto-created):
CREATE TABLE accum.inventory_totals_month (
    dim_hash   bigint        NOT NULL,
    period     date          NOT NULL,  -- first day of the month
    warehouse  integer       NOT NULL,
    product    integer       NOT NULL,
    lot        text,
    quantity   numeric(18,4) NOT NULL DEFAULT 0,
    amount     numeric(18,2) NOT NULL DEFAULT 0,
    PRIMARY KEY (dim_hash, period)
);
```

**Why turnovers instead of cumulative balances:**

```
Cumulative approach                    Turnover approach
(balance AT END of period)             (net change DURING period)

totals_month:                          totals_month:
  Jan: 100                               Jan: +100
  Feb: 150  (= 100 + 50)                Feb:  +50
  Mar: 120  (= 150 - 30)                Mar:  -30
  Apr: 130  (= 120 + 10)                Apr:  +10

Retroactive correction to Feb (+5):    Retroactive correction to Feb (+5):
  UPDATE Feb: 150 → 155                  UPDATE Feb: +50 → +55
  UPDATE Mar: 120 → 125  ← cascade       Nothing else!  ✅
  UPDATE Apr: 130 → 135  ← cascade

  O(N periods)                           O(1)
```

---

### Balance Cache

The balance cache holds the **current cumulative balance** — the sum of all movements ever recorded for each unique combination of dimensions.

```sql
-- Auto-created cache table structure:
CREATE TABLE accum.inventory_balance_cache (
    dim_hash         bigint          NOT NULL PRIMARY KEY,

    -- Denormalised dimension values
    warehouse        integer         NOT NULL,
    product          integer         NOT NULL,
    lot              text,

    -- Resources: current running balance
    quantity         numeric(18,4)   NOT NULL DEFAULT 0,
    amount           numeric(18,2)   NOT NULL DEFAULT 0,

    -- Metadata
    last_movement_at timestamptz     NOT NULL,
    last_movement_id uuid            NOT NULL,
    version          bigint          NOT NULL DEFAULT 0
);
```

**Update mechanism (simplified trigger logic):**

```sql
INSERT INTO accum.inventory_balance_cache
    (dim_hash, warehouse, product, lot, quantity, amount,
     last_movement_at, last_movement_id, version)
VALUES (...)
ON CONFLICT (dim_hash) DO UPDATE SET
    quantity         = inventory_balance_cache.quantity + EXCLUDED.quantity,
    amount           = inventory_balance_cache.amount   + EXCLUDED.amount,
    last_movement_at = EXCLUDED.last_movement_at,
    last_movement_id = EXCLUDED.last_movement_id,
    version          = inventory_balance_cache.version + 1;
```

**Consistency guarantees:**

| Property | Guarantee |
|---|---|
| Freshness | After `COMMIT`, always reflects all committed movements |
| Correctness | `cache = SUM(movements)` is always true |
| Rollback safety | Trigger rolls back with the transaction; cache is never partially updated |
| Isolation | MVCC: each reader sees a consistent snapshot |

---

### Append-Only Writes and Retroactive Corrections

The extension uses an **append-only** approach: movements are never updated, only added.

```
Physical write timeline (recorded_at):
══════════════════════════════════════════════════════════► time
  10:00       10:05       10:30        14:00         16:00
   │           │           │            │              │
   ▼           ▼           ▼            ▼              ▼
 +100         +50         -30       Correction        -20
 regular     regular     regular    adjustment        regular
 period:     period:     period:    period: MARCH     period:
 April       April       April      (retroactive)     April

Correction for March:
  - movement_type = 'adjustment'
  - period = March  (accounting date)
  - recorded_at = April 14:00  (physical write time)
  - Physically appended to the END of the log
  - Original movements are NOT modified
```

**Advantages:**

```
✅ Complete audit trail (who changed what, when)
✅ No locks on existing rows (INSERT vs UPDATE)
✅ Better WAL profile (INSERT-only)
✅ Simpler logical replication
✅ Retroactive corrections are O(1) in totals
```

---

### Trigger Chain

Full chain of events for a single movement write:

```
INSERT INTO movements (period=March, qty=+10, ...)
        │
        ▼
┌────────────────────────────────────┐
│  BEFORE INSERT trigger            │
│  1. Compute dim_hash              │
│  2. Validate required fields      │
│  3. Set recorded_at = now()       │
│  4. Set movement_type             │
└─────────────────┬──────────────────┘
                  │
                  ▼
┌────────────────────────────────────┐
│  Physical INSERT recorded         │
└─────────────────┬──────────────────┘
                  │
                  ▼
┌────────────────────────────────────┐
│  AFTER INSERT trigger             │
│                                   │
│  1. UPSERT totals_month           │
│     SET qty += 10                 │
│     WHERE dim_hash = X            │
│       AND period = '2026-03'      │
│                                   │
│  2. UPSERT totals_year            │
│     SET qty += 10                 │
│     WHERE dim_hash = X            │
│       AND period = '2026'         │
│                                   │
│  3. UPSERT balance_cache          │
│     SET qty += 10                 │
│     WHERE dim_hash = X            │
└─────────────────┬──────────────────┘
                  │
                  ▼
         COMMIT → data visible
```

DELETE triggers mirror this chain, decrementing values instead.

---

## Concurrency and Performance

### Row-Level Locking

In standard mode, the trigger performs an `UPDATE` on `dim_hash` rows in the totals and cache tables. PostgreSQL uses row-level locks:

```
Writer A (warehouse=1, product=42)    Writer B (warehouse=1, product=99)
──────────────────────────────────    ──────────────────────────────────
UPDATE cache WHERE dim_hash = H1      UPDATE cache WHERE dim_hash = H2
  → locks row H1                        → locks row H2
  → NO CONFLICT ✅                      → NO CONFLICT ✅

Writer C (warehouse=1, product=42)
──────────────────────────────────
UPDATE cache WHERE dim_hash = H1
  → waits for Writer A to COMMIT
  → then proceeds ✅
```

**Different dimension combinations do not conflict.** Contention occurs only when multiple writers simultaneously write to the same `dim_hash`.

---

### Delta Buffer (High-Write Mode)

For scenarios with high contention on a single `dim_hash` (e.g., a page-view counter for one URL), enable `high_write`:

```sql
SELECT register_create(
    name       := 'page_views',
    dimensions := '{"page": "text"}',
    resources  := '{"views": "int"}',
    kind       := 'balance',
    high_write := true
);
```

**How it works:**

| | Standard mode | High-write mode |
|---|---|---|
| Write | `UPDATE cache` (row lock) | `INSERT delta` (no lock on cache) |
| Write contention | Queue on hot rows | None ✅ |
| Read | `SELECT cache` | `SELECT cache + SUM(delta)` |
| Read complexity | O(1) | O(1) + O(pending deltas) |
| Background work | Not needed | Delta merge every N seconds |

```sql
-- Delta buffer table (auto-created, UNLOGGED for performance):
CREATE UNLOGGED TABLE accum.page_views_balance_cache_delta (
    id         bigserial    PRIMARY KEY,
    dim_hash   bigint       NOT NULL,
    views      integer      NOT NULL DEFAULT 0,
    created_at timestamptz  DEFAULT now()
);
```

**Background merge (runs every `delta_merge_interval`):**

```sql
WITH consumed AS (
    DELETE FROM accum.page_views_balance_cache_delta
    WHERE created_at < now() - interval '2 seconds'
    ORDER BY id LIMIT 10000
    RETURNING dim_hash, views
),
agg AS (
    SELECT dim_hash, SUM(views) AS views
    FROM consumed GROUP BY dim_hash
)
UPDATE accum.page_views_balance_cache c
SET views   = c.views + a.views,
    version = c.version + 1
FROM agg a
WHERE c.dim_hash = a.dim_hash;
```

**Balance reads automatically include pending delta rows** (via `UNION ALL` in `_balance_internal`), so the result is always accurate regardless of whether a merge has occurred.

You can also trigger a merge manually:

```sql
SELECT accum._force_delta_merge();
```

---

### Benchmarks

Results measured on the Docker environment (PostgreSQL 17, macOS ARM, single connection):

| Scenario | ops/sec | avg latency |
|---|---|---|
| `register_post()` single insert | ~1,160 | 0.86 ms |
| `register_post()` batch 10 | ~2,500 | 0.40 ms |
| `register_post()` batch 100 | ~3,590 | 0.28 ms |
| `register_post()` batch 1000 | ~4,925 | 0.20 ms |
| `balance_cache` direct SELECT | ~9,150 | 0.11 ms |
| `<register>_balance()` function | ~2,540 | 0.39 ms |
| `register_post()` high_write mode | ~1,618 | 0.62 ms |
| `register_unpost()` | ~813 | 1.23 ms |

> Run `make bench-docker` to reproduce on your hardware.

**Key takeaways:**
- Batch mode delivers **3–4× higher throughput** than individual inserts.
- High-write mode eliminates contention under concurrent load but adds overhead in single-connection scenarios.
- Balance cache point lookups are extremely fast (~0.1 ms) and suitable for hot paths.

---

## Retroactive Corrections

Full algorithm for correcting historical data:

```
Scenario: In April, an error is found in a March receipt.
          100 units were recorded; the correct value is 110.

Step 1: Call register_repost('inventory', 'purchase_order:7001', <new_data>)

Step 2 (automatic):
  DELETE old movement (period=March, qty=100)
    → trigger: totals_month[March]  -= 100
    → trigger: totals_year[2026]    -= 100
    → trigger: balance_cache        -= 100

  INSERT new movement (period=March, qty=110, type='adjustment')
    → trigger: totals_month[March]  += 110
    → trigger: totals_year[2026]    += 110
    → trigger: balance_cache        += 110

  Net result:
    totals_month[March]:  +10  ✅
    totals_year[2026]:    +10  ✅
    balance_cache:        +10  ✅
    totals_month[April]:  UNTOUCHED ✅  (no cascade!)

Step 3: All queries return correct data immediately after COMMIT.
```

The original movement is deleted from the log; the corrected movement is appended. The full correction history is preserved: the audit shows that `purchase_order:7001` was re-posted on April 14 at 14:00, changing the quantity from 100 to 110.

---

## Design Guidelines

### Choosing Dimensions

```
✅ Good:
   dimensions = warehouse, product, lot
   (concrete, bounded number of combinations)

❌ Problematic:
   dimensions = user_id, timestamp, request_id
   (too granular — millions of rows in balance_cache)
```

**Rule of thumb:** the number of unique dimension combinations equals the number of rows in `balance_cache`. Keep this count manageable. For fan-out scenarios (e.g., per-user balances), ensure the number of users is bounded or the cache size is acceptable.

### Choosing Totals Granularity

| Scenario | Recommendation |
|---|---|
| < 1K movements/day per dim_hash | `totals_period := 'month'` |
| 1K – 100K movements/day | `totals_period := 'day'` |
| History < 2 years | `totals_month` alone is sufficient |
| History > 10 years | Use `totals_year` + `totals_month` |

### Choosing the `recorder` Format

```sql
-- Good: type:id pattern
recorder = 'purchase_order:7001'
recorder = 'sales_invoice:2024-001'
recorder = 'adjustment:manual:admin:2026-04-18'

-- Avoid: opaque identifiers
recorder = '7001'           -- ambiguous
recorder = NULL             -- cannot be cancelled
recorder = '...'            -- cannot be reliably filtered
```

Choosing a consistent, human-readable format makes `register_unpost()` and `register_repost()` safe and predictable.

### When to Enable High-Write Mode

```
✅ Enable high_write when:
   - More than ~100 writes/second target a SINGLE dim_hash
   - You observe lock waits on balance_cache rows
   - A ~2–5 second staleness window in the cache is acceptable
     (reads via _balance() are always accurate because they
      include pending delta rows)

❌ Not needed when:
   - Writes are spread across many distinct dim_hash values
   - Total write rate is below ~50 writes/second
   - Architecture simplicity is a priority
```

---

## Usage Examples

### Warehouse Inventory

```sql
-- Create the register
SELECT accum.register_create(
    name       := 'warehouse_stock',
    dimensions := '{
        "warehouse":  "int",
        "product":    "int",
        "lot_number": "text",
        "quality":    "text"
    }',
    resources  := '{
        "quantity": "numeric(18,4)",
        "weight":   "numeric(18,3)",
        "cost":     "numeric(18,2)"
    }',
    kind := 'balance'
);

-- Goods receipt
SELECT accum.register_post('warehouse_stock', '{
    "recorder":    "grn:2026-04-001",
    "period":      "2026-04-18",
    "warehouse":   1,
    "product":     42,
    "lot_number":  "LOT-2026-04-A",
    "quality":     "grade_a",
    "quantity":    1000,
    "weight":      500.000,
    "cost":        25000.00
}');

-- Shipment (negative movement)
SELECT accum.register_post('warehouse_stock', '{
    "recorder":    "shipment:2026-04-055",
    "period":      "2026-04-18",
    "warehouse":   1,
    "product":     42,
    "lot_number":  "LOT-2026-04-A",
    "quality":     "grade_a",
    "quantity":    -150,
    "weight":      -75.000,
    "cost":        -3750.00
}');

-- Check balance for a specific product
SELECT * FROM accum.warehouse_stock_balance(
    dimensions := '{"warehouse": 1, "product": 42}'
);
-- {"quantity": 850.0000, "weight": 425.000, "cost": 21250.00}

-- All non-zero balances across all warehouses for product 42
SELECT warehouse, SUM(quantity) AS qty, SUM(cost) AS total_cost
FROM accum.warehouse_stock_balance_cache
WHERE product = 42 AND quantity > 0
GROUP BY warehouse;

-- Monthly turnover by lot
SELECT * FROM accum.warehouse_stock_turnover(
    from_date  := '2026-04-01',
    to_date    := '2026-04-30',
    dimensions := '{"warehouse": 1, "product": 42}',
    group_by   := '["lot_number"]'
);
```

---

### Financial Accounting

```sql
SELECT accum.register_create(
    name       := 'account_balance',
    dimensions := '{
        "account":     "int",
        "currency":    "text",
        "cost_center": "int"
    }',
    resources  := '{
        "debit":  "numeric(18,2)",
        "credit": "numeric(18,2)",
        "net":    "numeric(18,2)"
    }',
    kind := 'balance'
);

-- Inter-account transfer (two movements, one transaction)
BEGIN;
    SELECT accum.register_post('account_balance', '[
        {
            "recorder":    "transfer:T-2026-001",
            "period":      "2026-04-18",
            "account":     1001,
            "currency":    "USD",
            "cost_center": 10,
            "debit":       0,
            "credit":      5000.00,
            "net":         -5000.00
        },
        {
            "recorder":    "transfer:T-2026-001",
            "period":      "2026-04-18",
            "account":     2001,
            "currency":    "USD",
            "cost_center": 20,
            "debit":       5000.00,
            "credit":      0,
            "net":         5000.00
        }
    ]');
COMMIT;

-- Account balance
SELECT * FROM accum.account_balance_balance(
    dimensions := '{"account": 1001, "currency": "USD"}'
);

-- Monthly turnover for an account
SELECT * FROM accum.account_balance_turnover(
    from_date  := '2026-04-01',
    to_date    := '2026-04-30',
    dimensions := '{"account": 1001}'
);

-- Historical balance at end of Q1
SELECT * FROM accum.account_balance_balance(
    dimensions := '{"account": 1001, "currency": "USD"}',
    at_date    := '2026-03-31'
);
```

---

### Subscription Usage Tracking

```sql
SELECT accum.register_create(
    name       := 'subscription_usage',
    dimensions := '{
        "tenant":  "uuid",
        "plan":    "text",
        "feature": "text"
    }',
    resources  := '{
        "used":  "bigint",
        "quota": "bigint"
    }',
    kind       := 'balance',
    high_write := true          -- many writes per tenant per feature
);

-- Charge API usage
SELECT accum.register_post('subscription_usage', '{
    "recorder": "api_call:req_abc123",
    "period":   "2026-04-18T14:30:00Z",
    "tenant":   "550e8400-e29b-41d4-a716-446655440000",
    "plan":     "pro",
    "feature":  "api_calls",
    "used":     1,
    "quota":    0
}');

-- Check whether limit is exceeded (O(1) from cache)
SELECT used
FROM accum.subscription_usage_balance_cache
WHERE tenant  = '550e8400-e29b-41d4-a716-446655440000'
  AND feature = 'api_calls';

-- Grant quota at the start of the billing cycle
SELECT accum.register_post('subscription_usage', '{
    "recorder": "billing_cycle:2026-05",
    "period":   "2026-05-01",
    "tenant":   "550e8400-e29b-41d4-a716-446655440000",
    "plan":     "pro",
    "feature":  "api_calls",
    "used":     0,
    "quota":    100000
}');
```

---

## Diagnostics and Maintenance

### Verify Data Consistency

```sql
-- Compare balance_cache against the actual SUM of movements,
-- and verify totals_month and totals_year coherence.
SELECT * FROM accum.register_verify('inventory');
```

| check_type | dim_hash | period | expected | actual | status |
|---|---|---|---|---|---|
| balance_cache | 123456 | NULL | {"quantity": 100} | {"quantity": 100} | OK |
| totals_month | 123456 | 2026-04-01 | {"quantity": 50} | {"quantity": 50} | OK |
| totals_year | 123456 | 2026-01-01 | {"quantity": 100} | {"quantity": 100} | OK |

Possible status values: `OK`, `MISMATCH`, `MISSING_IN_CACHE`, `ORPHAN_IN_CACHE`, `MISSING_IN_TOTALS`, `ORPHAN_IN_TOTALS`.

### Rebuild Totals and Cache

```sql
-- Full rebuild of totals_month and totals_year from movements
SELECT accum.register_rebuild_totals('inventory');

-- Rebuild balance_cache from movements
SELECT accum.register_rebuild_cache('inventory');
```

Use these after a manual data repair, after importing historical data directly into the movements table, or when `register_verify()` reports mismatches.

### Monitor Background Workers

```sql
-- Check running maintenance workers
SELECT * FROM accum._maintenance_status();
```

| pid | worker_name | state | query | started_at |
|---|---|---|---|---|
| 1234 | pg_accumulator maintenance 0 | active | delta merge | 2026-04-18 10:00:00 |

### Partitioning

```sql
-- Manually create partitions ahead of time
SELECT accum.register_create_partitions('inventory', 3);
-- Creates the next 3 monthly partitions

-- Detach old partitions (data is not deleted, just unattached)
SELECT accum.register_detach_partitions('inventory', '2024-01-01'::date);
```

### Force Delta Merge

```sql
-- Merge all pending delta rows immediately
SELECT accum._force_delta_merge();

-- Merge with specific age threshold and batch size
SELECT accum._force_delta_merge(
    p_max_age    := interval '0 seconds',
    p_batch_size := 1000000
);
```

---

## Configuration

Set in `postgresql.conf` or via `ALTER SYSTEM`:

```ini
# Required for the background worker
shared_preload_libraries = 'pg_accumulator'

# Number of background maintenance workers (requires restart)
pg_accumulator.background_workers = 1          # Default: 1, range: 0..8
                                                # Set to 0 to disable

# Delta buffer merge settings (reload with SELECT pg_reload_conf())
pg_accumulator.delta_merge_interval = 5000     # ms between merge cycles (default: 5000)
pg_accumulator.delta_merge_delay    = 2000     # ms minimum delta age before merge (default: 2000)
pg_accumulator.delta_merge_batch_size = 10000  # max delta rows per merge cycle (default: 10000)

# Partition management
pg_accumulator.partitions_ahead = 3            # Future partitions to maintain (default: 3)
pg_accumulator.maintenance_interval = 3600000  # ms between maintenance runs (default: 3600000 = 1h)
```

**View current settings:**

```sql
SELECT * FROM accum._config;
```

---

## Compatibility

| Component | Minimum version | Recommended |
|---|---|---|
| PostgreSQL | 15 | 17+ |
| pg_cron (optional) | 1.5 | 1.6+ |

**Supported dimension types:** `int`, `bigint`, `smallint`, `text`, `varchar(n)`, `uuid`, `date`, `boolean`

**Supported resource types:** `numeric(p,s)`, `integer`, `bigint`, `double precision`, `real`

---

## FAQ

**Q: What happens if `register_post()` and `register_unpost()` for the same recorder run concurrently?**  
A: PostgreSQL row-level locks on `balance_cache` serialise the operations. One will wait for the other to commit. The result is always consistent.

**Q: Can the extension be used with logical replication?**  
A: Yes. All tables are standard PostgreSQL tables. It is recommended to replicate only the `movements` table and rebuild totals and cache on the replica using `register_rebuild_totals()` and `register_rebuild_cache()`.

**Q: What if `dim_hash` collides?**  
A: The hash is 64-bit (xxhash64). The probability of collision is below 1 in 10¹⁸ for any realistic number of dimension combinations. Additionally, the UPSERT trigger uses the `dim_hash` only as a lookup key; the full dimension values are stored and kept consistent.

**Q: Can dimensions be NULL?**  
A: Yes. NULL is treated as a distinct dimension value and gets its own `dim_hash`. `(warehouse=1, lot=NULL)` and `(warehouse=1, lot='A')` are different cells.

**Q: How do I delete old data?**  
A: Use `register_detach_partitions()` to detach old movements partitions. Then run `register_rebuild_totals()` if you want the totals tables to reflect only the retained data. The balance cache is not affected by detaching partitions (it reflects the total of all movements ever recorded).

**Q: How is multi-tenancy supported?**  
A: Add `tenant_id` as a dimension. Each tenant gets isolated balances. For physical isolation, apply Row Level Security (RLS) on the movements and cache tables.

**Q: What is the `recorder` field for?**  
A: It is a reference to the business document that caused the movements (e.g., a purchase order ID, a transfer ID). It enables `register_unpost()` and `register_repost()` to operate on the complete set of movements for a document atomically. Treat it as a foreign key to your documents table.

**Q: Can I add dimensions to an existing register with data?**  
A: Yes, via `register_alter()`. The new dimension column is added with a nullable type; existing movements get NULL for the new dimension. Totals and cache are fully rebuilt from movements. New movements must supply a value for the new dimension.

**Q: Does the extension work without the background worker?**  
A: Yes, for most features. The background worker handles delta buffer merges (required for high-write mode consistency) and proactive partition creation. Without it, set `pg_accumulator.background_workers = 0` and manage these manually using `_force_delta_merge()` and `register_create_partitions()`.

---

## Module Documentation

Detailed documentation for each extension module:

| Module | Documentation |
|---|---|
| Core | [src/core/MODULE.md](../src/core/MODULE.md) |
| DDL Generator | [src/ddl/MODULE.md](../src/ddl/MODULE.md) |
| Hash | [src/hash/MODULE.md](../src/hash/MODULE.md) |
| Triggers | [src/triggers/MODULE.md](../src/triggers/MODULE.md) |
| Write API | [src/write_api/MODULE.md](../src/write_api/MODULE.md) |
| Read API | [src/read_api/MODULE.md](../src/read_api/MODULE.md) |
| Registry API | [src/registry_api/MODULE.md](../src/registry_api/MODULE.md) |
| Delta Buffer | [src/delta_buffer/MODULE.md](../src/delta_buffer/MODULE.md) |
| Partitioning | [src/partitioning/MODULE.md](../src/partitioning/MODULE.md) |
| Maintenance | [src/maintenance/MODULE.md](../src/maintenance/MODULE.md) |
| Background Worker | [src/bgworker/MODULE.md](../src/bgworker/MODULE.md) |

---

## License

pg_accumulator is distributed under the [PostgreSQL License](https://www.postgresql.org/about/licence/).
