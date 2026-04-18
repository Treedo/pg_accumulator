# pg_accumulator

**Accumulation Registers for PostgreSQL** — an extension that turns PostgreSQL into a full-featured resource accounting platform with instant access to balances and turnovers.

---

## Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Quick Start](#quick-start)
- [Installation](#installation)
- [Concepts](#concepts)
  - [What is an Accumulation Register](#what-is-an-accumulation-register)
  - [Dimensions and Resources](#dimensions-and-resources)
  - [Register Types](#register-types)
- [API](#api)
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
- [Benchmarks](#benchmarks)
- [Architecture](#architecture)
- [Concurrency and Performance](#concurrency-and-performance)
- [Diagnostics and Maintenance](#diagnostics-and-maintenance)
- [Configuration](#configuration)
- [Compatibility](#compatibility)
- [FAQ](#faq)
- [License](#license)

---

## Overview

`pg_accumulator` is a PostgreSQL extension that provides a declarative mechanism for **accumulation registers** — a high-level abstraction for tracking resource balances and turnovers across arbitrary dimensions.

**The Problem.** Building accounting systems often requires hand-rolling movement tables, totals tables, trigger logic to keep aggregates current, recalculation logic for historical corrections, and indexes for fast reads.

**The Solution.** A single call to `register_create()` generates the full infrastructure: tables, indexes, partitions, triggers, read functions, and background maintenance.

```sql
SELECT register_create(
    name       := 'inventory',
    dimensions := '{"warehouse": "int", "product": "int", "lot": "text"}',
    resources  := '{"quantity": "numeric", "amount": "numeric"}',
    kind       := 'balance',
    totals_period := 'day'
);
```

Then:

```sql
SELECT register_post('inventory', '{
    "recorder":  "purchase_order:7001",
    "period":    "2026-04-18",
    "warehouse": 1,
    "product":   42,
    "lot":       "LOT-A",
    "quantity":  100,
    "amount":    5000
}');

SELECT * FROM accum.inventory_balance(
    dimensions := '{"warehouse": 1, "product": 42}'
);
```

---

## Key Features

- Declarative register creation with a single API call
- Append-only movement journal
- Hierarchical totals for year/month/day aggregation
- Balance cache for O(1) current balance queries
- Synchronous update of totals and cache inside the same transaction
- Retroactive corrections without cascading recalculation
- Recorder pattern for atomic document post/unpost/repost
- High-write delta buffer mode for hot-row contention
- Automatic partition management for movement history
- Full audit trail of original movements and adjustments

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

### 3. Post a movement

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

### 4. Read a balance

```sql
SELECT * FROM accum.inventory_balance(
    dimensions := '{"warehouse": 1, "product": 42}'
);
```

---

## Installation

### Requirements

- PostgreSQL 15 or later
- Superuser privileges to install the extension
- Build tools for PostgreSQL extension compilation

### From Source

```bash
git clone https://github.com/example/pg_accumulator.git
cd pg_accumulator
make PG_CONFIG=/path/to/pg_config
sudo make install PG_CONFIG=/path/to/pg_config
```

### Docker

```dockerfile
FROM postgres:17

RUN apt-get update     && apt-get install -y pg-accumulator-17     && rm -rf /var/lib/apt/lists/*
```

### Enable in PostgreSQL

```sql
CREATE EXTENSION pg_accumulator;
-- Optionally:
-- CREATE EXTENSION pg_accumulator SCHEMA my_schema;
```

---

## Concepts

### What is an Accumulation Register

An accumulation register records **movements** (resource additions and removals) and provides fast summaries of balances and turnovers across defined dimensions.

### Dimensions and Resources

- **Dimensions** are the axes of analysis, like `warehouse`, `product`, or `lot`.
- **Resources** are numeric quantities that accumulate, like `quantity` or `amount`.

### Register Types

| Type | Purpose | Balance | Turnover |
|---|---|---|---|
| `balance` | Tracks current balances and turnovers | ✅ | ✅ |
| `turnover` | Tracks period turnovers only | ❌ | ✅ |

---

## API

All functions are created in the extension schema, typically `accum`.

### Registry Management

#### `register_create()`

Creates a new register and all supporting objects.

```sql
SELECT register_create(
    name          := 'inventory',
    dimensions    := '{"warehouse": "int", "product": "int", "lot": "text"}',
    resources     := '{"quantity": "numeric(18,4)", "amount": "numeric(18,2)"}',
    kind          := 'balance',
    totals_period := 'day',
    partition_by  := 'month',
    high_write    := false,
    recorder_type := 'text'
);
```

#### `register_alter()`

Alters an existing register by adding dimensions, resources, or toggling high-write mode.

#### `register_drop()`

Drops a register and all its infrastructure.

#### `register_list()`

Lists all registers in the system.

```sql
SELECT * FROM accum.register_list();
```

#### `register_info()`

Returns metadata and status for a register.

---

### Writing Data

#### `register_post()`

Posts one or more movements for a register.

```sql
SELECT register_post('inventory', '{
    "recorder":  "purchase_order:7001",
    "period":    "2026-04-18T14:30:00",
    "warehouse": 1,
    "product":   42,
    "lot":       "LOT-A",
    "quantity":  100,
    "amount":    5000.00
}');
```

#### `register_unpost()`

Cancels all movements for a given recorder.

```sql
SELECT register_unpost('inventory', 'sales_order:8001');
```

#### `register_repost()`

Atomically replaces a recorder's movements with new data.

```sql
SELECT register_repost('inventory', 'sales_order:8001', '[ ... ]');
```

#### Direct INSERT

Direct inserts into the `movements` table are supported, and triggers will update totals and cache.

---

### Reading Data

#### `<register>_balance()`

Returns current or historical balances for a register.

#### `<register>_turnover()`

Returns turnovers for a time interval, optionally grouped by dimensions.

#### `<register>_movements()`

Returns movement rows with optional filters.

#### `<register>_balance_cache`

A direct table for current balance rows.

---

## Benchmarks

The benchmark suite is available in `bench/sql/bench.sql` and can be run with `make bench-docker`.

### Example results

| Scenario | Iterations | Total | Avg/op | ops/sec |
|---|---|---|---|---|
| `register_post()` single insert | 5,000 | 4,310 ms | 0.86 ms | 1,160 |
| `register_post()` batch 10 | 5,000 | 1,999 ms | 0.40 ms | 2,501 |
| `register_post()` batch 100 | 10,000 | 2,784 ms | 0.28 ms | 3,591 |
| `register_post()` batch 1000 | 10,000 | 2,030 ms | 0.20 ms | 4,925 |
| `balance_cache` direct SELECT | 2,000 | 219 ms | 0.11 ms | 9,151 |
| `<register>_balance()` function | 1,000 | 394 ms | 0.39 ms | 2,540 |
| `register_post()` high_write mode | 5,000 | 3,091 ms | 0.62 ms | 1,618 |
| `register_unpost()` | 1,000 | 1,229 ms | 1.23 ms | 813 |

These values are representative of the current benchmark configuration and can vary by hardware and PostgreSQL settings.

---

## Architecture

The extension generates a three-layer architecture:

- `movements`: append-only source of truth
- `totals_month` / `totals_year`: period turnovers
- `balance_cache`: current balance cache

The trigger chain keeps derived data synchronized in the same transaction.

---

## Concurrency and Performance

- Row-level locking allows concurrent writes to different dimension combinations.
- High-write mode uses a delta buffer to reduce hot-row contention.
- Balance reads remain fast via the cache and totals hierarchy.

---

## Diagnostics and Maintenance

- `register_verify()` checks consistency between movements, totals, and cache.
- `register_rebuild_totals()` rebuilds totals from movements.
- `register_rebuild_cache()` rebuilds the balance cache.
- `_force_delta_merge()` flushes pending delta rows in high-write mode.

---

## Configuration

Key PostgreSQL settings for `pg_accumulator` include:

- `pg_accumulator.background_workers`
- `pg_accumulator.delta_merge_interval`
- `pg_accumulator.delta_merge_delay`
- `pg_accumulator.delta_merge_batch_size`
- `pg_accumulator.partitions_ahead`
- `pg_accumulator.maintenance_interval`

---

## Compatibility

- PostgreSQL 15 and newer
- Recommended PostgreSQL 17+

Supported dimension and resource types include standard numeric, integer, text, uuid, date, and boolean variants.

---

## FAQ

**Q: What happens if `register_post()` and `register_unpost()` run concurrently for the same recorder?**
A: PostgreSQL row-level locking serializes the operations and preserves consistency.

**Q: Can this extension be used with logical replication?**
A: Yes. The tables are standard PostgreSQL tables. It is usually best to replicate `movements` and rebuild totals/cache on replicas if needed.

**Q: What if `dim_hash` collides?**
A: The extension uses a 64-bit hash with a very low collision probability. The cache strategy also maintains full dimension consistency.

**Q: Can dimensions contain NULL values?**
A: Yes. NULL is treated as a valid dimension value and is considered distinct from any concrete value.

---

## License

`pg_accumulator` is distributed under the PostgreSQL License.
