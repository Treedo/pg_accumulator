# pg_accumulator

**A high-performance data accounting engine for PostgreSQL.**

Declarative accumulation registers that provide instant access to balances and turnovers across arbitrary dimensions — with full transactional consistency, retroactive corrections, and zero application-side aggregation logic.

> **Release status:** v1.0.3 — all [22 test suites](test/sql/) passing (pgTAP).

---

## Table of Contents

- [Overview](#overview)
- [Core Concepts](#core-concepts)
  - [Movement](#movement)
  - [Balance](#balance)
  - [Turnover](#turnover)
  - [Delta (Compaction)](#delta-compaction)
- [Quick Start](#quick-start)
- [SQL API Contracts](#sql-api-contracts)
  - [Post Movement — `register_post()`](#post-movement--register_post)
  - [Update Movement — `register_repost()`](#update-movement--register_repost)
  - [Cancel Movement — `register_unpost()`](#cancel-movement--register_unpost)
  - [Get Balance — `<register>_balance()`](#get-balance--register_balance)
  - [Get Turnover — `<register>_turnover()`](#get-turnover--register_turnover)
- [Architecture](#architecture)
  - [Data Flow](#data-flow)
  - [Module Reference](#module-reference)
- [Consistency Strategy](#consistency-strategy)
- [Compaction Strategy](#compaction-strategy)
- [Benchmarks](#benchmarks)
- [Demo Applications](#demo-applications)
- [Package Ecosystem](#package-ecosystem)
- [Prisma ORM Integration](#prisma-orm-integration)
- [Testing](#testing)
- [Configuration](#configuration)
- [Full API Reference](#full-api-reference)
- [License](#license)

---

## Overview

Building accounting systems in PostgreSQL typically requires hand-crafting movement tables, aggregate tables, trigger logic for keeping totals current, recalculation logic for historical corrections, and indexes for fast reads. Every project re-invents this infrastructure.

`pg_accumulator` solves this with a single declarative API call:

```sql
SELECT register_create(
    name       := 'inventory',
    dimensions := '{"warehouse": "int", "product": "int", "lot": "text"}',
    resources  := '{"quantity": "numeric", "amount": "numeric"}',
    kind       := 'balance'
);
```

This creates the complete infrastructure automatically:
- **Partitioned movements table** — append-only source of truth
- **Hierarchical totals** (`totals_day`, `totals_month`, `totals_year`) — pre-aggregated turnovers
- **Balance cache** — O(1) current balance lookup
- **Triggers** — synchronous consistency within the same transaction
- **Query functions** — optimized balance and turnover reads
- **Indexes** — dimension hash, recorder, period

The concept of accumulation registers is a proven pattern from enterprise accounting systems (ERP), adapted for PostgreSQL with modern concurrency and performance characteristics.

---

## Core Concepts

### Movement

A **movement** is an atomic record of resource change. Movements are always appended — never updated or deleted by the application. Each movement belongs to a **recorder** (business document identifier) and has an accounting **period** (date).

```sql
-- Receipt of 100 units at warehouse 1
SELECT register_post('inventory', '{
    "recorder":  "purchase:7001",
    "period":    "2026-04-18",
    "warehouse": 1,
    "product":   42,
    "lot":       "LOT-A",
    "quantity":  100,
    "amount":    5000
}');
```

Positive values represent receipts, negative values represent expenditures.

**Tested in:** [test/sql/03_register_post.sql](test/sql/03_register_post.sql), [test/sql/12_direct_insert.sql](test/sql/12_direct_insert.sql)

### Balance

A **balance** is the cumulative sum of all movements for a given combination of dimensions. The `balance_cache` table stores the current balance and is updated synchronously within the same transaction as the movement write.

```sql
-- O(1) current balance — reads directly from cache
SELECT * FROM accum.inventory_balance(
    dimensions := '{"warehouse": 1, "product": 42}'
);

-- Historical balance at any point in time — uses totals hierarchy
SELECT * FROM accum.inventory_balance(
    dimensions := '{"warehouse": 1, "product": 42}',
    at_date    := '2026-03-15'
);
```

Historical balance queries use the hierarchical totals optimization: sum complete years from `totals_year`, add complete months from `totals_month`, then scan only the remaining partial period from `totals_day` or raw movements — producing sub-millisecond results regardless of total data volume.

**Tested in:** [test/sql/09_balance_cache.sql](test/sql/09_balance_cache.sql), [test/sql/22_consistency.sql](test/sql/22_consistency.sql)

### Turnover

A **turnover** is the net change in resources during a specific time period. Turnovers are stored at day, month, and year granularity in the totals tables. Unlike cumulative balances, turnovers record only the delta for each period — this is the key design decision that makes retroactive corrections O(1).

```sql
SELECT * FROM accum.inventory_turnover(
    from_date  := '2026-04-01',
    to_date    := '2026-04-30',
    dimensions := '{"warehouse": 1}',
    group_by   := '["product"]'
);
```

**Why turnovers, not cumulative totals:**

| Approach | Retroactive correction complexity |
|---|---|
| Cumulative totals in each period | O(N) — must update all subsequent periods |
| Per-period turnovers (pg_accumulator) | O(1) — update only the affected period |

**Tested in:** [test/sql/08_triggers_totals.sql](test/sql/08_triggers_totals.sql), [test/sql/11_turnover_register.sql](test/sql/11_turnover_register.sql), [test/sql/10_correction_retroactive.sql](test/sql/10_correction_retroactive.sql)

### Delta (Compaction)

When multiple concurrent writers target the same dimension combination (same `dim_hash`), standard mode requires row-level locking on the `balance_cache` row. For high-contention scenarios, **high-write mode** replaces the `UPDATE` with an append-only `INSERT` into a delta buffer table. A background worker periodically **compacts** (merges) accumulated deltas into the balance cache.

```sql
-- Enable high-write mode at register creation
SELECT register_create(
    name       := 'page_views',
    dimensions := '{"page": "text"}',
    resources  := '{"views": "int"}',
    kind       := 'balance',
    high_write := true
);
```

Balance reads in high-write mode automatically include pending (unmerged) deltas, so query results are always accurate regardless of compaction timing.

**Tested in:** [test/sql/16_high_write_mode.sql](test/sql/16_high_write_mode.sql)

---

## Quick Start

### 1. Run with Docker

```bash
git clone https://github.com/example/pg_accumulator.git
cd pg_accumulator

# Start PostgreSQL with the extension pre-installed
docker compose -f docker/docker-compose.yml up --build -d

# Connect
psql -h localhost -p 5432 -U postgres
```

### 2. Create a register

```sql
CREATE EXTENSION pg_accumulator;

SELECT register_create(
    name       := 'inventory',
    dimensions := '{"warehouse": "int", "product": "int"}',
    resources  := '{"quantity": "numeric(18,4)", "amount": "numeric(18,2)"}',
    kind       := 'balance'
);
```

### 3. Post movements

```sql
SELECT register_post('inventory', '[
    {
        "recorder": "receipt:1",
        "period":   "2026-04-18",
        "warehouse": 1, "product": 42,
        "quantity":  100, "amount": 5000.00
    },
    {
        "recorder": "shipment:1",
        "period":   "2026-04-18",
        "warehouse": 1, "product": 42,
        "quantity":  -10, "amount": -500.00
    }
]');
```

### 4. Read the balance

```sql
SELECT * FROM accum.inventory_balance(
    dimensions := '{"warehouse": 1, "product": 42}'
);
-- {"quantity": 90.0000, "amount": 4500.00}
```

### 5. Cancel a document

```sql
SELECT register_unpost('inventory', 'shipment:1');
-- Balance reverts to {"quantity": 100.0000, "amount": 5000.00}
```

### 6. Run the test suite

```bash
docker compose -f docker/docker-compose.test.yml up --build \
    --abort-on-container-exit --exit-code-from test-runner
```

---

## SQL API Contracts

All functions reside in the `accum` schema (configurable at extension creation time).

### Post Movement — `register_post()`

Records one or more movements for a register. Accepts a single JSON object or a JSON array of objects. Returns the count of inserted movements.

```sql
SELECT accum.register_post(
    p_register := 'inventory',
    p_data     := '{
        "recorder":  "purchase:7001",
        "period":    "2026-04-18",
        "warehouse": 1,
        "product":   42,
        "quantity":  100,
        "amount":    5000.00
    }'
);
```

**What happens inside one transaction:**

```
1. Validate JSON → compute dim_hash → INSERT INTO movements
2. UPSERT totals_day   += delta
3. UPSERT totals_month += delta
4. UPSERT totals_year  += delta
5. UPSERT balance_cache += delta (or INSERT into delta buffer in high-write mode)
6. COMMIT → fully consistent, visible to all readers
```

| Field | Type | Required | Description |
|---|---|---|---|
| `recorder` | text (configurable) | Yes | Business document identifier |
| `period` | timestamp / date | Yes | Accounting period of the movement |
| `<dimension>` | as declared | Yes | Value for each dimension |
| `<resource>` | numeric | Yes | Delta value (positive = receipt, negative = expenditure) |

**Tested in:** [test/sql/03_register_post.sql](test/sql/03_register_post.sql)

### Update Movement — `register_repost()`

Atomically replaces all movements for a recorder with new data. Equivalent to unpost + post, but optimized to compute and apply only the net delta within a single transaction.

```sql
SELECT accum.register_repost('inventory', 'purchase:7001', '{
    "period":    "2026-04-18",
    "warehouse": 1,
    "product":   42,
    "quantity":  110,
    "amount":    5500.00
}');
```

This is the correct tool for correcting already-posted documents. Old movements are deleted, new movements are inserted, and all derived tables are updated with the net change.

**Tested in:** [test/sql/05_register_repost.sql](test/sql/05_register_repost.sql), [test/sql/10_correction_retroactive.sql](test/sql/10_correction_retroactive.sql)

### Cancel Movement — `register_unpost()`

Cancels all movements belonging to a recorder, reversing their effect on totals and balance cache.

```sql
SELECT accum.register_unpost('inventory', 'purchase:7001');
```

**Tested in:** [test/sql/04_register_unpost.sql](test/sql/04_register_unpost.sql)

### Get Balance — `<register>_balance()`

Returns a JSONB object with the current (or historical) balance of resources for a given dimension filter.

```sql
-- Current balance (O(1) from balance_cache)
SELECT * FROM accum.inventory_balance(
    dimensions := '{"warehouse": 1, "product": 42}'
);

-- Historical balance (uses totals hierarchy)
SELECT * FROM accum.inventory_balance(
    dimensions := '{"warehouse": 1}',
    at_date    := '2026-03-31'
);

-- Aggregate across all dimensions
SELECT * FROM accum.inventory_balance();
```

**Tested in:** [test/sql/09_balance_cache.sql](test/sql/09_balance_cache.sql), [test/sql/14_end_to_end_warehouse.sql](test/sql/14_end_to_end_warehouse.sql)

### Get Turnover — `<register>_turnover()`

Returns turnovers (net resource change) for a date range, with optional grouping.

```sql
SELECT * FROM accum.inventory_turnover(
    from_date  := '2026-04-01',
    to_date    := '2026-04-30',
    dimensions := '{"warehouse": 1}',
    group_by   := '["product"]'
);
```

**Tested in:** [test/sql/11_turnover_register.sql](test/sql/11_turnover_register.sql)

---

## Architecture

### Data Flow

```
                    register_post() / register_repost()
                                  │
                                  ▼
┌──────────────────────────────────────────────────────────┐
│  movements  (partitioned, append-only, source of truth)  │
└──────────────────────────┬───────────────────────────────┘
                           │ AFTER INSERT/DELETE triggers
                           ▼
         ┌─────────────────────────────────────┐
         │  totals_day    (daily turnovers)     │
         │  totals_month  (monthly turnovers)   │
         │  totals_year   (annual turnovers)    │
         └─────────────────┬───────────────────┘
                           │
                           ▼
         ┌─────────────────────────────────────┐
         │  balance_cache  (current balance)    │
         │  O(1) point lookup by dim_hash       │
         └─────────────────────────────────────┘
                           │
              (high-write mode only)
                           ▼
         ┌─────────────────────────────────────┐
         │  balance_cache_delta  (UNLOGGED)     │
         │  Append-only buffer, merged by       │
         │  background worker (compaction)       │
         └─────────────────────────────────────┘
```

All derived tables (totals, cache) are updated **synchronously within the same transaction** as the movement write. After `COMMIT`, every reader sees fully consistent data.

### Module Reference

The extension source is organized into focused modules. Each module has its own documentation:

| Module | Purpose | Documentation |
|---|---|---|
| **Core** | Extension init, schema, internal registry | [src/core/MODULE.md](src/core/MODULE.md) |
| **DDL Generator** | Table, index, function generation on `register_create()` | [src/ddl/MODULE.md](src/ddl/MODULE.md) |
| **Hash** | 64-bit dimension hashing (xxhash64) for fast lookups | [src/hash/MODULE.md](src/hash/MODULE.md) |
| **Triggers** | Synchronous totals and cache updates on movement writes | [src/triggers/MODULE.md](src/triggers/MODULE.md) |
| **Write API** | `register_post`, `register_unpost`, `register_repost` | [src/write_api/MODULE.md](src/write_api/MODULE.md) |
| **Read API** | `_balance()`, `_turnover()`, `_movements()` with hierarchy optimization | [src/read_api/MODULE.md](src/read_api/MODULE.md) |
| **Registry API** | `register_create`, `register_alter`, `register_drop`, `register_list`, `register_info` | [src/registry_api/MODULE.md](src/registry_api/MODULE.md) |
| **Delta Buffer** | High-write mode: append-only delta buffer and merge logic | [src/delta_buffer/MODULE.md](src/delta_buffer/MODULE.md) |
| **Partitioning** | Automatic partition management for movements tables | [src/partitioning/MODULE.md](src/partitioning/MODULE.md) |
| **Maintenance** | Consistency verification, totals/cache rebuild, diagnostics | [src/maintenance/MODULE.md](src/maintenance/MODULE.md) |
| **Background Worker** | Delta merge, partition creation, periodic maintenance | [src/bgworker/MODULE.md](src/bgworker/MODULE.md) |

---

## Consistency Strategy

pg_accumulator provides **strong transactional consistency**: after `COMMIT`, all derived data (totals and balance cache) reflects the committed movements exactly.

### Guarantees

| Property | Mechanism |
|---|---|
| **Freshness** | Triggers update totals and cache in the same transaction as the movement write |
| **Correctness** | `balance_cache = SUM(all movements)` is always true (provable via `register_verify()`) |
| **Rollback safety** | If the transaction rolls back, all trigger-side effects roll back with it |
| **MVCC isolation** | Each reader sees a consistent snapshot; no dirty reads |
| **Concurrent writes** | Row-level locks on `dim_hash` rows serialize writes to the same dimension combination |

### Protection Triggers

Derived tables (`totals_day`, `totals_month`, `totals_year`, `balance_cache`) are protected from direct modification. Any attempt to `INSERT`, `UPDATE`, or `DELETE` rows directly raises an exception, unless:
- The modification originates from the internal trigger chain (`pg_trigger_depth() > 1`), or
- The `pg_accumulator.allow_internal` GUC is explicitly enabled (used by maintenance functions).

This prevents accidental corruption of derived data.

### Verification

The `register_verify()` function performs a full consistency audit:

```sql
SELECT * FROM accum.register_verify('inventory');
```

It compares:
- `balance_cache` values against `SUM(movements)` per `dim_hash`
- `totals_day` values against `SUM(movements)` per `(dim_hash, day)`
- `totals_month` values against `SUM(movements)` per `(dim_hash, month)`
- `totals_year` values against `SUM(movements)` per `(dim_hash, year)`

Detected statuses: `OK`, `MISMATCH`, `MISSING_IN_CACHE`, `ORPHAN_IN_CACHE`, `MISSING_IN_TOTALS`, `ORPHAN_IN_TOTALS`.

**Recovery:** If mismatches are detected, use `register_rebuild_totals()` and `register_rebuild_cache()` to reconstruct all derived data from the movements source of truth.

### Test Coverage for Consistency

| Test | What it verifies |
|---|---|
| [test/sql/22_consistency.sql](test/sql/22_consistency.sql) | Full aggregation chain: movements → totals_day → totals_month → totals_year → balance_cache. Multi-resource, cross-month, unpost/repost, protection triggers. **36 assertions.** |
| [test/sql/08_triggers_totals.sql](test/sql/08_triggers_totals.sql) | Trigger chain correctness: INSERT creates totals, DELETE reverses them, delta propagation. **17 assertions.** |
| [test/sql/09_balance_cache.sql](test/sql/09_balance_cache.sql) | Balance cache creation, accumulation, counter-movements, edge cases. **11 assertions.** |
| [test/sql/10_correction_retroactive.sql](test/sql/10_correction_retroactive.sql) | Retroactive corrections: O(1) update of past periods without cascade. |
| [test/sql/16_high_write_mode.sql](test/sql/16_high_write_mode.sql) | Delta buffer creation, merge correctness, balance accuracy with pending deltas. **26 assertions.** |
| [test/sql/20_maintenance.sql](test/sql/20_maintenance.sql) | `register_verify()`, `register_rebuild_totals()`, `register_rebuild_cache()`. |

---

## Compaction Strategy

In **high-write mode**, the balance cache is not updated directly. Instead, resource deltas are appended to an `UNLOGGED` delta buffer table, avoiding row-level lock contention on hot `dim_hash` rows.

### Write Path

```
Standard mode:                       High-write mode:
  UPDATE balance_cache                 INSERT INTO balance_cache_delta
  SET qty += 10                        (dim_hash, views) VALUES (H, 10)
  WHERE dim_hash = H                   — no lock on cache row
  — row lock on H                     — no contention
```

### Compaction (Delta Merge)

The background worker periodically merges accumulated deltas into the balance cache:

```sql
-- Simplified merge logic (runs automatically):
WITH consumed AS (
    DELETE FROM balance_cache_delta
    WHERE created_at < now() - interval '2 seconds'
    ORDER BY id LIMIT 10000
    RETURNING dim_hash, views
),
agg AS (
    SELECT dim_hash, SUM(views) AS views
    FROM consumed GROUP BY dim_hash
)
UPDATE balance_cache c
SET views = c.views + a.views, version = c.version + 1
FROM agg a WHERE c.dim_hash = a.dim_hash;
```

### Read Accuracy

Balance reads in high-write mode **always include pending deltas** via `UNION ALL` in the internal query, so results are accurate regardless of whether compaction has run.

### Compaction Configuration

| Setting | Default | Description |
|---|---|---|
| `pg_accumulator.delta_merge_interval` | 5000 ms | Time between merge cycles |
| `pg_accumulator.delta_merge_delay` | 2000 ms | Minimum delta age before merge |
| `pg_accumulator.delta_merge_batch_size` | 10000 | Max delta rows per merge cycle |

### Manual Compaction

```sql
-- Force immediate merge of all pending deltas
SELECT accum._force_delta_merge(
    p_max_age    := interval '0 seconds',
    p_batch_size := 1000000
);
```

> **Important:** The delta buffer table is `UNLOGGED` for performance. In the event of a PostgreSQL crash, pending (unmerged) deltas are lost. After a crash, run `register_rebuild_cache()` to restore consistency.

**Tested in:** [test/sql/16_high_write_mode.sql](test/sql/16_high_write_mode.sql)

---

## Benchmarks

Measured results from the benchmark suite ([bench/sql/bench.sql](bench/sql/bench.sql)) on PostgreSQL 17, Docker, macOS ARM, single connection.

### 1x scale

| Scenario | Iterations | Total | Avg/op | ops/sec |
|---|---|---|---|---|
| `register_post()` single insert | 5,000 | 5,226.50 ms | 1.0453 ms | 956.7 |
| `register_post()` batch of 10 | 5,000 | 2,498.61 ms | 0.4997 ms | 2,001.1 |
| `register_post()` batch of 100 | 10,000 | 4,460.77 ms | 0.4461 ms | 2,241.8 |
| `register_post()` batch of 1000 | 10,000 | 2,845.90 ms | 0.2846 ms | 3,513.8 |
| `balance_cache` direct SELECT | 2,000 | 236.65 ms | 0.1183 ms | 8,451.2 |
| `b_std_balance()` function | 1,000 | 408.49 ms | 0.4085 ms | 2,448.1 |
| `register_post()` high-write mode | 5,000 | 4,427.16 ms | 0.8854 ms | 1,129.4 |
| `register_unpost()` | 1,000 | 715.70 ms | 0.7157 ms | 1,397.2 |

### 10x scale

| Scenario | Iterations | Total | Avg/op | ops/sec |
|---|---|---|---|---|
| `register_post()` single insert | 50,000 | 116,504.98 ms | 2.3301 ms | 429.2 |
| `register_post()` batch of 10 | 50,000 | 61,083.96 ms | 1.2217 ms | 818.5 |
| `register_post()` batch of 100 | 100,000 | 101,164.23 ms | 1.0116 ms | 988.5 |
| `register_post()` batch of 1000 | 100,000 | 47,264.90 ms | 0.4726 ms | 2,115.7 |
| `balance_cache` direct SELECT | 20,000 | 561.07 ms | 0.0281 ms | 35,646.3 |
| `b_std_10_balance()` function | 10,000 | 4,997.46 ms | 0.4997 ms | 2,001.0 |
| `register_post()` high-write mode | 50,000 | 106,581.80 ms | 2.1316 ms | 469.1 |
| `register_unpost()` | 10,000 | 15,811.21 ms | 1.5811 ms | 632.5 |

These values were captured from the 1x and 10x benchmark runs on this environment; actual performance may vary by hardware and PostgreSQL configuration.

Key takeaways:
- **Batch mode** delivers much higher throughput than single-item inserts
- **Balance cache** point lookups remain extremely fast even at 10x scale
- **High-write mode** is useful for larger write workloads, though average per-op latency is higher than the standard register

Run the benchmark suite:

```bash
# Via Docker
docker compose -f bench/docker-compose.bench.yml up --build

# Or directly
psql -f bench/sql/bench.sql
```

---

## Demo Applications

Interactive demo applications are available for different technology stacks:

| Demo | Stack | App URL | PostgreSQL Port | Documentation |
|---|---|---|---|---|
| **Python** | Flask + psycopg2 | `localhost:3301` | `5434` | [demo/python/README.md](demo/python/README.md) |
| **TypeScript** | Express + pg | `localhost:3302` | `5435` | [demo/typescript/README.md](demo/typescript/README.md) |
| **Prisma** | Express + Prisma ORM + prisma-accumulator | `localhost:3303` | `5436` | [demo/prisma/README.md](demo/prisma/README.md) |
| **SQLAlchemy** | Flask + SQLAlchemy + sqlalchemy-accumulator | `localhost:3304` | `5437` | [demo/sqlalchemy/README.md](demo/sqlalchemy/README.md) |

Each demo demonstrates:
- Register creation and initialization
- Posting movements (`register_post`)
- Canceling documents (`register_unpost`)
- Real-time balance display from `balance_cache`
- Historical balance queries (`_balance()` with `at_date`)
- Movement history browsing

```bash
# Start any demo
cd demo/python   # or demo/typescript, demo/prisma, demo/sqlalchemy
docker compose up --build
```

---

## Package Ecosystem

The repository currently contains three integration packages:

| Package | Language | Purpose | Documentation |
|---|---|---|---|
| **prisma-accumulator** | TypeScript | Type-safe adapter for Prisma ORM | [packages/prisma-accumulator/README.md](packages/prisma-accumulator/README.md) |
| **sqlalchemy-accumulator** | Python | SQLAlchemy-first adapter with typed operations | [packages/sqlalchemy-accumulator/CONCEPT.md](packages/sqlalchemy-accumulator/CONCEPT.md) |
| **go-accumulator** | Go | Planned Go integration package | [packages/go-accumulator/CONCEPT.md](packages/go-accumulator/CONCEPT.md) |

---

## Prisma ORM Integration

The **[prisma-accumulator](packages/prisma-accumulator/README.md)** package provides a type-safe TypeScript adapter for using pg_accumulator with Prisma ORM:

```typescript
import { defineRegister, AccumulatorClient } from 'prisma-accumulator';

const inventory = defineRegister({
  name: 'inventory',
  kind: 'balance',
  dimensions: { warehouse: 'int', product: 'int' },
  resources:  { quantity: 'numeric', amount: 'numeric' },
});

const accum = new AccumulatorClient(prisma);
await accum.use(inventory).post({ recorder: 'receipt:1', period: '2026-04-18', ... });
const balance = await accum.use(inventory).balance({ warehouse: 1 });
```

See the [concept document](packages/prisma-accumulator/CONCEPT.md) for the architecture and development roadmap.

---

## Testing

The test suite comprises **22 pgTAP test files** covering every aspect of the system. All tests pass on the current release.

```bash
# Run the full test suite
docker compose -f docker/docker-compose.test.yml up \
    --build --abort-on-container-exit --exit-code-from test-runner
```

### Test Matrix

| Test | File | Scope |
|---|---|---|
| Core registry CRUD | [01_core_registry.sql](test/sql/01_core_registry.sql) | `_registers` metadata table |
| Register create | [02_register_create.sql](test/sql/02_register_create.sql) | DDL generation, table structure |
| register_post | [03_register_post.sql](test/sql/03_register_post.sql) | Single and batch posting |
| register_unpost | [04_register_unpost.sql](test/sql/04_register_unpost.sql) | Document cancellation |
| register_repost | [05_register_repost.sql](test/sql/05_register_repost.sql) | Atomic movement replacement |
| register_drop | [06_register_drop.sql](test/sql/06_register_drop.sql) | Infrastructure teardown |
| register_list / info | [07_register_list_info.sql](test/sql/07_register_list_info.sql) | Registry introspection |
| Trigger chain | [08_triggers_totals.sql](test/sql/08_triggers_totals.sql) | Totals consistency on INSERT/DELETE |
| Balance cache | [09_balance_cache.sql](test/sql/09_balance_cache.sql) | Cache creation, accumulation, edge cases |
| Retroactive corrections | [10_correction_retroactive.sql](test/sql/10_correction_retroactive.sql) | O(1) past-period corrections |
| Turnover register | [11_turnover_register.sql](test/sql/11_turnover_register.sql) | Turnover-only registers |
| Direct INSERT | [12_direct_insert.sql](test/sql/12_direct_insert.sql) | Trigger-based updates via raw INSERT |
| Multiple dimensions | [13_multiple_dimensions.sql](test/sql/13_multiple_dimensions.sql) | Multi-dimension combinations |
| End-to-end: warehouse | [14_end_to_end_warehouse.sql](test/sql/14_end_to_end_warehouse.sql) | Full warehouse scenario |
| End-to-end: finance | [15_end_to_end_finance.sql](test/sql/15_end_to_end_finance.sql) | Full financial accounting scenario |
| High-write mode | [16_high_write_mode.sql](test/sql/16_high_write_mode.sql) | Delta buffer, merge, read accuracy |
| Recorder pattern | [17_recorder_pattern.sql](test/sql/17_recorder_pattern.sql) | Document-based post/unpost/repost |
| register_alter | [18_register_alter.sql](test/sql/18_register_alter.sql) | Adding dimensions/resources |
| Partitioning | [19_partitioning.sql](test/sql/19_partitioning.sql) | Partition creation and management |
| Maintenance | [20_maintenance.sql](test/sql/20_maintenance.sql) | verify, rebuild_totals, rebuild_cache |
| Background worker | [21_bgworker.sql](test/sql/21_bgworker.sql) | Worker registration and lifecycle |
| Full consistency | [22_consistency.sql](test/sql/22_consistency.sql) | Comprehensive aggregation chain validation |

---

## Configuration

Set in `postgresql.conf` or via `ALTER SYSTEM`:

```ini
# Required for the background worker
shared_preload_libraries = 'pg_accumulator'

# Background worker (requires restart)
pg_accumulator.background_workers = 1        # 0 to disable, range: 0..8

# Delta buffer compaction (reload with SELECT pg_reload_conf())
pg_accumulator.delta_merge_interval   = 5000   # ms between merge cycles
pg_accumulator.delta_merge_delay      = 2000   # ms minimum delta age
pg_accumulator.delta_merge_batch_size = 10000  # max deltas per cycle

# Partition management
pg_accumulator.partitions_ahead     = 3        # future partitions to maintain
pg_accumulator.maintenance_interval = 3600000  # ms between maintenance runs
```

---

## Full API Reference

For the complete API documentation with detailed parameter tables, return types, usage examples, and architectural diagrams, see:

**[docs/README.md](docs/README.md)**

---

## License

pg_accumulator is distributed under the [PostgreSQL License](https://www.postgresql.org/about/licence/).
