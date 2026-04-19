# Module: DDL Generator

**Purpose:** Generates all DDL statements when creating a register — movements table (partitioned), totals tables (day/month/year), balance cache, delta buffer, indexes, and read functions.

## Files

- `ddl_generator.c` — Orchestrates the full DDL generation pipeline
- `ddl_tables.c` — Table creation: movements, totals, balance_cache, delta buffer
- `ddl_indexes.c` — Index creation: dim_hash+period, recorder, per-dimension on cache
- `ddl_functions.c` — Read function generation: balance, turnover, movements

## Responsibilities

### 1. Movements Table

Partitioned by `RANGE` on the `period` column. Granularity is configurable: `day`, `month`, `quarter`, `year`. Includes columns for `id` (UUID), `recorded_at`, `recorder`, `period`, `movement_type`, `dim_hash`, all declared dimensions, and all declared resources.

### 2. Totals Tables

- `totals_day` — daily turnover aggregates (PK: `dim_hash` + `period`)
- `totals_month` — monthly turnover aggregates (PK: `dim_hash` + `period`)
- `totals_year` — annual turnover aggregates (PK: `dim_hash` + `period`)

Totals store **turnovers** (net change per period), not cumulative balances. This design enables O(1) retroactive corrections without cascading recalculations.

### 3. Balance Cache

Created only for `kind = 'balance'` registers. Stores the current cumulative balance per unique dimension combination, identified by `dim_hash` (PK). Includes denormalized dimension columns and a `version` counter for optimistic locking.

### 4. Delta Buffer

Created only when `high_write = true`. An `UNLOGGED` table with a `bigserial` PK for append-only writes, indexed by `(dim_hash, created_at)`.

### 5. Indexes

- `(dim_hash, period)` on movements — fast filtered scans
- `(recorder)` on movements — unpost/repost lookup
- `(period)` on movements — partition pruning
- Per-dimension indexes on balance_cache — application query support

### 6. Read Functions

Auto-generated per-register wrapper functions:
- `<register>_balance(dimensions, at_date)` — balance query
- `<register>_turnover(from_date, to_date, dimensions, group_by)` — turnover query
- `<register>_movements(recorder, from_date, to_date, dimensions)` — movement listing

## Security

All identifiers are escaped with `quote_ident()`. Types are validated against an allowlist. Register names are checked against a strict regex pattern.

## SQL Sources

- [sql/03_ddl.sql](../../sql/03_ddl.sql) — DDL generation functions

## Related Tests

- [test/sql/02_register_create.sql](../../test/sql/02_register_create.sql) — Verifies DDL output: table structure, indexes, functions, partitions
