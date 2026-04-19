# Module: Core

**Purpose:** Extension initialization, base schema creation, and internal metadata registry for all accumulation registers.

## Files

- `pg_accumulator.c` — Entry point: `_PG_init()`, GUC parameter registration, background worker launch
- `schema.c` — Creation and validation of the `accum` service schema (or user-specified schema)
- `registry.c` — CRUD operations on the internal `_registers` metadata table

## Responsibilities

### 1. Extension Initialization (`_PG_init`)

- Registration of all GUC parameters (`pg_accumulator.schema`, `pg_accumulator.hash_function`, etc.)
- Background worker launch for maintenance tasks (delta merge, partition management)
- Hook registration as needed

### 2. Schema Management (`schema.c`)

- `accum` schema is created at `CREATE EXTENSION` time
- Creates internal service tables:

```sql
CREATE TABLE accum._registers (
    name           text PRIMARY KEY,
    kind           text NOT NULL CHECK (kind IN ('balance', 'turnover')),
    dimensions     jsonb NOT NULL,
    resources      jsonb NOT NULL,
    totals_period  text NOT NULL DEFAULT 'day',
    partition_by   text NOT NULL DEFAULT 'month',
    high_write     boolean NOT NULL DEFAULT false,
    recorder_type  text NOT NULL DEFAULT 'text',
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);
```

- Validates schema existence before operations

### 3. Metadata Registry (`registry.c`)

- `_register_get(name)` — Retrieve register metadata
- `_register_put(name, ...)` — Store or update metadata
- `_register_delete(name)` — Delete metadata
- `_register_exists(name)` — Check existence
- `_register_list()` — List all registered registers
- Name validation (Latin letters, digits, `_`)
- Dimension and resource type validation

## Dependencies

None — this is the foundational module.

## SQL Sources

- [sql/00_schema.sql](../../sql/00_schema.sql) — Service table DDL
- [sql/01_registry.sql](../../sql/01_registry.sql) — Registry functions

## Related Tests

- [test/sql/01_core_registry.sql](../../test/sql/01_core_registry.sql) — Schema creation/deletion, registry CRUD, name/type validation, concurrent access
