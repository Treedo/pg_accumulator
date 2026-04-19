# Module: Triggers

**Purpose:** Trigger functions that synchronously update totals and balance_cache whenever movements are inserted or deleted. This is the core consistency mechanism of the extension.

## Files

- `trigger_engine.c` — Trigger registration and dispatch logic
- `trigger_totals.c` — UPSERT logic for totals_day, totals_month, totals_year
- `trigger_cache.c` — UPSERT logic for balance_cache (or delta buffer INSERT in high-write mode)

## Responsibilities

### 1. BEFORE INSERT Trigger

- Computes `dim_hash` from the dimension values using the per-register hash function
- Sets `recorded_at = now()`
- Assigns `movement_type` (`regular`, `adjustment`, `reversal`)
- Validates required fields

### 2. AFTER INSERT Trigger

Fires in the same transaction as the INSERT. Updates all derived tables:

```
UPSERT totals_day    SET resources += NEW.resources WHERE dim_hash = NEW.dim_hash AND period = date_trunc('day', NEW.period)
UPSERT totals_month  SET resources += NEW.resources WHERE dim_hash = NEW.dim_hash AND period = date_trunc('month', NEW.period)
UPSERT totals_year   SET resources += NEW.resources WHERE dim_hash = NEW.dim_hash AND period = date_trunc('year', NEW.period)
UPSERT balance_cache SET resources += NEW.resources WHERE dim_hash = NEW.dim_hash
```

In high-write mode, the balance_cache step is replaced with an `INSERT` into the delta buffer table.

### 3. AFTER DELETE Trigger

Mirrors the INSERT trigger, but subtracts the deleted movement's resources from totals and cache. Fires during `register_unpost()` and `register_repost()`.

### 4. Hash Collision Protection

The UPSERT uses `dim_hash` as the conflict key, but the full dimension values are stored in each row for integrity. If a hash collision were to occur (extremely unlikely with 64-bit xxhash), the ON CONFLICT clause still operates correctly because the PK is `dim_hash` alone.

### 5. Trigger Chain

The complete chain for a single movement write:

```
BEFORE INSERT → Physical INSERT → AFTER INSERT (totals_day → totals_month → totals_year → balance_cache)
```

All steps execute in the same transaction. COMMIT makes everything visible atomically; ROLLBACK undoes everything.

## SQL Sources

- [sql/04_triggers.sql](../../sql/04_triggers.sql) — Trigger function definitions and registration

## Related Tests

- [test/sql/08_triggers_totals.sql](../../test/sql/08_triggers_totals.sql) — INSERT creates totals, DELETE reverses them, delta propagation (17 assertions)
- [test/sql/22_consistency.sql](../../test/sql/22_consistency.sql) — Full aggregation chain validation, protection triggers (36 assertions)
