# Module: Maintenance

**Purpose:** Data consistency verification, totals/cache rebuild, and diagnostic statistics.

## Files

- `verify.c` — `register_verify()` implementation
- `rebuild.c` — `register_rebuild_totals()` and `register_rebuild_cache()` implementations
- `stats.c` — `register_stats()` implementation

## Responsibilities

### 1. `register_verify(name)` — Consistency Verification

Performs a FULL JOIN between the balance_cache and `SUM(movements)` grouped by `dim_hash`. Also verifies each totals level (day, month, year) against movements.

Returns SETOF rows with columns: `check_type`, `dim_hash`, `period`, `expected`, `actual`, `status`.

Possible status values:
- `OK` — values match
- `MISMATCH` — values differ
- `MISSING_IN_CACHE` — movements exist but no cache row
- `ORPHAN_IN_CACHE` — cache row exists but no matching movements
- `MISSING_IN_TOTALS` — movements exist but no totals row
- `ORPHAN_IN_TOTALS` — totals row exists but no matching movements

### 2. `register_rebuild_totals(name)` — Totals Rebuild

Truncates all totals tables (day, month, year) and reconstructs them from the movements source of truth using `INSERT ... SELECT ... GROUP BY`. Used after manual data repair or when `register_verify()` reports totals mismatches.

### 3. `register_rebuild_cache(name, dim_hash)` — Cache Rebuild

Full or partial rebuild of the balance_cache from movements. When `dim_hash` is specified, only that specific dimension combination is rebuilt. Used after crash recovery in high-write mode, or when `register_verify()` reports cache mismatches.

### 4. `register_stats(name)` — Diagnostic Statistics

Returns a JSONB object with counts, sizes, and pending delta information for a register.

## Internal Safety

Maintenance functions set `pg_accumulator.allow_internal = 'on'` to bypass protection triggers during rebuild, and reset it to `''` after completion to prevent GUC leakage.

## SQL Sources

- [sql/10_maintenance.sql](../../sql/10_maintenance.sql) — Maintenance and diagnostic functions

## Related Tests

- [test/sql/20_maintenance.sql](../../test/sql/20_maintenance.sql) — verify, rebuild_totals, rebuild_cache correctness
- [test/sql/22_consistency.sql](../../test/sql/22_consistency.sql) — register_verify() used to validate full aggregation chain
