# Module: Read API

**Purpose:** Query functions for balances, turnovers, and movements with hierarchical optimization via the totals layer.

## Files

- `balance.c` — `<register>_balance()` implementation
- `turnover.c` — `<register>_turnover()` implementation
- `movements.c` — `<register>_movements()` implementation

## Responsibilities

### 1. `<register>_balance(dimensions, at_date)` — Balance Query

- **Current balance (no `at_date`):** O(1) direct read from `balance_cache`. In high-write mode, adds pending deltas via `UNION ALL` from the delta buffer.
- **Historical balance (`at_date` specified):** Uses hierarchical aggregation — sums complete years from `totals_year`, complete months from `totals_month`, remaining days from `totals_day`, and any partial-period movements. Maximum rows scanned: ~20 years + ~11 months + ~31 days ≈ 62 rows, regardless of total data volume.
- **Partial dimensions:** When not all dimensions are specified, aggregates across omitted dimensions.
- **No dimensions:** Returns the total balance across the entire register.

### 2. `<register>_turnover(from_date, to_date, dimensions, group_by)` — Turnover Query

- Computes net resource change within [from_date, to_date]
- Optimizes by reading from `totals_month` / `totals_year` for complete periods, falling back to `totals_day` or raw movements only for partial boundary periods
- `group_by` parameter returns SETOF JSONB, one object per distinct group value
- Supports partial dimension filtering

### 3. `<register>_movements(recorder, from_date, to_date, dimensions)` — Movement Listing

- Direct filtered access to the movements table
- Uses partition pruning on `period` for efficient range scans
- Supports filtering by `recorder`, date range, and/or dimension values
- Returns SETOF JSONB

## SQL Sources

- [sql/06_read_api.sql](../../sql/06_read_api.sql) — Internal generic functions (`_balance_internal`, `_turnover_internal`) and per-register wrapper generation

## Related Tests

- [test/sql/09_balance_cache.sql](../../test/sql/09_balance_cache.sql) — Balance reads, edge cases (11 assertions)
- [test/sql/11_turnover_register.sql](../../test/sql/11_turnover_register.sql) — Turnover-only registers, period queries
- [test/sql/14_end_to_end_warehouse.sql](../../test/sql/14_end_to_end_warehouse.sql) — Full warehouse balance/turnover scenario
- [test/sql/15_end_to_end_finance.sql](../../test/sql/15_end_to_end_finance.sql) — Full financial accounting scenario
