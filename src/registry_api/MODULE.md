# Module: Registry API

**Purpose:** Public API for creating, altering, dropping, listing, and inspecting accumulation registers.

## Files

- `create.c` — `register_create()` implementation
- `alter.c` — `register_alter()` implementation
- `drop.c` — `register_drop()` implementation
- `list.c` — `register_list()` implementation
- `info.c` — `register_info()` implementation

## Responsibilities

### 1. `register_create(name, dimensions, resources, kind, ...)`

Full orchestration pipeline:
1. Validate parameters (name format, dimension/resource types, kind)
2. Insert metadata into `_registers`
3. Generate DDL (movements, totals, balance_cache, delta buffer)
4. Create triggers (BEFORE INSERT, AFTER INSERT, AFTER DELETE)
5. Create initial partitions (current + `partitions_ahead` future)
6. Create read functions (balance, turnover, movements)
7. Create dimension hash function

### 2. `register_alter(name, ...)`

Supports:
- Adding new dimensions — triggers full rebuild of totals and cache from movements
- Adding new resources — adds column with `DEFAULT 0`; existing data is unaffected
- Toggling `high_write` mode — creates or drops the delta buffer table
- Changing partition granularity

Restrictions: cannot remove dimensions, change types, or change register kind.

### 3. `register_drop(name, force)`

Drops all infrastructure created by `register_create()` via `DROP CASCADE`:
- All tables (movements, totals, balance_cache, delta buffer)
- All indexes, triggers, and functions
- Metadata row from `_registers`

Without `force := true`, fails if the register contains any movements.

### 4. `register_list()`

Returns a SETOF RECORD summary: name, kind, dimension count, resource count, movements count, created_at.

### 5. `register_info(name)`

Returns a JSONB object with complete metadata: name, kind, dimensions, resources, totals_period, partition_by, high_write, recorder_type, movements_count, created_at.

## SQL Sources

- [sql/07_registry_api.sql](../../sql/07_registry_api.sql) — Public registry management functions

## Related Tests

- [test/sql/02_register_create.sql](../../test/sql/02_register_create.sql) — DDL generation, table structure validation
- [test/sql/06_register_drop.sql](../../test/sql/06_register_drop.sql) — Infrastructure teardown, force mode
- [test/sql/07_register_list_info.sql](../../test/sql/07_register_list_info.sql) — List and info output format
- [test/sql/18_register_alter.sql](../../test/sql/18_register_alter.sql) — Adding dimensions/resources, high_write toggle
