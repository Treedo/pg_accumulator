# Module: Partitioning

**Purpose:** Automatic creation, management, and maintenance of range partitions on the movements table.

## Files

- `partition_manager.c` — Partition creation, detachment, listing
- `auto_create.c` — Proactive partition creation (background worker) and lazy creation (trigger fallback)

## Responsibilities

### 1. Partition Creation at `register_create()`

Creates partitions for the current period and `partitions_ahead` future periods, plus a default partition for any data outside defined ranges. Granularity: `day`, `month`, `quarter`, `year`.

### 2. Proactive Partition Creation (Background Worker)

The background worker periodically checks all registers and creates missing future partitions. This ensures that INSERT into movements never fails due to a missing partition.

### 3. Lazy Partition Creation (Trigger Fallback)

If a movement targets a period without a partition (and the background worker hasn't created it yet), the system falls back to the default partition.

### 4. Manual Partition Management

- `register_create_partitions(name, ahead)` — create the next N partitions manually
- `register_detach_partitions(name, older_than)` — detach old partitions (data is not deleted, just unattached from the parent table)
- `register_partitions(name)` — list all partitions with metadata (row count, size)

### 5. Naming Convention

Partition names follow the pattern: `<register>_movements_<suffix>` where suffix is:
- Daily: `YYYY_MM_DD`
- Monthly: `YYYY_MM`
- Quarterly: `YYYY_qN`
- Yearly: `YYYY`

## SQL Sources

- [sql/09_partitioning.sql](../../sql/09_partitioning.sql) — Partition management functions

## Related Tests

- [test/sql/19_partitioning.sql](../../test/sql/19_partitioning.sql) — Partition creation, detachment, listing
- [test/sql/02_register_create.sql](../../test/sql/02_register_create.sql) — Initial partition creation during register setup
