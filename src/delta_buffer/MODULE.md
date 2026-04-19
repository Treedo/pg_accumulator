# Module: Delta Buffer (High-Write Mode)

**Purpose:** Scalable writes under high contention on a single `dim_hash`. Instead of `UPDATE`-ing the balance_cache row (which requires a row-level lock), movements append resource deltas to an UNLOGGED buffer table that is periodically compacted into the cache.

## Files

- `delta.c` — Delta write logic (INSERT into buffer instead of UPDATE cache)
- `merge.c` — Delta merge (compaction) logic

## Responsibilities

### 1. Delta Write

When `high_write = true`, the AFTER INSERT trigger inserts a delta row instead of updating `balance_cache`:

```sql
INSERT INTO <register>_balance_cache_delta (dim_hash, <resources>)
VALUES (NEW.dim_hash, NEW.<resource1>, NEW.<resource2>, ...);
```

No lock is acquired on the `balance_cache` row, eliminating all contention.

### 2. Delta-Aware Reads

The `_balance_internal` function in high-write mode computes:

```
result = balance_cache + COALESCE(SUM(pending deltas), 0)
```

This ensures reads are always accurate, regardless of whether compaction has run.

### 3. Delta Merge (Compaction)

The `_delta_merge_register()` function atomically consumes and applies accumulated deltas:

```sql
WITH consumed AS (
    DELETE FROM <register>_balance_cache_delta
    WHERE created_at < now() - p_max_age
    ORDER BY id LIMIT p_batch_size
    RETURNING dim_hash, <resources>
),
agg AS (
    SELECT dim_hash, SUM(<resource1>) AS <resource1>, ...
    FROM consumed GROUP BY dim_hash
)
UPDATE <register>_balance_cache c
SET <resource1> = c.<resource1> + a.<resource1>, ...,
    version = c.version + 1
FROM agg a WHERE c.dim_hash = a.dim_hash;
```

### 4. Delta Buffer Table

```sql
CREATE UNLOGGED TABLE <register>_balance_cache_delta (
    id         bigserial    PRIMARY KEY,
    dim_hash   bigint       NOT NULL,
    <resources>,
    created_at timestamptz  DEFAULT now()
);
CREATE INDEX ON <register>_balance_cache_delta (dim_hash, created_at);
```

`UNLOGGED` for maximum write throughput. Trade-off: deltas are lost on PostgreSQL crash — run `register_rebuild_cache()` after recovery.

## Configuration

| GUC Parameter | Default | Description |
|---|---|---|
| `pg_accumulator.delta_merge_interval` | 5000 ms | Interval between merge cycles |
| `pg_accumulator.delta_merge_delay` | 2000 ms | Minimum delta age before merge eligibility |
| `pg_accumulator.delta_merge_batch_size` | 10000 | Maximum delta rows consumed per merge cycle |

## SQL Sources

- [sql/08_delta_buffer.sql](../../sql/08_delta_buffer.sql) — `_delta_merge_register`, `_force_delta_merge`, delta count functions

## Related Tests

- [test/sql/16_high_write_mode.sql](../../test/sql/16_high_write_mode.sql) — Delta buffer creation, writes, merge correctness, read accuracy with pending deltas (26 assertions)
