# Module: Background Worker

**Purpose:** A PostgreSQL background worker process that performs periodic maintenance tasks: delta buffer compaction, automatic partition creation, and statistics collection.

## Files

- `worker.c` — Background worker implementation

## Responsibilities

### 1. Worker Registration

Registered via `_PG_init` with `BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION` flags. The number of workers is configurable via `pg_accumulator.background_workers` (default: 1, range: 0–8). Requires `shared_preload_libraries = 'pg_accumulator'`.

### 2. Maintenance Loop

The worker runs a continuous loop:

```
while (!shutdown_requested) {
    1. Delta merge — compact all pending deltas for high_write registers
    2. Partition maintenance — create missing future partitions
    3. Sleep for maintenance_interval
}
```

### 3. Delta Merge

For each register with `high_write = true`:
- Acquires an advisory lock to prevent concurrent merge operations
- Executes `_delta_merge_register()` with configured age threshold and batch size
- Releases the lock

### 4. Partition Maintenance

For each register:
- Checks if `partitions_ahead` future partitions exist
- Creates any missing partitions

### 5. Graceful Shutdown

Responds to `SIGTERM` by setting a shutdown flag and exiting cleanly after the current operation completes.

## Configuration

| GUC Parameter | Default | Restart | Description |
|---|---|---|---|
| `pg_accumulator.background_workers` | 1 | Yes | Number of maintenance workers |
| `pg_accumulator.maintenance_interval` | 3600000 ms | No | Full maintenance cycle interval |
| `pg_accumulator.delta_merge_interval` | 5000 ms | No | Delta merge cycle interval |
| `pg_accumulator.delta_merge_delay` | 2000 ms | No | Minimum delta age before merge |
| `pg_accumulator.delta_merge_batch_size` | 10000 | No | Max deltas per merge cycle |

## Dependencies

- `core/registry` — reads the list of registers and their configurations
- `delta_buffer/merge` — calls `_delta_merge_register()` for compaction
- `partitioning/auto_create` — calls partition creation functions

## Related Tests

- [test/sql/21_bgworker.sql](../../test/sql/21_bgworker.sql) — Worker registration and lifecycle
