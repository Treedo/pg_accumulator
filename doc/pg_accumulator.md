# pg_accumulator

A high-performance data accounting engine for PostgreSQL.

Declarative accumulation registers that provide instant access to balances and
turnovers across arbitrary dimensions — with full transactional consistency,
retroactive corrections, and zero application-side aggregation logic.

## Synopsis

```sql
CREATE EXTENSION pg_accumulator;

-- Create a register with one API call
SELECT register_create(
    name       := 'inventory',
    dimensions := '{"warehouse": "int", "product": "int"}',
    resources  := '{"quantity": "numeric", "amount": "numeric"}',
    kind       := 'balance'
);

-- Post movements
SELECT register_post('inventory', '{
    "recorder":  "receipt:1",
    "period":    "2026-04-18",
    "warehouse": 1,
    "product":   42,
    "quantity":  100,
    "amount":    5000
}');

-- O(1) current balance
SELECT * FROM accum.inventory_balance(
    dimensions := '{"warehouse": 1, "product": 42}'
);

-- Historical balance at any date
SELECT * FROM accum.inventory_balance(
    dimensions := '{"warehouse": 1}',
    at_date    := '2026-03-31'
);

-- Turnovers for a period
SELECT * FROM accum.inventory_turnover(
    from_date  := '2026-04-01',
    to_date    := '2026-04-30',
    dimensions := '{"warehouse": 1}',
    group_by   := '["product"]'
);

-- Cancel a document
SELECT register_unpost('inventory', 'receipt:1');

-- Atomically replace movements
SELECT register_repost('inventory', 'receipt:1', '{ ... }');
```

## Description

`pg_accumulator` solves the data accounting infrastructure problem. Building
accounting systems in PostgreSQL typically requires hand-crafting movement
tables, aggregate tables, trigger logic for keeping totals current,
recalculation logic for historical corrections, and indexes for fast reads.

A single `register_create()` call generates the complete infrastructure:

- **Partitioned movements table** — append-only source of truth
- **Hierarchical totals** (`totals_day`, `totals_month`, `totals_year`) — pre-aggregated turnovers
- **Balance cache** — O(1) current balance lookup
- **Triggers** — synchronous consistency within the same transaction
- **Query functions** — optimized balance and turnover reads
- **Indexes** — dimension hash, recorder, period

The concept of accumulation registers is a proven pattern from enterprise
accounting systems (ERP), adapted for PostgreSQL with modern concurrency and
performance characteristics.

## Key Features

- **Declarative API** — define dimensions and resources as JSON, get full infrastructure
- **Transactional consistency** — all derived data updated in the same transaction
- **O(1) balance lookups** — from pre-computed balance cache
- **Historical queries** — hierarchical totals optimization (years → months → days)
- **Retroactive corrections** — O(1) via per-period turnovers (not cumulative)
- **High-write mode** — UNLOGGED delta buffer with background worker compaction
- **Protection triggers** — derived tables are read-only for applications
- **Consistency verification** — `register_verify()` auditing + `register_rebuild_*()` recovery
- **Partition management** — automatic partition creation for movements tables

## Installation

### From source

```bash
make
make install
```

Or if `pg_config` is not in your path:

```bash
make PG_CONFIG=/path/to/pg_config
make install PG_CONFIG=/path/to/pg_config
```

### Docker

```bash
docker compose -f docker/docker-compose.yml up --build -d
```

### Loading the extension

```sql
CREATE EXTENSION pg_accumulator;
```

For background worker support (delta merge, partition management), add to
`postgresql.conf`:

```ini
shared_preload_libraries = 'pg_accumulator'
```

## SQL API

### Registry Management

| Function | Description |
|---|---|
| `register_create(name, dimensions, resources, kind, ...)` | Create a new accumulation register |
| `register_alter(name, add_dimensions, add_resources)` | Add dimensions/resources to existing register |
| `register_drop(name)` | Drop register and all its infrastructure |
| `register_list()` | List all registered accumulation registers |
| `register_info(name)` | Detailed information about a register |

### Write Operations

| Function | Description |
|---|---|
| `register_post(name, data)` | Post one or more movements (JSON object or array) |
| `register_unpost(name, recorder)` | Cancel all movements for a recorder |
| `register_repost(name, recorder, data)` | Atomically replace movements for a recorder |

### Read Operations

| Function | Description |
|---|---|
| `<register>_balance(dimensions, at_date)` | Current or historical balance |
| `<register>_turnover(from_date, to_date, dimensions, group_by)` | Turnovers for a period |
| `<register>_movements(recorder, from_date, to_date, dimensions)` | Raw movement history |

### Maintenance

| Function | Description |
|---|---|
| `register_verify(name)` | Consistency audit of all derived tables |
| `register_rebuild_totals(name)` | Rebuild totals from movements |
| `register_rebuild_cache(name)` | Rebuild balance cache from movements |

## Data Flow

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
```

## Configuration

```ini
# Background worker (requires restart)
pg_accumulator.background_workers = 1

# Delta buffer compaction
pg_accumulator.delta_merge_interval   = 5000   # ms between merge cycles
pg_accumulator.delta_merge_delay      = 2000   # ms minimum delta age
pg_accumulator.delta_merge_batch_size = 10000  # max deltas per cycle

# Partition management
pg_accumulator.partitions_ahead     = 3
pg_accumulator.maintenance_interval = 3600000  # ms between maintenance runs
```

## Testing

22 pgTAP test suites covering registry CRUD, posting, cancellation, reposting,
trigger chains, balance cache, retroactive corrections, high-write mode,
partitioning, maintenance, background worker, and full consistency validation.

```bash
docker compose -f docker/docker-compose.test.yml up \
    --build --abort-on-container-exit --exit-code-from test-runner
```

## Author

Treedo <treedo@github.com>

## License

The PostgreSQL License.
