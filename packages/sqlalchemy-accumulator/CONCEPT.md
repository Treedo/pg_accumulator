# sqlalchemy-accumulator

**SQLAlchemy adapter for pg_accumulator** — type-safe accumulation registers in your SQLAlchemy project.

---

## Problem

SQLAlchemy ORM does not natively support:
- PostgreSQL function calls as a first-class API (`SELECT accum.register_post(...)`)
- Dynamically created tables (register infrastructure is generated at runtime)
- JSON-based function signatures (`register_post(name, jsonb)`)
- Type inference for SQL function results returning varying column sets

Developers are forced to write raw SQL via `session.execute(text(...))` without autocompletion, type inference, or validation — exactly what ORM should eliminate.

## Solution

`sqlalchemy-accumulator` is a Python package that:
1. **Declaratively describes registers** via Python dataclasses and type hints
2. **Provides generic types** for dimensions, resources, movements, balance, and turnover
3. **Provides a type-safe client** for all pg_accumulator operations
4. **Works alongside SQLAlchemy** — complements, does not replace
5. **Integrates with Alembic** for migration generation

---

## Architecture

```
┌─────────────────────────────────────────────────┐
│                  Your Application               │
├─────────────────────────────────────────────────┤
│  SQLAlchemy ORM         │  AccumulatorClient    │
│  (models: User,         │  (type-safe wrapper   │
│   Order, Product)       │   for pg_accumulator) │
├─────────────┬───────────┼───────────────────────┤
│  sqlalchemy │  sqlalchemy-accumulator            │
│  Session    │  ├─ define_register()              │
│             │  ├─ .post() / .unpost() / .repost()│
│             │  ├─ .balance() / .turnover()       │
│             │  └─ .movements()                   │
├─────────────┴───────────────────────────────────┤
│              PostgreSQL + pg_accumulator         │
│  ┌──────────┐  ┌──────────┐  ┌───────────────┐  │
│  │movements │  │ totals   │  │balance_cache  │  │
│  └──────────┘  └──────────┘  └───────────────┘  │
└─────────────────────────────────────────────────┘
```

---

## Core Design Principles

### 1. Pythonic API

Follow Python idioms: context managers for transactions, `@dataclass` for schemas,
type hints everywhere, exceptions with clear messages.

### 2. Dual-mode: Engine or Session

Works both with raw `Engine`/`Connection` (for scripts) and with `Session` (for ORM apps).
Transaction context is inherited — if the user is already inside `session.begin()`, the
accumulator operates within the same transaction.

### 3. Type Safety via Generics

Uses Python `TypedDict`, `Generic[D, R]`, and `TypeVar` to propagate dimension/resource
types through the call chain — providing IDE autocompletion and mypy/pyright validation.

### 4. Zero Dependencies Beyond SQLAlchemy

Only SQLAlchemy is a runtime dependency. Pydantic is optional (for enhanced validation).
Alembic integration is a separate optional extra.

---

## API Design

### 1. Register Definition (Schema)

```python
from sqlalchemy_accumulator import define_register

# Balance register — tracks current state (inventory, accounts)
inventory = define_register(
    name="inventory",
    kind="balance",
    dimensions={
        "warehouse": "int",
        "product": "int",
        "lot": "text",
    },
    resources={
        "quantity": "numeric",
        "amount": "numeric",
    },
    totals_period="day",
    partition_by="month",
)

# Turnover register — tracks flow (sales, purchases)
sales = define_register(
    name="sales",
    kind="turnover",
    dimensions={
        "customer": "int",
        "product": "int",
    },
    resources={
        "quantity": "numeric",
        "revenue": "numeric",
    },
)
```

With full type hints (advanced usage):

```python
from sqlalchemy_accumulator import define_register
from typing import TypedDict

class InventoryDims(TypedDict):
    warehouse: int
    product: int
    lot: str

class InventoryRes(TypedDict):
    quantity: float
    amount: float

inventory = define_register(
    name="inventory",
    kind="balance",
    dimensions={"warehouse": "int", "product": "int", "lot": "text"},
    resources={"quantity": "numeric", "amount": "numeric"},
    dims_type=InventoryDims,
    res_type=InventoryRes,
)
# inventory: Register[InventoryDims, InventoryRes]
# → .post(), .balance(), etc. will accept/return typed dicts
```

### 2. Client (AccumulatorClient)

```python
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy_accumulator import AccumulatorClient

engine = create_engine("postgresql://user:pass@localhost/mydb")

# Option A: engine-based client (auto-manages connections)
accum = AccumulatorClient(engine, schema="accum")

# Option B: session-based client (inherits existing transaction)
with Session(engine) as session:
    accum = AccumulatorClient(session, schema="accum")
```

#### Writing

```python
# Single movement
accum.use(inventory).post({
    "recorder": "purchase:7001",
    "period": "2026-04-19",
    "warehouse": 1,
    "product": 42,
    "lot": "LOT-A",
    "quantity": 100,
    "amount": 5000,
})

# Batch — list of movements
accum.use(inventory).post([
    {"recorder": "purchase:7001", "period": "2026-04-19",
     "warehouse": 1, "product": 42, "quantity": 50, "amount": 2500},
    {"recorder": "purchase:7001", "period": "2026-04-19",
     "warehouse": 1, "product": 43, "quantity": 200, "amount": 8000},
])

# Cancel all movements by recorder
accum.use(inventory).unpost("purchase:7001")

# Atomic re-post (cancel old + post new)
accum.use(inventory).repost("purchase:7001", [
    {"recorder": "purchase:7001", "period": "2026-04-19",
     "warehouse": 1, "product": 42, "quantity": 120, "amount": 6000},
])
```

#### Reading

```python
# Current balance (all dimensions specified → single row)
bal = accum.use(inventory).balance(
    warehouse=1,
    product=42,
)
# → {"quantity": Decimal("100"), "amount": Decimal("5000")}

# Partial dimensions → aggregated result
bal = accum.use(inventory).balance(warehouse=1)
# → {"quantity": Decimal("300"), "amount": Decimal("15000")}

# Historical balance
bal = accum.use(inventory).balance(
    warehouse=1,
    at_date="2026-01-01",
)

# Turnovers for a period
turns = accum.use(inventory).turnover(
    warehouse=1,
    date_from="2026-01-01",
    date_to="2026-03-31",
)
# → [{"warehouse": 1, "product": 42, "quantity": ..., "amount": ...}, ...]

# Movements query
moves = accum.use(inventory).movements(
    warehouse=1,
    product=42,
    limit=50,
    order_by="period",
    order="desc",
)
# → [{"id": "...", "recorder": "...", "period": ..., ...}, ...]
```

### 3. DDL (Administration)

```python
# Create register in the database
accum.create_register(inventory)

# Alter register — add new columns
accum.alter_register("inventory",
    add_dimensions={"color": "text"},
    add_resources={"weight": "numeric"},
)

# Drop register and all data
accum.drop_register("inventory")

# List all registers
registers = accum.list_registers()
# → [{"name": "inventory", "kind": "balance", ...}, ...]

# Detailed info about one register
info = accum.register_info("inventory")
# → {"name": "inventory", "kind": "balance", "dimensions": {...}, ...}
```

### 4. Transactions (SQLAlchemy integration)

```python
from sqlalchemy.orm import Session

with Session(engine) as session:
    accum = AccumulatorClient(session, schema="accum")

    with session.begin():
        # SQLAlchemy ORM operations
        order = Order(customer_id=1, total=5000)
        session.add(order)
        session.flush()  # get order.id

        # pg_accumulator in the same transaction
        accum.use(inventory).post({
            "recorder": f"order:{order.id}",
            "period": "2026-04-19",
            "warehouse": order.warehouse_id,
            "product": order.product_id,
            "quantity": -order.quantity,
            "amount": -order.total,
        })
    # COMMIT — both the Order and the movement are committed atomically
```

#### Nested transactions (savepoints)

```python
with session.begin():
    accum.use(inventory).post(movement_a)

    try:
        with session.begin_nested():  # SAVEPOINT
            accum.use(inventory).post(movement_b)
            # if this fails → savepoint is rolled back
    except AccumulatorError:
        pass  # continue with movement_a only

    # COMMIT — movement_a is saved
```

### 5. Async Support (SQLAlchemy 2.0)

```python
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy_accumulator import AsyncAccumulatorClient

engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/mydb")

async with AsyncSession(engine) as session:
    accum = AsyncAccumulatorClient(session, schema="accum")

    async with session.begin():
        await accum.use(inventory).post({
            "recorder": "purchase:8001",
            "period": "2026-04-19",
            "warehouse": 1,
            "product": 42,
            "quantity": 100,
            "amount": 5000,
        })

        bal = await accum.use(inventory).balance(warehouse=1, product=42)
```

### 6. Alembic Integration (Migrations)

```python
# alembic/versions/xxxx_add_inventory_register.py
# Generated by: sqlalchemy-accumulator generate-migration

from alembic import op

def upgrade():
    op.execute("""
        SELECT accum.register_create(
            name       := 'inventory',
            dimensions := '{"warehouse": "int", "product": "int", "lot": "text"}'::jsonb,
            resources  := '{"quantity": "numeric", "amount": "numeric"}'::jsonb,
            kind       := 'balance',
            totals_period := 'day',
            partition_by  := 'month'
        );
    """)

def downgrade():
    op.execute("SELECT accum.register_drop('inventory');")
```

CLI integration:
```bash
# Generate Alembic migration from register definitions
sqlalchemy-accumulator generate-migration --registers myapp/registers.py

# Introspect registers from DB and generate Python definitions
sqlalchemy-accumulator introspect --url postgresql://localhost/mydb
```

---

## PostgreSQL → Python Type Mapping

| PostgreSQL type | Python type | Runtime value |
|---|---|---|
| `int` / `integer` | `int` | `int` |
| `bigint` | `int` | `int` |
| `numeric` / `decimal` | `Decimal` | `decimal.Decimal` |
| `text` / `varchar` | `str` | `str` |
| `boolean` | `bool` | `bool` |
| `date` | `date \| str` | `datetime.date` |
| `timestamptz` | `datetime \| str` | `datetime.datetime` |
| `uuid` | `str \| UUID` | `uuid.UUID` |

---

## Error Hierarchy

```python
class AccumulatorError(Exception):
    """Base exception for all pg_accumulator errors."""

class RegisterNotFoundError(AccumulatorError):
    """Register does not exist in the database."""
    register_name: str

class RecorderNotFoundError(AccumulatorError):
    """Recorder value not found in movements table."""
    recorder: str

class ValidationError(AccumulatorError):
    """Input validation failed (missing fields, wrong types)."""
    field: str
    message: str

class RegisterExistsError(AccumulatorError):
    """Attempt to create a register that already exists."""
    register_name: str
```

PostgreSQL `RAISE EXCEPTION` messages are mapped to typed exceptions:
- `'Register "%" does not exist'` → `RegisterNotFoundError`
- `'recorder is required'` → `ValidationError`
- `'Register "%" already exists'` → `RegisterExistsError`

---

## Internal Architecture

```
sqlalchemy_accumulator/
├── __init__.py              # Public API exports
├── client.py                # AccumulatorClient, AsyncAccumulatorClient
├── register.py              # define_register(), Register dataclass
├── handle.py                # RegisterHandle — bound register operations
├── types.py                 # TypedDict, type aliases, PG→Python mapping
├── errors.py                # Exception classes + pg_error_map()
├── validation.py            # Input validation (names, dimensions, movements)
├── sql.py                   # SQL query builders (parameterized)
├── async_client.py          # Async wrapper (AsyncSession support)
├── operations/
│   ├── __init__.py
│   ├── post.py              # post, unpost, repost SQL builders
│   ├── read.py              # balance, turnover, movements SQL builders
│   └── ddl.py               # create, alter, drop, list, info SQL builders
├── alembic/
│   ├── __init__.py
│   └── ops.py               # Alembic custom operations (optional)
└── cli.py                   # CLI: generate-migration, introspect
```

---

## Security

### SQL Injection Prevention

All queries use SQLAlchemy parameterized queries via `text()` + `bindparams`:

```python
from sqlalchemy import text

# SAFE — parameterized call
stmt = text("SELECT accum.register_post(:name, :data::jsonb)")
session.execute(stmt, {"name": register_name, "data": json.dumps(movements)})
```

Register names are validated against pattern `^[a-z_][a-z0-9_]*$` (same as pg_accumulator).
Schema names are validated identically. No string interpolation of user input into SQL.

### Input Validation

- Register name: `^[a-z_][a-z0-9_]{0,62}$`
- Dimension/resource keys: same pattern
- Movement data: required fields (`recorder`, `period`, all dimensions) validated before SQL exec
- Types: Python type checks at client boundary, PostgreSQL casts catch the rest

---

## Development Phases

### Phase 1 — Core Client (MVP)
**Goal:** Minimum viable package — type-safe wrapper over pg_accumulator SQL functions.

- [ ] Initialize Python package (`sqlalchemy-accumulator`) with pyproject.toml
- [ ] `define_register()` with dataclass + Generic types
- [ ] `AccumulatorClient` with `Engine` and `Session` support
- [ ] `RegisterHandle` with `.post()` (single + batch)
- [ ] `.unpost()`, `.repost()`
- [ ] `.balance()` (current + historical)
- [ ] `.turnover()` (with period filters)
- [ ] `.movements()` (with pagination)
- [ ] SQL injection prevention (parameterized queries only)
- [ ] Input validation (register names, dimensions, movement data)
- [ ] Error mapping (PG exceptions → Python exceptions)
- [ ] Unit tests (pytest + mock)
- [ ] README with Quick Start

**Result:** `pip install sqlalchemy-accumulator` → working type-safe client.

---

### Phase 2 — Async + Transaction Support
**Goal:** Full async support and advanced transaction patterns.

- [ ] `AsyncAccumulatorClient` (SQLAlchemy 2.0 async)
- [ ] Transaction context inheritance (Session.begin() / begin_nested())
- [ ] Custom error classes with structured fields
- [ ] Retry logic for serialization failures (optional decorator)
- [ ] Client-side validation with typed errors
- [ ] Integration tests with real PostgreSQL + pg_accumulator
- [ ] py.typed marker for PEP 561 compliance

**Result:** Production-ready async client with transactions.

---

### Phase 3 — DDL + Alembic CLI
**Goal:** Automate register lifecycle and integrate with Alembic migrations.

- [ ] `accum.create_register(definition)` — DDL via client
- [ ] `accum.alter_register()` — add dimensions/resources
- [ ] `accum.drop_register()`
- [ ] `accum.list_registers()` / `accum.register_info()`
- [ ] CLI: `sqlalchemy-accumulator generate-migration`
  - Reads `define_register()` from Python module
  - Compares with current DB state (`register_info`)
  - Generates Alembic migration file
- [ ] CLI: `sqlalchemy-accumulator introspect`
  - Reads registers from DB
  - Generates Python register definitions
- [ ] Alembic custom operations (optional `op.create_register(...)`)

**Result:** Full register lifecycle via CLI + Alembic migrations.

---

### Phase 4 — Advanced Features
**Goal:** FastAPI integration, bulk operations, observability.

- [ ] FastAPI dependency injection (`Depends(get_accumulator)`)
- [ ] Bulk post optimization (streaming for large batches)
- [ ] Connection pool awareness (NullPool, QueuePool)
- [ ] Logging integration (SQLAlchemy echo mode, structlog)
- [ ] Prometheus metrics (optional: operation count, latency)
- [ ] Flask-SQLAlchemy extension support

**Result:** Enterprise-ready package with framework integrations.

---

## Comparison: Raw SQL vs sqlalchemy-accumulator

### Before (raw SQL)

```python
import json
from sqlalchemy import text

with session.begin():
    # No type safety, no validation, no autocompletion
    session.execute(
        text("SELECT accum.register_post(:name, :data::jsonb)"),
        {"name": "inventory", "data": json.dumps({
            "recorder": "purchase:7001",
            "period": "2026-04-19",
            "warehouse": 1,
            "product": 42,
            "quantity": 100,
            "amount": 5000,
        })}
    )

    # Reading — manual result parsing
    result = session.execute(
        text("SELECT * FROM accum.inventory_balance(dimensions := :dims::jsonb)"),
        {"dims": json.dumps({"warehouse": 1, "product": 42})}
    ).fetchone()
    balance = dict(result._mapping) if result else {}
```

### After (sqlalchemy-accumulator)

```python
with session.begin():
    # Type-safe, validated, autocomplete-friendly
    accum.use(inventory).post({
        "recorder": "purchase:7001",
        "period": "2026-04-19",
        "warehouse": 1,
        "product": 42,
        "quantity": 100,
        "amount": 5000,
    })

    # Clean API, typed result
    balance = accum.use(inventory).balance(warehouse=1, product=42)
    # → {"quantity": Decimal("100"), "amount": Decimal("5000")}
```

---

## Testing Strategy

### Unit Tests (Phase 1)
- Mock `session.execute()` via `unittest.mock` / `pytest-mock`
- Assert generated SQL text and bind parameters
- Validate error mapping, input validation, type conversions
- No database required

### Integration Tests (Phase 2+)
- Docker Compose: PostgreSQL + pg_accumulator extension
- pytest fixtures: create/drop test registers per test
- Full round-trip: post → balance/turnover → unpost → verify zeros
- Async tests via `pytest-asyncio`

### Type Checking
- `mypy --strict` on package source
- `pyright` compatibility verified
- PEP 561 `py.typed` marker included

---

## Packaging

```toml
# pyproject.toml
[project]
name = "sqlalchemy-accumulator"
version = "0.1.0"
description = "SQLAlchemy adapter for pg_accumulator — type-safe accumulation registers"
requires-python = ">=3.10"
dependencies = [
    "sqlalchemy>=2.0",
]

[project.optional-dependencies]
async = ["asyncpg>=0.29"]
alembic = ["alembic>=1.13"]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.23",
    "pytest-mock>=3.12",
    "mypy>=1.8",
    "ruff>=0.3",
]

[project.scripts]
sqlalchemy-accumulator = "sqlalchemy_accumulator.cli:main"
```
