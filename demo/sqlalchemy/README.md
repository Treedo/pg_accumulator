# pg_accumulator — SQLAlchemy Demo

> **Stop writing raw SQL for inventory, financial, and accounting logic.**
> Use `sqlalchemy-accumulator` for type-safe, Pythonic accumulation registers.

A hands-on demo app showing how [sqlalchemy-accumulator](../../packages/sqlalchemy-accumulator/) integrates with [pg_accumulator](../../) to give you enterprise-grade accumulation registers with zero SQL boilerplate.

## Why sqlalchemy-accumulator?

| Pain Point | Before (raw SQL) | After (sqlalchemy-accumulator) |
|---|---|---|
| **Define a register** | Multi-line `SELECT accum.register_create(...)` with JSON strings | `define_register(name="inventory", kind="balance", ...)` |
| **Post a movement** | `SELECT accum.register_post('inventory', '{"recorder":...}'::jsonb)` | `handle.post({"recorder": "receipt:1", ...})` |
| **Query balance** | `SELECT * FROM accum.inventory_balance(dimensions := '...'::jsonb)` | `handle.balance(warehouse=1, product=42)` |
| **Error handling** | Parse PostgreSQL error messages manually | Typed exceptions: `RegisterNotFoundError`, `ValidationError` |
| **Transaction safety** | Manual connection management | Works with Engine, Session, or Connection — your choice |

### Key Features

- **Type-safe register definitions** — define dimensions and resources as Python dicts, catch errors before they hit the database
- **Fluent API** — `accum.use(inventory).post({...})` reads like English
- **Session integration** — accumulator operations participate in your SQLAlchemy session transaction alongside ORM models
- **Instant balances** — O(1) reads from materialized balance cache
- **Historical queries** — get the state of any register at any point in time
- **Atomic corrections** — `repost()` atomically replaces all movements for a document

## Quick Start

```bash
cd demo/sqlalchemy
docker compose up --build
```

Open **http://localhost:5002** in your browser.

## What You'll See

### Operations Tab
Post, unpost, and repost inventory movements using the sqlalchemy-accumulator API. Every button maps to a real API call:

```python
handle = accum.use(inventory)
handle.post({"recorder": "receipt:1", "period": "2026-04-01", ...})
handle.unpost("receipt:1")
handle.repost("receipt:1", {"recorder": "receipt:1", ...})
```

### Orders Tab (ORM + Accumulator)
The star of the show — create a sales order using standard SQLAlchemy ORM, and the app
posts inventory movements through `sqlalchemy-accumulator` **in the same transaction**.
Cancel an order and watch the inventory restore automatically.

```python
with Session(engine) as session:
    # ORM: create order
    session.add(order)
    session.flush()

    # Accumulator: post movement — same transaction!
    accum = AccumulatorClient(session)
    accum.use(inventory).post({...})

    session.commit()  # atomic — both or neither
```

### Catalog Tab (Pure ORM)
Manage Warehouses, Products, and Clients with standard SQLAlchemy ORM models.
These entities are referenced by accumulator register dimensions, showing how
ORM and pg_accumulator coexist naturally.

### Live Data Tab
Watch real-time balances and movement history update after each operation.
Balances are enriched with product/warehouse names from ORM models.

### Query API Tab
Interactive forms for `handle.balance()`, `handle.turnover()`, and `handle.movements()` — see raw JSON responses.

### Code Examples Tab
Copy-paste-ready code snippets showing the complete workflow — including ORM + Accumulator in one transaction.

## How It Works

```
┌──────────────────────┐     ┌───────────────────────────┐     ┌──────────────────────┐
│   Flask App          │────▶│  sqlalchemy-accumulator   │────▶│   pg_accumulator     │
│                      │     │  (Python adapter)          │     │   (PG extension)     │
│  SQLAlchemy ORM      │     │  AccumulatorClient         │     │   Balance cache      │
│  ┌────────────────┐  │     │  RegisterHandle            │     │   Partitioned tables │
│  │ Product        │  │     │  Type-safe operations      │     │   Automatic totals   │
│  │ Warehouse      │  │     └───────────────────────────┘     └──────────────────────┘
│  │ Client         │  │
│  │ Order ─────────┼──┼──── same session.commit() ──── accumulator movement
│  └────────────────┘  │
└──────────────────────┘
```

## For Developers

### Minimal Integration (4 lines)

```python
from sqlalchemy import create_engine
from sqlalchemy_accumulator import AccumulatorClient, define_register

engine = create_engine("postgresql://localhost/mydb")
accum = AccumulatorClient(engine)

# Define your register once
inventory = define_register(
    name="inventory",
    kind="balance",
    dimensions={"warehouse": "int", "product": "int"},
    resources={"quantity": "numeric(18,4)", "amount": "numeric(18,2)"},
)

# Create it in the database
accum.create_register(inventory)

# Use it
handle = accum.use(inventory)
handle.post({
    "recorder": "order:42",
    "period": "2026-04-01",
    "warehouse": 1,
    "product": 101,
    "quantity": 100,
    "amount": 25000,
})

# O(1) balance query
balance = handle.balance(warehouse=1, product=101)
# → {'quantity': Decimal('100'), 'amount': Decimal('25000')}
```

### ORM + Accumulator in One Transaction

```python
from sqlalchemy.orm import Session

with Session(engine) as session:
    # 1) Standard ORM — create order with line items
    order = Order(client_id=1, warehouse_id=1, status="posted")
    session.add(order)
    session.flush()  # get order.id

    session.add(OrderLine(
        order_id=order.id, product_id=101,
        quantity=50, unit_price=250, amount=12500,
    ))

    # 2) Accumulator — post inventory movement
    accum = AccumulatorClient(session)  # pass session, not engine!
    accum.use(inventory).post({
        "recorder": f"order:{order.id}",
        "period": "2026-04-19",
        "warehouse": 1, "product": 101,
        "quantity": -50, "amount": -12500,
    })

    # 3) Everything commits atomically
    session.commit()
    # If anything fails — both ORM and accumulator roll back!
```

### Typed Error Handling

```python
from sqlalchemy_accumulator import (
    AccumulatorError,
    RegisterNotFoundError,
    ValidationError,
)

try:
    handle.post({"recorder": "", "period": "2026-01-01"})
except ValidationError as e:
    print(f"Bad input: {e}")  # catches missing dimensions, invalid data

try:
    accum.register_info("nonexistent")
except RegisterNotFoundError as e:
    print(f"Register not found: {e.register_name}")
```

## Project Structure

```
demo/sqlalchemy/
├── app.py               — Flask app with ORM models + sqlalchemy-accumulator
├── templates/
│   └── index.html       — Interactive web UI with 7 tabs
├── init.sql             — ORM tables, seed data, register + movements
├── docker-compose.yml   — PostgreSQL + Flask containers
├── Dockerfile           — App container
├── requirements.txt     — Python dependencies
└── README.md            — This file
```

### ORM Models

| Model | Purpose | Accumulator Link |
|---|---|---|
| `Warehouse` | Storage locations | `warehouse` dimension in inventory register |
| `Product` | Catalog items | `product` dimension in inventory register |
| `Client` | Customers | Referenced by orders |
| `Order` | Sales orders | `recorder` = `order:{id}` |
| `OrderLine` | Order details | Drives `quantity` and `amount` in movements |

## Shutdown

```bash
docker compose down -v
```

## Learn More

- [pg_accumulator documentation](../../docs/README.md)
- [sqlalchemy-accumulator package](../../packages/sqlalchemy-accumulator/)
- [Other demos](../) — Python (raw psycopg2), TypeScript, Prisma
