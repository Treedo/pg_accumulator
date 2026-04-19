# prisma-accumulator

**Type-safe Prisma ORM adapter for [pg_accumulator](../../README.MD)** — accumulation registers (balance & turnover) in your Prisma project.

```
npm install prisma-accumulator
```

> **Peer dependency:** `@prisma/client >= 5.0.0`

---

## Quick Start

### 1. Define a register

```typescript
import { defineRegister } from 'prisma-accumulator';

const inventory = defineRegister({
  name: 'inventory',
  kind: 'balance',
  dimensions: {
    warehouse: 'int',
    product: 'int',
    lot: 'text',
  },
  resources: {
    quantity: 'numeric',
    amount: 'numeric',
  },
});
```

TypeScript infers the exact shape of dimensions and resources — all subsequent calls to `.post()`, `.balance()`, etc. are fully typed.

### 2. Create the client

```typescript
import { PrismaClient } from '@prisma/client';
import { AccumulatorClient } from 'prisma-accumulator';

const prisma = new PrismaClient();
const accum = new AccumulatorClient(prisma);
// or with custom schema:
// const accum = new AccumulatorClient(prisma, { schema: 'my_schema' });
```

### 3. Post movements

```typescript
// Single movement
await accum.use(inventory).post({
  recorder: 'purchase:7001',
  period: '2026-04-19',
  warehouse: 1,
  product: 42,
  lot: 'LOT-A',
  quantity: 100,
  amount: 5000,
});

// Batch — array of movements
await accum.use(inventory).post([
  { recorder: 'purchase:7001', period: '2026-04-19', warehouse: 1, product: 42, quantity: 50, amount: 2500 },
  { recorder: 'purchase:7001', period: '2026-04-19', warehouse: 1, product: 43, quantity: 200, amount: 8000 },
]);
```

### 4. Read balance

```typescript
// Current balance
const bal = await accum.use(inventory).balance({
  warehouse: 1,
  product: 42,
});
// => { quantity: 100, amount: 5000 }

// Historical balance (at a specific date)
const balHist = await accum.use(inventory).balance(
  { warehouse: 1 },
  { atDate: '2026-01-01' },
);
```

### 5. Query turnover

```typescript
const turn = await accum.use(inventory).turnover(
  { warehouse: 1 },
  {
    dateFrom: '2026-01-01',
    dateTo: '2026-03-31',
    groupBy: ['product'],
  },
);
// => [{ product: 42, quantity: 300, amount: 15000 }, ...]
```

### 6. Query movements

```typescript
const moves = await accum.use(inventory).movements(
  { warehouse: 1, product: 42 },
  { limit: 50 },
);
```

### 7. Unpost / Repost

```typescript
// Cancel all movements by recorder
await accum.use(inventory).unpost('purchase:7001');

// Atomic replace — unpost old + post new
await accum.use(inventory).repost('purchase:7001', [
  { recorder: 'purchase:7001', period: '2026-04-19', warehouse: 1, product: 42, quantity: 120, amount: 6000 },
]);
```

---

## Transactions

`prisma-accumulator` works inside Prisma transactions via `.withTransaction()`:

```typescript
await prisma.$transaction(async (tx) => {
  // Regular Prisma operations
  const order = await tx.order.create({
    data: { customerId: 1, total: 250 },
  });

  // pg_accumulator operations in the same transaction
  const txAccum = accum.withTransaction(tx);
  await txAccum.use(inventory).post({
    recorder: `order:${order.id}`,
    period: new Date(),
    warehouse: order.warehouseId,
    product: order.productId,
    quantity: -order.quantity,
    amount: -order.total,
  });
});
```

---

## DDL — Register Management

### Create a register

```typescript
await accum.createRegister(inventory);
```

This calls `accum.register_create()` with all parameters from your `defineRegister()` definition.

### Alter a register

```typescript
await accum.alterRegister('inventory', {
  addDimensions: { color: 'text' },
  addResources: { weight: 'numeric' },
});
```

### Drop a register

```typescript
await accum.dropRegister('inventory');
// or force-drop even if movements exist:
await accum.dropRegister('inventory', true);
```

### List & inspect registers

```typescript
const registers = await accum.listRegisters();
// => [{ name: 'inventory', kind: 'balance', dimensions: 3, resources: 2, ... }]

const info = await accum.registerInfo('inventory');
// => { name, kind, dimensions, resources, tables, partitions, ... }
```

---

## Register Definition Options

```typescript
defineRegister({
  name: 'sales',               // Register name (required)
  kind: 'turnover',            // 'balance' | 'turnover' (required)
  dimensions: {                // Grouping columns (required)
    customer: 'int',
    product: 'int',
  },
  resources: {                 // Numeric accumulable columns (required)
    quantity: 'numeric',
    revenue: 'numeric',
  },
  // Optional:
  totals_period: 'day',        // 'day' | 'month' | 'year' — aggregation level
  partition_by: 'month',       // 'day' | 'month' | 'quarter' | 'year' — table partitioning
  high_write: false,           // Enable delta buffer for high-throughput writes
  recorder_type: 'text',       // PostgreSQL type for recorder column
});
```

---

## Type Mapping

| PostgreSQL | TypeScript | Runtime |
|---|---|---|
| `int` / `integer` | `number` | `number` |
| `bigint` | `bigint \| number` | `BigInt` or `number` |
| `numeric` / `decimal` | `number \| string` | `number` |
| `text` / `varchar` | `string` | `string` |
| `boolean` | `boolean` | `boolean` |
| `date` / `timestamptz` | `string \| Date` | `Date` |
| `uuid` | `string` | `string` |

---

## Error Handling

The package provides typed error classes mapped from PostgreSQL exceptions:

```typescript
import {
  AccumulatorError,
  RegisterNotFoundError,
  RecorderNotFoundError,
  ValidationError,
} from 'prisma-accumulator';

try {
  await accum.use(inventory).balance({ warehouse: 1 });
} catch (err) {
  if (err instanceof RegisterNotFoundError) {
    console.error(`Register "${err.registerName}" does not exist`);
  }
  if (err instanceof ValidationError) {
    console.error(`Validation failed on field "${err.field}": ${err.message}`);
  }
}
```

Validation happens both client-side (before sending to DB) and server-side (PostgreSQL RAISE EXCEPTION is mapped to typed errors).

---

## SQL Injection Prevention

All queries use **parameterized arguments** (`$1`, `$2`, ...) — dimension values, resource values, recorder names, and dates are never interpolated into SQL strings. Register names are validated against a strict whitelist pattern (`[a-zA-Z_][a-zA-Z0-9_]*`).

---

## Prisma Migrate Integration

Generate migration SQL for your registers:

```sql
-- prisma/migrations/XXXX_add_inventory_register/migration.sql

SELECT accum.register_create(
    name          := 'inventory',
    dimensions    := '{"warehouse": "int", "product": "int", "lot": "text"}'::jsonb,
    resources     := '{"quantity": "numeric", "amount": "numeric"}'::jsonb,
    kind          := 'balance',
    totals_period := 'day',
    partition_by  := 'month'
);
```

---

## Architecture

```
┌─────────────────────────────────────────────────┐
│                  Your Application               │
├─────────────────────────────────────────────────┤
│  Prisma Client          │  AccumulatorClient    │
│  (ORM for business      │  (type-safe wrapper   │
│   tables: users,        │   for pg_accumulator) │
│   orders, products)     │                       │
├─────────────┬───────────┼───────────────────────┤
│  @prisma/   │  prisma-accumulator               │
│  client     │  ├─ defineRegister()               │
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

## API Reference

### `defineRegister(definition)`

Creates a typed register handle. Does not communicate with the database.

### `new AccumulatorClient(prisma, config?)`

- `prisma` — `PrismaClient` instance (or compatible `$queryRawUnsafe` interface)
- `config.schema` — PostgreSQL schema name (default: `'accum'`)

### `accum.use(register)`

Returns a `RegisterHandle` with:

| Method | Description |
|---|---|
| `.post(data)` | Post one or more movements. Returns count. |
| `.unpost(recorder)` | Delete all movements by recorder. Returns count. |
| `.repost(recorder, data)` | Atomic unpost + post. Returns count of new movements. |
| `.balance(dims?, options?)` | Query current or historical balance. |
| `.turnover(dims?, options?)` | Query turnover for a period with optional grouping. |
| `.movements(dims?, options?)` | Query movements with filters and pagination. |

### `accum.withTransaction(tx)`

Returns a new `AccumulatorClient` bound to a Prisma transaction client.

### DDL Methods

| Method | Description |
|---|---|
| `accum.createRegister(register)` | Create register infrastructure in PostgreSQL |
| `accum.alterRegister(name, options)` | Add dimensions/resources, toggle high_write |
| `accum.dropRegister(name, force?)` | Drop register (force to ignore existing data) |
| `accum.listRegisters()` | List all registers with summary stats |
| `accum.registerInfo(name)` | Get detailed register info as JSON |

---

## Requirements

- **PostgreSQL** 15+ with `pg_accumulator` extension installed
- **@prisma/client** >= 5.0.0
- **Node.js** >= 18

---

## License

MIT
