# prisma-accumulator

**Prisma ORM adapter for pg_accumulator** — type-safe accumulation registers in your Prisma project.

---

## Problem

Prisma ORM does not natively support:
- PostgreSQL function calls as a first-class API
- Dynamically created tables (`register_create` generates tables at runtime)
- JSON-based function signatures (`register_post(name, jsonb)`)
- Type inference for SQL function results

Developers are forced to write `$queryRaw` / `$executeRaw` manually without autocompletion or type safety.

## Solution

`prisma-accumulator` is an npm package that:
1. **Declaratively describes registers** via TypeScript interfaces
2. **Generates types** for dimensions, resources, movements, balance, and turnover
3. **Provides a type-safe client** for all pg_accumulator operations
4. **Works alongside Prisma** — complements, does not replace

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

## API Design

### 1. Register Definition (Schema Definition)

```typescript
import { defineRegister, BalanceRegister, TurnoverRegister } from 'prisma-accumulator';

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
  totals_period: 'day',
  partition_by: 'month',
});

const sales = defineRegister({
  name: 'sales',
  kind: 'turnover',
  dimensions: {
    customer: 'int',
    product: 'int',
  },
  resources: {
    quantity: 'numeric',
    revenue: 'numeric',
  },
});
```

Types are inferred automatically:
```typescript
// Auto-generated types:
type InventoryDimensions = { warehouse: number; product: number; lot?: string };
type InventoryResources  = { quantity: number; amount: number };
type InventoryMovement   = { recorder: string; period: string | Date }
                         & InventoryDimensions
                         & InventoryResources;
```

### 2. Client (AccumulatorClient)

```typescript
import { PrismaClient } from '@prisma/client';
import { AccumulatorClient } from 'prisma-accumulator';

const prisma = new PrismaClient();
const accum  = new AccumulatorClient(prisma, { schema: 'accum' });

// --- Writing ---

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
  { recorder: 'purchase:7001', period: '2026-04-19', warehouse: 1, product: 42, quantity: 50,  amount: 2500 },
  { recorder: 'purchase:7001', period: '2026-04-19', warehouse: 1, product: 43, quantity: 200, amount: 8000 },
]);

// Cancel
await accum.use(inventory).unpost('purchase:7001');

// Repost
await accum.use(inventory).repost('purchase:7001', [
  { recorder: 'purchase:7001', period: '2026-04-19', warehouse: 1, product: 42, quantity: 120, amount: 6000 },
]);

// --- Reading ---

// Current balance
const bal = await accum.use(inventory).balance({
  warehouse: 1,
  product: 42,
});
// => { quantity: 100, amount: 5000 }

// Historical balance
const balHist = await accum.use(inventory).balance(
  { warehouse: 1 },
  { atDate: '2026-01-01' }
);

// Turnovers for a period
const turn = await accum.use(inventory).turnover(
  { warehouse: 1 },
  { dateFrom: '2026-01-01', dateTo: '2026-03-31' }
);

// Movements
const moves = await accum.use(inventory).movements(
  { warehouse: 1, product: 42 },
  { limit: 50, orderBy: 'period', order: 'desc' }
);
```

### 3. DDL (Administration)

```typescript
// Create register in the database
await accum.createRegister(inventory);

// Alter register
await accum.alterRegister('inventory', {
  addDimensions: { color: 'text' },
  addResources:  { weight: 'numeric' },
});

// Drop
await accum.dropRegister('inventory');

// List
const registers = await accum.listRegisters();

// Info
const info = await accum.registerInfo('inventory');
```

### 4. Transactions (Prisma compatibility)

```typescript
// Works inside Prisma transactions
await prisma.$transaction(async (tx) => {
  // Prisma operations
  const order = await tx.order.create({ data: { ... } });

  // pg_accumulator via the same transaction
  const txAccum = accum.withTransaction(tx);
  await txAccum.use(inventory).post({
    recorder: `order:${order.id}`,
    period: new Date().toISOString(),
    warehouse: order.warehouseId,
    product: order.productId,
    quantity: -order.quantity,
    amount: -order.total,
  });
});
```

### 5. Migrations (Prisma Migrate compatibility)

```sql
-- prisma/migrations/XXXX_add_inventory_register/migration.sql
-- Generated by CLI: npx prisma-accumulator generate-migration

SELECT accum.register_create(
    name       := 'inventory',
    dimensions := '{"warehouse": "int", "product": "int", "lot": "text"}'::jsonb,
    resources  := '{"quantity": "numeric", "amount": "numeric"}'::jsonb,
    kind       := 'balance',
    totals_period := 'day',
    partition_by  := 'month'
);
```

---

## PostgreSQL → TypeScript Type Mapping

| PostgreSQL type | TypeScript type | JS runtime |
|---|---|---|
| `int` / `integer` | `number` | `number` |
| `bigint` | `bigint \| number` | `BigInt` or `number` |
| `numeric` / `decimal` | `number \| string` | `Prisma.Decimal` or `number` |
| `text` / `varchar` | `string` | `string` |
| `boolean` | `boolean` | `boolean` |
| `date` / `timestamptz` | `string \| Date` | `Date` |
| `uuid` | `string` | `string` |

---

## Development Phases

### Phase 1 — Core Client (MVP)
**Goal:** Minimum viable package — type-safe wrapper over SQL functions.

- [ ] Initialize npm package (`prisma-accumulator`)
- [ ] `defineRegister()` with TypeScript generic types
- [ ] `AccumulatorClient` with `PrismaClient` support
- [ ] Type-safe `.post()` (single + batch)
- [ ] Type-safe `.unpost()`
- [ ] Type-safe `.repost()`
- [ ] Type-safe `.balance()` (current + historical)
- [ ] Type-safe `.turnover()` (with period filters)
- [ ] Type-safe `.movements()` (with pagination)
- [ ] SQL injection prevention (parameterized queries)
- [ ] Unit tests (`vitest`)
- [ ] README with Quick Start

**Result:** `npm install prisma-accumulator` → working type-safe client.

---

### Phase 2 — Transaction Support + Error Handling
**Goal:** Full Prisma transaction support and clear error messages.

- [ ] `.withTransaction(tx)` — compatibility with `prisma.$transaction()`
- [ ] Custom error classes: `RegisterNotFoundError`, `RecorderNotFoundError`, `ValidationError`
- [ ] Map PostgreSQL `RAISE EXCEPTION` → typed errors
- [ ] Retry logic for deadlock/serialization (optional)
- [ ] Client-side dimensions/resources validation (before sending to DB)
- [ ] Integration tests with real PostgreSQL + pg_accumulator

**Result:** Production-ready client with transactions and clear errors.

---

### Phase 3 — DDL + Migration CLI
**Goal:** Automate register create/alter and integrate with Prisma Migrate.

- [ ] `accum.createRegister(definition)` — DDL via client
- [ ] `accum.alterRegister()` — add dimensions/resources
- [ ] `accum.dropRegister()`
- [ ] `accum.listRegisters()` / `accum.registerInfo()`
- [ ] CLI: `npx prisma-accumulator generate-migration`
  - Reads `defineRegister()` from code
  - Compares with current DB state (`register_info`)
  - Generates `.sql` file for `prisma/migrations/`
- [ ] CLI: `npx prisma-accumulator introspect`
  - Reads registers from DB
  - Generates TypeScript definitions

**Result:** Full register lifecycle via CLI + migrations.

---

### Phase 4 — Prisma Generator (Code Generation)
**Goal:** Auto-generate types from DB, similar to how Prisma generates model types.

- [ ] Prisma Generator plugin (`prisma-accumulator-generator`)
- [ ] Register introspection at `npx prisma generate`
- [ ] Generate `.d.ts` files with exact types
- [ ] Generate client instances per register
- [ ] Support `prisma.schema` annotations:
  ```prisma
  generator accumulator {
    provider = "prisma-accumulator-generator"
    schema   = "accum"
  }
  ```
- [ ] Hot-reload types on DB changes

**Result:** `npx prisma generate` → automatic register types.

---

### Phase 5 — Advanced Features
**Goal:** High-write mode, maintenance, monitoring.

- [ ] High-write mode support in client
- [ ] `accum.maintenance.flush()` — manual delta buffer flush
- [ ] `accum.maintenance.rebuildTotals(register)`
- [ ] `accum.maintenance.rebuildCache(register)`
- [ ] `accum.diagnostics(register)` — register health
- [ ] Event hooks: `onPost`, `onUnpost`, `onBalanceRead`
- [ ] Connection pooling awareness (PgBouncer support)
- [ ] Logging/tracing integration (OpenTelemetry)

**Result:** Enterprise-ready package with full lifecycle.

---

### Phase 6 — Ecosystem & DX
**Goal:** Developer Experience and ecosystem.

- [ ] VS Code extension: autocomplete for register names
- [ ] Zod integration for API-level validation
- [ ] tRPC adapter
- [ ] Next.js / Remix examples
- [ ] Documentation (docusaurus / nextra)
- [ ] GitHub Actions CI/CD pipeline
- [ ] Benchmarks: prisma-accumulator vs raw SQL overhead
- [ ] Changelog and semantic versioning

**Result:** Complete open-source project with ecosystem.

---

## Package Structure

```
prisma-accumulator/
├── package.json
├── tsconfig.json
├── vitest.config.ts
├── README.md
├── CHANGELOG.md
├── src/
│   ├── index.ts                 # Public API exports
│   ├── client.ts                # AccumulatorClient
│   ├── register.ts              # defineRegister() + type inference
│   ├── operations/
│   │   ├── post.ts              # post / batch post
│   │   ├── unpost.ts            # unpost
│   │   ├── repost.ts            # repost
│   │   ├── balance.ts           # balance query
│   │   ├── turnover.ts          # turnover query
│   │   └── movements.ts         # movements query
│   ├── ddl/
│   │   ├── create.ts            # register_create
│   │   ├── alter.ts             # register_alter
│   │   ├── drop.ts              # register_drop
│   │   └── introspect.ts        # register_list / register_info
│   ├── types/
│   │   ├── pg-types.ts          # PostgreSQL → TypeScript type mapping
│   │   ├── register.ts          # Register definition types
│   │   └── results.ts           # Query result types
│   ├── errors/
│   │   └── index.ts             # Custom error classes
│   ├── sql/
│   │   └── builder.ts           # Safe SQL query builder
│   └── utils/
│       ├── validation.ts        # Client-side validation
│       └── json.ts              # JSON serialization helpers
├── cli/
│   ├── index.ts                 # CLI entry point
│   ├── generate-migration.ts
│   └── introspect.ts
├── generator/                   # Prisma Generator (Phase 4)
│   ├── index.ts
│   └── templates/
├── test/
│   ├── unit/
│   │   ├── register.test.ts
│   │   ├── post.test.ts
│   │   ├── balance.test.ts
│   │   └── sql-builder.test.ts
│   └── integration/
│       ├── docker-compose.yml
│       ├── setup.sql
│       └── e2e.test.ts
└── examples/
    ├── basic/
    ├── with-transactions/
    └── nextjs-app/
```

---

## Peer Dependencies

```json
{
  "name": "prisma-accumulator",
  "version": "0.1.0",
  "peerDependencies": {
    "@prisma/client": ">=5.0.0"
  },
  "devDependencies": {
    "vitest": "^1.0.0",
    "typescript": "^5.3.0",
    "@prisma/client": "^5.0.0",
    "prisma": "^5.0.0"
  }
}
```

---

## License

MIT — compatible with Prisma and pg_accumulator.
