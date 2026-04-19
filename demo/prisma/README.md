# Prisma + pg_accumulator Demo — Warehouse Inventory

A demo warehouse application using the **prisma-accumulator** package for type-safe interaction with pg_accumulator accumulation registers via Prisma ORM.

## What It Demonstrates

- `defineRegister()` — declaring a typed `inventory` register
- `AccumulatorClient` — client initialization with Prisma
- `.post()` — receiving and shipping goods
- `.unpost()` — canceling operations
- `.balance()` — querying current balances
- `.movements()` — browsing movement history
- `.listRegisters()` — listing registered registers

## Getting Started

```bash
cd demo/prisma
docker compose up --build
```

Open in browser: **http://localhost:3303**

## Architecture

```
┌─────────────────────────────────────────────┐
│  Browser UI (public/index.html)             │
│  Tabs: Balances │ Operations │ History │ Regs│
└─────────────────┬───────────────────────────┘
                  │ REST API
┌─────────────────▼───────────────────────────┐
│  Express + Prisma + prisma-accumulator      │
│  src/index.ts — API endpoints               │
│  src/registers.ts — defineRegister()        │
└─────────────────┬───────────────────────────┘
                  │ SQL (via Prisma $queryRawUnsafe)
┌─────────────────▼───────────────────────────┐
│  PostgreSQL + pg_accumulator extension      │
│  Register: inventory (balance)              │
│  Dimensions: warehouse_id, product_id       │
│  Resources: quantity, cost                  │
└─────────────────────────────────────────────┘
```

## Project Structure

```
demo/prisma/
├── docker-compose.yml      # Services: postgres + app
├── Dockerfile              # Multi-stage build
├── init.sql                # Seed: extension + register + data
├── package.json
├── tsconfig.json
├── prisma/
│   └── schema.prisma       # Models: Product, Warehouse
├── src/
│   ├── index.ts            # Express API server
│   └── registers.ts        # Register definition (inventory)
└── public/
    └── index.html          # SPA interface
```

## API Endpoints

| Method | URL | Description |
|--------|-----|-------------|
| GET | `/api/products` | List products |
| GET | `/api/warehouses` | List warehouses |
| GET | `/api/balances` | Balances by warehouse/product |
| GET | `/api/movements` | Last 50 movements |
| GET | `/api/turnover` | Turnovers (query params: dateFrom, dateTo, warehouse_id) |
| GET | `/api/registers` | List registers |
| POST | `/api/receipt` | Receive goods |
| POST | `/api/shipment` | Ship goods |
| POST | `/api/unpost` | Cancel an operation |

## Shutdown

```bash
docker compose down -v
```
