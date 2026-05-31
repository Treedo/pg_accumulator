# Prisma + pg_accumulator Demo — Warehouse & General Ledger

A demo application using the **prisma-accumulator** package for type-safe interaction with pg_accumulator accumulation registers via Prisma ORM. It showcases both standard inventory tracking and complex double-entry bookkeeping (General Ledger).

## What It Demonstrates

- **`defineRegister()`** — declaring typed `inventory` (balance) and `general_ledger` (ledger) registers.
- **`AccumulatorClient`** — client initialization integrated seamlessly with Prisma.
- **Inventory Operations** — receiving (`.post()`), shipping, canceling (`.unpost()`), balances (`.balance()`), and turnover logic.
- **Double-Entry Bookkeeping** — debit & credit mechanics, Trial Balance (ОСВ), JSON subconto analytics (`account_dr`, `subconto_dr`, etc.).
- **Ledger Verification** — native PostgreSQL `accum.register_ledger_verify()` soundness checks.
- **System Management** — browsing movement history (`.movements()`) and introspecting system architecture (`.listRegisters()`).

## Getting Started

```bash
cd demo/prisma
docker compose up --build
```

Open in browser: **http://localhost:3303**

## Architecture

```
┌────────────────────────────────────────────────────────┐
│  Browser UI (public/index.html)                        │
│  Tabs: Balances │ Bookkeeping │ Operations │ History   │
└─────────────────┬──────────────────────────────────────┘
                  │ REST API
┌─────────────────▼──────────────────────────────────────┐
│  Express + Prisma + prisma-accumulator                 │
│  src/index.ts — API endpoints (Inventory & Ledger)     │
│  src/registers.ts — defineRegister() definitions       │
└─────────────────┬──────────────────────────────────────┘
                  │ SQL (via Prisma client & extensions)
┌─────────────────▼──────────────────────────────────────┐
│  PostgreSQL + pg_accumulator extension                 │
│  Registers:                                            │
│  1) inventory (balance): warehouse_id, product_id      │
│  2) general_ledger (ledger): double-entry tracking     │
└────────────────────────────────────────────────────────┘
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
│   └── registers.ts        # Register definition (inventory & ledger)
└── public/
    └── index.html          # SPA interface
```

## API Endpoints

| Method | URL | Description |
|--------|-----|-------------|
| GET | `/api/products` | List products |
| GET | `/api/warehouses` | List warehouses |
| GET | `/api/balances` | Balances by warehouse/product |
| GET | `/api/movements` | Last 50 movements across inventory |
| GET | `/api/turnover` | Turnovers (query params: dateFrom, dateTo, warehouse_id) |
| GET | `/api/registers` | List all system registers |
| POST | `/api/receipt` | Receive goods (inventory) |
| POST | `/api/shipment` | Ship goods (inventory) |
| POST | `/api/unpost` | Cancel an operation |
| GET | `/api/ledger/balances` | Trial Balance (Оборотно-сальдова відомість) |
| GET | `/api/ledger/movements`| General Journal of double-entry records |
| GET | `/api/ledger/verify` | Ledger balanced/soundness check |
| POST | `/api/ledger/post` | Post new double-entry journal movement |
| POST | `/api/ledger/unpost` | Cancel/Reverse a journal movement |

## Shutdown

```bash
docker compose down -v
```
