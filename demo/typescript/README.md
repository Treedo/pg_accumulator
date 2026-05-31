# pg_accumulator — TypeScript Demo

A demonstration of the `pg_accumulator` extension in the form of a simple **financial tracker** and a **Double-Entry General Ledger**.

## What It Demonstrates

### 1. Simple Financial Tracker (Balance-type Register)
Tracks personal account balances categorized by expenses and income.

| Feature | Description |
|---|---|
| Current balance | Cards showing the sum per account (`finance_balance_cache`) |
| New transaction | `accum.register_post()` — post a movement |
| Cancellation | `accum.register_unpost()` — cancel/delete a movement (reversion) |
| Balance at date | `accum.finance_balance()` — exact state at any date |
| Movement list | Table of the last 50 transactions |

### 2. General Ledger Bookkeeping (Ledger-type Register)
Demonstrates a multi-dimensional Double-Entry Ledger which records transaction movements that simultaneously affect debit and credit accounts.

| Feature | Description |
|---|---|
| Double-entry posting | One-row transaction posting using dynamic JSON subconto (debit-credit split logic) |
| Reverse posting (Storno) | Cancel / reverse previous transaction by its recorder ID with `accum.register_unpost()` |
| Soundness verification | Continuous integrity auditing of Debit ≡ Credit equality in real-time with `accum.register_ledger_verify()` |
| Trial Balance (OSV) | Displays Debit/Credit turnovers and end balances dynamically formatted by account type (Active or Passive) |
| General Journal | Lists recent double-entry ledger transactions |

## Stack

- **Node.js 20** + **Express 4** (TypeScript)
- **pg** — PostgreSQL driver
- Static `index.html` with Vanilla JS (no frameworks)

## Getting Started

```bash
cd demo/typescript
docker compose up --build
```

Open in browser: **http://localhost:3302**

> Database data is stored in the `pgdata_ts` volume. For a clean start:
> ```bash
> docker compose down -v && docker compose up --build
> ```

## Ports

| Service | Port |
|---|---|
| Web app | `3302` |
| PostgreSQL | `5435` |

## Shutdown

```bash
docker compose down -v
```
