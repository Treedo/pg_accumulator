# pg_accumulator — Python Demo

A simple web application demonstrating the core capabilities of the `pg_accumulator` extension.

## What It Demonstrates

- **Register creation** — `register_create()` (runs automatically on startup)
- **Posting movements** — `register_post()` via a web form
- **Canceling documents** — `register_unpost()`
- **Current balances** — reading from `balance_cache` (O(1))
- **Balance queries** — `inventory_balance()` with filters and historical dates
- **Movement history** — browsing recent operations

## Getting Started

```bash
cd demo/python
docker compose up --build
```

Open in browser: **http://localhost:5001**

## Project Structure

```
demo/python/
  app.py               — Flask backend
  templates/index.html  — Web interface
  init.sql              — Register creation + seed data
  docker-compose.yml    — PostgreSQL with pg_accumulator + Flask app
  Dockerfile            — Flask container
```

## Demo Walkthrough

1. Open http://localhost:5001
2. See initial balances (6 movements are pre-loaded)
3. Post a new receipt: `receipt:5`, warehouse 1, product 1, quantity 50, amount 25000
4. Observe that the balance updates instantly
5. Cancel document `receipt:5` — the balance reverts to its previous state
6. Query balance at date `2026-04-05` — see the warehouse state at that historical point

## Shutdown

```bash
docker compose down -v
```
