# pg_accumulator — TypeScript Demo

A demonstration of the `pg_accumulator` extension in the form of a simple **financial tracker**.

## What It Demonstrates

| Feature | Description |
|---|---|
| Current balance | Cards showing the sum per account (`balance_cache`) |
| New transaction | `accum.register_post()` — post a movement |
| Cancellation | `accum.register_unpost()` — cancel/delete a movement |
| Balance at date | `accum.finance_balance()` — exact state at any date |
| Movement list | Table of the last 50 transactions |

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
