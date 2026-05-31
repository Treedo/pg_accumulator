# pg_accumulator — Python Demo App (Flask)

A simple web application demonstrating the core capabilities of the `pg_accumulator` extension, including a classic two-sided inventory balance register (`balance`) and a full double-entry accounting ledger (`ledger`).

## What this demo shows

### 1. Inventory balance register (`inventory`, type `balance`)
- **Fast current balances** — real-time inventory totals through `inventory_balance_cache` at $O(1)$.
- **Historical snapshots** — get a balance snapshot for any past date via `accum.inventory_balance(dimensions, at_date)`.
- **Posting and unposting** — automatic balance updates using `accum.register_post()` and `accum.register_unpost()`.

### 2. Double-entry accounting ledger (`general_ledger`, type `ledger`)
- **Two-sided journal entries** — classic double-entry bookkeeping with automatic validation. Each transaction includes a debit account (`account_dr`), a credit account (`account_cr`), debit subconto (`subconto_dr`), and credit subconto (`subconto_cr`).
- **Active and passive accounts** — automatic computation of debit and credit turnovers, plus balances according to accounting rules:
  - Asset accounts (`10` Cash, `28` Inventory, `90` Expense): `Balance = Debit - Credit`.
  - Capital and liability accounts (`40` Share Capital, `50` Loans): `Balance = Credit - Debit`.
- **Trial balance view** — a full trial balance table that visualizes the system's asset and equity/liability totals.
- **Global integrity audit** — instant debit-credit equality verification using `accum.register_ledger_verify('general_ledger')`.

---

## Quick start

Run the demo with:

```bash
cd demo/python
docker compose up --build
```

Then open your browser at: **http://localhost:3301**

---

## Project structure

- [app.py](app.py) — Flask backend with routes for inventory and ledger operations.
- [templates/index_i18n.html](templates/index_i18n.html) — interactive frontend for inventory and ledger demos.
- [init.sql](init.sql) — database initialization script: creates `pg_accumulator`, registers both registers, and loads initial seed data.
- [docker-compose.yml](docker-compose.yml) — PostgreSQL and Python app container configuration.

---

## Demo scenario

### Part A. Inventory transactions (balance register)
1. View the initial warehouse balances on the main page (six initial movements are loaded).
2. Post a new movement using the left form: `receipt:5`, Warehouse 1, Product 1, Quantity 50, Amount 25000 USD.
3. Confirm that Warehouse 1's current balance updates immediately.
4. Request a historical balance for `2026-04-05` to inspect inventory state before that movement.

### Part B. Double-entry accounting
The demo starts with the following initial ledger state:
- **10 Cash/Bank** (Asset): Debit 150,000 USD, Credit 32,000 USD. Current balance: **118,000.00 USD (asset)**
- **28 Inventory** (Asset): Debit 30,000 USD, Credit 0. Current balance: **30,000.00 USD (asset)**
- **90 Rent Expense** (Asset/Expense): Debit 2,000 USD, Credit 0. Current balance: **2,000.00 USD (expense)**
- **40 Share Capital** (Liability): Credit 100,000 USD. Current balance: **100,000.00 USD (liability)**
- **50 Short-term Loans** (Liability): Credit 50,000 USD. Current balance: **50,000.00 USD (liability)**

The balance equation holds exactly:
$$\text{Assets} = 118{,}000 \text{ (Cash)} + 30{,}000 \text{ (Inventory)} + 2{,}000 \text{ (Expense)} = 150{,}000\text{ USD}$$
$$\text{Liabilities \& Equity} = 100{,}000 \text{ (Equity)} + 50{,}000 \text{ (Loans)} = 150{,}000\text{ USD}$$

**Try this:**
1. Post a new ledger entry:
   - Document: `purchase:2`
   - Debit account: `28` (Inventory), Debit subconto: `{"item_id": 2, "supplier": "Tech Supplies"}`
   - Credit account: `10` (Cash), Credit subconto: `{"bank": "Main Bank"}`
   - Amount: `5000.00` USD, Currency: `USD`.
2. Watch how the ledger values flow into the trial balance table in real time.
3. The green top badge confirms that the system audit passed and Debit ≡ Credit.

---

## Cleanup

```bash
docker compose down -v
```
