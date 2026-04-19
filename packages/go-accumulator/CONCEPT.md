# go-accumulator

**Go client for pg_accumulator** — high-performance, type-safe adapter for accumulation registers.

Designed to work seamlessly with `sqlc`, `pgx/v5`, and any Go ORM (GORM, ent, sqlx).

---

## Проблема

`pg_accumulator` для Go потребує:
- Виклик SQL-функцій (`register_post`, `register_unpost`) через parametrized queries
- Серіалізацію `map[string]any` → JSONB без SQL injection
- Type-safe читання результатів `balance()` / `turnover()` у Go-структури
- Сумісності з транзакційними інтерфейсами (`pgx.Tx`, `sql.Tx`, `gorm.DB`)

Hand-written raw queries — повторюваний, ненадійний код без типів і централізованої обробки помилок.

---

## Рішення

`go-accumulator` — легкий Go-пакет, який:

1. **Надає єдиний інтерфейс** для всіх операцій pg_accumulator
2. **Підтримує генерацію коду через `sqlc`** — SQL-файли включені у пакет
3. **Сумісний з будь-яким PostgreSQL-драйвером** через `DBTX` інтерфейс
4. **Не нав'язує ORM** — workс equally well з pgx, database/sql, GORM

---

## Архітектура

```
┌─────────────────────────────────────────────────────────────────┐
│                        Your Application                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   GORM / ent / sqlx        go-accumulator                      │
│   (бізнес-таблиці:         ├── Register[D, R]                  │
│    users, orders)          │    ├── Post(ctx, movement)        │
│                            │    ├── Unpost(ctx, recorder)      │
│                            │    ├── Repost(ctx, ...)           │
│                            │    ├── Balance(ctx, dims)         │
│                            │    ├── Turnover(ctx, dims, ...)   │
│                            │    └── Movements(ctx, ...)        │
│                            │                                   │
│                            ├── AccumulatorClient               │
│                            │    ├── Use[D,R](def)              │
│                            │    └── WithTx(tx)                 │
│                            │                                   │
│                            └── sqlc queries (generated)        │
│                                 ├── PostMovement()             │
│                                 ├── UnpostMovement()           │
│                                 ├── GetBalance()               │
│                                 └── GetTurnover()              │
├─────────────────────────────────────────────────────────────────┤
│                   DBTX interface                                │
│   pgx.Pool │ pgx.Tx │ *sql.DB │ *sql.Tx │ GORM tx             │
├─────────────────────────────────────────────────────────────────┤
│              PostgreSQL + pg_accumulator                        │
│  ┌───────────┐  ┌──────────┐  ┌──────────────┐                │
│  │ movements │  │  totals  │  │balance_cache │                │
│  └───────────┘  └──────────┘  └──────────────┘                │
└─────────────────────────────────────────────────────────────────┘
```

---

## Ключовий принцип: `DBTX` Interface

Вся логіка будується на одному інтерфейсі — пакет не залежить від конкретного драйвера:

```go
// internal/db/db.go — генерується sqlc, адаптується вручну
type DBTX interface {
    ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error)
    QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error)
    QueryRowContext(ctx context.Context, q string, args ...any) *sql.Row
}

// Підтримуються:
// *sql.DB, *sql.Tx          — database/sql
// *pgxpool.Pool, pgx.Tx    — pgx/v5 через pgxpool
// *gorm.DB (через db.DB()) — GORM
// *sqlx.DB, *sqlx.Tx       — sqlx
```

Завдяки цьому один і той самий `AccumulatorClient` працює у будь-якому контексті.

---

## API Design

### 1. Визначення реєстру

```go
import "github.com/pg-accumulator/go-accumulator/v2"

// RegisterDef описує схему реєстру в коді Go.
// Відповідає параметрам register_create().
var Inventory = accum.RegisterDef{
    Name:         "inventory",
    Kind:         accum.Balance,
    TotalsPeriod: accum.Day,
    PartitionBy:  accum.Month,
    HighWrite:    false,
    Dimensions: accum.Dims{
        "warehouse": accum.Int,
        "product":   accum.Int,
        "lot":       accum.Text,
    },
    Resources: accum.Res{
        "quantity": accum.Numeric,
        "amount":   accum.Numeric,
    },
}

// For kind=turnover:
var Sales = accum.RegisterDef{
    Name: "sales",
    Kind: accum.Turnover,
    Dimensions: accum.Dims{
        "customer": accum.Int,
        "product":  accum.Int,
    },
    Resources: accum.Res{
        "quantity": accum.Numeric,
        "revenue":  accum.Numeric,
    },
}
```

### 2. Typed-structs для конкретного реєстру

```go
// Визначаються у вашому проекті (або генеруються CLI):
type InventoryDims struct {
    Warehouse int  `json:"warehouse"`
    Product   int  `json:"product"`
    Lot       *string `json:"lot,omitempty"`
}

type InventoryResources struct {
    Quantity decimal.Decimal `json:"quantity"`
    Amount   decimal.Decimal `json:"amount"`
}

type InventoryMovement struct {
    Recorder string    `json:"recorder"`
    Period   time.Time `json:"period"`
    InventoryDims
    InventoryResources
}
```

### 3. AccumulatorClient

```go
import (
    "context"
    accum "github.com/pg-accumulator/go-accumulator/v2"
)

// Ініціалізація
pool, _ := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
client    := accum.New(pool, accum.Options{Schema: "accum"})

// Отримати клієнт для конкретного реєстру (generics)
inventory := accum.Use[InventoryDims, InventoryResources](client, Inventory)
```

### 4. Write API

```go
ctx := context.Background()

// --- POST — один рух ---
count, err := inventory.Post(ctx, InventoryMovement{
    Recorder: "purchase:7001",
    Period:   time.Now(),
    InventoryDims{Warehouse: 1, Product: 42, Lot: ptr("LOT-A")},
    InventoryResources{Quantity: d("100"), Amount: d("5000.00")},
})

// --- POST BATCH — масив рухів (один SQL запит) ---
count, err := inventory.PostBatch(ctx, []InventoryMovement{
    {Recorder: "purchase:7001", Period: time.Now(), ...},
    {Recorder: "purchase:7001", Period: time.Now(), ...},
})

// --- UNPOST ---
err := inventory.Unpost(ctx, "purchase:7001")

// --- REPOST ---
err := inventory.Repost(ctx, "purchase:7001", []InventoryMovement{
    {Recorder: "purchase:7001", Period: time.Now(), ...},
})
```

### 5. Read API

```go
// --- BALANCE (поточний) ---
bal, err := inventory.Balance(ctx, InventoryDims{
    Warehouse: 1,
    Product:   42,
})
// bal.Quantity = 100, bal.Amount = 5000.00

// --- BALANCE (на дату) ---
atDate := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
bal, err := inventory.BalanceAt(ctx, InventoryDims{Warehouse: 1}, atDate)

// --- BALANCE (частковий filter — всі продукти на складі 1) ---
// передати тільки warehouse, product = nil
bal, err := inventory.Balance(ctx, InventoryDims{Warehouse: 1})

// --- TURNOVER ---
turn, err := inventory.Turnover(ctx,
    InventoryDims{Warehouse: 1},
    accum.TurnoverOptions{
        DateFrom: ptr(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)),
        DateTo:   ptr(time.Date(2026, 3, 31, 23, 59, 59, 0, time.UTC)),
    },
)
// turn.Quantity, turn.Amount — агрегат за квартал

// --- MOVEMENTS ---
moves, err := inventory.Movements(ctx,
    InventoryDims{Warehouse: 1, Product: 42},
    accum.MovementsOptions{
        DateFrom: ptr(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)),
        Limit:    50,
        Offset:   0,
        OrderBy:  accum.OrderByPeriod,
        Order:    accum.OrderDesc,
    },
)
```

### 6. DDL Management

```go
// Створення реєстру в БД
err := client.CreateRegister(ctx, Inventory)

// Зміна реєстру
err := client.AlterRegister(ctx, "inventory", accum.AlterOptions{
    AddDimensions: accum.Dims{"color": accum.Text},
    AddResources:  accum.Res{"weight": accum.Numeric},
})

// Видалення
err := client.DropRegister(ctx, "inventory")

// Список реєстрів
registers, err := client.ListRegisters(ctx)

// Детальна інформація
info, err := client.RegisterInfo(ctx, "inventory")
```

### 7. Transactions — сумісність з усіма ORM

```go
// --- З database/sql ---
tx, _ := db.BeginTx(ctx, nil)
txClient := client.WithTx(tx)
txInv := accum.Use[InventoryDims, InventoryResources](txClient, Inventory)
err = txInv.Post(ctx, ...)
tx.Commit()

// --- З pgx ---
tx, _ := pool.Begin(ctx)
txClient := client.WithTx(pgxstdlib.OpenDBFromPool(tx))
// або використати pgx-adapter
txClient := client.WithPgxTx(tx)
err = accum.Use[InventoryDims, InventoryResources](txClient, Inventory).Post(ctx, ...)
tx.Commit(ctx)

// --- З GORM ---
db.Transaction(func(tx *gorm.DB) error {
    sqlDB, _ := tx.DB()
    txAccum := client.WithTx(sqlDB)
    txInv := accum.Use[InventoryDims, InventoryResources](txAccum, Inventory)
    return txInv.Post(ctx, InventoryMovement{...})
})
```

---

## sqlc Integration

`go-accumulator` надає готові `.sql` файли для `sqlc` генерації.

### sqlc.yaml

```yaml
# sqlc.yaml
version: "2"
sql:
  - engine: "postgresql"
    queries:
      - "queries/app.sql"
      - "${GOPATH}/pkg/mod/github.com/pg-accumulator/go-accumulator/v2@latest/sqlc/queries.sql"
    schema:
      - "schema/"
    gen:
      go:
        out: "internal/db"
        package: "db"
        sql_package: "pgx/v5"
        emit_json_tags: true
        emit_interface: true
        emit_exact_table_names: false
```

### Вбудовані sqlc queries (`sqlc/queries.sql`)

Пакет включає готові SQL-шаблони з іменованими параметрами:

```sql
-- sqlc/queries.sql

-- name: RegisterPost :one
-- Записати один або батч рухів у реєстр
SELECT accum.register_post(@register::text, @data::jsonb) AS count;

-- name: RegisterUnpost :exec
-- Скасувати рухи за recorder
SELECT accum.register_unpost(@register::text, @recorder::text);

-- name: RegisterRepost :exec
-- Перепровести рухи
SELECT accum.register_repost(@register::text, @recorder::text, @data::jsonb);

-- name: RegisterBalance :one
-- Поточний баланс (jsonb — серіалізований результат)
SELECT row_to_json(b.*)::jsonb AS result
FROM accum.register_balance_internal(@register::text, @dims::jsonb, NULL) b;

-- name: RegisterBalanceAt :one  
-- Баланс на дату
SELECT row_to_json(b.*)::jsonb AS result
FROM accum.register_balance_internal(@register::text, @dims::jsonb, @at_date::timestamptz) b;

-- name: RegisterTurnover :one
-- Обороти за період
SELECT row_to_json(t.*)::jsonb AS result
FROM accum.register_turnover_internal(
    @register::text, @dims::jsonb,
    @date_from::timestamptz, @date_to::timestamptz
) t;

-- name: RegisterMovements :many
-- Журнал рухів
SELECT row_to_json(m.*)::jsonb AS movement
FROM accum.register_movements_internal(
    @register::text, @dims::jsonb,
    @date_from::timestamptz, @date_to::timestamptz,
    @lim::int, @off::int
) m;

-- name: RegisterCreate :exec
SELECT accum.register_create(
    @name::text,
    @dimensions::jsonb,
    @resources::jsonb,
    @kind::text,
    @totals_period::text,
    @partition_by::text,
    @high_write::boolean,
    @recorder_type::text
);

-- name: RegisterAlter :exec
SELECT accum.register_alter(
    @name::text,
    @add_dimensions::jsonb,
    @add_resources::jsonb,
    @high_write::boolean
);

-- name: RegisterDrop :exec
SELECT accum.register_drop(@name::text);

-- name: RegisterList :many
SELECT * FROM accum.register_list();

-- name: RegisterInfo :one
SELECT * FROM accum.register_info(@name::text);
```

---

## Обробка помилок

Кастомні типи помилок з чітким маппінгом PostgreSQL RAISE EXCEPTION:

```go
// errors.go
var (
    ErrRegisterNotFound  = &AccumError{Code: "register_not_found"}
    ErrRecorderNotFound  = &AccumError{Code: "recorder_not_found"}
    ErrRegisterExists    = &AccumError{Code: "register_exists"}
    ErrDimensionRequired = &AccumError{Code: "dimension_required"}
    ErrInvalidKind       = &AccumError{Code: "invalid_kind"}
)

type AccumError struct {
    Code    string
    Message string
    Detail  string
    Cause   error
}

func (e *AccumError) Error() string { return e.Message }
func (e *AccumError) Unwrap() error { return e.Cause }

// Використання у коді:
err := inventory.Post(ctx, ...)
var accumErr *accum.AccumError
if errors.As(err, &accumErr) {
    switch accumErr.Code {
    case "register_not_found":
        // ...
    case "dimension_required":
        // ...
    }
}
```

Зсередини — автоматичний парсинг PostgreSQL `pgerrcode`:

```go
// internal/errors/parse.go
func parsePostgresError(err error) error {
    var pgErr *pgconn.PgError
    if !errors.As(err, &pgErr) {
        return err
    }
    switch {
    case strings.Contains(pgErr.Message, "does not exist"):
        return &AccumError{Code: "register_not_found", Message: pgErr.Message, Cause: err}
    case strings.Contains(pgErr.Message, "recorder"):
        return &AccumError{Code: "recorder_not_found", Message: pgErr.Message, Cause: err}
    default:
        return &AccumError{Code: "pg_error", Message: pgErr.Message, Cause: err}
    }
}
```

---

## Performance Design

### 1. `PostBatch` — один SQL, масив рухів

`register_post` вже підтримує `jsonb[]`. `go-accumulator` серіалізує весь batch в один JSON-масив → один round-trip:

```go
// Один запит замість N:
SELECT accum.register_post('inventory', $1::jsonb)
-- $1 = '[{"recorder":"...","period":"...","warehouse":1,...}, ...]'
```

### 2. Prepared Statements через pgx

При використанні `pgx/v5` — автоматичне підготовлення запитів:

```go
opts := accum.Options{
    Schema:           "accum",
    PrepareStatements: true,   // кешує prepared statements
}
client := accum.New(pool, opts)
```

### 3. Connection Pool awareness

```go
// PgBouncer transaction-mode compatible
opts := accum.Options{
    Schema:            "accum",
    PrepareStatements: false,  // вимкнути для PgBouncer transaction mode
    StatementCacheSize: 0,
}
```

### 4. Decimal без float64

`pgtype.Numeric` зберігає точність:

```go
type InventoryResources struct {
    Quantity pgtype.Numeric `json:"quantity"` // або shopspring/decimal
    Amount   pgtype.Numeric `json:"amount"`
}
```

---

## ORM Compatibility

### GORM

```go
// gorm_adapter.go
func GORMAdapter(db *gorm.DB) DBTX {
    sqlDB, err := db.DB()
    if err != nil {
        panic(err)
    }
    return sqlDB // *sql.DB реалізує DBTX
}

// Транзакція з GORM
db.Transaction(func(tx *gorm.DB) error {
    txDB, _ := tx.DB()
    txAccum := client.WithTx(txDB)
    inv := accum.Use[InventoryDims, InventoryResources](txAccum, Inventory)
    
    // Разом з GORM операціями
    if err := tx.Create(&order).Error; err != nil {
        return err
    }
    _, err := inv.Post(ctx, InventoryMovement{
        Recorder: fmt.Sprintf("order:%d", order.ID),
        ...
    })
    return err
})
```

### sqlx

```go
// sqlx_adapter.go — без змін, sqlx.DB реалізує database/sql інтерфейс
client := accum.New(sqlxDB, opts)

// Транзакція
tx := sqlxDB.MustBeginTx(ctx, nil)
txAccum := client.WithTx(tx)
```

### ent

```go
// ent_adapter.go
entClient.Use(func(next ent.Mutator) ent.Mutator {
    return ent.MutateFunc(func(ctx context.Context, m ent.Mutation) (ent.Value, error) {
        v, err := next.Mutate(ctx, m)
        // пост-хук для списання зі складу
        if m.Type() == "Order" && m.Op().Is(ent.OpCreate) {
            // ...
        }
        return v, err
    })
})
```

---

## CLI: `accumulatorctl`

Окремий CLI для управління реєстрами у CI/CD і міграціях:

```bash
# Встановлення
go install github.com/pg-accumulator/go-accumulator/v2/cmd/accumulatorctl@latest

# Генерація Go-коду з визначень реєстрів
accumulatorctl generate --def registers.go --out internal/accum/

# Інтроспекція БД → Go structs
accumulatorctl introspect \
  --dsn "$DATABASE_URL" \
  --schema accum \
  --out internal/accum/generated.go

# Перевірка розбіжностей між RegisterDef і БД
accumulatorctl diff \
  --dsn "$DATABASE_URL" \
  --def registers.go

# Генерація SQL-міграції (для golang-migrate / goose)
accumulatorctl migrate create \
  --def registers.go \
  --out migrations/

# Застосувати міграції
accumulatorctl migrate up --dsn "$DATABASE_URL"
```

Генерований файл (`internal/accum/generated.go`):
```go
// Code generated by accumulatorctl. DO NOT EDIT.
package accum

import accum "github.com/pg-accumulator/go-accumulator/v2"

// InventoryDims — generated from register "inventory"
type InventoryDims struct {
    Warehouse int     `json:"warehouse"`
    Product   int     `json:"product"`
    Lot       *string `json:"lot,omitempty"`
}

// InventoryResources — generated from register "inventory"
type InventoryResources struct {
    Quantity decimal.Decimal `json:"quantity"`
    Amount   decimal.Decimal `json:"amount"`
}

// InventoryMovement — full movement record
type InventoryMovement = accum.Movement[InventoryDims, InventoryResources]

// InventoryRegister — ready-to-use register definition
var InventoryRegister = accum.RegisterDef{
    Name:         "inventory",
    Kind:         accum.Balance,
    TotalsPeriod: accum.Day,
    PartitionBy:  accum.Month,
    Dimensions:   accum.Dims{"warehouse": accum.Int, "product": accum.Int, "lot": accum.Text},
    Resources:    accum.Res{"quantity": accum.Numeric, "amount": accum.Numeric},
}
```

---

## Etапи розробки

### Етап 1 — Core Library (MVP)
**Ціль:** Мінімальний пакет — надійні read/write операції з pg_accumulator через DBTX.

**Задачі:**
- [ ] Ініціалізація Go-модуля (`github.com/pg-accumulator/go-accumulator`)
- [ ] `DBTX` інтерфейс + адаптери для `*sql.DB`, `*sql.Tx`
- [ ] `AccumulatorClient.New()` з `Options`
- [ ] `RegisterDef` з типами `Kind`, `Dims`, `Res`
- [ ] `Use[D, R](client, def) *Register[D, R]`
- [ ] `Register.Post()` — single movement
- [ ] `Register.PostBatch()` — masiv movements (один SQL)
- [ ] `Register.Unpost()`
- [ ] `Register.Repost()`
- [ ] `Register.Balance()` / `BalanceAt()`
- [ ] `Register.Turnover()` з `TurnoverOptions`
- [ ] `Register.Movements()` з `MovementsOptions`
- [ ] JSON serialization без `map[string]any` (struct-based)
- [ ] SQL injection prevention (тільки parametrized queries, ніяки `fmt.Sprintf` у SQL)
- [ ] Unit-тести (`testify`)
- [ ] README з Quick Start

**Результат:** `go get github.com/pg-accumulator/go-accumulator` → повністю функціональний type-safe клієнт.

---

### Етап 2 — pgx/v5 + Error Handling
**Ціль:** Native pgx адаптер та структуровані помилки.

**Задачі:**
- [ ] `pgx/v5` адаптер — `WithPgxPool()`, `WithPgxTx()`
- [ ] `pgxpool.Pool` реалізація `DBTX` через bridge
- [ ] Кастомні типи помилок: `RegisterNotFoundError`, `RecorderNotFoundError`, `ValidationError`
- [ ] Парсинг PostgreSQL `pgerrcode` / `pgconn.PgError`
- [ ] Підтримка `context.Context` скрізь (timeout, cancellation)
- [ ] Prepared statements через pgx statement cache
- [ ] `pgtype.Numeric` підтримка як альтернатива `shopspring/decimal`
- [ ] Integration-тести з testcontainers-go + реальним pg_accumulator

**Результат:** Production-ready клієнт з pgx та зрозумілими помилками.

---

### Етап 3 — Transactions + ORM Adapters
**Ціль:** Universal `WithTx()` та офіційні адаптери для популярних ORM.

**Задачі:**
- [ ] `client.WithTx(tx DBTX) *AccumulatorClient`
- [ ] GORM adapter: `accum.GORMAdapter(*gorm.DB) DBTX`
- [ ] sqlx adapter: `accum.SqlxAdapter(*sqlx.DB) DBTX`
- [ ] ent adapter: `accum.EntAdapter(drv dialect.Driver) DBTX`
- [ ] Документація та тести для кожного адаптера

**Результат:** Один пакет — будь-який Go ORM.

---

### Етап 4 — sqlc Queries + DDL Client
**Ціль:** Готові sqlc-файли у пакеті та повний DDL API.

**Задачі:**
- [ ] `sqlc/queries.sql` — підготовлені queries для вбудовування
- [ ] `client.CreateRegister(ctx, RegisterDef)` через `register_create()`
- [ ] `client.AlterRegister(ctx, name, AlterOptions)`
- [ ] `client.DropRegister(ctx, name)`
- [ ] `client.ListRegisters(ctx)` → `[]RegisterInfo`
- [ ] `client.RegisterInfo(ctx, name)` → `RegisterInfo`
- [ ] `client.Diagnostics(ctx, name)` → `DiagnosticsResult`
- [ ] Документація: як вставити sqlc queries у свій проект

**Результат:** Повна DDL автоматизація через Go код.

---

### Етап 5 — CLI (`accumulatorctl`)
**Ціль:** DevTools для міграцій та code generation.

**Задачі:**
- [ ] `accumulatorctl generate` — Go struct/type generation з RegisterDef
- [ ] `accumulatorctl introspect` — читання реєстрів з БД → Go code
- [ ] `accumulatorctl diff` — порівняння коду і БД, вивід розбіжностей
- [ ] `accumulatorctl migrate create` — генерація SQL-файлів міграцій
- [ ] Підтримка `golang-migrate` та `goose` форматів міграцій
- [ ] Integration з `go generate` директивами:
  ```go
  //go:generate accumulatorctl generate --def ./registers.go --out ./internal/accum/
  ```

**Результат:** Повний devops-цикл: код → diff → міграція → генерація типів.

---

### Етап 6 — Advanced Features
**Ціль:** High-write mode, maintenance, observability.

**Задачі:**
- [ ] `HighWrite` mode: `client.Flush(ctx, register)` — ручний delta buffer merge
- [ ] `client.Maintenance.RebuildTotals(ctx, name)`
- [ ] `client.Maintenance.RebuildCache(ctx, name)`
- [ ] OpenTelemetry tracing — інструментація всіх операцій
- [ ] Prometheus metrics: `post_duration_seconds`, `balance_cache_hits_total`
- [ ] `slog` структурований logging
- [ ] PgBouncer compatibility mode
- [ ] Retry + backoff для serialization failures

**Результат:** Enterprise-ready пакет з observability.

---

### Етап 7 — Ecosystem
**Ціль:** Документація, приклади, community.

**Задачі:**
- [ ] pkgsite-ready документація (godoc comments)
- [ ] Приклади: `examples/gorm/`, `examples/pgx/`, `examples/sqlc/`
- [ ] Benchmark: vs raw pgx queries (overhead measurement)
- [ ] GitHub Actions: CI, lint, test (unit + integration via testcontainers)
- [ ] Semantic versioning + CHANGELOG
- [ ] pkg.go.dev badge

**Результат:** Open-source пакет готовий до публікації.

---

## Структура пакету

```
go-accumulator/
├── go.mod
├── go.sum
├── README.md
├── CHANGELOG.md
│
├── accum.go                   # Public API entry: New(), Use()
├── client.go                  # AccumulatorClient
├── register.go                # Register[D, R] — generic client
├── register_def.go            # RegisterDef, Kind, Dims, Res, type constants
├── options.go                 # Options, TurnoverOptions, MovementsOptions, AlterOptions
├── errors.go                  # AccumError, sentinel errors
│
├── operations/
│   ├── post.go                # Post / PostBatch SQL execution
│   ├── unpost.go              # Unpost SQL execution
│   ├── repost.go              # Repost SQL execution
│   ├── balance.go             # Balance / BalanceAt
│   ├── turnover.go            # Turnover
│   └── movements.go           # Movements with pagination
│
├── ddl/
│   ├── create.go              # CreateRegister
│   ├── alter.go               # AlterRegister
│   ├── drop.go                # DropRegister
│   └── introspect.go          # ListRegisters, RegisterInfo, Diagnostics
│
├── adapters/
│   ├── gorm.go                # GORMAdapter
│   ├── pgx.go                 # WithPgxPool, WithPgxTx
│   ├── sqlx.go                # SqlxAdapter
│   └── ent.go                 # EntAdapter
│
├── internal/
│   ├── db/
│   │   ├── db.go              # DBTX interface (sqlc generated or manual)
│   │   └── queries.go         # Low-level parametrized query functions
│   ├── json/
│   │   └── marshal.go         # Safe JSON serialization для JSONB
│   ├── errors/
│   │   └── parse.go           # PgError → AccumError mapping
│   └── types/
│       └── decimal.go         # Decimal handling helpers
│
├── sqlc/
│   ├── queries.sql            # Ready-to-use sqlc query templates
│   └── README.md              # How to use with your sqlc setup
│
├── cmd/
│   └── accumulatorctl/
│       ├── main.go
│       ├── generate.go
│       ├── introspect.go
│       ├── diff.go
│       └── migrate.go
│
├── test/
│   ├── integration/
│   │   ├── testcontainer_test.go   # testcontainers-go setup
│   │   ├── post_test.go
│   │   ├── balance_test.go
│   │   ├── turnover_test.go
│   │   └── transaction_test.go
│   └── unit/
│       ├── json_test.go
│       ├── errors_test.go
│       └── register_def_test.go
│
└── examples/
    ├── basic-pgx/
    │   ├── main.go
    │   └── README.md
    ├── gorm-inventory/
    │   ├── main.go
    │   └── README.md
    ├── sqlc-finance/
    │   ├── main.go
    │   ├── sqlc.yaml
    │   └── README.md
    └── testcontainers/
        ├── main_test.go
        └── README.md
```

---

## go.mod

```go
module github.com/pg-accumulator/go-accumulator/v2

go 1.22

require (
    github.com/jackc/pgx/v5         v5.5.4
    github.com/shopspring/decimal    v1.3.1
)

require (
    // Optional adapters — не нав'язуємо залежності
    // Кожен адаптер у окремому sub-module або build tag
)
```

---

## Ліцензія

MIT — сумісна з pgx, GORM, sqlc, ent.
