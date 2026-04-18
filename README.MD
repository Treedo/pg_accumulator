# pg_accumulator

**Accumulation Registers for PostgreSQL** — розширення, яке перетворює PostgreSQL на повноцінну платформу обліку ресурсів із миттєвим доступом до залишків і оборотів.

---

## Зміст

- [Огляд](#огляд)
- [Ключові можливості](#ключові-можливості)
- [Швидкий старт](#швидкий-старт)
- [Встановлення](#встановлення)
- [Концепція](#концепція)
  - [Що таке регістр накопичення](#що-таке-регістр-накопичення)
  - [Виміри та ресурси](#виміри-та-ресурси)
  - [Типи регістрів](#типи-регістрів)
- [API](#api)
  - [Управління регістрами](#управління-регістрами)
    - [`register_create()`](#register_create)
    - [`register_alter()`](#register_alter)
    - [`register_drop()`](#register_drop)
    - [`register_list()`](#register_list)
    - [`register_info()`](#register_info)
  - [Запис даних](#запис-даних)
    - [`register_post()`](#register_post)
    - [`register_unpost()`](#register_unpost)
    - [`register_repost()`](#register_repost)
    - [Прямий INSERT](#прямий-insert)
  - [Читання даних](#читання-даних)
    - [`<register>_balance()`](#register_balance)
    - [`<register>_turnover()`](#register_turnover)
    - [`<register>_movements()`](#register_movements)
    - [`<register>_balance_cache`](#register_balance_cache)
- [Архітектура](#архітектура)
  - [Загальна схема](#загальна-схема)
  - [Шар рухів (Movements)](#шар-рухів-movements)
  - [Шар підсумків (Totals)](#шар-підсумків-totals)
  - [Balance Cache](#balance-cache)
  - [Лінійний запис і корекції](#лінійний-запис-і-корекції)
  - [Тригерний ланцюг](#тригерний-ланцюг)
- [Конкурентність та продуктивність](#конкурентність-та-продуктивність)
  - [Row-Level Locking](#row-level-locking)
  - [Delta Buffer (High-Write режим)](#delta-buffer-high-write-режим)
  - [Бенчмарки](#бенчмарки)
- [Корекція заднім числом](#корекція-заднім-числом)
- [Рекомендації щодо проєктування](#рекомендації-щодо-проєктування)
- [Приклади використання](#приклади-використання)
  - [Складський облік](#складський-облік)
  - [Фінансові транзакції](#фінансові-транзакції)
  - [Облік підписок](#облік-підписок)
- [Діагностика та обслуговування](#діагностика-та-обслуговування)
- [Конфігурація](#конфігурація)
- [Сумісність](#сумісність)
- [FAQ](#faq)
- [Ліцензія](#ліцензія)

---

## Огляд

`pg_accumulator` — це PostgreSQL-розширення, яке надає декларативний механізм **регістрів накопичення** — високорівневу абстракцію для обліку залишків і оборотів ресурсів у розрізі довільних вимірів.

**Проблема.** Розробники та AI-агенти, будуючи облікові системи, вимушені щоразу вручну проєктувати таблиці рухів, таблиці підсумків, тригери для підтримки актуальності, логіку перерахунку при зміні минулих періодів та індекси для швидкого читання.

**Рішення.** Один виклик `register_create()` автоматично створює всю інфраструктуру: таблиці, індекси, партиції, тригери, функції читання та фоновий процес обслуговування.

```sql
SELECT register_create(
    name       := 'inventory',
    dimensions := '{"warehouse": "int", "product": "int", "lot": "text"}',
    resources  := '{"quantity": "numeric", "amount": "numeric"}',
    kind       := 'balance',
    totals_period := 'day'
);
```

Після цього:

```sql
-- Записати рух
SELECT register_post('inventory', '{
    "recorder":  "purchase_order:7001",
    "period":    "2026-04-18",
    "warehouse": 1,
    "product":   42,
    "lot":       "LOT-A",
    "quantity":  100,
    "amount":    5000
}');

-- Отримати поточний залишок — O(1), ~0.1ms
SELECT * FROM accum.inventory_balance(
    dimensions := '{"warehouse": 1, "product": 42}'
);
```

---

## Ключові можливості

| Можливість | Опис |
|---|---|
| **Декларативне створення** | Один виклик замість десятків DDL-інструкцій |
| **Append-only рухи** | Лінійний запис: вставка, ніколи не оновлення |
| **Ієрархічні підсумки** | Обороти за рік → місяць → день для швидкого обчислення |
| **Balance Cache** | Поточний залишок завжди доступний за O(1) |
| **Синхронне оновлення** | Підсумки та кеш актуальні одразу після COMMIT |
| **Корекція заднім числом** | O(1) — без каскадного перерахунку |
| **Recorder-паттерн** | Атомарне проведення/скасування документа |
| **High-write mode** | Delta buffer для зменшення contention при високому навантаженні |
| **Partitioning** | Автоматичне партиціювання рухів по періодах |
| **Аудит** | Повна історія: оригінали та корекції збережені окремо |

---

## Швидкий старт

### 1. Встановлення

```bash
# З вихідного коду
git clone https://github.com/example/pg_accumulator.git
cd pg_accumulator
make && sudo make install

# У базі даних
psql -c "CREATE EXTENSION pg_accumulator;"
```

### 2. Створення регістру

```sql
SELECT register_create(
    name       := 'inventory',
    dimensions := '{"warehouse": "int", "product": "int"}',
    resources  := '{"quantity": "numeric(18,4)", "amount": "numeric(18,2)"}',
    kind       := 'balance'
);
```

### 3. Запис даних

```sql
SELECT register_post('inventory', '{
    "recorder":  "receipt:1",
    "period":    "2026-04-18",
    "warehouse": 1,
    "product":   42,
    "quantity":  100,
    "amount":    5000.00
}');
```

### 4. Читання залишку

```sql
-- Поточний залишок
SELECT * FROM accum.inventory_balance(
    dimensions := '{"warehouse": 1, "product": 42}'
);
-- Результат: quantity = 100.0000, amount = 5000.00
```

---

## Встановлення

### Вимоги

- PostgreSQL 15 або новіше
- Права суперкористувача для встановлення розширення
- (Рекомендовано) `pg_cron` для фонових завдань обслуговування

### З вихідного коду

```bash
git clone https://github.com/example/pg_accumulator.git
cd pg_accumulator
make PG_CONFIG=/path/to/pg_config
sudo make install PG_CONFIG=/path/to/pg_config
```

### Docker

```dockerfile
FROM postgres:17

RUN apt-get update \
    && apt-get install -y pg-accumulator-17 \
    && rm -rf /var/lib/apt/lists/*
```

### Активація в базі

```sql
-- Створює схему 'accum' та службові таблиці
CREATE EXTENSION pg_accumulator;

-- Або у власній схемі
CREATE EXTENSION pg_accumulator SCHEMA my_schema;
```

### Оновлення

```bash
sudo make install  # нова версія
psql -c "ALTER EXTENSION pg_accumulator UPDATE;"
```

---

## Концепція

### Що таке регістр накопичення

Регістр накопичення — це структурований механізм для фіксації **рухів** (приходів та витрат) ресурсів і швидкого отримання **підсумків** (залишків та оборотів) у розрізі довільних вимірів.

```
Реальний світ:                         pg_accumulator:

  "На складі 1 зараз                    register: inventory
   42 одиниці товару X"                  dimensions: warehouse, product
                                         resources: quantity, amount
  "В березні зі складу 1                 kind: balance
   відвантажили 15 одиниць"
```

### Виміри та ресурси

**Виміри (dimensions)** — осі аналізу, за якими розрізняються залишки:

```
warehouse ──┐
product   ──┼── Комбінація вимірів = унікальна "комірка"
lot       ──┘   (одна точка в просторі обліку)
```

**Ресурси (resources)** — числові величини, які накопичуються:

```
quantity ──┐
amount   ──┴── Значення, що додаються/віднімаються з кожним рухом
```

### Типи регістрів

| Тип | Призначення | Підтримує залишки | Підтримує обороти |
|---|---|---|---|
| `balance` | Залишки ресурсів (склад, рахунок) | ✅ | ✅ |
| `turnover` | Лише обороти (продажі, статистика) | ❌ | ✅ |

```sql
-- Регістр залишків: "скільки зараз на складі"
SELECT register_create(name := 'stock', kind := 'balance', ...);

-- Регістр оборотів: "скільки продали за місяць"
SELECT register_create(name := 'sales', kind := 'turnover', ...);
```

---

## API

Усі функції розширення знаходяться у схемі, обраній під час `CREATE EXTENSION` (за замовчуванням — `accum`).

### Управління регістрами

#### `register_create()`

Створює новий регістр накопичення з повною внутрішньою інфраструктурою.

```sql
SELECT register_create(
    name          := 'inventory',          -- Унікальне ім'я регістру
    dimensions    := '{                    -- Виміри: JSON {name: type}
        "warehouse": "int",
        "product":   "int",
        "lot":       "text"
    }',
    resources     := '{                    -- Ресурси: JSON {name: type}
        "quantity": "numeric(18,4)",
        "amount":   "numeric(18,2)"
    }',
    kind          := 'balance',            -- 'balance' або 'turnover'
    totals_period := 'day',                -- 'day', 'month', 'year'
    partition_by  := 'month',              -- Партиціювання рухів
    high_write    := false,                -- Увімкнути delta buffer
    recorder_type := 'text'                -- Тип поля recorder
);
```

**Параметри:**

| Параметр | Тип | Обов'язковий | За замовчуванням | Опис |
|---|---|---|---|---|
| `name` | `text` | ✅ | — | Унікальне ім'я регістру. Допускаються латинські літери, цифри, `_` |
| `dimensions` | `jsonb` | ✅ | — | Виміри у форматі `{"name": "pg_type", ...}` |
| `resources` | `jsonb` | ✅ | — | Ресурси у форматі `{"name": "pg_type", ...}` |
| `kind` | `text` | ❌ | `'balance'` | Тип: `'balance'` або `'turnover'` |
| `totals_period` | `text` | ❌ | `'day'` | Гранулярність підсумків: `'day'`, `'month'`, `'year'` |
| `partition_by` | `text` | ❌ | `'month'` | Партиціювання рухів: `'day'`, `'month'`, `'quarter'`, `'year'` |
| `high_write` | `boolean` | ❌ | `false` | Увімкнути delta buffer для зменшення contention |
| `recorder_type` | `text` | ❌ | `'text'` | PostgreSQL-тип для поля `recorder` |

**Що створюється автоматично:**

```
accum.inventory_movements          -- Таблиця рухів (partitioned)
accum.inventory_totals_month       -- Підсумки-обороти за місяць
accum.inventory_totals_year        -- Підсумки-обороти за рік
accum.inventory_balance_cache      -- Кешований поточний залишок
accum.inventory_balance(...)       -- Функція отримання залишку
accum.inventory_turnover(...)      -- Функція отримання оборотів
accum.inventory_movements(...)     -- Функція фільтрованих рухів
accum._trg_inventory_*             -- Системні тригери
accum._hash_inventory(...)         -- Функція хешування вимірів
```

#### `register_alter()`

Змінює структуру існуючого регістру.

```sql
-- Додати новий вимір
SELECT register_alter(
    name           := 'inventory',
    add_dimensions := '{"color": "text"}'
);

-- Додати новий ресурс
SELECT register_alter(
    name           := 'inventory',
    add_resources  := '{"weight": "numeric(12,3)"}'
);

-- Змінити режим high_write
SELECT register_alter(
    name       := 'inventory',
    high_write := true
);
```

> ⚠️ Додавання вимірів вимагає перерахунку підсумків. Для великих регістрів це може зайняти час. Розширення виконує перерахунок в online-режимі без блокування читачів.

#### `register_drop()`

Видаляє регістр і всю його інфраструктуру.

```sql
SELECT register_drop('inventory');

-- З підтвердженням (за замовчуванням вимагає підтвердження
-- якщо в регістрі є дані)
SELECT register_drop('inventory', force := true);
```

#### `register_list()`

Повертає список усіх регістрів у базі.

```sql
SELECT * FROM register_list();

-- name       | kind    | dimensions | resources | movements_count | created_at
-- -----------+---------+------------+-----------+-----------------+-----------
-- inventory  | balance | 3          | 2         | 1,234,567       | 2026-01-15
-- sales      | turnover| 2          | 1         | 456,789         | 2026-02-01
```

#### `register_info()`

Повертає детальну інформацію про регістр.

```sql
SELECT * FROM register_info('inventory');

-- Повертає JSON з повною структурою:
-- {
--   "name": "inventory",
--   "kind": "balance",
--   "dimensions": {"warehouse": "integer", "product": "integer", "lot": "text"},
--   "resources": {"quantity": "numeric(18,4)", "amount": "numeric(18,2)"},
--   "totals_period": "day",
--   "partition_by": "month",
--   "high_write": false,
--   "movements_count": 1234567,
--   "partitions": [...],
--   "cache_size": 8456,
--   "created_at": "2026-01-15T10:30:00Z"
-- }
```

---

### Запис даних

#### `register_post()`

Записує рух (один або кілька) з прив'язкою до документа-реєстратора.

```sql
-- Один рух
SELECT register_post('inventory', '{
    "recorder":  "purchase_order:7001",
    "period":    "2026-04-18T14:30:00",
    "warehouse": 1,
    "product":   42,
    "lot":       "LOT-A",
    "quantity":  100,
    "amount":    5000.00
}');

-- Кілька рухів одним викликом (batch)
SELECT register_post('inventory', '[
    {
        "recorder": "sales_order:8001",
        "period":   "2026-04-18",
        "warehouse": 1, "product": 42, "lot": "LOT-A",
        "quantity": -10, "amount": -500.00
    },
    {
        "recorder": "sales_order:8001",
        "period":   "2026-04-18",
        "warehouse": 2, "product": 42, "lot": "LOT-A",
        "quantity": 10, "amount": 500.00
    }
]');
```

**Параметри JSON для кожного руху:**

| Поле | Тип | Обов'язковий | Опис |
|---|---|---|---|
| `recorder` | відповідно до `recorder_type` | ✅ | Ідентифікатор документа-реєстратора |
| `period` | `timestamp` / `date` / ISO-8601 | ✅ | Дата/час руху |
| `<dimension>` | відповідно до оголошення | ✅ | Значення для кожного виміру |
| `<resource>` | `numeric` | ✅ | Значення для кожного ресурсу (додатні та від'ємні) |

**Повертає:**

```sql
-- Кількість записаних рухів
register_post := 2
```

**Що відбувається під капотом:**

```
register_post('inventory', data)
        │
        ▼
  1. Валідація JSON (всі виміри і ресурси присутні)
  2. Обчислення dim_hash для кожного руху
  3. INSERT INTO accum.inventory_movements
        │
        │ AFTER trigger (та сама транзакція)
        │
  4. UPDATE totals_month SET resource += delta
  5. UPDATE totals_year  SET resource += delta
  6. UPSERT balance_cache SET resource += delta
        │
        ▼
  COMMIT → все видно іншим клієнтам
```

#### `register_unpost()`

Скасовує всі рухи за документом-реєстратором.

```sql
SELECT register_unpost('inventory', 'sales_order:8001');
```

**Що відбувається:**

```
register_unpost('inventory', 'sales_order:8001')
        │
        ▼
  1. Пошук: SELECT * FROM movements WHERE recorder = 'sales_order:8001'
  2. DELETE FROM movements WHERE recorder = 'sales_order:8001'
        │
        │ AFTER DELETE trigger
        │
  3. UPDATE totals_month -= видалений_ресурс
  4. UPDATE totals_year  -= видален��й_ресурс
  5. UPDATE balance_cache -= видалений_ресурс
        │
        ▼
  Залишки коректні, ніби документа не існувало
```

**Повертає:**

```sql
-- Кількість скасованих рухів
register_unpost := 2
```

#### `register_repost()`

Атомарне перепроведення документа: скасовує старі рухи та записує нові.

```sql
SELECT register_repost('inventory', 'sales_order:8001', '[
    {
        "period":   "2026-04-18",
        "warehouse": 1, "product": 42, "lot": "LOT-A",
        "quantity": -12, "amount": -600.00
    }
]');
```

Еквівалент:
```sql
BEGIN;
    SELECT register_unpost('inventory', 'sales_order:8001');
    SELECT register_post('inventory', '...');
COMMIT;
```

Але `register_repost()` оптимізований: обчислює дельту між старими та новими рухами і виконує мінімальну кількість оновлень підсумків.

#### Прямий INSERT

Для масових операцій або інтеграцій можна писати безпосередньо в таблицю рухів:

```sql
INSERT INTO accum.inventory_movements
    (recorder, period, warehouse_id, product_id, lot, quantity, amount)
VALUES
    ('import:batch_42', '2026-04-18', 1, 42, 'LOT-A', 100, 5000.00),
    ('import:batch_42', '2026-04-18', 1, 43, 'LOT-B', 200, 8000.00),
    ('import:batch_42', '2026-04-18', 2, 42, 'LOT-A',  50, 2500.00);

-- Тригери оновлять підсумки автоматично
```

> 💡 `register_post()` виконує додаткову валідацію JSON. При прямому INSERT ви відповідаєте за коректність даних.

---

### Читання даних

#### `<register>_balance()`

Повертає залишок ресурсів для заданих вимірів. Доступна лише для регістрів типу `balance`.

```sql
-- Поточний залишок (з balance_cache — O(1))
SELECT * FROM accum.inventory_balance(
    dimensions := '{"warehouse": 1, "product": 42}'
);

-- Залишок на конкретну дату (з підсумків + дорахунок)
SELECT * FROM accum.inventory_balance(
    dimensions := '{"warehouse": 1, "product": 42}',
    at_date    := '2026-03-15'
);

-- Залишок по неповному набору вимірів (агрегація)
SELECT * FROM accum.inventory_balance(
    dimensions := '{"warehouse": 1}'
);
-- Поверне сумарний залишок по всіх товарах і лотах на складі 1

-- Без фільтрів — загальний залишок по всьому регістру
SELECT * FROM accum.inventory_balance();
```

**Результат:**

| quantity | amount |
|---|---|
| 100.0000 | 5000.00 |

**Алгоритм обчислення залишку на дату (під капотом):**

```
Запит: залишок на 15 березня 2026

Крок 1: Знайти найближчий підсумок по роках
         → totals_year за 2025: SUM оборотів усіх років до 2025

Крок 2: Додати підсумки за завершені місяці 2026
         → totals_month за січень + лютий

Крок 3: Додати рухи за неповний місяць
         → SUM(movements) з 01.03 по 15.03

Результат = Крок1 + Крок2 + Крок3

Максимальна кількість рядків для сканування:
  ~20 (років) + ~11 (місяців) + ~31 (днів рухів) ≈ 62 rows
  замість мільйонів рухів за всю історію
```

#### `<register>_turnover()`

Повертає обороти (прихід, витрата, або нетто) за період.

```sql
-- Обороти за місяць
SELECT * FROM accum.inventory_turnover(
    from_date  := '2026-04-01',
    to_date    := '2026-04-30',
    dimensions := '{"warehouse": 1}'
);

-- Обороти з розгортанням по виміру
SELECT * FROM accum.inventory_turnover(
    from_date  := '2026-04-01',
    to_date    := '2026-04-30',
    dimensions := '{"warehouse": 1}',
    group_by   := '{"product"}'     -- розгорнути по товарах
);
```

**Результат з `group_by`:**

| product | quantity | amount |
|---|---|---|
| 42 | -10.0000 | -500.00 |
| 43 | 200.0000 | 8000.00 |

#### `<register>_movements()`

Повертає рухи з фільтрами.

```sql
-- Рухи за документом
SELECT * FROM accum.inventory_movements(
    recorder := 'sales_order:8001'
);

-- Рухи за період та вимірами
SELECT * FROM accum.inventory_movements(
    from_date  := '2026-04-01',
    to_date    := '2026-04-18',
    dimensions := '{"warehouse": 1, "product": 42}'
);
```

#### `<register>_balance_cache`

Таблиця кешованого поточного залишку. Доступна для прямих SQL-запитів з максимальною гнучкістю.

```sql
-- Усі залишки на складі 1
SELECT product_id, lot, quantity, amount
FROM accum.inventory_balance_cache
WHERE warehouse_id = 1
  AND quantity != 0
ORDER BY product_id;

-- Товари з від'ємним залишком (alert)
SELECT warehouse_id, product_id, quantity
FROM accum.inventory_balance_cache
WHERE quantity < 0;

-- JOIN з іншими таблицями
SELECT
    p.name,
    c.quantity,
    c.amount
FROM accum.inventory_balance_cache c
JOIN products p ON p.id = c.product_id
WHERE c.warehouse_id = 1
ORDER BY c.amount DESC
LIMIT 10;
```

**Структура таблиці:**

| Колонка | Тип | Опис |
|---|---|---|
| `dim_hash` | `bigint` | PK — хеш комбінаці�� вимірів |
| `<dimension>_id` / `<dimension>` | відповідно до оголошення | Денормалізовані значення вимірів |
| `<resource>` | відповідно до оголошення | Поточний залишок ресурсу |
| `last_movement_at` | `timestamptz` | Час останнього руху |
| `last_movement_id` | `uuid` | ID останнього руху |
| `version` | `bigint` | Лічильник оновлень (optimistic lock) |

---

## Архітектура

### Загальна схема

```
┌─────────────────────────────────────────────────────────────────┐
│                        PUBLIC API                               │
│   register_create/alter/drop/post/unpost/repost                │
│   <register>_balance() / _turnover() / _movements()            │
└────────────────────────────┬────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────┐
│                     pg_accumulator core                         │
│                                                                 │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────────────┐ │
│  │   DDL Gen    │  │ Trigger Eng. │  │   Query Builder        │ │
│  │             │  │              │  │                        │ │
│  │ Таблиці     │  │ Оновлення    │  │ Оптимальні запити     │ │
│  │ Індекси     │  │ підсумків    │  │ balance/turnover       │ │
│  │ Партиції    │  │ та кешу в    │  │ з урахуванням         │ │
│  │ Функції     │  │ тій самій TX │  │ ієрархії підсумків    │ │
│  └─────────────┘  └──────────────┘  └────────────────────────┘ │
│  ┌─────────────┐  ┌──────────────┐                             │
│  │  Hash Func  │  │  BG Worker   │                             │
│  │             │  │              │                             │
│  │ dim_hash    │  │ Delta merge  │                             │
│  │ обчислення  │  │ Partition    │                             │
│  │             │  │ maintenance  │                             │
│  └─────────────┘  └──────────────┘                             │
└─────────────────────────────────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────┐
│                      STORAGE LAYER                              │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ movements (partitioned by period)                         │  │
│  │ Append-only рухи, джерело істини                         │  │
│  └──────────────────────────────────────────────────────────┘  │
│  ┌─────────────────────┐  ┌──────────────────────────────┐    │
│  │ totals_year         │  │ totals_month                  │    │
│  │ Обороти за рік      │  │ Обороти за місяць             │    │
│  └─────────────────────┘  └──────────────────────────────┘    │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ balance_cache                                             │  │
│  │ Поточний кумулятивний залишок, O(1) читання              │  │
│  └──────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ balance_cache_delta  (high_write mode only)              │  │
│  │ Append-only буфер дельт                                  │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### Шар рухів (Movements)

Рухи — це **джерело істини** та **append-only журнал**.

```sql
-- Автоматично створена таблиця рухів:
CREATE TABLE accum.inventory_movements (
    id             uuid          DEFAULT gen_random_uuid() PRIMARY KEY,
    recorded_at    timestamptz   DEFAULT now() NOT NULL,
    recorder       text          NOT NULL,
    period         timestamptz   NOT NULL,
    movement_type  text          DEFAULT 'regular' NOT NULL,
    dim_hash       bigint        NOT NULL,

    -- Виміри (денормалізовані)
    warehouse_id   integer       NOT NULL,
    product_id     integer       NOT NULL,
    lot            text,

    -- Ресурси
    quantity       numeric(18,4) NOT NULL DEFAULT 0,
    amount         numeric(18,2) NOT NULL DEFAULT 0
) PARTITION BY RANGE (period);

-- Автоматичні індекси:
CREATE INDEX ON accum.inventory_movements (dim_hash, period);
CREATE INDEX ON accum.inventory_movements (recorder);
CREATE INDEX ON accum.inventory_movements (period);
```

**Ключові поля:**

| Поле | Призначення |
|---|---|
| `id` | Унікальний ідентифікатор руху |
| `recorded_at` | Фактичний час запису (коли рядок створений, `now()`) |
| `recorder` | Посилання на документ-реєстратор |
| `period` | Облікова дата руху (може бути в минулому — корекція) |
| `movement_type` | `'regular'`, `'adjustment'`, `'reversal'` |
| `dim_hash` | Хеш комбінації вимірів для швидкого lookup |

**Партиціювання:**

```
accum.inventory_movements
├── accum.inventory_movements_2026_01  (period: 2026-01-01 .. 2026-02-01)
├── accum.inventory_movements_2026_02  (period: 2026-02-01 .. 2026-03-01)
├── accum.inventory_movements_2026_03  (period: 2026-03-01 .. 2026-04-01)
├── accum.inventory_movements_2026_04  (period: 2026-04-01 .. 2026-05-01)  ← поточна
└── accum.inventory_movements_default  (fallback)

Нові партиції створюються автоматично background worker'ом
або при першому INSERT в новий період.
```

### Шар підсумків (Totals)

Підсумки зберігають **обороти** (не кумулятивні залишки) за період. Це ключове архітектурне рішення, яке робить корекцію заднім числом O(1).

```sql
-- Підсумки за місяць
CREATE TABLE accum.inventory_totals_month (
    dim_hash       bigint        NOT NULL,
    period         date          NOT NULL,  -- перший день місяця

    warehouse_id   integer       NOT NULL,
    product_id     integer       NOT NULL,
    lot            text,

    quantity       numeric(18,4) NOT NULL DEFAULT 0,
    amount         numeric(18,2) NOT NULL DEFAULT 0,

    PRIMARY KEY (dim_hash, period)
);

-- Підсумки за рік
CREATE TABLE accum.inventory_totals_year (
    dim_hash       bigint        NOT NULL,
    period         date          NOT NULL,  -- перший день року

    warehouse_id   integer       NOT NULL,
    product_id     integer       NOT NULL,
    lot            text,

    quantity       numeric(18,4) NOT NULL DEFAULT 0,
    amount         numeric(18,2) NOT NULL DEFAULT 0,

    PRIMARY KEY (dim_hash, period)
);
```

**Чому обороти, а не кумулятивні залишки:**

```
Кумулятивний підхід                    Оборотний підхід
(залишок НА КІНЕЦЬ)                    (оборот ЗА період)

totals_month:                          totals_month:
  Січ: 100                               Січ: +100
  Лют: 150 (= 100 + 50)                 Лют:  +50
  Бер: 120 (= 150 - 30)                 Бер:  -30
  Кві: 130 (= 120 + 10)                 Кві:  +10

Корекція лютого на +5:                 Корекція лютого на +5:
  UPDATE Лют: 150 → 155                  UPDATE Лют: +50 → +55
  UPDATE Бер: 120 → 125  ← ��аскад       Більше нічого! ✅
  UPDATE Кві: 130 → 135  ← каскад

  O(N місяців)                           O(1)
```

### Balance Cache

Balance cache зберігає **поточний кумулятивний залишок** — суму всіх рухів за всю історію для кожної комбінації вимірів.

```sql
CREATE TABLE accum.inventory_balance_cache (
    dim_hash         bigint          NOT NULL PRIMARY KEY,

    -- Денормалізовані виміри
    warehouse_id     integer         NOT NULL,
    product_id       integer         NOT NULL,
    lot              text,

    -- Ресурси: поточний залишок
    quantity         numeric(18,4)   NOT NULL DEFAULT 0,
    amount           numeric(18,2)   NOT NULL DEFAULT 0,

    -- Службові
    last_movement_at timestamptz     NOT NULL,
    last_movement_id uuid            NOT NULL,
    version          bigint          NOT NULL DEFAULT 0
);

-- Індекси для запитів по окремих вимірах
CREATE INDEX ON accum.inventory_balance_cache (warehouse_id);
CREATE INDEX ON accum.inventory_balance_cache (product_id);
CREATE INDEX ON accum.inventory_balance_cache (warehouse_id, product_id);
```

**Механізм оновлення (тригер):**

```sql
-- Спрощена логіка тригера на movements:
INSERT INTO accum.inventory_balance_cache
    (dim_hash, warehouse_id, product_id, lot,
     quantity, amount, last_movement_at, last_movement_id, version)
VALUES
    (_dim_hash, NEW.warehouse_id, NEW.product_id, NEW.lot,
     NEW.quantity, NEW.amount, NEW.recorded_at, NEW.id, 1)
ON CONFLICT (dim_hash) DO UPDATE SET
    quantity         = inventory_balance_cache.quantity + NEW.quantity,
    amount           = inventory_balance_cache.amount   + NEW.amount,
    last_movement_at = EXCLUDED.last_movement_at,
    last_movement_id = EXCLUDED.last_movement_id,
    version          = inventory_balance_cache.version + 1;
```

**Гарантії:**

| Властивість | Гарантія |
|---|---|
| Актуальність | Після `COMMIT` — завжди відображає всі committed рухи |
| Консистентність | `cache = SUM(movements)` — завжди, математично |
| Відкат | При `ROLLBACK` тригер відкочується разом — cache не змінюється |
| Ізоляція | MVCC: кожен читач бачить snapshot своєї транзакції |

**Коли cache не існує (нова комбінація вимірів):** UPSERT (`ON CONFLICT`) автоматично створює новий рядок при першому русі.

### Лінійний запис і корекції

Розширення використовує **append-only** підхід: рухи ніколи не оновлюються, лише додаються.

```
Часова вісь (recorded_at):
═══════════════════════════════════════════════════════════► час
  10:00      10:05      10:30       14:00         16:00
   │          │          │           │              │
   ▼          ▼          ▼           ▼              ▼
 +100       +50        -30        Корекція       -20
 regular    regular    regular    adjustment     regular
 period:    period:    period:    period:        period:
 Квітень    Квітень    Квітень    БЕРЕЗЕНЬ       Квітень
                                  (заднім
                                   числом)

Корекція за березень:
  - movement_type = 'adjustment'
  - period = березень (облікова дата)
  - recorded_at = квітень 14:00 (фактичний час запису)
  - Фізично записана В КІНЕЦЬ журналу
  - Оригінальні рухи НЕ змінювались
```

**Переваги:**

```
✅ Повний аудит-трейл (хто, коли, що змінив)
✅ Немає блокувань на оновлення існуючих рядків
✅ Кращий WAL-паттерн (тільки INSERT, без UPDATE)
✅ Простіша реплікація
✅ Корекція заднім числом — O(1) в підсумках
```

### Тригерний ланцюг

Повний ланцюг подій при записі руху:

```
INSERT INTO movements (period=березень, qty=+10, ...)
                │
                ▼
    ┌───────────────────────────────┐
    │  BEFORE INSERT trigger       │
    │  1. Обчислити dim_hash       │
    │  2. Валідувати поля          │
    │  3. Встановити recorded_at   │
    │  4. Визначити movement_type  │
    └─────────────┬─────────────────┘
                  │
                  ▼
    ┌───────────────────────────────┐
    │  INSERT фізично записаний    │
    └─────────────┬─────────────────┘
                  │
                  ▼
    ┌───────────────────────────────┐
    │  AFTER INSERT trigger        │
    │                              │
    │  1. UPDATE totals_month      │
    │     SET qty += 10            │
    │     WHERE dim_hash = X       │
    │       AND period = '2026-03' │
    │     ON CONFLICT → INSERT     │
    │                              │
    │  2. UPDATE totals_year       │
    │     SET qty += 10            │
    │     WHERE dim_hash = X       │
    │       AND period = '2026'    │
    │     ON CONFLICT → INSERT     │
    │                              │
    │  3. UPSERT balance_cache     │
    │     SET qty += 10            │
    │     WHERE dim_hash = X       │
    └─────────────┬─────────────────┘
                  │
                  ▼
            COMMIT → дані видимі
```

---

## Конкурентність та продуктивність

### Row-Level Locking

У стандартному режимі тригер виконує `UPDATE` по `dim_hash` в таблицях підсумків та cache. PostgreSQL використовує row-level locks:

```
Writer A (warehouse=1, product=42)    Writer B (warehouse=1, product=99)
─────────────────────────────────     ─────────────────────────────────
UPDATE cache WHERE dim_hash=H1       UPDATE cache WHERE dim_hash=H2
  → lock row H1                        → lock row H2
  → NO CONFLICT ✅                     → NO CONFLICT ✅

Writer C (warehouse=1, product=42)
─────────────────────────────────
UPDATE cache WHERE dim_hash=H1
  → WAIT for Writer A to COMMIT
  → then proceed ✅
```

**Різні комбінації вимірів — немає конфлікту.** Конфлікт лише при одночасному записі в ту саму комбінацію.

### Delta Buffer (High-Write режим)

Для сценаріїв з високим contention на один `dim_hash` (наприклад, лічильник відвідувань одного ресурсу):

```sql
-- Увімкнути при створенні
SELECT register_create(
    name       := 'page_views',
    dimensions := '{"page": "text"}',
    resources  := '{"views": "int"}',
    kind       := 'turnover',
    high_write := true           -- ← увімкнути delta buffer
);
```

**Як працює:**

```
                  Стандартний режим              High-Write режим
                  ──────────────────             ────────────────────
Запис             UPDATE cache (lock)            INSERT delta (no lock)
Contention        Черга на hot rows              Немає ✅
Читання           SELECT cache (1 row)           SELECT cache + SUM(deltas)
Складність        O(1)                           O(1) + O(pending deltas)
Background        Не потрібен                    Merge кожні N секунд
```

```sql
-- Delta buffer таблиця (автоматично створюється):
CREATE UNLOGGED TABLE accum.inventory_balance_cache_delta (
    id         bigserial   PRIMARY KEY,
    dim_hash   bigint      NOT NULL,
    quantity   numeric(18,4) NOT NULL DEFAULT 0,
    amount     numeric(18,2) NOT NULL DEFAULT 0,
    created_at timestamptz DEFAULT now()
);

-- Background worker (merge):
-- Кожні pg_accumulator.delta_merge_interval секунд
WITH consumed AS (
    DELETE FROM accum.inventory_balance_cache_delta
    WHERE created_at < now() - pg_accumulator.delta_merge_delay
    RETURNING dim_hash, quantity, amount
),
agg AS (
    SELECT dim_hash, SUM(quantity) AS qty, SUM(amount) AS amt
    FROM consumed
    GROUP BY dim_hash
)
UPDATE accum.inventory_balance_cache c
SET quantity = c.quantity + a.qty,
    amount   = c.amount   + a.amt,
    version  = c.version + 1
FROM agg a
WHERE c.dim_hash = a.dim_hash;
```

**Читання з урахуванням буфера (автоматично в `_balance()`):**

```sql
SELECT
    c.quantity + COALESCE(d.qty, 0) AS quantity,
    c.amount   + COALESCE(d.amt, 0) AS amount
FROM accum.inventory_balance_cache c
LEFT JOIN (
    SELECT dim_hash, SUM(quantity) AS qty, SUM(amount) AS amt
    FROM accum.inventory_balance_cache_delta
    WHERE dim_hash = _hash
    GROUP BY dim_hash
) d USING (dim_hash)
WHERE c.dim_hash = _hash;
```

### Бенчмарки

Очікувані характеристики продуктивності (PostgreSQL 17, SSD, 16 CPU):

| Операція | Стандартний режим | High-Write режим |
|---|---|---|
| `register_post()` 1 рух | ~0.3ms | ~0.15ms |
| `register_post()` batch 100 | ~5ms | ~3ms |
| `register_unpost()` 10 рухів | ~1ms | ~1ms |
| Поточний залишок (точковий) | ~0.1ms | ~0.2ms |
| Залишок на дату | ~1-3ms | ~1-3ms |
| Обороти за місяць | ~1-5ms | ~1-5ms |
| Конкурентний запис (100 writers, різні dim) | ~50K ops/s | ~80K ops/s |
| Конкурентний запис (100 writers, один dim) | ~5K ops/s | ~60K ops/s |

---

## Корекція заднім числом

Детальний алгоритм роботи при корекції даних минулих періодів:

```
Сценарій: В квітні виявлено помилку в березневому надходженні.
          Замість 100 одиниць повинно бути 110.

Крок 1: register_repost('inventory', 'purchase_order:7001', ...)
        або: unpost + post з правильними даними

Крок 2 (автоматично):
  DELETE старий рух (period=березень, qty=100)
    → trigger: totals_month[березень] -= 100
    → trigger: totals_year[2026]      -= 100
    → trigger: balance_cache          -= 100

  INSERT новий рух (period=березень, qty=110, type='adjustment')
    → trigger: totals_month[березень] += 110
    → trigger: totals_year[2026]      += 110
    → trigger: balance_cache          += 110

  Результат:
    totals_month[березень]: +10 (нетто)
    totals_year[2026]:      +10 (нетто)
    balance_cache:          +10 (нетто)
    ❌ totals_month[квітень]: НЕ ЧІПАЄТЬСЯ
    ❌ жодного каскаду

Крок 3: Всі запити повертають коректні дані одразу після COMMIT
```

---

## Рекомендації щодо проєктування

### Вибір вимірів

```
✅ Правильно:
   dimensions = warehouse, product, lot
   (конкретні, обмежена кількість комбінацій)

❌ Проблемно:
   dimensions = user_id, timestamp, request_id
   (занадто гранулярно — мільйони рядків у cache)
```

**Правило**: кількість унікальних комбінацій вимірів = кількість рядків у `balance_cache`. Тримайте це число керованим.

### Вибір гранулярності підсумків

| Сценарій | Рекомендація |
|---|---|
| < 1K рухів/день на dim_hash | `totals_period := 'month'` |
| 1K–100K рухів/день | `totals_period := 'day'` |
| Історія < 2 років | Вистачить `totals_month` |
| Історія > 10 років | Обов'язково `totals_year` + `totals_month` |

### Вибір `recorder`

```sql
-- Хороша практика: тип_документа:id
recorder = 'purchase_order:7001'
recorder = 'sales_invoice:2024-001'
recorder = 'adjustment:manual:2026-04-18:admin'

-- Поганий підхід: без структури
recorder = '7001'        -- незрозуміло що це
recorder = NULL          -- не можна скасувати
```

### Коли вмикати High-Write

```
✅ Увімкніть high_write якщо:
   - > 100 записів/сек в ОДИН dim_hash
   - Спостерігаєте lock waits на balance_cache
   - Допускаєте затримку ~5 сек в balance_cache (читання з буфера все одно актуальне)

❌ НЕ потрібен якщо:
   - Записи розподілені по різних dim_hash
   - < 50 записів/сек загалом
   - Критично важлива простота архітектури
```

---

## Приклади використання

### Складський облік

```sql
-- Створення регістру
SELECT register_create(
    name       := 'warehouse_stock',
    dimensions := '{
        "warehouse":  "int",
        "product":    "int",
        "lot_number": "text",
        "quality":    "text"
    }',
    resources  := '{
        "quantity": "numeric(18,4)",
        "weight":   "numeric(18,3)",
        "cost":     "numeric(18,2)"
    }',
    kind := 'balance'
);

-- Оприбуткування товару
SELECT register_post('warehouse_stock', '{
    "recorder":    "grn:2026-04-001",
    "period":      "2026-04-18",
    "warehouse":   1,
    "product":     42,
    "lot_number":  "LOT-2026-04-A",
    "quality":     "grade_a",
    "quantity":    1000,
    "weight":      500.000,
    "cost":        25000.00
}');

-- Відвантаження
SELECT register_post('warehouse_stock', '{
    "recorder":    "shipment:2026-04-055",
    "period":      "2026-04-18",
    "warehouse":   1,
    "product":     42,
    "lot_number":  "LOT-2026-04-A",
    "quality":     "grade_a",
    "quantity":    -150,
    "weight":      -75.000,
    "cost":        -3750.00
}');

-- Перевірити залишок
SELECT * FROM accum.warehouse_stock_balance(
    dimensions := '{"warehouse": 1, "product": 42}'
);
-- quantity: 850, weight: 425.000, cost: 21250.00

-- Залишки по всіх складах
SELECT warehouse_id, SUM(quantity) AS qty, SUM(cost) AS total_cost
FROM accum.warehouse_stock_balance_cache
WHERE product_id = 42 AND quantity > 0
GROUP BY warehouse_id;
```

### Фінансові транзакції

```sql
SELECT register_create(
    name       := 'account_balance',
    dimensions := '{
        "account":    "int",
        "currency":   "text",
        "cost_center":"int"
    }',
    resources  := '{
        "debit":  "numeric(18,2)",
        "credit": "numeric(18,2)",
        "net":    "numeric(18,2)"
    }',
    kind := 'balance'
);

-- Переказ між рахунками (2 рухи в одній транзакції)
BEGIN;
    SELECT register_post('account_balance', '[
        {
            "recorder":    "transfer:T-2026-001",
            "period":      "2026-04-18",
            "account":     1001,
            "currency":    "UAH",
            "cost_center": 10,
            "debit":       0,
            "credit":      5000.00,
            "net":         -5000.00
        },
        {
            "recorder":    "transfer:T-2026-001",
            "period":      "2026-04-18",
            "account":     2001,
            "currency":    "UAH",
            "cost_center": 20,
            "debit":       5000.00,
            "credit":      0,
            "net":         5000.00
        }
    ]');
COMMIT;

-- Баланс рахунку
SELECT * FROM accum.account_balance_balance(
    dimensions := '{"account": 1001, "currency": "UAH"}'
);

-- Обороти по рахунку за місяць
SELECT * FROM accum.account_balance_turnover(
    from_date  := '2026-04-01',
    to_date    := '2026-04-30',
    dimensions := '{"account": 1001}'
);
```

### Облік підписок

```sql
SELECT register_create(
    name       := 'subscription_usage',
    dimensions := '{
        "tenant":   "uuid",
        "plan":     "text",
        "feature":  "text"
    }',
    resources  := '{
        "used":  "bigint",
        "limit": "bigint"
    }',
    kind       := 'balance',
    high_write := true          -- багато записів на один tenant
);

-- Списати використання
SELECT register_post('subscription_usage', '{
    "recorder": "api_call:req_abc123",
    "period":   "2026-04-18T14:30:00Z",
    "tenant":   "550e8400-e29b-41d4-a716-446655440000",
    "plan":     "pro",
    "feature":  "api_calls",
    "used":     1,
    "limit":    0
}');

-- Перевірка ліміту (O(1))
SELECT used
FROM accum.subscription_usage_balance_cache
WHERE tenant = '550e8400-e29b-41d4-a716-446655440000'
  AND feature = 'api_calls';
```

---

## Діагностика та обслуговування

### Перевірка консистентності

```sql
-- Порівняти balance_cache з фактичною сумою рухів
SELECT * FROM register_verify('inventory');

-- Результат:
-- dim_hash | cache_qty | actual_qty | status
-- ---------+-----------+------------+--------
-- 123456   | 100.0000  | 100.0000   | OK
-- 789012   | 50.0000   | 50.0000    | OK

-- Або при розбіжності:
-- 345678   | 99.0000   | 100.0000   | MISMATCH  ← потребує перебудови
```

### Перебудова підсумків та кешу

```sql
-- Повна перебудова підсумків з рухів
SELECT register_rebuild_totals('inventory');

-- Перебудова тільки balance_cache
SELECT register_rebuild_cache('inventory');

-- Перебудова конкретного dim_hash
SELECT register_rebuild_cache('inventory', dim_hash := 123456);
```

### Моніторинг

```sql
-- Статистика регістру
SELECT * FROM register_stats('inventory');

-- Результат:
-- movements_count:      1,234,567
-- partitions_count:     48
-- cache_rows:           8,456
-- totals_month_rows:    101,472
-- totals_year_rows:     16,912
-- delta_buffer_pending: 0         (high_write mode)
-- last_delta_merge:     NULL
-- avg_write_latency:    0.28ms
-- avg_read_latency:     0.09ms
```

### Управління партиціями

```sql
-- Створити партиції наперед
SELECT register_create_partitions('inventory', ahead := '3 months');

-- Від'єднати ста��і партиції
SELECT register_detach_partitions('inventory', older_than := '2 years');

-- Переглянути партиції
SELECT * FROM register_partitions('inventory');
```

---

## Конфігурація

Параметри в `postgresql.conf` або `ALTER SYSTEM`:

```ini
# Схема для об'єктів розширення
pg_accumulator.schema = 'accum'

# Delta buffer (high_write mode)
pg_accumulator.delta_merge_interval = '5s'       # Як часто зливати дельти
pg_accumulator.delta_merge_delay = '2s'           # Мінімальний вік дельти перед злиттям
pg_accumulator.delta_merge_batch_size = 10000     # Макс. дельт за одне злиття

# Партиціювання
pg_accumulator.auto_create_partitions = on        # Автоматичне створення партицій
pg_accumulator.partitions_ahead = '3 months'      # Створювати наперед

# Background worker
pg_accumulator.background_workers = 2             # Кількість фонових процесів
pg_accumulator.maintenance_interval = '1 hour'    # Інтервал обслуговування

# Hash
pg_accumulator.hash_function = 'xxhash64'         # Функція хешування вимірів
                                                   # Варіанти: xxhash64, murmur3
```

---

## Сумісність

| Компонент | Мінімальна версія | Рекомендована |
|---|---|---|
| PostgreSQL | 15 | 17+ |
| pg_cron (опціонально) | 1.5 | 1.6+ |
| pg_partman (опціонально) | 4.7 | 5.0+ |

**Підтримувані типи вимірів:** `int`, `bigint`, `smallint`, `text`, `varchar(n)`, `uuid`, `date`, `boolean`

**Підтримувані типи ресурсів:** `numeric(p,s)`, `integer`, `bigint`, `double precision`, `real`

---

## FAQ

**Q: Що станеться при одночасному `register_post()` і `register_unpost()` одного recorder?**
A: PostgreSQL row-level locks гарантують серіалізацію. Одна операція почекає завершення іншої. Результат завжди консистентний.

**Q: Чи можна використовувати розширення з логічною реплікацією?**
A: Так. Всі таблиці — стандартні PostgreSQL-таблиці. Рекомендується реплікувати тільки `movements`, а підсумки та кеш перебудовувати на репліці.

**Q: Що, якщо `dim_hash` має колізію?**
A: Використовується 64-bit hash (xxhash64). Імовірність колізії < 1/10^18. Додатково тригер перевіряє повну рівність вимірів при UPSERT в кеш.

**Q: Чи можна мати виміри з NULL?**
A: Так. NULL вважається окремим значенням виміру та має окремий `dim_hash`. `(warehouse=1, lot=NULL)` і `(warehouse=1, lot='A')` — різні комбінації.

**Q: Як видалити старі дані?**
A: Використовуйте `register_detach_partitions()` для від'єднання старих партицій рухів. Після цього запустіть `register_rebuild_totals()` для перерахунку підсумків.

**Q: Чи підтримується multi-tenancy?**
A: Додайте `tenant_id` як вимір. Кожен tenant матиме ізольовані залишки. Для фізичної ізоляції — використовуйте Row Level Security (RLS) на таблицях рухів та кешу.

---

## Ліцензія

pg_accumulator розповсюджується під ліцензією PostgreSQL License.