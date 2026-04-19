# Модуль: DDL Generator (Генерація структур даних)

## Призначення
Генерація всіх DDL-інструкцій (CREATE TABLE, CREATE INDEX, CREATE FUNCTION) при створенні нового регістру. Модуль перетворює декларативний опис регістру на конкретну інфраструктуру PostgreSQL.

## Файли
- `ddl_generator.c` — Координатор генерації, головна функція генерації
- `ddl_tables.c` — Генерація таблиць (movements, totals, cache)
- `ddl_indexes.c` — Генерація індексів
- `ddl_functions.c` — Генерація SQL/PLPGSQL-функцій читання

## Відповідальність

### 1. Генерація таблиці рухів (`ddl_tables.c`)
```sql
-- Генерується для кожного регістру:
CREATE TABLE accum.<name>_movements (
    id             uuid          DEFAULT gen_random_uuid(),
    recorded_at    timestamptz   DEFAULT now() NOT NULL,
    recorder       <recorder_type> NOT NULL,
    period         timestamptz   NOT NULL,
    movement_type  text          DEFAULT 'regular' NOT NULL,
    dim_hash       bigint        NOT NULL,
    -- динамічні колонки вимірів
    <dim1>         <type1>       NOT NULL,
    <dim2>         <type2>       NOT NULL,
    ...
    -- динамічні колонки ресурсів
    <res1>         <type1>       NOT NULL DEFAULT 0,
    <res2>         <type2>       NOT NULL DEFAULT 0,
    ...
    PRIMARY KEY (id, period)
) PARTITION BY RANGE (period);
```

### 2. Генерація таблиць підсумків (`ddl_tables.c`)
- `<name>_totals_month` — обороти за місяць, PK: (dim_hash, period)
- `<name>_totals_year` — обороти за рік, PK: (dim_hash, period)
- Включає денормалізовані виміри + ресурси (які підсумовуються)
- Гранулярність залежить від `totals_period`

### 3. Генерація balance_cache (`ddl_tables.c`)
- Тільки для `kind='balance'`
- PK: `dim_hash`
- Денормалізовані виміри + поточні залишки ресурсів
- Службові поля: `last_movement_at`, `last_movement_id`, `version`

### 4. Генерація delta buffer (`ddl_tables.c`)
- Тільки якщо `high_write=true`
- `UNLOGGED TABLE` з `bigserial` PK
- Ресурсні колонки + `dim_hash` + `created_at`

### 5. Генерація індексів (`ddl_indexes.c`)
```
movements:
  - (dim_hash, period) — основний lookup
  - (recorder) — для unpost/repost
  - (period) — для partition pruning

balance_cache:
  - (dim1) — для фільтрованих запитів
  - (dim1, dim2) — для часто фільтрованих пар
  - Індекси створюються для кожного виміру окремо

totals:
  - PK (dim_hash, period) — покриваючий
```

### 6. Генерація функцій читання (`ddl_functions.c`)
- `<name>_balance(dimensions, at_date)` — обчислення залишку
- `<name>_turnover(from_date, to_date, dimensions, group_by)` — обороти
- `<name>_movements(recorder, from_date, to_date, dimensions)` — фільтрація рухів

## Безпека
- Всі ідентифікатори обробляються через `quote_ident()` для захисту від SQL injection
- Типи даних валідуються перед генерацією DDL
- Імена регістрів перевіряються regex `^[a-z][a-z0-9_]*$`

## Залежності
- `core/registry` — для отримання метаданих регістру
- `hash` — для генерації хеш-функцій

## SQL-файли
- `sql/03_ddl.sql` — Шаблони та допоміжні функції генерації

## Тести
- Створення регістру з різними наборами вимірів/ресурсів
- Перевірка структури всіх створених таблиць
- Перевірка наявності всіх індексів
- Перевірка генерованих функцій
- Валідація некоректних типів (має повернути помилку)
- Перевірка захисту від SQL injection в іменах
- Створення регістру `balance` vs `turnover` (різна інфраструктура)
