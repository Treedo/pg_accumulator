# Модуль: Registry API (Управління регістрами)

## Призначення
Публічний API для створення, зміни, видалення та інспекції регістрів накопичення. Це головний інтерфейс адміністратора/розробника для декларативного управління обліковою інфраструктурою.

## Файли
- `create.c` — Реалізація `register_create()`
- `alter.c` — Реалізація `register_alter()`
- `drop.c` — Реалізація `register_drop()`
- `list.c` — Реалізація `register_list()`
- `info.c` — Реалізація `register_info()`

## Відповідальність

### 1. `register_create(name, dimensions, resources, kind, ...)` (`create.c`)

Оркестрація створення повної інфраструктури регістру.

**Послідовність:**
```
1. Валідація вхідних параметрів:
   a) name — regex ^[a-z][a-z0-9_]*$, не зайнято
   b) dimensions — валідний JSON, типи підтримуються
   c) resources — валідний JSON, числові типи
   d) kind — 'balance' або 'turnover'
   e) totals_period — 'day', 'month', 'year'
   f) partition_by — 'day', 'month', 'quarter', 'year'

2. Зберегти метадані в _registers (core/registry)

3. Генерація інфраструктури (ddl):
   a) CREATE TABLE movements (partitioned)
   b) CREATE TABLE totals_month
   c) CREATE TABLE totals_year
   d) CREATE TABLE balance_cache (якщо kind='balance')
   e) CREATE TABLE balance_cache_delta (якщо high_write=true)
   f) CREATE INDEX (усі необхідні)
   g) CREATE FUNCTION _hash_<name>()
   h) CREATE FUNCTION <name>_balance()
   i) CREATE FUNCTION <name>_turnover()
   j) CREATE FUNCTION <name>_movements()

4. Генерація тригерів (triggers):
   a) BEFORE INSERT trigger на movements
   b) AFTER INSERT trigger на movements
   c) AFTER DELETE trigger на movements

5. Створення початкових партицій (partitioning):
   a) Поточний місяць
   b) + N місяців наперед (згідно конфігурації)
   c) Default партиція
```

### 2. `register_alter(name, add_dimensions, add_resources, high_write, ...)` (`alter.c`)

Зміна структури існуючого регістру.

**Підтримувані операції:**
- Додавання нових вимірів (потребує перерахунку)
- Додавання нових ресурсів (без перерахунку, DEFAULT 0)
- Зміна high_write режиму
- Зміна параметрів партиціювання

**Обмеження:**
- Неможливо видалити вимір
- Неможливо змінити тип існуючого виміру/ресурсу
- Неможливо змінити kind (balance ↔ turnover)

**Перерахунок при додаванні виміру:**
```
1. ALTER TABLE movements ADD COLUMN <dim> <type>
2. ALTER TABLE totals_month ADD COLUMN <dim> <type>
3. ALTER TABLE totals_year ADD COLUMN <dim> <type>
4. ALTER TABLE balance_cache ADD COLUMN <dim> <type>
5. Перегенерувати _hash_<name>() з новим виміром
6. UPDATE movements SET dim_hash = _hash_<name>(<all dims>)
7. TRUNCATE totals_month, totals_year, balance_cache
8. Перебудувати підсумки з рухів (register_rebuild_totals)
→ Виконується в online-режимі без блокування читачів
```

### 3. `register_drop(name, force)` (`drop.c`)

Видалення регістру та всієї інфраструктури.

**Алгоритм:**
```
1. Перевірка існування
2. Якщо force=false і є дані → ERROR з підказкою
3. DROP TABLE movements CASCADE
4. DROP TABLE totals_month
5. DROP TABLE totals_year
6. DROP TABLE balance_cache (якщо існує)
7. DROP TABLE balance_cache_delta (якщо існує)
8. DROP FUNCTION _hash_<name>
9. DROP FUNCTION <name>_balance (якщо існує)
10. DROP FUNCTION <name>_turnover
11. DROP FUNCTION <name>_movements
12. DELETE FROM _registers WHERE name = $1
```

### 4. `register_list()` (`list.c`)
Повертає `SETOF RECORD` з колонками: name, kind, dimensions count, resources count, movements_count, created_at.

### 5. `register_info(name)` (`info.c`)
Повертає JSONB з повною інформацією: структура, статистика, партиції.

## Залежності
- `core/registry` — збереження метаданих
- `ddl` — генерація інфраструктури
- `triggers` — генерація тригерів
- `hash` — генерація хеш-функцій
- `partitioning` — створення початкових партицій

## SQL-файли
- `sql/07_registry_api.sql` — SQL-обгортки для C-функцій

## Тести
- create → вся інфраструктура створена
- create дублікат → ERROR
- create з невалідним ім'ям → ERROR
- alter → додавання виміру працює
- alter → додавання ресурсу працює
- alter → зміна high_write
- drop → все видалено
- drop без force з даними → ERROR
- list → повертає всі регістри
- info → повна інформація
- create balance vs turnover → різна інфраструктура
