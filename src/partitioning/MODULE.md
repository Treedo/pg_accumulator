# Модуль: Partitioning (Управління партиціями)

## Призначення
Автоматичне створення, управління та обслуговування партицій таблиці рухів (movements). Забезпечує partition pruning при запитах по періоду та контрольоване зростання таблиці.

## Файли
- `partition_manager.c` — Основна логіка управління партиціями
- `auto_create.c` — Автоматичне створення партицій наперед

## Відповідальність

### 1. Створення партицій при `register_create()` (`partition_manager.c`)

Стратегії партиціювання за `partition_by`:
```
'day'     → одна партиція на день     (movements_2026_04_18)
'month'   → одна партиція на місяць   (movements_2026_04)
'quarter' → одна партиція на квартал  (movements_2026_q1)
'year'    → одна партиція на рік      (movements_2026)
```

Початкове створення:
```sql
-- partition_by = 'month'
CREATE TABLE accum.inventory_movements_2026_04
    PARTITION OF accum.inventory_movements
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');

-- + N місяців наперед (pg_accumulator.partitions_ahead)
CREATE TABLE accum.inventory_movements_2026_05 ...
CREATE TABLE accum.inventory_movements_2026_06 ...
CREATE TABLE accum.inventory_movements_2026_07 ...

-- Default partition (всі інші)
CREATE TABLE accum.inventory_movements_default
    PARTITION OF accum.inventory_movements DEFAULT;
```

### 2. Автоматичне створення наперед (`auto_create.c`)

Background worker або trigger при INSERT:
```
1. Перевірити: існує партиція для period + partitions_ahead?
2. Якщо ні → CREATE PARTITION
3. Lock: advisory lock для уникнення конкурентного створення
```

Два режими:
- **Eager** (background worker): періодично створює партиції наперед
- **Lazy** (trigger): створює при першому INSERT в новий період

### 3. `register_create_partitions(name, ahead)` (`partition_manager.c`)

Ручне створення партицій:
```sql
SELECT register_create_partitions('inventory', ahead := '6 months');
-- Створить партиції на 6 місяців вперед від поточної дати
```

### 4. `register_detach_partitions(name, older_than)` (`partition_manager.c`)

Від'єднання старих партицій (для архівації або видалення):
```sql
SELECT register_detach_partitions('inventory', older_than := '2 years');

-- Від'єднує:
ALTER TABLE accum.inventory_movements
    DETACH PARTITION accum.inventory_movements_2024_01;
-- ...
-- Партиції не видаляються, лише від'єднуються
-- Можна DROP TABLE окремо або переміст в archive
```

### 5. `register_partitions(name)` (`partition_manager.c`)

Список партицій з метаданими:
```sql
SELECT * FROM register_partitions('inventory');
-- partition_name       | from_date  | to_date    | rows    | size
-- ---------------------+------------+------------+---------+--------
-- inventory_mv_2026_01 | 2026-01-01 | 2026-02-01 | 45,231  | 12 MB
-- inventory_mv_2026_02 | 2026-02-01 | 2026-03-01 | 52,108  | 14 MB
```

### 6. Іменування партицій
```
Формат: <register>_movements_<period_suffix>

period_suffix залежно від partition_by:
  'day'     → YYYY_MM_DD  (2026_04_18)
  'month'   → YYYY_MM     (2026_04)
  'quarter' → YYYY_qN     (2026_q2)
  'year'    → YYYY        (2026)
```

## Залежності
- `core/registry` — метадані (partition_by)
- `bgworker` — для eager режиму автоматичного створення

## SQL-файли
- `sql/09_partitioning.sql` — Функції управління партиціями

## Тести
- Створення регістру → початкові партиції створені
- INSERT в новий період → партиція створена автоматично
- Partition pruning: запит по period → скануються тільки релевантні партиції (EXPLAIN)
- Detach → партиція від'єднана, рухи недоступні
- create_partitions наперед → партиції створені
- Різні partition_by → коректні діапазони
- Default partition → ловить дані поза діапазоном
- Конкурентне створення партицій → advisory lock запобігає дублюванню
