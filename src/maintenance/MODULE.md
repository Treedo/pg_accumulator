# Модуль: Maintenance (Діагностика та обслуговування)

## Призначення
Інструменти для перевірки консистентності даних, перебудови підсумків та кешу, збору статистики. Забезпечує можливість відновлення та моніторингу стану регістрів.

## Файли
- `verify.c` — Реалізація `register_verify()`
- `rebuild.c` — Реалізація `register_rebuild_totals()`, `register_rebuild_cache()`
- `stats.c` — Реалізація `register_stats()`

## Відповідальність

### 1. `register_verify(name)` (`verify.c`)

Порівнює balance_cache з фактичною сумою рухів та перевіряє підсумки.

**Алгоритм:**
```sql
-- Перевірка balance_cache
WITH actual AS (
    SELECT dim_hash,
           SUM(<res1>) AS <res1>,
           SUM(<res2>) AS <res2>
    FROM accum.<name>_movements
    GROUP BY dim_hash
),
cached AS (
    SELECT dim_hash, <res1>, <res2>
    FROM accum.<name>_balance_cache
)
SELECT
    COALESCE(a.dim_hash, c.dim_hash) AS dim_hash,
    c.<res1> AS cache_<res1>,
    a.<res1> AS actual_<res1>,
    CASE
        WHEN c.<res1> = a.<res1> THEN 'OK'
        WHEN c.dim_hash IS NULL THEN 'MISSING_IN_CACHE'
        WHEN a.dim_hash IS NULL THEN 'ORPHAN_IN_CACHE'
        ELSE 'MISMATCH'
    END AS status
FROM actual a
FULL OUTER JOIN cached c USING (dim_hash);
```

**Перевірка підсумків:**
```
Для кожного (dim_hash, month):
  SUM(movements WHERE period IN month) == totals_month
Для кожного (dim_hash, year):
  SUM(totals_month WHERE period IN year) == totals_year
```

**Повертає:** SETOF (dim_hash, cache_value, actual_value, status)

### 2. `register_rebuild_totals(name)` (`rebuild.c`)

Повна перебудова підсумків з рухів.

**Алгоритм:**
```sql
-- 1. Очистити
TRUNCATE accum.<name>_totals_month;
TRUNCATE accum.<name>_totals_year;

-- 2. Перерахувати totals_month
INSERT INTO accum.<name>_totals_month (dim_hash, period, <dims>, <resources>)
SELECT dim_hash,
       date_trunc('month', period) AS period,
       <first dim values>,
       SUM(<res1>), SUM(<res2>)
FROM accum.<name>_movements
GROUP BY dim_hash, date_trunc('month', period), <dim columns>;

-- 3. Перерахувати totals_year
INSERT INTO accum.<name>_totals_year (dim_hash, period, <dims>, <resources>)
SELECT dim_hash,
       date_trunc('year', period) AS period,
       <first dim values>,
       SUM(<res1>), SUM(<res2>)
FROM accum.<name>_totals_month
GROUP BY dim_hash, date_trunc('year', period), <dim columns>;
```

**Повертає:** кількість перебудованих рядків

### 3. `register_rebuild_cache(name, dim_hash)` (`rebuild.c`)

Перебудова balance_cache (повна або часткова).

**Повна перебудова:**
```sql
TRUNCATE accum.<name>_balance_cache;
-- Також злити дельти якщо high_write
DELETE FROM accum.<name>_balance_cache_delta;

INSERT INTO accum.<name>_balance_cache (dim_hash, <dims>, <resources>, ...)
SELECT dim_hash,
       <first dim values>,
       SUM(<res1>), SUM(<res2>),
       MAX(recorded_at),
       -- last_movement_id потрібен subquery
FROM accum.<name>_movements
GROUP BY dim_hash, <dim columns>;
```

**Часткова перебудова (конкретний dim_hash):**
```sql
DELETE FROM accum.<name>_balance_cache WHERE dim_hash = $1;

INSERT INTO accum.<name>_balance_cache (...)
SELECT ...
FROM accum.<name>_movements
WHERE dim_hash = $1
GROUP BY ...;
```

### 4. `register_stats(name)` (`stats.c`)

Збирає статистику регістру:
```json
{
    "movements_count":      1234567,
    "partitions_count":     48,
    "cache_rows":           8456,
    "totals_month_rows":    101472,
    "totals_year_rows":     16912,
    "delta_buffer_pending": 0,
    "last_delta_merge":     null,
    "table_sizes": {
        "movements": "2.1 GB",
        "totals_month": "45 MB",
        "totals_year": "8 MB",
        "balance_cache": "3 MB"
    }
}
```

Використовує `pg_relation_size()`, `pg_stat_user_tables`, та COUNT запити.

## Залежності
- `core/registry` — метадані регістру
- `ddl` — інформація про структуру таблиць

## SQL-файли
- `sql/10_maintenance.sql` — Функції обслуговування

## Тести
- verify на консистентному регістрі → всі OK
- Штучний MISMATCH (прямий UPDATE cache) → verify виявляє
- rebuild_totals → підсумки перераховані коректно
- rebuild_cache → кеш відповідає сумі рухів
- rebuild_cache часткова → тільки один dim_hash перебудований
- stats → повертає валідні числа
- verify + rebuild circle: verify MISMATCH → rebuild → verify OK
- rebuild на порожньому регістрі → працює (0 рядків)
- rebuild великого регістру → не блокує читачів (read committed)
