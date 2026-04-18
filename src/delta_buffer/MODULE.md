# Модуль: Delta Buffer (High-Write Mode)

## Призначення
Забезпечення масштабованості при високому contention на один dim_hash. Замість UPDATE balance_cache (який блокує рядок), виконується INSERT у append-only delta buffer, який періодично зливається з основним кешем фоновим процесом.

## Файли
- `delta.c` — Логіка запису в delta buffer та читання з урахуванням буфера
- `merge.c` — Логіка злиття (merge) дельт у balance_cache

## Відповідальність

### 1. Delta запис (`delta.c`)

При `high_write = true` тригер на movements замість:
```sql
-- Стандартний режим (lock на рядок)
UPDATE balance_cache SET qty += delta WHERE dim_hash = X
```
Робить:
```sql
-- High-write режим (без lock)
INSERT INTO balance_cache_delta (dim_hash, qty, amount, created_at)
VALUES (X, delta_qty, delta_amount, now())
```

**Характеристики:**
- Таблиця `UNLOGGED` — не пишеться в WAL (швидше)
- `bigserial` PK — мінімальний overhead
- Жодних UPDATE/блокувань — тільки INSERT
- **ВАЖЛИВО:** дані delta buffer втрачаються при crash (UNLOGGED)
  → При відновленні потрібен `register_rebuild_cache()`

### 2. Читання з урахуванням дельт (`delta.c`)

Функція `_balance_with_delta()`:
```sql
SELECT
    c.<res> + COALESCE(d.<res>, 0) AS <res>
FROM balance_cache c
LEFT JOIN (
    SELECT dim_hash, SUM(<res>) AS <res>
    FROM balance_cache_delta
    WHERE dim_hash = $1
    GROUP BY dim_hash
) d USING (dim_hash)
WHERE c.dim_hash = $1;
```

### 3. Delta Merge (`merge.c`)

Фоновий процес, який зливає накопичені дельти в balance_cache:

```sql
WITH consumed AS (
    DELETE FROM balance_cache_delta
    WHERE created_at < now() - interval '<delta_merge_delay>'
    RETURNING dim_hash, <resources>
),
agg AS (
    SELECT dim_hash, SUM(<res1>) AS <res1>, SUM(<res2>) AS <res2>
    FROM consumed
    GROUP BY dim_hash
)
UPDATE balance_cache c
SET <res1> = c.<res1> + a.<res1>,
    <res2> = c.<res2> + a.<res2>,
    version = c.version + 1
FROM agg a
WHERE c.dim_hash = a.dim_hash;
```

**Параметри:**
| GUC | Опис | Default |
|-----|------|---------|
| `delta_merge_interval` | Як часто запускати merge | 5s |
| `delta_merge_delay` | Мінімальний вік дельти | 2s |
| `delta_merge_batch_size` | Макс. дельт за раз | 10000 |

### 4. Таблиця delta buffer
```sql
CREATE UNLOGGED TABLE accum.<name>_balance_cache_delta (
    id         bigserial     PRIMARY KEY,
    dim_hash   bigint        NOT NULL,
    <res1>     <type1>       NOT NULL DEFAULT 0,
    <res2>     <type2>       NOT NULL DEFAULT 0,
    created_at timestamptz   DEFAULT now()
);

CREATE INDEX ON accum.<name>_balance_cache_delta (dim_hash);
CREATE INDEX ON accum.<name>_balance_cache_delta (created_at);
```

## Залежності
- `core/registry` — перевірка high_write режиму
- `triggers` — інтеграція з тригерним ланцюгом
- `bgworker` — запуск merge процесу

## SQL-файли
- `sql/08_delta_buffer.sql` — DDL та функції delta buffer

## Тести
- high_write POST → дельти записані
- Читання з pending дельтами → правильний баланс
- Merge → дельти злиті, buffer порожній
- Merge часткового batch → старі дельти злиті, нові лишились
- Конкурентний запис (100 writers, 1 dim_hash) → немає deadlock
- Crash recovery → rebuild cache із movements
- Переключення high_write → дельти злиті, режим змінений
- Продуктивність: 60K ops/s на один dim_hash
