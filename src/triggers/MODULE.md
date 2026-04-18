# Модуль: Triggers (Тригерний ланцюг)

## Призначення
Генерація та управління тригерами, які автоматично оновлюють підсумки (totals) та кеш залишків (balance_cache) при кожній зміні в таблиці рухів. Забезпечує синхронну актуалізацію всіх похідних даних у тій самій транзакції.

## Файли
- `trigger_engine.c` — Генерація та реєстрація тригерних функцій
- `trigger_totals.c` — Логіка оновлення totals_month і totals_year
- `trigger_cache.c` — Логіка оновлення balance_cache (стандартний + high_write режим)

## Відповідальність

### 1. BEFORE INSERT тригер (`trigger_engine.c`)
Виконується перед фізичним записом руху:
```
1. Обчислити dim_hash = _hash_<register>(dim1, dim2, ...)
2. Встановити recorded_at = now()
3. Визначити movement_type:
   - 'regular' — стандартний рух
   - 'adjustment' — якщо period < now() (корекція заднім числом)
   - 'reversal' — якщо створений через unpost
4. Валідувати обов'язкові поля
```

### 2. AFTER INSERT тригер — оновлення підсумків (`trigger_totals.c`)
```sql
-- Для кожного вставленого руху:

-- 2.1 Оновити обороти за місяць
INSERT INTO accum.<name>_totals_month (dim_hash, period, <dims>, <resources>)
VALUES (_dim_hash, date_trunc('month', NEW.period), <dim_values>, <res_values>)
ON CONFLICT (dim_hash, period) DO UPDATE SET
    <res1> = <name>_totals_month.<res1> + EXCLUDED.<res1>,
    <res2> = <name>_totals_month.<res2> + EXCLUDED.<res2>;

-- 2.2 Оновити обороти за рік
INSERT INTO accum.<name>_totals_year (dim_hash, period, <dims>, <resources>)
VALUES (_dim_hash, date_trunc('year', NEW.period), <dim_values>, <res_values>)
ON CONFLICT (dim_hash, period) DO UPDATE SET
    <res1> = <name>_totals_year.<res1> + EXCLUDED.<res1>,
    <res2> = <name>_totals_year.<res2> + EXCLUDED.<res2>;
```

### 3. AFTER INSERT тригер — оновлення кешу (`trigger_cache.c`)

**Стандартний режим (high_write = false):**
```sql
INSERT INTO accum.<name>_balance_cache (dim_hash, <dims>, <resources>, ...)
VALUES (_dim_hash, <dim_values>, <res_values>, now(), NEW.id, 1)
ON CONFLICT (dim_hash) DO UPDATE SET
    <res1> = <name>_balance_cache.<res1> + EXCLUDED.<res1>,
    <res2> = <name>_balance_cache.<res2> + EXCLUDED.<res2>,
    last_movement_at = EXCLUDED.last_movement_at,
    last_movement_id = EXCLUDED.last_movement_id,
    version = <name>_balance_cache.version + 1;
```

**High-write режим (high_write = true):**
```sql
-- Замість UPDATE cache — дешевий INSERT delta
INSERT INTO accum.<name>_balance_cache_delta (dim_hash, <resources>)
VALUES (_dim_hash, <res_values>);
```

### 4. AFTER DELETE тригер (`trigger_totals.c` + `trigger_cache.c`)
Обернена логіка: віднімає ресурси з підсумків та кешу при видаленні руху.
```
totals_month[period] -= OLD.resource
totals_year[period]  -= OLD.resource
balance_cache        -= OLD.resource
```

### 5. Перевірка консистентності при UPSERT
При `ON CONFLICT` в balance_cache додатково перевіряється повна рівність вимірів,
щоб виключити false-positive при колізії dim_hash (хоча ймовірність <1/10^18).

## Ланцюг подій (повна послідовність)
```
INSERT INTO movements
  │
  ├── BEFORE INSERT: обчислити dim_hash, validated
  │
  ├── [INSERT фізично]
  │
  └── AFTER INSERT:
        ├── UPDATE totals_month (UPSERT)
        ├── UPDATE totals_year (UPSERT)
        └── UPSERT balance_cache (або INSERT delta)
```

## Залежності
- `hash` — для обчислення dim_hash
- `core/registry` — для отримання метаданих (список ресурсів)

## SQL-файли
- `sql/04_triggers.sql` — Шаблони тригерних функцій

## Тести
- INSERT → перевірити що totals оновлені
- INSERT → перевірити що balance_cache оновлений
- DELETE → перевірити що totals зменшені
- DELETE → перевірити що cache зменшений
- Batch INSERT → перевірити коректність агрегації
- INSERT з різними period → перевірити розподіл по totals_month/year
- Корекція заднім числом → перевірити movement_type = 'adjustment'
- ROLLBACK → перевірити що cache не змінився
- Конкурентні INSERT на різні dim_hash → немає блокувань
- Конкурентні INSERT на один dim_hash → серіалізація через row lock
- High-write mode → INSERT delta замість UPDATE cache
