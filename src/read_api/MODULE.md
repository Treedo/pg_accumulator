# Модуль: Read API (Читання даних)

## Призначення
Функції для отримання залишків, оборотів та рухів з регістрів накопичення. Оптимізовані запити з використанням ієрархії підсумків для швидкого обчислення.

## Файли
- `balance.c` — Генерація та реалізація `<register>_balance()`
- `turnover.c` — Генерація та реалізація `<register>_turnover()`
- `movements.c` — Генерація та реалізація `<register>_movements()`

## Відповідальність

### 1. `<register>_balance(dimensions, at_date)` (`balance.c`)

Повертає залишок ресурсів. Доступна тільки для `kind='balance'`.

**Два режими:**

#### Поточний залишок (at_date IS NULL)
```
SELECT <resources>
FROM accum.<register>_balance_cache
WHERE dim_hash = _hash(dimensions)
```
- Складність: O(1) — один lookup в cache
- Час: ~0.1ms

Якщо `dimensions` задано частково (не всі виміри):
```
SELECT SUM(<res1>), SUM(<res2>)
FROM accum.<register>_balance_cache
WHERE <задані виміри> = <значення>
```
- Агрегація по наявних індексах

#### Залишок на дату (at_date IS NOT NULL)
```
Алгоритм "складання ієрархії":

1. SUM(totals_year WHERE period < date_trunc('year', at_date))
   → Усі повні роки до at_date

2. + SUM(totals_month WHERE period >= date_trunc('year', at_date)
                         AND period < date_trunc('month', at_date))
   → Повні місяці поточного року до at_date

3. + SUM(movements WHERE period >= date_trunc('month', at_date)
                      AND period <= at_date)
   → Окремі рухи поточного місяця до at_date

Максимум сканованих рядків: ~20 + ~11 + ~31 = ~62
```

#### High-write mode — доповнення
```sql
-- Поточний залишок = cache + pending deltas
SELECT
    c.<res1> + COALESCE(d.<res1>, 0),
    c.<res2> + COALESCE(d.<res2>, 0)
FROM accum.<register>_balance_cache c
LEFT JOIN (
    SELECT dim_hash, SUM(<res1>) AS <res1>, SUM(<res2>) AS <res2>
    FROM accum.<register>_balance_cache_delta
    WHERE dim_hash = _hash
    GROUP BY dim_hash
) d USING (dim_hash)
WHERE c.dim_hash = _hash;
```

### 2. `<register>_turnover(from_date, to_date, dimensions, group_by)` (`turnover.c`)

Повертає обороти (нетто) за період.

**Алгоритм оптимізації:**
```
1. Повні місяці в діапазоні → з totals_month
2. Неповні місяці (початок/кінець) → з movements
3. Якщо діапазон >= 1 рік → використати totals_year для повних років

Приклад: turnover 15 лютого — 20 квітня
  → movements 15-28 лютого (неповний місяць)
  → totals_month березень (повний місяць)
  → movements 1-20 квітня (неповний місяць)
```

**group_by:**
- Якщо вказано `group_by := '{"product"}'`, результат розгортається по вказаному виміру
- Генерується `GROUP BY product_id` у фінальному запиті

### 3. `<register>_movements(recorder, from_date, to_date, dimensions)` (`movements.c`)

Повертає рухи з фільтрацією. Пряма вибірка з таблиці movements з фільтрами.

**Фільтри:**
```sql
SELECT * FROM accum.<register>_movements
WHERE TRUE
  AND (recorder = $1 OR $1 IS NULL)           -- фільтр по документу
  AND (period >= $2 OR $2 IS NULL)             -- фільтр по початку
  AND (period <= $3 OR $3 IS NULL)             -- фільтр по кінцю
  AND (dim_hash = _hash($4) OR $4 IS NULL)    -- фільтр по вимірах
ORDER BY period, recorded_at;
```

## Залежності
- `core/registry` — метадані для визначення структури
- `hash` — для обчислення dim_hash при фільтрації
- `delta_buffer` — для high-write mode читання

## SQL-файли
- `sql/06_read_api.sql` — Шаблони генерації функцій читання

## Тести
- balance() поточний → повертає правильний залишок
- balance() на дату → ієрархічне обчислення коректне
- balance() з частковими вимірами → агрегація
- balance() без фільтрів → загальний залишок
- balance() для turnover-регістру → ERROR
- turnover() за повний місяць → з totals_month
- turnover() за неповний місяць → z movements
- turnover() з group_by → розгортання по виміру
- movements() по recorder → фільтрує правильно
- movements() по періоду → partition pruning працює
- balance() в high-write mode → cache + deltas
- Продуктивність: balance() < 1ms на великому dataset
